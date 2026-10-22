package com.datasync.adapter.out.orion;

import com.datasync.adapter.out.orion.dto.OrionSeriesDto;
import com.datasync.adapter.out.orion.dto.OrionSeriesListResponse;
import com.datasync.adapter.out.orion.dto.OrionSeriesValuesResponse;
import com.datasync.adapter.out.orion.dto.OrionValueDto;
import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionClientException;
import com.datasync.domain.exception.OrionRateLimitedException;
import com.datasync.domain.exception.OrionTransientException;
import com.datasync.domain.exception.UnsupportedTimeZoneException;
import com.datasync.domain.model.RawSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.port.out.SeriesSourcePort;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Outbound adapter implementing {@link SeriesSourcePort} against the Orion REST API (spec
 * &sect;4). Owns token acquisition/refresh, the shared rate limiter, and the retry policy -
 * none of which leak into the domain or application layers (ADR-003/ADR-004).
 */
@Component
public class OrionClientAdapter implements SeriesSourcePort {

    private static final Logger log = LoggerFactory.getLogger(OrionClientAdapter.class);

    static final String REQUESTS_METRIC = "datasync.orion.requests";

    private final RestClient orionRestClient;
    private final OrionTokenCache tokenCache;
    private final OrionProperties properties;
    private final RateLimiter rateLimiter;
    private final Retry retry;
    private final MeterRegistry meterRegistry;

    public OrionClientAdapter(RestClient orionRestClient,
                               OrionTokenCache tokenCache,
                               OrionProperties properties,
                               RateLimiterRegistry rateLimiterRegistry,
                               RetryRegistry retryRegistry,
                               MeterRegistry meterRegistry) {
        this.orionRestClient = orionRestClient;
        this.tokenCache = tokenCache;
        this.properties = properties;
        this.rateLimiter = rateLimiterRegistry.rateLimiter("orion");
        this.retry = retryRegistry.retry("orion");
        this.meterRegistry = meterRegistry;
    }

    @Override
    public List<SeriesDescriptor> listSeries(SeriesType type) {
        Supplier<List<SeriesDescriptor>> decorated =
                Decorators.ofSupplier(honoringRetryAfter(() -> doListSeries(type)))
                .withRateLimiter(rateLimiter)
                .withRetry(retry)
                .decorate();
        return decorated.get();
    }

    private List<SeriesDescriptor> doListSeries(SeriesType type) {
        String token = tokenCache.getToken();
        try {
            return callAndMap(type, token);
        } catch (OrionAuthException authException) {
            log.warn("Orion rejected the cached token, refreshing and retrying once: {}", authException.getMessage());
            tokenCache.invalidate();
            String refreshedToken = tokenCache.getToken();
            return callAndMap(type, refreshedToken);
        }
    }

    private List<SeriesDescriptor> callAndMap(SeriesType type, String token) {
        OrionSeriesListResponse response = instrumented("list_series", () ->
                withErrorMapping(orionRestClient.get()
                        .uri(uriBuilder -> uriBuilder.path("/v1/series").queryParam("type", type.wireValue()).build())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .retrieve())
                        .body(OrionSeriesListResponse.class));
        return mapToDomain(type, response);
    }

    private List<SeriesDescriptor> mapToDomain(SeriesType type, OrionSeriesListResponse response) {
        List<SeriesDescriptor> result = new ArrayList<>();
        if (response == null || response.series() == null) {
            return result;
        }
        for (OrionSeriesDto dto : response.series()) {
            // A timezone code outside spec 6.2 must not drop the series: keep it so its Task fails
            // with UNSUPPORTED_TIMEZONE instead of silently disappearing.
            TimeZoneCode timeZone = null;
            try {
                timeZone = TimeZoneCode.fromOrionCode(dto.timeZone());
            } catch (UnsupportedTimeZoneException ex) {
                log.warn("Series {} has an unsupported timezone code: {}", dto.seriesName(), ex.getMessage());
            }
            result.add(new SeriesDescriptor(
                    null, // not yet persisted; SeriesRepositoryPort.upsertAll assigns the id
                    dto.seriesName(),
                    type,
                    timeZone,
                    OrionDateFormat.parse(dto.lastUpdatedDate()),
                    OrionDateFormat.parse(dto.earliestDataDate()),
                    OrionDateFormat.parse(dto.latestDataDate()),
                    null, // last_synced_date is service-side state, not known from Orion's list call
                    dto.timeZone()));
        }
        return result;
    }

    @Override
    public SeriesValuesResult fetchValues(String seriesName, LocalDateTime startPrevailing, LocalDateTime endPrevailing) {
        // Counts real HTTP calls: the retry policy re-invokes the supplier, and a 401 refresh adds one more.
        AtomicInteger httpCalls = new AtomicInteger();
        Supplier<SeriesValuesResult> decorated =
                Decorators.ofSupplier(honoringRetryAfter(
                                () -> doFetchValues(seriesName, startPrevailing, endPrevailing, httpCalls)))
                        .withRateLimiter(rateLimiter)
                        .withRetry(retry)
                        .decorate();
        return decorated.get().withOrionCallCount(httpCalls.get());
    }

    private SeriesValuesResult doFetchValues(String seriesName, LocalDateTime start, LocalDateTime end,
                                              AtomicInteger httpCalls) {
        String token = tokenCache.getToken();
        try {
            return callAndMapValues(seriesName, start, end, token, httpCalls);
        } catch (OrionAuthException authException) {
            log.warn("Orion rejected the cached token, refreshing and retrying once: {}", authException.getMessage());
            tokenCache.invalidate();
            String refreshedToken = tokenCache.getToken();
            return callAndMapValues(seriesName, start, end, refreshedToken, httpCalls);
        }
    }

    private SeriesValuesResult callAndMapValues(String seriesName, LocalDateTime start, LocalDateTime end,
                                                 String token, AtomicInteger httpCalls) {
        httpCalls.incrementAndGet();
        OrionSeriesValuesResponse response = instrumented("fetch_values", () ->
                withErrorMapping(orionRestClient.get()
                        .uri(uriBuilder -> uriBuilder.path("/v1/series/{seriesName}/values")
                                .queryParam("start_prevailing", OrionDateFormat.format(start))
                                .queryParam("end_prevailing", OrionDateFormat.format(end))
                                .build(seriesName))
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .retrieve())
                        .body(OrionSeriesValuesResponse.class));
        return mapValuesToDomain(response);
    }

    /**
     * Maps Orion's HTTP error statuses onto domain exceptions (spec &sect;4, &sect;8): 401 -&gt; auth,
     * 429 -&gt; rate limited (carrying {@code Retry-After}), 5xx -&gt; transient, other 4xx -&gt; client
     * error (never retried).
     */
    private RestClient.ResponseSpec withErrorMapping(RestClient.ResponseSpec spec) {
        return spec
                .onStatus(status -> status.value() == 401, (req, res) -> {
                    throw new OrionAuthException("Orion rejected the access token");
                })
                .onStatus(status -> status.value() == 429, (req, res) -> {
                    throw new OrionRateLimitedException("Orion rate limited the request (429)",
                            parseRetryAfter(res.getHeaders()));
                })
                .onStatus(HttpStatusCode::is5xxServerError, (req, res) -> {
                    throw new OrionTransientException("Orion transient error: " + res.getStatusCode());
                })
                .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> {
                    throw new OrionClientException("Orion rejected the request: " + res.getStatusCode());
                });
    }

    /** Spec &sect;4: {@code Retry-After} is a number of seconds. Anything else is ignored. */
    static Duration parseRetryAfter(HttpHeaders headers) {
        String value = headers.getFirst(HttpHeaders.RETRY_AFTER);
        if (value == null) {
            return null;
        }
        try {
            long seconds = Long.parseLong(value.trim());
            return seconds < 0 ? null : Duration.ofSeconds(seconds);
        } catch (NumberFormatException e) {
            log.warn("Ignoring unparseable Retry-After header from Orion");
            return null;
        }
    }

    /**
     * Honors Orion's {@code Retry-After} on a 429 (spec &sect;8): before the retry policy's own backoff
     * kicks in, wait at least the requested time (capped by {@code orion.max-retry-after}). The wait
     * happens on the calling virtual thread, so it is cheap. No wait is added after the last allowed
     * attempt because no retry follows it.
     */
    private <T> Supplier<T> honoringRetryAfter(Supplier<T> call) {
        AtomicInteger attempts = new AtomicInteger();
        return () -> {
            int attempt = attempts.incrementAndGet();
            try {
                return call.get();
            } catch (OrionRateLimitedException rateLimited) {
                if (attempt < retry.getRetryConfig().getMaxAttempts()) {
                    pauseFor(rateLimited.getRetryAfter());
                }
                throw rateLimited;
            }
        };
    }

    private void pauseFor(Duration retryAfter) {
        if (retryAfter == null || retryAfter.isZero() || retryAfter.isNegative()) {
            return;
        }
        Duration cap = properties.getMaxRetryAfter();
        Duration wait = cap != null && retryAfter.compareTo(cap) > 0 ? cap : retryAfter;
        log.info("Orion asked us to back off; waiting {} before retrying", wait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Runs one HTTP attempt and counts its outcome ({@code datasync.orion.requests}). */
    private <T> T instrumented(String operation, Supplier<T> httpCall) {
        String outcome = "success";
        try {
            return httpCall.get();
        } catch (OrionRateLimitedException e) {
            outcome = "rate_limited";
            throw e;
        } catch (OrionTransientException e) {
            outcome = "transient_error";
            throw e;
        } catch (OrionAuthException e) {
            outcome = "auth_failed";
            throw e;
        } catch (OrionClientException e) {
            outcome = "client_error";
            throw e;
        } catch (ResourceAccessException networkException) {
            outcome = "transient_error";
            throw new OrionTransientException("Orion network error: " + networkException.getMessage());
        } finally {
            meterRegistry.counter(REQUESTS_METRIC, "operation", operation, "outcome", outcome).increment();
        }
    }

    private SeriesValuesResult mapValuesToDomain(OrionSeriesValuesResponse response) {
        if (response == null) {
            throw new OrionClientException("Orion returned an empty values response");
        }
        List<RawSeriesValue> rawValues = response.values() == null
                ? List.of()
                : response.values().stream()
                        .map(this::toRawValue)
                        .toList();
        return new SeriesValuesResult(
                response.seriesName(),
                response.unit(),
                response.granularity(),
                TimeZoneCode.fromOrionCode(response.timeZone()),
                OrionDateFormat.parse(response.asOfDate()),
                rawValues);
    }

    private RawSeriesValue toRawValue(OrionValueDto dto) {
        return new RawSeriesValue(OrionDateFormat.parse(dto.startDate()), dto.repeatedHour(), dto.value());
    }
}
