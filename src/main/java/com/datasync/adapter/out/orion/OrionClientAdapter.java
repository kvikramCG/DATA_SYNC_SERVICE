package com.datasync.adapter.out.orion;

import com.datasync.adapter.out.orion.dto.OrionSeriesDto;
import com.datasync.adapter.out.orion.dto.OrionSeriesListResponse;
import com.datasync.adapter.out.orion.dto.OrionSeriesValuesResponse;
import com.datasync.adapter.out.orion.dto.OrionValueDto;
import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionClientException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Outbound adapter implementing {@link SeriesSourcePort} against the Orion REST API (spec
 * &sect;4). Owns token acquisition/refresh, the shared rate limiter, and the retry policy -
 * none of which leak into the domain or application layers (ADR-003/ADR-004).
 */
@Component
public class OrionClientAdapter implements SeriesSourcePort {

    private static final Logger log = LoggerFactory.getLogger(OrionClientAdapter.class);

    private final RestClient orionRestClient;
    private final OrionTokenCache tokenCache;
    private final RateLimiter rateLimiter;
    private final Retry retry;

    public OrionClientAdapter(RestClient orionRestClient,
                               OrionTokenCache tokenCache,
                               RateLimiterRegistry rateLimiterRegistry,
                               RetryRegistry retryRegistry) {
        this.orionRestClient = orionRestClient;
        this.tokenCache = tokenCache;
        this.rateLimiter = rateLimiterRegistry.rateLimiter("orion");
        this.retry = retryRegistry.retry("orion");
    }

    @Override
    public List<SeriesDescriptor> listSeries(SeriesType type) {
        Supplier<List<SeriesDescriptor>> decorated = Decorators.ofSupplier(() -> doListSeries(type))
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
        OrionSeriesListResponse response;
        try {
            response = orionRestClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/v1/series").queryParam("type", type.wireValue()).build())
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .retrieve()
                    .onStatus(status -> status.value() == 401, (req, res) -> {
                        throw new OrionAuthException("Orion rejected the access token");
                    })
                    .onStatus(status -> status.value() == 429 || status.is5xxServerError(), (req, res) -> {
                        throw new OrionTransientException("Orion transient error: " + res.getStatusCode());
                    })
                    .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> {
                        throw new OrionClientException("Orion rejected the request: " + res.getStatusCode());
                    })
                    .body(OrionSeriesListResponse.class);
        } catch (ResourceAccessException networkException) {
            throw new OrionTransientException("Orion network error: " + networkException.getMessage());
        }
        return mapToDomain(type, response);
    }

    private List<SeriesDescriptor> mapToDomain(SeriesType type, OrionSeriesListResponse response) {
        List<SeriesDescriptor> result = new ArrayList<>();
        if (response == null || response.series() == null) {
            return result;
        }
        for (OrionSeriesDto dto : response.series()) {
            try {
                result.add(new SeriesDescriptor(
                        null, // not yet persisted; SeriesRepositoryPort.upsertAll assigns the id
                        dto.seriesName(),
                        type,
                        TimeZoneCode.fromOrionCode(dto.timeZone()),
                        OrionDateFormat.parse(dto.lastUpdatedDate()),
                        OrionDateFormat.parse(dto.earliestDataDate()),
                        OrionDateFormat.parse(dto.latestDataDate()),
                        null // last_synced_date is service-side state, not known from Orion's list call
                ));
            } catch (UnsupportedTimeZoneException ex) {
                log.warn("Skipping series {} from catalog: {}", dto.seriesName(), ex.getMessage());
            }
        }
        return result;
    }

    @Override
    public SeriesValuesResult fetchValues(String seriesName, LocalDateTime startPrevailing, LocalDateTime endPrevailing) {
        Supplier<SeriesValuesResult> decorated =
                Decorators.ofSupplier(() -> doFetchValues(seriesName, startPrevailing, endPrevailing))
                        .withRateLimiter(rateLimiter)
                        .withRetry(retry)
                        .decorate();
        return decorated.get();
    }

    private SeriesValuesResult doFetchValues(String seriesName, LocalDateTime start, LocalDateTime end) {
        String token = tokenCache.getToken();
        try {
            return callAndMapValues(seriesName, start, end, token);
        } catch (OrionAuthException authException) {
            log.warn("Orion rejected the cached token, refreshing and retrying once: {}", authException.getMessage());
            tokenCache.invalidate();
            String refreshedToken = tokenCache.getToken();
            return callAndMapValues(seriesName, start, end, refreshedToken);
        }
    }

    private SeriesValuesResult callAndMapValues(String seriesName, LocalDateTime start, LocalDateTime end, String token) {
        OrionSeriesValuesResponse response;
        try {
            response = orionRestClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/v1/series/{seriesName}/values")
                            .queryParam("start_prevailing", OrionDateFormat.format(start))
                            .queryParam("end_prevailing", OrionDateFormat.format(end))
                            .build(seriesName))
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .retrieve()
                    .onStatus(status -> status.value() == 401, (req, res) -> {
                        throw new OrionAuthException("Orion rejected the access token");
                    })
                    .onStatus(status -> status.value() == 429 || status.is5xxServerError(), (req, res) -> {
                        throw new OrionTransientException("Orion transient error: " + res.getStatusCode());
                    })
                    .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> {
                        throw new OrionClientException("Orion rejected the request: " + res.getStatusCode());
                    })
                    .body(OrionSeriesValuesResponse.class);
        } catch (ResourceAccessException networkException) {
            throw new OrionTransientException("Orion network error: " + networkException.getMessage());
        }
        return mapValuesToDomain(response);
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
