package com.datasync.adapter.out.ledgerpoint;

import com.datasync.domain.exception.LedgerPointAuthException;
import com.datasync.domain.exception.LedgerPointTransientException;
import com.datasync.domain.exception.LedgerPointUnknownSeriesException;
import com.datasync.domain.exception.LedgerPointValidationException;
import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.LedgerPointPublishResult;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plain unit test (no Spring context) proving the LedgerPoint adapter's batching/retry/error
 * mapping behavior directly against WireMock (spec &sect;5/&sect;8).
 */
class LedgerPointClientAdapterTest {

    private WireMockServer wireMockServer;
    private LedgerPointProperties properties;
    private RestClient restClient;
    private UUID correlationId;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(options().dynamicPort());
        wireMockServer.start();

        properties = new LedgerPointProperties();
        properties.setBaseUrl(wireMockServer.baseUrl());
        properties.setApiKey("test-api-key");
        properties.setConnectTimeoutMs(2000);
        properties.setReadTimeoutMs(2000);

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(2000);
        restClient = RestClient.builder().baseUrl(properties.getBaseUrl()).requestFactory(factory).build();

        correlationId = UUID.randomUUID();
        meterRegistry = new SimpleMeterRegistry();
    }

    private double ledgerPointRequests(String outcome) {
        var counter = meterRegistry.find(LedgerPointClientAdapter.REQUESTS_METRIC).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    private LedgerPointClientAdapter adapter(int maxBatchSize) {
        properties.setMaxBatchSize(maxBatchSize);
        RetryRegistry retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(10))
                .retryExceptions(LedgerPointTransientException.class)
                .build());
        return new LedgerPointClientAdapter(restClient, properties, new ObjectMapper(), retryRegistry, meterRegistry);
    }

    private ClassifiedSeriesValue classified(String value, ChangeType type) {
        NormalizedSeriesValue normalized = new NormalizedSeriesValue(
                LocalDate.of(2026, 11, 1), "2", Instant.parse("2026-11-01T06:00:00Z"), false, new BigDecimal(value));
        return new ClassifiedSeriesValue(normalized, type);
    }

    @Test
    void publishesSuccessfullyAndParsesTheResponse() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(okJson("""
                {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                """)));

        LedgerPointPublishResult result = adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId);

        assertThat(result.received()).isEqualTo(1);
        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.batchIds()).containsExactly(UUID.fromString("f7c1e2a4-1b2c-4d5e-9f00-1234567890ab"));

        wireMockServer.verify(postRequestedFor(urlEqualTo("/api/v2/series/values"))
                .withHeader("X-Api-Key", com.github.tomakehurst.wiremock.client.WireMock.equalTo("test-api-key"))
                .withHeader("X-Correlation-Id", com.github.tomakehurst.wiremock.client.WireMock.equalTo(correlationId.toString())));
    }

    @Test
    void throwsValidationExceptionWithParsedErrorsOn400() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse()
                .withStatus(400)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"batchId":null,"errors":[{"index":2,"field":"interval","message":"Interval '2x' is only valid on a fall-back day"}]}
                        """)));

        assertThatThrownBy(() -> adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId))
                .isInstanceOf(LedgerPointValidationException.class)
                .satisfies(ex -> {
                    LedgerPointValidationException validationEx = (LedgerPointValidationException) ex;
                    assertThat(validationEx.getErrors()).hasSize(1);
                    assertThat(validationEx.getErrors().get(0).field()).isEqualTo("interval");
                });
    }

    @Test
    void throwsUnknownSeriesExceptionOn409() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse().withStatus(409)));

        assertThatThrownBy(() -> adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId))
                .isInstanceOf(LedgerPointUnknownSeriesException.class);
    }

    @Test
    void throwsAuthExceptionOn401() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse().withStatus(401)));

        assertThatThrownBy(() -> adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId))
                .isInstanceOf(LedgerPointAuthException.class);
    }

    @Test
    void retriesOn5xxThenSucceeds() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("ledgerpoint-5xx")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("recovered"));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("ledgerpoint-5xx")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("""
                        {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":0,"updated":1}
                        """)));

        LedgerPointPublishResult result = adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.UPDATED)), correlationId);

        assertThat(result.updated()).isEqualTo(1);
    }

    @Test
    void retriesOn429ThenSucceedsAndCountsItAsRateLimited() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("lp-429")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(429))
                .willSetStateTo("recovered"));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("lp-429")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("""
                        {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                        """)));

        LedgerPointPublishResult result = adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId);

        assertThat(result.inserted()).isEqualTo(1);
        assertThat(ledgerPointRequests("rate_limited")).isEqualTo(1);
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/api/v2/series/values")));
    }

    @Test
    void a429OnEveryAttemptExhaustsTheThreeTriesAsATransientFailure() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse().withStatus(429)));

        assertThatThrownBy(() -> adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId))
                .isInstanceOf(LedgerPointTransientException.class);

        wireMockServer.verify(3, postRequestedFor(urlEqualTo("/api/v2/series/values")));
    }

    @Test
    void honorsRetryAfterOn429CappedByTheConfiguredMaximum() {
        properties.setMaxRetryAfter(Duration.ofMillis(400));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("lp-429-wait")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "30"))
                .willSetStateTo("recovered"));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("lp-429-wait")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("""
                        {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                        """)));

        long start = System.nanoTime();
        adapter(10_000).publish("SERIES_ALPHA_001", "U1", "TZ_C",
                List.of(classified("31.25", ChangeType.NEW)), correlationId);
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        // Waited the capped 400 ms (not the 30 s the server asked for).
        assertThat(elapsedMs).isBetween(350L, 5_000L);
    }

    @Test
    void aMissingOrNonUuidBatchIdDoesNotFailTheBatch() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("batch-ids")
                .whenScenarioStateIs(STARTED)
                .willReturn(okJson("{\"batchId\":\"not-a-uuid\",\"received\":1,\"inserted\":1,\"updated\":0}"))
                .willSetStateTo("second"));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("batch-ids")
                .whenScenarioStateIs("second")
                .willReturn(okJson("{\"batchId\":null,\"received\":1,\"inserted\":1,\"updated\":0}")));

        LedgerPointPublishResult result = adapter(1).publish("SERIES_ALPHA_001", "U1", "TZ_C",
                List.of(classified("1", ChangeType.NEW), classified("2", ChangeType.NEW)), correlationId);

        assertThat(result.received()).isEqualTo(2);
        assertThat(result.batchIds()).isEmpty();
    }

    @Test
    void anEmptyBodyOn200IsTreatedAsAccepted() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse().withStatus(200)));

        LedgerPointPublishResult result = adapter(10_000).publish("SERIES_ALPHA_001", "U1", "TZ_C",
                List.of(classified("31.25", ChangeType.NEW)), correlationId);

        assertThat(result.batchIds()).isEmpty();
    }

    @Test
    void sendsValuesAsPlainDecimalStringsNeverScientificNotation() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(okJson("""
                {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                """)));

        adapter(10_000).publish("SERIES_ALPHA_001", "U1", "TZ_C",
                List.of(classified("1E+3", ChangeType.NEW)), correlationId);

        wireMockServer.verify(postRequestedFor(urlEqualTo("/api/v2/series/values"))
                .withRequestBody(com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath(
                        "$[0].value", com.github.tomakehurst.wiremock.client.WireMock.equalTo("1000"))));
    }

    @Test
    void requestOutcomesAreCounted() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("metrics")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("recovered"));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).inScenario("metrics")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("""
                        {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                        """)));

        adapter(10_000).publish("SERIES_ALPHA_001", "U1", "TZ_C",
                List.of(classified("31.25", ChangeType.NEW)), correlationId);

        assertThat(ledgerPointRequests("transient_error")).isEqualTo(1);
        assertThat(ledgerPointRequests("success")).isEqualTo(1);
    }

    @Test
    void authFailureIsCountedAsAuthFailed() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(aResponse().withStatus(403)));

        assertThatThrownBy(() -> adapter(10_000).publish(
                "SERIES_ALPHA_001", "U1", "TZ_C", List.of(classified("31.25", ChangeType.NEW)), correlationId))
                .isInstanceOf(LedgerPointAuthException.class);

        assertThat(ledgerPointRequests("auth_failed")).isEqualTo(1);
    }

    @Test
    void splitsIntoMultipleBatchesWhenExceedingMaxBatchSize() {
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(okJson("""
                {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
                """)));

        List<ClassifiedSeriesValue> fiveValues = List.of(
                classified("1", ChangeType.NEW), classified("2", ChangeType.NEW), classified("3", ChangeType.NEW),
                classified("4", ChangeType.NEW), classified("5", ChangeType.NEW));

        LedgerPointPublishResult result = adapter(2).publish("SERIES_ALPHA_001", "U1", "TZ_C", fiveValues, correlationId);

        // 5 values split at maxBatchSize=2 -> 3 requests (2 + 2 + 1), each stub response counted once
        assertThat(result.received()).isEqualTo(3);
        assertThat(result.batchIds()).hasSize(3);
        wireMockServer.verify(3, postRequestedFor(urlEqualTo("/api/v2/series/values")));
    }
}
