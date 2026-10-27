package com.datasync.acceptance;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import com.datasync.adapter.out.ledgerpoint.LedgerPointClientAdapter;
import com.datasync.adapter.out.ledgerpoint.LedgerPointProperties;
import com.datasync.adapter.out.metrics.MicrometerSyncMetrics;
import com.datasync.adapter.out.orion.OrionClientAdapter;
import com.datasync.adapter.out.orion.OrionDateFormat;
import com.datasync.adapter.out.orion.OrionProperties;
import com.datasync.adapter.out.orion.OrionTokenCache;
import com.datasync.application.JobAbortRegistry;
import com.datasync.application.SeriesCatalogService;
import com.datasync.application.SeriesSyncWorker;
import com.datasync.application.SyncOrchestrationService;
import com.datasync.domain.exception.JobAlreadyRunningException;
import com.datasync.domain.exception.LedgerPointTransientException;
import com.datasync.domain.exception.OrionTransientException;
import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobDetail;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.JobSummary;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesRuleOverride;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.WarningType;
import com.datasync.domain.port.out.ChunkRepositoryPort;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import com.datasync.domain.port.out.RuleOverrideRepositoryPort;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import com.datasync.domain.port.out.SeriesValuePort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Executable form of spec &sect;11 "Acceptance Criteria" (one happy-path test per criterion) plus the
 * failure and edge cases of &sect;5, &sect;6 and &sect;8 below them. The real Orion and
 * LedgerPoint adapters, {@link SeriesSyncWorker} and {@link SyncOrchestrationService} run
 * end-to-end against WireMock; only persistence is replaced by in-memory fakes so the suite needs
 * neither Docker nor a database.
 */
class AcceptanceCriteriaTest {

    private static final String ORION_PASSWORD = "orion-pw-s3cr3t";
    private static final String ORION_TOKEN = "orion-token-s3cr3t";
    private static final String LEDGERPOINT_KEY = "lp-key-s3cr3t";
    private static final String LEDGERPOINT_PATH = "/api/v2/series/values";
    private static final String LEDGERPOINT_OK = """
            {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
            """;

    /** Two days after the fixtures' lastUpdatedDate (10/12/2026 6:00 PM Central) - never stale. */
    private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");
    private static final Instant NOV_1 = Instant.parse("2026-11-01T00:00:00Z");

    private final ObjectMapper json = new ObjectMapper();

    private WireMockServer wireMock;
    private InMemoryStore store;
    private AtomicReference<Instant> now;
    private ExecutorService executor;
    private LedgerPointProperties ledgerPointProperties;
    private SyncOrchestrationService service;

    @BeforeEach
    void setUp() {
        wireMock = new WireMockServer(options().dynamicPort());
        wireMock.start();
        store = new InMemoryStore();
        now = new AtomicReference<>(NOW);
        executor = Executors.newVirtualThreadPerTaskExecutor();

        OrionProperties orionProperties = new OrionProperties();
        orionProperties.setBaseUrl(wireMock.baseUrl());
        orionProperties.setLogin("svc-data-sync");
        orionProperties.setPassword(ORION_PASSWORD);
        orionProperties.setConnectTimeoutMs(2000);
        orionProperties.setReadTimeoutMs(5000);

        ledgerPointProperties = new LedgerPointProperties();
        ledgerPointProperties.setBaseUrl(wireMock.baseUrl());
        ledgerPointProperties.setApiKey(LEDGERPOINT_KEY);
        ledgerPointProperties.setConnectTimeoutMs(2000);
        ledgerPointProperties.setReadTimeoutMs(5000);
        ledgerPointProperties.setMaxBatchSize(10_000);

        RestClient orionClient = restClient(wireMock.baseUrl());
        RestClient ledgerPointClient = restClient(wireMock.baseUrl());

        RateLimiterRegistry rateLimiters = RateLimiterRegistry.of(RateLimiterConfig.custom()
                .limitForPeriod(100)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofSeconds(10))
                .build());
        RetryRegistry retries = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(10))
                .retryExceptions(OrionTransientException.class, LedgerPointTransientException.class)
                .build());
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        MicrometerSyncMetrics metrics = new MicrometerSyncMetrics(meters);
        ClockPort clock = () -> now.get();

        OrionClientAdapter orion = new OrionClientAdapter(orionClient,
                new OrionTokenCache(orionClient, orionProperties), orionProperties, rateLimiters, retries, meters);
        LedgerPointClientAdapter ledgerPoint = new LedgerPointClientAdapter(ledgerPointClient,
                ledgerPointProperties, json, retries, meters);

        SeriesSyncWorker worker = new SeriesSyncWorker(orion, store, ledgerPoint, store, store, store, store,
                clock, metrics, 5);
        service = new SyncOrchestrationService(new SeriesCatalogService(orion, store), store, store, worker,
                executor, clock, metrics, new JobAbortRegistry());

        wireMock.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(
                "{\"accessToken\":\"" + ORION_TOKEN + "\",\"expiresInSeconds\":3600}")));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(10).willReturn(okJson(LEDGERPOINT_OK)));
    }

    @AfterEach
    void tearDown() {
        executor.close();
        wireMock.stop();
    }

    // ---------------------------------------------------------------- criteria

    @Test
    void criterion01_syncCoversEverySeriesReturnedByOrionAndReportsTaskCounts() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003");
        for (String name : List.of("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003")) {
            stubValues(name, valuesBody(name, 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        }

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED);
        int succeeded = summary.taskCounts().getOrDefault(TaskStatus.SUCCESS, 0)
                + summary.taskCounts().getOrDefault(TaskStatus.SUCCESS_WITH_WARNINGS, 0);
        assertThat(succeeded).isEqualTo(3);
        assertThat(summary.taskCounts()).doesNotContainKey(TaskStatus.FAILED);
        assertThat(store.chunkStatuses()).isNotEmpty().allMatch(s -> s == ChunkStatus.PERSISTED);
    }

    @Test
    void criterion02_threeMonthWindowCausesExactlyThreeOrionValueCalls() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", "9/15/2026 12:00:00 AM",
                valuesBody("SERIES_ALPHA_001", 1440, value(LocalDateTime.of(2026, 9, 15, 0, 0), false, "1.0")));
        stubValues("SERIES_ALPHA_001", "10/1/2026 12:00:00 AM",
                valuesBody("SERIES_ALPHA_001", 1440, value(LocalDateTime.of(2026, 10, 1, 0, 0), false, "2.0")));
        stubValues("SERIES_ALPHA_001", "11/1/2026 12:00:00 AM",
                valuesBody("SERIES_ALPHA_001", 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "3.0")));

        sync(Instant.parse("2026-09-15T00:00:00Z"), Instant.parse("2026-11-10T00:00:00Z"));

        wireMock.verify(3, getRequestedFor(urlPathMatching("/v1/series/SERIES_ALPHA_001/values")));
    }

    @Test
    void criterion03_fallBackDayProducesTwentyFiveRowsIncludingOneRepeatedInterval() {
        stubSeriesList("SERIES_ALPHA_001");
        List<String> values = new ArrayList<>();
        for (int hour = 0; hour < 24; hour++) {
            String amount = hour == 1 ? "29.80" : String.valueOf(20 + hour);
            values.add(value(LocalDateTime.of(2026, 11, 1, hour, 0), false, amount));
        }
        values.add(value(LocalDateTime.of(2026, 11, 1, 1, 0), true, "29.10"));
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60, values.toArray(String[]::new)));

        sync(NOV_1, NOV_1);

        List<JsonNode> rows = postedRows();
        assertThat(rows).hasSize(25);
        assertThat(rows).filteredOn(r -> r.get("interval").asText().endsWith("x")).hasSize(1);
        assertThat(rowFor(rows, "2")).isEqualTo("29.80");
        assertThat(rowFor(rows, "2x")).isEqualTo("29.10");
    }

    @Test
    void criterion04_springForwardDayProducesTwentyFourRowsWithSkippedHourFilledAndNoXSuffix() {
        stubSeriesList("SERIES_ALPHA_001");
        List<String> values = new ArrayList<>();
        values.add(value(LocalDateTime.of(2026, 3, 8, 0, 0), false, "20.00"));
        values.add(value(LocalDateTime.of(2026, 3, 8, 1, 0), false, "22.10"));
        for (int hour = 3; hour < 24; hour++) {
            values.add(value(LocalDateTime.of(2026, 3, 8, hour, 0), false, hour == 3 ? "23.40" : "30.00"));
        }
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60, values.toArray(String[]::new)));

        sync(Instant.parse("2026-03-08T00:00:00Z"), Instant.parse("2026-03-08T00:00:00Z"));

        List<JsonNode> rows = postedRows();
        assertThat(rows).hasSize(24);
        assertThat(rows).noneMatch(r -> r.get("interval").asText().endsWith("x"));
        assertThat(rowFor(rows, "2")).isEqualTo("22.10");
        assertThat(rowFor(rows, "3")).isEqualTo("22.10"); // skipped 2 AM filled from the previous value
        assertThat(rowFor(rows, "4")).isEqualTo("23.40");
    }

    @Test
    void criterion04b_rerunningASpringForwardDaySendsNothingAndAChangedValueResendsItsFiller() {
        stubSeriesList("SERIES_ALPHA_001");
        Instant day = Instant.parse("2026-03-08T00:00:00Z");

        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60, springForwardDayValues("22.10")));
        sync(day, day);
        assertThat(postedRows()).hasSize(24);

        // Fillers are derived, not stored: an unchanged day must not resend the filler (spec AC6).
        wireMock.resetRequests();
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60, springForwardDayValues("22.10")));
        sync(day, day);
        assertThat(postedRows()).isEmpty();

        // The value the filler copies changed: both the real row and its filler go out (spec AC7 + 6.4).
        wireMock.resetRequests();
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60, springForwardDayValues("25.00")));
        sync(day, day);
        List<JsonNode> rows = postedRows();
        assertThat(rows).extracting(r -> r.get("interval").asText()).containsExactlyInAnyOrder("2", "3");
        assertThat(rowFor(rows, "2")).isEqualTo("25.00");
        assertThat(rowFor(rows, "3")).isEqualTo("25.00");
    }

    private String[] springForwardDayValues(String oneAmValue) {
        List<String> values = new ArrayList<>();
        values.add(value(LocalDateTime.of(2026, 3, 8, 0, 0), false, "20.00"));
        values.add(value(LocalDateTime.of(2026, 3, 8, 1, 0), false, oneAmValue));
        for (int hour = 3; hour < 24; hour++) {
            values.add(value(LocalDateTime.of(2026, 3, 8, hour, 0), false, hour == 3 ? "23.40" : "30.00"));
        }
        return values.toArray(String[]::new);
    }

    @Test
    void criterion05_fifteenMinuteDataUsesIntervals1To96AndDailyDataUsesInterval1() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 15,
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "1"),
                value(LocalDateTime.of(2026, 11, 2, 0, 15), false, "2"),
                value(LocalDateTime.of(2026, 11, 2, 23, 45), false, "96")));
        sync(NOV_1, NOV_1);
        assertThat(postedRows()).extracting(r -> r.get("interval").asText()).containsExactlyInAnyOrder("1", "2", "96");

        wireMock.resetRequests();
        store.reset();
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "5")));
        sync(NOV_1, NOV_1);
        assertThat(postedRows()).extracting(r -> r.get("interval").asText()).containsExactly("1");
    }

    @Test
    void criterion06_rerunningTheSameSyncSendsNothingToLedgerPoint() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));

        sync(NOV_1, NOV_1);
        assertThat(postedRows()).hasSize(1);

        wireMock.resetRequests();
        JobSummary second = sync(NOV_1, NOV_1);

        assertThat(second.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(postedRows()).isEmpty();
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void criterion07_changingOneValueAndRerunningSendsExactlyThatValue() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25"),
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "32.00"),
                value(LocalDateTime.of(2026, 11, 3, 0, 0), false, "33.00")));
        sync(NOV_1, NOV_1);
        assertThat(postedRows()).hasSize(3);

        wireMock.resetRequests();
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25"),
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "35.50"),
                value(LocalDateTime.of(2026, 11, 3, 0, 0), false, "33.00")));
        sync(NOV_1, NOV_1);

        List<JsonNode> rows = postedRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("date").asText()).isEqualTo("2026-11-02");
        assertThat(rows.get(0).get("value").asText()).isEqualTo("35.50");
    }

    @Test
    void criterion08_orion429IsRetriedAfterTheRetryAfterDelayAndSucceeds() {
        stubSeriesList("SERIES_ALPHA_001");
        String url = "/v1/series/SERIES_ALPHA_001/values";
        wireMock.stubFor(get(urlPathEqualTo(url)).inScenario("429")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "1"))
                .willSetStateTo("ok"));
        wireMock.stubFor(get(urlPathEqualTo(url)).inScenario("429")
                .whenScenarioStateIs("ok")
                .willReturn(okJson(valuesBody("SERIES_ALPHA_001", 1440,
                        value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")))));

        long startNanos = System.nanoTime();
        JobSummary summary = sync(NOV_1, NOV_1);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(950));
        wireMock.verify(2, getRequestedFor(urlPathEqualTo(url)));
        assertThat(postedRows()).hasSize(1);
    }

    @Test
    void criterion09_orion401MidRunRefreshesTheTokenOnceAndTheJobContinues() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002");
        String alphaUrl = "/v1/series/SERIES_ALPHA_001/values";
        wireMock.stubFor(get(urlPathEqualTo(alphaUrl)).inScenario("401")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(401))
                .willSetStateTo("refreshed"));
        wireMock.stubFor(get(urlPathEqualTo(alphaUrl)).inScenario("401")
                .whenScenarioStateIs("refreshed")
                .willReturn(okJson(valuesBody("SERIES_ALPHA_001", 1440,
                        value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")))));
        stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(summary.taskCounts()).doesNotContainKey(TaskStatus.FAILED);
        wireMock.verify(2, getRequestedFor(urlPathEqualTo(alphaUrl)));            // 401, then exactly one retry
        wireMock.verify(2, postRequestedFor(urlEqualTo("/auth/token")));          // initial + one refresh
        assertThat(postedRows()).hasSize(2);
    }

    @Test
    void criterion10_ledgerPoint400FailsOnlyThatTaskWithTargetRejected() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003");
        for (String name : List.of("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003")) {
            stubValues(name, valuesBody(name, 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        }
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1)
                .withRequestBody(containing("SERIES_BETA_002"))
                .willReturn(aResponse().withStatus(400).withHeader("Content-Type", "application/json")
                        .withBody("{\"batchId\":null,\"errors\":[{\"index\":0,\"field\":\"value\",\"message\":\"bad\"}]}")));

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_BETA_002").status()).isEqualTo(TaskStatus.FAILED);
        assertThat(tasks.get("SERIES_BETA_002").failureReason()).isEqualTo(FailureReason.TARGET_REJECTED);
        assertThat(tasks.get("SERIES_ALPHA_001").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(tasks.get("SERIES_GAMMA_003").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED_WITH_FAILURES);
    }

    @Test
    void criterion11_staleSeriesFailsWithStaleSeriesAndNothingIsPosted() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        now.set(Instant.parse("2026-10-20T00:00:00Z")); // 7+ days after lastUpdatedDate 10/12/2026 6:00 PM

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.STALE_SERIES);
        wireMock.verify(0, getRequestedFor(urlPathMatching("/v1/series/.*/values")));
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void criterion12_valuesArePostedAsPlainDecimalStrings() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "0.0000001"),
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "2.5E+3"),
                value(LocalDateTime.of(2026, 11, 3, 0, 0), false, "12345678.90")));

        sync(NOV_1, NOV_1);

        List<JsonNode> rows = postedRows();
        assertThat(rows).extracting(r -> r.get("value").asText())
                .containsExactlyInAnyOrder("0.0000001", "2500", "12345678.90");
        assertThat(rows).allSatisfy(r -> assertThat(r.get("value").isTextual()).isTrue());
        assertThat(rows).extracting(r -> r.get("value").asText()).noneMatch(v -> v.matches(".*[eE].*"));
    }

    @Test
    void criterion13_noCredentialsAppearInLogsOrInTheCommittedConfiguration() throws IOException {
        ch.qos.logback.classic.Logger appLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datasync");
        Level previousLevel = appLogger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        appLogger.addAppender(appender);
        appLogger.setLevel(Level.DEBUG);
        try {
            // Exercise success, a 401 token refresh and a LedgerPoint rejection so error paths log too.
            stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002");
            wireMock.stubFor(get(urlPathEqualTo("/v1/series/SERIES_ALPHA_001/values")).inScenario("log-401")
                    .whenScenarioStateIs(STARTED)
                    .willReturn(aResponse().withStatus(401))
                    .willSetStateTo("refreshed"));
            wireMock.stubFor(get(urlPathEqualTo("/v1/series/SERIES_ALPHA_001/values")).inScenario("log-401")
                    .whenScenarioStateIs("refreshed")
                    .willReturn(okJson(valuesBody("SERIES_ALPHA_001", 1440,
                            value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")))));
            stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                    value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));
            wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1)
                    .withRequestBody(containing("SERIES_BETA_002"))
                    .willReturn(aResponse().withStatus(400).withHeader("Content-Type", "application/json")
                            .withBody("{\"batchId\":null,\"errors\":[]}")));

            sync(NOV_1, NOV_1);

            List<String> logged = new ArrayList<>();
            for (ILoggingEvent event : new ArrayList<>(appender.list)) {
                logged.add(event.getFormattedMessage());
                if (event.getThrowableProxy() != null) {
                    logged.add(ThrowableProxyUtil.asString(event.getThrowableProxy()));
                }
            }
            assertThat(logged).isNotEmpty();
            assertThat(logged).noneMatch(line -> line.contains(ORION_PASSWORD)
                    || line.contains(ORION_TOKEN) || line.contains(LEDGERPOINT_KEY));
        } finally {
            appLogger.detachAppender(appender);
            appLogger.setLevel(previousLevel);
        }

        try (var in = getClass().getResourceAsStream("/application.yml")) {
            assertThat(in).isNotNull();
            String yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(yaml).contains("password: ${ORION_PASSWORD:}").contains("api-key: ${LEDGERPOINT_API_KEY:}");
            assertThat(yaml).doesNotContain(ORION_PASSWORD).doesNotContain(LEDGERPOINT_KEY);
        }
    }

    // ---------------------------------------------------------------- failure and edge cases

    @Test
    void failure08_orion429OnEveryAttemptExhaustsTheRetriesAndFailsOnlyThatTask() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002");
        String alphaUrl = "/v1/series/SERIES_ALPHA_001/values";
        wireMock.stubFor(get(urlPathEqualTo(alphaUrl)).willReturn(aResponse().withStatus(429)));
        stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_ALPHA_001").status()).isEqualTo(TaskStatus.FAILED);
        assertThat(tasks.get("SERIES_ALPHA_001").failureReason()).isEqualTo(FailureReason.SOURCE_UNAVAILABLE);
        assertThat(tasks.get("SERIES_BETA_002").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED_WITH_FAILURES);
        wireMock.verify(3, getRequestedFor(urlPathEqualTo(alphaUrl))); // three attempts, then give up
    }

    @Test
    void failure09_orion401AfterTheTokenRefreshFailsTheTaskInsteadOfLooping() {
        stubSeriesList("SERIES_ALPHA_001");
        String url = "/v1/series/SERIES_ALPHA_001/values";
        wireMock.stubFor(get(urlPathEqualTo(url)).willReturn(aResponse().withStatus(401)));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.SOURCE_UNAVAILABLE);
        wireMock.verify(2, getRequestedFor(urlPathEqualTo(url)));        // the 401 and exactly one retry
        wireMock.verify(2, postRequestedFor(urlEqualTo("/auth/token"))); // first login and exactly one refresh
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void failure_orion404ForOneSeriesIsNotRetriedAndDoesNotStopTheOthers() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002");
        String alphaUrl = "/v1/series/SERIES_ALPHA_001/values";
        wireMock.stubFor(get(urlPathEqualTo(alphaUrl)).willReturn(aResponse().withStatus(404)));
        stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_ALPHA_001").status()).isEqualTo(TaskStatus.FAILED);
        assertThat(tasks.get("SERIES_ALPHA_001").failureReason()).isEqualTo(FailureReason.SOURCE_UNAVAILABLE);
        assertThat(tasks.get("SERIES_BETA_002").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED_WITH_FAILURES);
        wireMock.verify(1, getRequestedFor(urlPathEqualTo(alphaUrl))); // a 404 is never retried
    }

    @Test
    void failure_orionSeriesListDownFailsTheJobWithoutCreatingTasksOrPosting() {
        wireMock.stubFor(get(urlPathEqualTo("/v1/series")).willReturn(aResponse().withStatus(503)));

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.FAILED);
        assertThat(store.findByJobId(summary.jobId())).isEmpty();
        wireMock.verify(3, getRequestedFor(urlPathEqualTo("/v1/series")));
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void failure10_ledgerPoint409FailsWithUnknownSeriesAndIsNotRetried() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).willReturn(aResponse().withStatus(409)));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.UNKNOWN_SERIES);
        wireMock.verify(1, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void failure10_ledgerPoint5xxOnEveryAttemptFailsWithTargetUnavailableAfterThreeTries() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).willReturn(aResponse().withStatus(503)));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.TARGET_UNAVAILABLE);
        wireMock.verify(3, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
        assertThat(store.chunkStatuses()).containsExactly(ChunkStatus.FAILED);
    }

    @Test
    void failure10_ledgerPoint400DetailExplainsWhichRowWasRejected() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1)
                .willReturn(aResponse().withStatus(400).withHeader("Content-Type", "application/json")
                        .withBody("{\"batchId\":null,\"errors\":[{\"index\":2,\"field\":\"interval\","
                                + "\"message\":\"Interval '2x' is only valid on a fall-back day\"}]}")));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.failureReason()).isEqualTo(FailureReason.TARGET_REJECTED);
        assertThat(task.failureDetail())
                .contains("batch row 2 interval: Interval '2x' is only valid on a fall-back day");
        wireMock.verify(1, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH))); // a 400 is never retried
    }

    @Test
    void failure10_ledgerPoint429IsRetriedAndExhaustsAsTargetUnavailable() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).willReturn(aResponse().withStatus(429)));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.TARGET_UNAVAILABLE);
        wireMock.verify(3, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH))); // same 3 attempts as Orion (spec §8)
    }

    @Test
    void failure10_ledgerPoint401FailsTheWholeJobAndLeavesNoTaskUnfinished() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003");
        for (String name : List.of("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003")) {
            stubValues(name, valuesBody(name, 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        }
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).willReturn(aResponse().withStatus(401)));

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.FAILED);
        // Every Task either got the 401 itself or stopped because a sibling did; none stays PENDING/RUNNING.
        assertThat(tasksByName(summary).values()).hasSize(3).allMatch(t -> t.status() == TaskStatus.FAILED);
    }

    @Test
    void failure_aRejectedBatchIsNotPersistedSoTheNextRunSendsItAgain() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        // First POST is rejected, every later one is accepted.
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).inScenario("reject-once")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(400).withHeader("Content-Type", "application/json")
                        .withBody("{\"batchId\":null,\"errors\":[{\"index\":0,\"field\":\"value\",\"message\":\"no\"}]}"))
                .willSetStateTo("accepting"));
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1).inScenario("reject-once")
                .whenScenarioStateIs("accepting")
                .willReturn(okJson(LEDGERPOINT_OK)));

        JobSummary first = sync(NOV_1, NOV_1);

        assertThat(tasksByName(first).get("SERIES_ALPHA_001").failureReason()).isEqualTo(FailureReason.TARGET_REJECTED);
        assertThat(store.lastSyncedDate("SERIES_ALPHA_001")).isNull(); // watermark untouched
        assertThat(store.chunkStatuses()).containsExactly(ChunkStatus.FAILED);

        JobSummary second = sync(NOV_1, NOV_1);

        JobTask resent = tasksByName(second).get("SERIES_ALPHA_001");
        assertThat(resent.status()).isEqualTo(TaskStatus.SUCCESS);
        assertThat(resent.valuesSent()).isEqualTo(1); // nothing was stored by the rejected run, so it is NEW again
        // The window starts after the one-year default start, so it is not contiguous with "never synced":
        // the watermark must stay put rather than skip the dates in between.
        assertThat(store.lastSyncedDate("SERIES_ALPHA_001")).isNull();

        wireMock.resetRequests();
        sync(NOV_1, NOV_1);
        assertThat(postedRows()).isEmpty(); // now persisted, so a third run has nothing to send
    }

    @Test
    void edge01_anEmptySeriesListIsValidAndCompletesTheJobWithNoTasks() {
        stubSeriesList();

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(summary.taskCounts()).isEmpty();
        wireMock.verify(0, getRequestedFor(urlPathMatching("/v1/series/.*/values")));
    }

    @Test
    void edge01_seriesNameFilterSyncsOnlyThatSeries() {
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003");
        for (String name : List.of("SERIES_ALPHA_001", "SERIES_BETA_002", "SERIES_GAMMA_003")) {
            stubValues(name, valuesBody(name, 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        }

        JobSummary summary = sync("SERIES_BETA_002", NOV_1, NOV_1);

        assertThat(tasksByName(summary)).containsOnlyKeys("SERIES_BETA_002");
        wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_BETA_002/values")));
        wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_ALPHA_001/values")));
        wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_GAMMA_003/values")));
    }

    @Test
    void edge01_aSeriesNameThatOrionDoesNotListCompletesWithNoTasksAndLogsAWarning() {
        stubSeriesList("SERIES_ALPHA_001");
        ch.qos.logback.classic.Logger appLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datasync");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        appLogger.addAppender(appender);
        try {
            JobSummary summary = sync("DOES_NOT_EXIST\nforged line", NOV_1, NOV_1);

            assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED);
            assertThat(summary.taskCounts()).isEmpty();
        } finally {
            appLogger.detachAppender(appender);
        }

        assertThat(appender.list)
                .filteredOn(event -> event.getLevel() == Level.WARN)
                .extracting(ILoggingEvent::getFormattedMessage)
                .anySatisfy(message -> assertThat(message)
                        .contains("no series named 'DOES_NOT_EXIST forged line'") // newline replaced
                        .doesNotContain("\n"));
    }

    @Test
    void edge_aSeriesNameLongerThanTheColumnIsRejectedAsABadRequestBeforeAnyJobExists() {
        String tooLong = "S".repeat(101);

        assertThatThrownBy(() -> service.triggerSync(SeriesType.TYPE_A, tooLong, null, null, UUID.randomUUID()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("100 characters");
        assertThat(store.jobCount()).isZero();
    }

    @Test
    void edge01_aSecondTriggerWhileAJobIsRunningIsRejectedAndTheLockIsReleasedAfterwards() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        // A slow LedgerPoint keeps the first job RUNNING long enough to race it.
        wireMock.stubFor(post(urlEqualTo(LEDGERPOINT_PATH)).atPriority(1)
                .willReturn(okJson(LEDGERPOINT_OK).withFixedDelay(800)));

        JobSummary first = service.triggerSync(SeriesType.TYPE_A, null, NOV_1, NOV_1, UUID.randomUUID());
        assertThatThrownBy(() -> service.triggerSync(SeriesType.TYPE_A, null, NOV_1, NOV_1, UUID.randomUUID()))
                .isInstanceOf(JobAlreadyRunningException.class);

        assertThat(awaitJob(first).status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(sync(NOV_1, NOV_1).status()).isEqualTo(JobStatus.COMPLETED); // lock released
    }

    @Test
    void edge02_aWindowInsideOneMonthIsOneCallAndItsEndIsNormalizedToTheMonthEnd() {
        stubSeriesList("SERIES_ALPHA_001");
        String url = "/v1/series/SERIES_ALPHA_001/values";
        // Only this exact window is stubbed, so a wrong start/end makes the Task fail instead of passing silently.
        wireMock.stubFor(get(urlPathEqualTo(url))
                .withQueryParam("start_prevailing", equalTo("11/1/2026 12:00:00 AM"))
                .withQueryParam("end_prevailing", equalTo("11/30/2026 11:59:59 PM"))
                .willReturn(okJson(valuesBody("SERIES_ALPHA_001", 1440,
                        value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")))));

        JobSummary summary = sync(NOV_1, Instant.parse("2026-11-10T00:00:00Z"));

        assertThat(tasksByName(summary).get("SERIES_ALPHA_001").status())
                .isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        wireMock.verify(1, getRequestedFor(urlPathEqualTo(url)));
    }

    @Test
    void edge02_aWindowCrossingNewYearUsesOneCallPerCalendarMonth() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", "12/15/2026 12:00:00 AM", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 12, 15, 0, 0), false, "1.0")));
        stubValues("SERIES_ALPHA_001", "1/1/2027 12:00:00 AM", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2027, 1, 1, 0, 0), false, "2.0")));

        JobSummary summary = sync(Instant.parse("2026-12-15T00:00:00Z"), Instant.parse("2027-01-10T00:00:00Z"));

        assertThat(tasksByName(summary).get("SERIES_ALPHA_001").status())
                .isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        wireMock.verify(2, getRequestedFor(urlPathMatching("/v1/series/SERIES_ALPHA_001/values")));
    }

    @Test
    void edge03_fifteenMinuteFallBackDayHasHundredRowsWithFourRepeatedQuarterHours() {
        stubSeriesList("SERIES_ALPHA_001");
        List<String> values = new ArrayList<>();
        for (int quarter = 0; quarter < 96; quarter++) {
            values.add(value(LocalDateTime.of(2026, 11, 1, quarter / 4, (quarter % 4) * 15), false,
                    String.valueOf(quarter + 1)));
        }
        for (int minute = 0; minute < 60; minute += 15) {
            values.add(value(LocalDateTime.of(2026, 11, 1, 1, minute), true, "900." + minute));
        }
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 15, values.toArray(String[]::new)));

        sync(NOV_1, NOV_1);

        List<JsonNode> rows = postedRows();
        assertThat(rows).hasSize(100); // 96 normal quarter-hours + the 4 repeated ones
        assertThat(rows).filteredOn(r -> r.get("interval").asText().endsWith("x"))
                .extracting(r -> r.get("interval").asText())
                .containsExactlyInAnyOrder("5x", "6x", "7x", "8x");
    }

    @Test
    void edge04_fifteenMinuteSpringForwardDayIsFilledToNinetySixRowsWithNoRepeatedSuffix() {
        stubSeriesList("SERIES_ALPHA_001");
        List<String> values = new ArrayList<>();
        for (int quarter = 0; quarter < 96; quarter++) {
            if (quarter / 4 == 2) {
                continue; // 02:00-02:59 does not exist on 2026-03-08 in Central time
            }
            values.add(value(LocalDateTime.of(2026, 3, 8, quarter / 4, (quarter % 4) * 15), false,
                    String.valueOf(quarter + 1)));
        }
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 15, values.toArray(String[]::new)));

        sync(Instant.parse("2026-03-08T00:00:00Z"), Instant.parse("2026-03-08T00:00:00Z"));

        List<JsonNode> rows = postedRows();
        assertThat(rows).hasSize(96);
        assertThat(rows).noneMatch(r -> r.get("interval").asText().endsWith("x"));
        for (String filled : List.of("9", "10", "11", "12")) {
            assertThat(rowFor(rows, filled)).isEqualTo("8"); // each skipped quarter copies the last real one
        }
        assertThat(rowFor(rows, "13")).isEqualTo("13");
    }

    @Test
    void edge_missingIntervalsAreReportedAsWarningsAndNeverInvented() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60,
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "1"),
                value(LocalDateTime.of(2026, 11, 2, 1, 0), false, "2")));

        JobSummary summary = sync(Instant.parse("2026-11-02T00:00:00Z"), Instant.parse("2026-11-02T00:00:00Z"));

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(task.valuesSent()).isEqualTo(2);
        assertThat(postedRows()).extracting(r -> r.get("interval").asText()).containsExactlyInAnyOrder("1", "2");
        assertThat(store.warningCount()).isEqualTo(22); // intervals 3..24 of that day
    }

    @Test
    void edge_aSeriesWithAMissingCatalogDateFailsOnlyItsOwnTask() {
        stubSeriesJson(
                "{\"seriesName\":\"SERIES_BROKEN\",\"seriesType\":\"typeA\",\"timeZone\":6,"
                        + "\"earliestDataDate\":\"1/1/2020 12:00:00 AM\",\"latestDataDate\":\"11/30/2026 11:00:00 PM\"}",
                seriesJson("SERIES_ALPHA_001", 6, "10/12/2026 6:00:00 PM"));
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED_WITH_FAILURES);
        JobTask broken = tasksByName(summary).get("SERIES_BROKEN");
        assertThat(broken.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(broken.failureReason()).isEqualTo(FailureReason.INTERNAL_ERROR);
        assertThat(broken.failureDetail()).contains("lastUpdatedDate is missing");
        assertThat(tasksByName(summary).get("SERIES_ALPHA_001").status()).isEqualTo(TaskStatus.SUCCESS);
    }

    @Test
    void edge_aValueWithNoReadingIsAGapWarningAndTheRestIsStillSent() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60,
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "1"),
                value(LocalDateTime.of(2026, 11, 2, 1, 0), false, "null"),
                value(LocalDateTime.of(2026, 11, 2, 2, 0), false, "3")));

        JobSummary summary = sync(Instant.parse("2026-11-02T00:00:00Z"), Instant.parse("2026-11-02T00:00:00Z"));

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(postedRows()).extracting(r -> r.get("interval").asText()).containsExactlyInAnyOrder("1", "3");
    }

    @Test
    void edge_aValueWithAnUnreadableStartDateFailsThatSeriesWithInternalError() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 60,
                "{\"startDate\":\"garbage\",\"repeatedHour\":false,\"value\":1}"));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.INTERNAL_ERROR);
        assertThat(task.failureDetail()).contains("unreadable startDate 'garbage'");
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void edge_aMissingUnitDoesNotStopTheValuesFromBeingSent() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", "{\"seriesName\":\"SERIES_ALPHA_001\",\"granularity\":1440,\"timeZone\":6,"
                + "\"asOfDate\":\"bad\",\"values\":[" + value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")
                + "]}");

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(tasksByName(summary).get("SERIES_ALPHA_001").status()).isEqualTo(TaskStatus.SUCCESS);
        assertThat(postedRows()).hasSize(1);
    }

    @Test
    void edge05_unsupportedGranularityFailsTheTaskAndPostsNothing() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 30,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "1")));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.UNSUPPORTED_GRANULARITY);
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void edge_unsupportedTimezoneFailsTheTaskBeforeAnyValuesCallAndSparesTheOthers() {
        stubSeriesJson(
                seriesJson("SERIES_ALPHA_001", 99, "10/12/2026 6:00:00 PM"),
                seriesJson("SERIES_BETA_002", 6, "10/12/2026 6:00:00 PM"));
        stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_ALPHA_001").status()).isEqualTo(TaskStatus.FAILED);
        assertThat(tasks.get("SERIES_ALPHA_001").failureReason()).isEqualTo(FailureReason.UNSUPPORTED_TIMEZONE);
        assertThat(tasks.get("SERIES_ALPHA_001").failureDetail()).contains("99");
        assertThat(tasks.get("SERIES_BETA_002").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_ALPHA_001/values")));
    }

    @Test
    void edge_noValuesInTheWindowFailsWithNoDataAndKeepsTheWatermark() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440)); // empty values array

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.NO_DATA);
        assertThat(store.lastSyncedDate("SERIES_ALPHA_001")).isNull();
        wireMock.verify(0, postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
    }

    @Test
    void edge11_aSeriesExactlyAtTheStalenessLimitIsStillSynced() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        // lastUpdatedDate 10/12/2026 6:00 PM Central is 2026-10-12T23:00:00Z; exactly 5 days later is not "older than 5 days".
        now.set(Instant.parse("2026-10-17T23:00:00Z"));

        JobSummary summary = sync(NOV_1, NOV_1);

        assertThat(tasksByName(summary).get("SERIES_ALPHA_001").status())
                .isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
    }

    @Test
    void edge11_aSeriesOneSecondPastTheStalenessLimitIsStale() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        now.set(Instant.parse("2026-10-17T23:00:01Z"));

        JobSummary summary = sync(NOV_1, NOV_1);

        JobTask task = tasksByName(summary).get("SERIES_ALPHA_001");
        assertThat(task.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.failureReason()).isEqualTo(FailureReason.STALE_SERIES);
        wireMock.verify(0, getRequestedFor(urlPathMatching("/v1/series/.*/values")));
    }

    @Test
    void edge11_oneStaleSeriesDoesNotBlockTheFreshOnes() {
        stubSeriesJson(
                seriesJson("SERIES_ALPHA_001", 6, "10/1/2026 6:00:00 PM"),
                seriesJson("SERIES_BETA_002", 6, "10/12/2026 6:00:00 PM"));
        stubValues("SERIES_BETA_002", valuesBody("SERIES_BETA_002", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "40.00")));

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_ALPHA_001").failureReason()).isEqualTo(FailureReason.STALE_SERIES);
        assertThat(tasks.get("SERIES_BETA_002").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED_WITH_FAILURES);
        wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_ALPHA_001/values")));
    }

    @Test
    void edge_aSkipRuleSkipsTheSeriesWithoutContactingOrionOrLedgerPoint() {
        store.addOverride(new SeriesRuleOverride("SERIES_BETA_002", null, true));
        stubSeriesList("SERIES_ALPHA_001", "SERIES_BETA_002");
        for (String name : List.of("SERIES_ALPHA_001", "SERIES_BETA_002")) {
            stubValues(name, valuesBody(name, 1440, value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));
        }

        JobSummary summary = sync(NOV_1, NOV_1);

        Map<String, JobTask> tasks = tasksByName(summary);
        assertThat(tasks.get("SERIES_BETA_002").status()).isEqualTo(TaskStatus.SKIPPED);
        assertThat(tasks.get("SERIES_ALPHA_001").status()).isIn(TaskStatus.SUCCESS, TaskStatus.SUCCESS_WITH_WARNINGS);
        assertThat(summary.status()).isEqualTo(JobStatus.COMPLETED); // a skipped series is not a failure
        wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v1/series/SERIES_BETA_002/values")));
        assertThat(postedRows()).hasSize(1);
    }

    @Test
    void edge_aKeyOverrideChangesTheKeyPostedToLedgerPoint() {
        store.addOverride(new SeriesRuleOverride("SERIES_ALPHA_001", "LP_ALPHA_KEY", false));
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "31.25")));

        sync(NOV_1, NOV_1);

        assertThat(postedRows()).hasSize(1).allSatisfy(r -> assertThat(r.get("key").asText()).isEqualTo("LP_ALPHA_KEY"));
    }

    @Test
    void edge_aBatchAboveTheLimitIsSplitIntoSeveralRequests() {
        ledgerPointProperties.setMaxBatchSize(2);
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "1"),
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "2"),
                value(LocalDateTime.of(2026, 11, 3, 0, 0), false, "3"),
                value(LocalDateTime.of(2026, 11, 4, 0, 0), false, "4"),
                value(LocalDateTime.of(2026, 11, 5, 0, 0), false, "5")));

        sync(NOV_1, NOV_1);

        List<LoggedRequest> requests = wireMock.findAll(postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)));
        assertThat(requests).hasSize(3);
        assertThat(requests).extracting(r -> rowsIn(r)).containsExactlyInAnyOrder(2, 2, 1);
        assertThat(postedRows()).hasSize(5);
    }

    @Test
    void edge12_negativeZeroAndVerySmallValuesArePostedAsPlainDecimals() {
        stubSeriesList("SERIES_ALPHA_001");
        stubValues("SERIES_ALPHA_001", valuesBody("SERIES_ALPHA_001", 1440,
                value(LocalDateTime.of(2026, 11, 1, 0, 0), false, "-5.5"),
                value(LocalDateTime.of(2026, 11, 2, 0, 0), false, "0"),
                value(LocalDateTime.of(2026, 11, 3, 0, 0), false, "0.000125"),
                value(LocalDateTime.of(2026, 11, 4, 0, 0), false, "1.25E-4")));

        sync(NOV_1, NOV_1);

        assertThat(postedRows()).extracting(r -> r.get("value").asText())
                .containsExactlyInAnyOrder("-5.5", "0", "0.000125", "0.000125");
    }

    // ---------------------------------------------------------------- helpers

    private RestClient restClient(String baseUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(5000);
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    /** Triggers a sync and polls the job (as an API client would) until it leaves RUNNING. */
    private JobSummary sync(Instant start, Instant end) {
        return sync(null, start, end);
    }

    private JobSummary sync(String seriesName, Instant start, Instant end) {
        return awaitJob(service.triggerSync(SeriesType.TYPE_A, seriesName, start, end, UUID.randomUUID()));
    }

    private JobSummary awaitJob(JobSummary started) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        JobDetail detail = service.getJob(started.jobId());
        while (detail.status() == JobStatus.RUNNING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Job " + started.jobId() + " still RUNNING after 30s");
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for the job", e);
            }
            detail = service.getJob(started.jobId());
        }
        Map<TaskStatus, Integer> counts = new HashMap<>();
        detail.tasks().forEach(t -> counts.merge(t.status(), 1, Integer::sum));
        return new JobSummary(detail.jobId(), detail.seriesType(), detail.status(), started.startedAt(),
                Instant.now(), counts, detail.correlationId());
    }

    /** Like {@code stubSeriesList} but lets a test choose each series' timezone code and lastUpdatedDate. */
    private void stubSeriesJson(String... seriesObjects) {
        wireMock.stubFor(get(urlPathEqualTo("/v1/series"))
                .willReturn(okJson("{\"series\":[" + String.join(",", seriesObjects) + "]}")));
    }

    private static String seriesJson(String name, int timeZone, String lastUpdated) {
        return "{\"seriesName\":\"" + name + "\",\"seriesType\":\"typeA\",\"timeZone\":" + timeZone
                + ",\"lastUpdatedDate\":\"" + lastUpdated + "\","
                + "\"earliestDataDate\":\"1/1/2020 12:00:00 AM\",\"latestDataDate\":\"11/30/2026 11:00:00 PM\"}";
    }

    private int rowsIn(LoggedRequest request) {
        try {
            return json.readTree(request.getBodyAsString()).size();
        } catch (IOException e) {
            throw new IllegalStateException("LedgerPoint request body was not JSON", e);
        }
    }

    private Map<String, JobTask> tasksByName(JobSummary summary) {
        Map<String, JobTask> byName = new HashMap<>();
        store.findByJobId(summary.jobId()).forEach(t -> byName.put(t.seriesName(), t));
        return byName;
    }

    private void stubSeriesList(String... names) {
        StringBuilder body = new StringBuilder("{\"series\":[");
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                body.append(',');
            }
            body.append("{\"seriesName\":\"").append(names[i]).append("\",\"seriesType\":\"typeA\",\"timeZone\":6,")
                    .append("\"lastUpdatedDate\":\"10/12/2026 6:00:00 PM\",")
                    .append("\"earliestDataDate\":\"1/1/2020 12:00:00 AM\",")
                    .append("\"latestDataDate\":\"11/30/2026 11:00:00 PM\"}");
        }
        body.append("]}");
        wireMock.stubFor(get(urlPathEqualTo("/v1/series")).willReturn(okJson(body.toString())));
    }

    private void stubValues(String seriesName, String body) {
        wireMock.stubFor(get(urlPathEqualTo("/v1/series/" + seriesName + "/values")).willReturn(okJson(body)));
    }

    private void stubValues(String seriesName, String startPrevailing, String body) {
        wireMock.stubFor(get(urlPathEqualTo("/v1/series/" + seriesName + "/values"))
                .withQueryParam("start_prevailing", equalTo(startPrevailing))
                .willReturn(okJson(body)));
    }

    private static String value(LocalDateTime start, boolean repeatedHour, String amount) {
        return "{\"startDate\":\"" + OrionDateFormat.format(start) + "\",\"repeatedHour\":" + repeatedHour
                + ",\"value\":" + amount + "}";
    }

    private static String valuesBody(String seriesName, int granularity, String... values) {
        return "{\"seriesName\":\"" + seriesName + "\",\"unit\":\"UNIT_A\",\"granularity\":" + granularity
                + ",\"timeZone\":6,\"asOfDate\":\"10/12/2026 6:00:00 PM\",\"values\":["
                + String.join(",", values) + "]}";
    }

    private List<JsonNode> postedRows() {
        List<JsonNode> rows = new ArrayList<>();
        for (LoggedRequest request : wireMock.findAll(postRequestedFor(urlEqualTo(LEDGERPOINT_PATH)))) {
            try {
                json.readTree(request.getBodyAsString()).forEach(rows::add);
            } catch (IOException e) {
                throw new IllegalStateException("LedgerPoint request body was not JSON", e);
            }
        }
        return rows;
    }

    private static String rowFor(List<JsonNode> rows, String interval) {
        return rows.stream()
                .filter(r -> r.get("interval").asText().equals(interval))
                .map(r -> r.get("value").asText())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No LedgerPoint row for interval " + interval));
    }

    // ---------------------------------------------------------------- in-memory persistence

    /** Single in-memory implementation of every persistence-facing port used by the sync pipeline. */
    private static final class InMemoryStore implements JobRepositoryPort, TaskRepositoryPort,
            SeriesRepositoryPort, SeriesValuePort, ChunkRepositoryPort, RuleOverrideRepositoryPort {

        private long nextId = 1;
        private final Map<Long, Job> jobs = new HashMap<>();
        private final Map<Long, JobTask> tasks = new HashMap<>();
        private final Map<String, SeriesDescriptor> series = new HashMap<>();
        private final Map<Long, Map<SeriesValueKey, NormalizedSeriesValue>> values = new HashMap<>();
        private final Map<Long, ChunkStatus> chunks = new HashMap<>();
        private final Map<String, SeriesRuleOverride> overrides = new HashMap<>();
        private final List<String> warnings = new ArrayList<>();

        private synchronized long id() {
            return nextId++;
        }

        synchronized int jobCount() {
            return jobs.size();
        }

        synchronized void addOverride(SeriesRuleOverride override) {
            overrides.put(override.orionSeriesName(), override);
        }

        synchronized int warningCount() {
            return warnings.size();
        }

        synchronized LocalDate lastSyncedDate(String seriesName) {
            return series.get(seriesName).lastSyncedDate();
        }

        synchronized void reset() {
            series.clear();
            values.clear();
        }

        synchronized List<ChunkStatus> chunkStatuses() {
            return new ArrayList<>(chunks.values());
        }

        // JobRepositoryPort
        @Override
        public synchronized Job createRunningJob(SeriesType type, String requestedSeriesName, Instant start,
                                                  Instant end, UUID correlationId) {
            boolean running = jobs.values().stream()
                    .anyMatch(j -> j.seriesType() == type && j.status() == JobStatus.RUNNING);
            if (running) {
                throw new JobAlreadyRunningException("A job is already running for " + type);
            }
            Job job = new Job(id(), type, JobStatus.RUNNING, requestedSeriesName, start, end, NOW, null, correlationId);
            jobs.put(job.id(), job);
            return job;
        }

        @Override
        public synchronized boolean markJobFinished(Long jobId, JobStatus status) {
            Job j = jobs.get(jobId);
            // Mirrors the real adapter: first close wins, an already-closed job is left untouched.
            if (j.status() != JobStatus.RUNNING) {
                return false;
            }
            jobs.put(jobId, new Job(j.id(), j.seriesType(), status, j.requestedSeriesName(),
                    j.requestedStartDate(), j.requestedEndDate(), j.startedAt(), NOW, j.correlationId()));
            return true;
        }

        @Override
        public List<Long> failStaleRunningJobs(Instant startedBefore) {
            return List.of();
        }

        @Override
        public synchronized Optional<Job> findById(Long jobId) {
            return Optional.ofNullable(jobs.get(jobId));
        }

        // TaskRepositoryPort
        @Override
        public synchronized JobTask createTask(Long jobId, Long seriesId, String seriesName) {
            JobTask task = new JobTask(id(), jobId, seriesId, seriesName, TaskStatus.PENDING, null, null, 0, null, null);
            tasks.put(task.id(), task);
            return task;
        }

        @Override
        public synchronized List<JobTask> findByJobId(Long jobId) {
            return tasks.values().stream().filter(t -> t.jobId().equals(jobId)).toList();
        }

        @Override
        public synchronized void markTaskStarted(Long taskId) {
            JobTask t = tasks.get(taskId);
            tasks.put(taskId, new JobTask(t.id(), t.jobId(), t.seriesId(), t.seriesName(), TaskStatus.RUNNING,
                    null, null, 0, NOW, null));
        }

        @Override
        public synchronized void updateTaskStatus(Long taskId, TaskStatus status, FailureReason reason,
                                                   String detail, int valuesSent) {
            JobTask t = tasks.get(taskId);
            tasks.put(taskId, new JobTask(t.id(), t.jobId(), t.seriesId(), t.seriesName(), status, reason, detail,
                    valuesSent, t.startedAt() != null ? t.startedAt() : NOW, NOW));
        }

        @Override
        public synchronized int failUnfinishedTasks(Long jobId, FailureReason reason, String detail) {
            int changed = 0;
            for (JobTask t : new ArrayList<>(tasks.values())) {
                if (t.jobId().equals(jobId)
                        && (t.status() == TaskStatus.PENDING || t.status() == TaskStatus.RUNNING)) {
                    tasks.put(t.id(), new JobTask(t.id(), t.jobId(), t.seriesId(), t.seriesName(), TaskStatus.FAILED,
                            reason, detail, 0, t.startedAt(), NOW));
                    changed++;
                }
            }
            return changed;
        }

        // SeriesRepositoryPort
        @Override
        public synchronized List<SeriesDescriptor> upsertAll(List<SeriesDescriptor> descriptors) {
            List<SeriesDescriptor> result = new ArrayList<>();
            for (SeriesDescriptor d : descriptors) {
                SeriesDescriptor existing = series.get(d.name());
                SeriesDescriptor stored = new SeriesDescriptor(
                        existing != null ? existing.id() : id(), d.name(), d.type(), d.timeZone(),
                        d.lastUpdatedDate(), d.earliestDataDate(), d.latestDataDate(),
                        existing != null ? existing.lastSyncedDate() : null, d.orionTimeZoneCode());
                series.put(d.name(), stored);
                result.add(stored.withCatalogIssue(d.catalogIssue()));
            }
            return result;
        }

        @Override
        public synchronized List<SeriesDescriptor> findByType(SeriesType type) {
            return series.values().stream().filter(s -> s.type() == type).toList();
        }

        @Override
        public synchronized void updateLastSyncedDate(Long seriesId, LocalDate lastSyncedDate) {
            series.replaceAll((name, s) -> s.id().equals(seriesId)
                    ? new SeriesDescriptor(s.id(), s.name(), s.type(), s.timeZone(), s.lastUpdatedDate(),
                            s.earliestDataDate(), s.latestDataDate(), lastSyncedDate, s.orionTimeZoneCode())
                    : s);
        }

        // SeriesValuePort
        @Override
        public synchronized Map<SeriesValueKey, BigDecimal> findExistingValues(Long seriesId, LocalDate from,
                                                                                LocalDate to) {
            Map<SeriesValueKey, BigDecimal> result = new HashMap<>();
            values.getOrDefault(seriesId, Map.of()).forEach((key, v) -> {
                if (!v.localDate().isBefore(from) && !v.localDate().isAfter(to)) {
                    result.put(key, v.value());
                }
            });
            return result;
        }

        @Override
        public synchronized void upsertAll(Long seriesId, List<NormalizedSeriesValue> newValues) {
            Map<SeriesValueKey, NormalizedSeriesValue> forSeries = values.computeIfAbsent(seriesId, k -> new HashMap<>());
            for (NormalizedSeriesValue v : newValues) {
                forSeries.put(new SeriesValueKey(v.startInstant(), v.repeatedHour()), v);
            }
        }

        // ChunkRepositoryPort
        @Override
        public synchronized Long createChunk(Long taskId, LocalDate chunkStart, LocalDate chunkEnd) {
            long id = id();
            chunks.put(id, ChunkStatus.PENDING);
            return id;
        }

        @Override
        public synchronized void markChunkStage(Long chunkId, ChunkStatus stage) {
            chunks.put(chunkId, stage);
        }

        @Override
        public synchronized void updateChunkStatus(Long chunkId, ChunkStatus status, int fetched, int created,
                                                    int updated, int unchanged, UUID ledgerPointBatchId) {
            chunks.put(chunkId, status);
        }

        @Override
        public void updateUnit(Long seriesId, String unit) {
            // the in-memory descriptor has no unit column; not asserted by the acceptance suite
        }

        @Override
        public void updateOrionCallCount(Long chunkId, int orionCallCount) {
            // not asserted by the acceptance suite
        }

        @Override
        public synchronized void recordWarning(Long chunkId, WarningType type, LocalDate intervalDate,
                                               String intervalNumber, String detail) {
            warnings.add(type + ":" + intervalDate + ":" + intervalNumber);
        }

        // RuleOverrideRepositoryPort
        @Override
        public synchronized Optional<SeriesRuleOverride> findActiveByOrionSeriesName(String orionSeriesName) {
            return Optional.ofNullable(overrides.get(orionSeriesName));
        }
    }
}
