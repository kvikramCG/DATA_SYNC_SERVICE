package com.datasync.adapter.out.orion;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.datasync.domain.exception.OrionTransientException;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TimeZoneCode;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plain unit test (no Spring context) - proves the Orion adapter's auth/retry/mapping behavior
 * directly against WireMock, exercising the same ports the spec's acceptance tests expect.
 */
class OrionClientAdapterTest {

    private static final String TOKEN_JSON = """
            {"accessToken":"test-token-abc123","expiresInSeconds":3600}
            """;

    private static final String SERIES_JSON = """
            {"series":[{"seriesName":"SERIES_ALPHA_001","seriesType":"typeA","timeZone":6,
            "lastUpdatedDate":"10/12/2026 6:00:00 PM","earliestDataDate":"1/1/2020 12:00:00 AM",
            "latestDataDate":"10/13/2026 11:00:00 PM"}]}
            """;

    private WireMockServer wireMockServer;
    private OrionClientAdapter adapter;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(options().dynamicPort());
        wireMockServer.start();

        OrionProperties properties = new OrionProperties();
        properties.setBaseUrl(wireMockServer.baseUrl());
        properties.setLogin("svc-data-sync");
        properties.setPassword("secret");
        properties.setConnectTimeoutMs(2000);
        properties.setReadTimeoutMs(2000);

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(2000);
        RestClient restClient = RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(factory)
                .build();

        OrionTokenCache tokenCache = new OrionTokenCache(restClient, properties);

        RateLimiterRegistry rateLimiterRegistry = RateLimiterRegistry.of(RateLimiterConfig.custom()
                .limitForPeriod(5)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofSeconds(5))
                .build());

        RetryRegistry retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(10))
                .retryExceptions(OrionTransientException.class)
                .build());

        adapter = new OrionClientAdapter(restClient, tokenCache, rateLimiterRegistry, retryRegistry);
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void listsSeriesSuccessfully() {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series"))
                .withQueryParam("type", equalTo("typeA"))
                .willReturn(okJson(SERIES_JSON)));

        List<SeriesDescriptor> result = adapter.listSeries(SeriesType.TYPE_A);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).name()).isEqualTo("SERIES_ALPHA_001");
        assertThat(result.get(0).timeZone()).isEqualTo(TimeZoneCode.CENTRAL);
    }

    @Test
    void refreshesTokenOnceOn401ThenSucceeds() {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));

        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series")).inScenario("list-retry")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(401))
                .willSetStateTo("retried"));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series")).inScenario("list-retry")
                .whenScenarioStateIs("retried")
                .willReturn(okJson(SERIES_JSON)));

        List<SeriesDescriptor> result = adapter.listSeries(SeriesType.TYPE_A);

        assertThat(result).hasSize(1);
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/auth/token")));
    }

    @Test
    void retriesOn429ThenSucceeds() {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));

        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series")).inScenario("rate-limit")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(429))
                .willSetStateTo("second"));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series")).inScenario("rate-limit")
                .whenScenarioStateIs("second")
                .willReturn(okJson(SERIES_JSON)));

        List<SeriesDescriptor> result = adapter.listSeries(SeriesType.TYPE_A);

        assertThat(result).hasSize(1);
    }

    @Test
    void skipsSeriesWithUnsupportedTimezone() {
        String mixedJson = """
                {"series":[
                  {"seriesName":"SERIES_BAD_TZ","seriesType":"typeA","timeZone":99,
                   "lastUpdatedDate":"10/12/2026 6:00:00 PM","earliestDataDate":"1/1/2020 12:00:00 AM",
                   "latestDataDate":"10/13/2026 11:00:00 PM"},
                  {"seriesName":"SERIES_ALPHA_001","seriesType":"typeA","timeZone":6,
                   "lastUpdatedDate":"10/12/2026 6:00:00 PM","earliestDataDate":"1/1/2020 12:00:00 AM",
                   "latestDataDate":"10/13/2026 11:00:00 PM"}
                ]}
                """;
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series")).willReturn(okJson(mixedJson)));

        List<SeriesDescriptor> result = adapter.listSeries(SeriesType.TYPE_A);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).name()).isEqualTo("SERIES_ALPHA_001");
    }

    @Test
    void fetchValuesMapsTheFallBackDayExampleFromTheSpec() {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));
        wireMockServer.stubFor(get(urlPathMatching("/v1/series/SERIES_ALPHA_001/values"))
                .withQueryParam("start_prevailing", equalTo("11/1/2026 12:00:00 AM"))
                .withQueryParam("end_prevailing", equalTo("11/30/2026 11:59:59 PM"))
                .willReturn(okJson("""
                        {"seriesName":"SERIES_ALPHA_001","unit":"UNIT_A","granularity":60,"timeZone":6,
                        "asOfDate":"10/12/2026 6:00:00 PM",
                        "values":[
                          {"startDate":"11/1/2026 12:00:00 AM","repeatedHour":false,"value":31.25},
                          {"startDate":"11/1/2026 1:00:00 AM","repeatedHour":false,"value":29.80},
                          {"startDate":"11/1/2026 1:00:00 AM","repeatedHour":true,"value":29.10},
                          {"startDate":"11/1/2026 2:00:00 AM","repeatedHour":false,"value":28.75}
                        ]}
                        """)));

        SeriesValuesResult result = adapter.fetchValues("SERIES_ALPHA_001",
                LocalDateTime.of(2026, 11, 1, 0, 0, 0),
                LocalDateTime.of(2026, 11, 30, 23, 59, 59));

        assertThat(result.seriesName()).isEqualTo("SERIES_ALPHA_001");
        assertThat(result.unit()).isEqualTo("UNIT_A");
        assertThat(result.granularity()).isEqualTo(60);
        assertThat(result.timeZone()).isEqualTo(TimeZoneCode.CENTRAL);
        assertThat(result.values()).hasSize(4);
        assertThat(result.values().get(2).repeatedHour()).isTrue();
        assertThat(result.values().get(2).value()).isEqualByComparingTo("29.10");
    }
}

