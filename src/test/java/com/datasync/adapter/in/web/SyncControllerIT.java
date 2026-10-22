package com.datasync.adapter.in.web;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the real spec &sect;7.1/&sect;7.2 endpoints: REST controller -> application
 * orchestration -> Orion + LedgerPoint adapters (mocked via WireMock, sharing one server since
 * their paths never collide) -> persistence adapters (real TimescaleDB via Testcontainers).
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SyncControllerIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg16").asCompatibleSubstituteFor("postgres"));

    static WireMockServer wireMockServer;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        wireMockServer = new WireMockServer(options().dynamicPort());
        wireMockServer.start();
        registry.add("orion.base-url", wireMockServer::baseUrl);
        registry.add("orion.login", () -> "svc-data-sync");
        registry.add("orion.password", () -> "secret");
        registry.add("ledgerpoint.base-url", wireMockServer::baseUrl);
        registry.add("ledgerpoint.api-key", () -> "test-api-key");
        // Fixtures carry fixed Oct-2026 lastUpdatedDate values; disable the staleness check (spec 6.9).
        registry.add("datasync.sync.stale-after-days", () -> "100000");
    }

    @AfterAll
    static void stopWireMock() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Autowired
    private TestRestTemplate restTemplate;

    private static final String TOKEN_JSON = """
            {"accessToken":"test-token-abc123","expiresInSeconds":3600}
            """;
    private static final String LEDGERPOINT_SUCCESS_JSON = """
            {"batchId":"f7c1e2a4-1b2c-4d5e-9f00-1234567890ab","received":1,"inserted":1,"updated":0}
            """;

    @Test
    void triggersSyncAndCompletesJobSuccessfully() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series"))
                .withQueryParam("type", equalTo("typeA"))
                .willReturn(okJson("""
                        {"series":[{"seriesName":"SERIES_ALPHA_001","seriesType":"typeA","timeZone":6,
                        "lastUpdatedDate":"10/12/2026 6:00:00 PM","earliestDataDate":"1/1/2020 12:00:00 AM",
                        "latestDataDate":"10/13/2026 11:00:00 PM"}]}
                        """)));
        wireMockServer.stubFor(get(urlPathMatching("/v1/series/SERIES_ALPHA_001/values"))
                .withQueryParam("start_prevailing", equalTo("10/15/2026 12:00:00 AM"))
                .withQueryParam("end_prevailing", equalTo("10/15/2026 11:59:59 PM"))
                .willReturn(okJson("""
                        {"seriesName":"SERIES_ALPHA_001","unit":"UNIT_A","granularity":1440,"timeZone":6,
                        "asOfDate":"10/12/2026 6:00:00 PM",
                        "values":[{"startDate":"10/15/2026 12:00:00 AM","repeatedHour":false,"value":31.25}]}
                        """)));
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(okJson(LEDGERPOINT_SUCCESS_JSON)));

        String url = "/api/series/typeA/sync?startDate=2026-10-15T00:00:00Z&endDate=2026-10-15T23:59:59Z";
        ResponseEntity<JobSummaryResponse> response = restTemplate.postForEntity(url, null, JobSummaryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JobSummaryResponse summary = response.getBody();
        assertThat(summary).isNotNull();
        assertThat(summary.jobId()).isNotNull();
        assertThat(summary.seriesType()).isEqualTo("typeA");
        assertThat(summary.status()).isEqualTo("RUNNING");
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getHeaders().getLocation().getPath()).isEqualTo("/api/jobs/" + summary.jobId());

        JobDetailResponse detail = awaitJobFinished(summary.jobId());

        assertThat(detail.status()).isEqualTo("COMPLETED");
        assertThat(detail.tasks()).hasSize(1);
        assertThat(detail.tasks().get(0).seriesName()).isEqualTo("SERIES_ALPHA_001");
        assertThat(detail.tasks().get(0).status()).isEqualTo("SUCCESS");
        assertThat(detail.tasks().get(0).valuesSent()).isEqualTo(1);
    }

    private JobDetailResponse awaitJobFinished(Long jobId) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            ResponseEntity<JobDetailResponse> detail =
                    restTemplate.getForEntity("/api/jobs/" + jobId, JobDetailResponse.class);
            assertThat(detail.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(detail.getBody()).isNotNull();
            if (!"RUNNING".equals(detail.getBody().status())) {
                return detail.getBody();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Job " + jobId + " still RUNNING after 30s");
    }

    @Test
    void secondSyncForSameRunningTypeReturns409() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson(TOKEN_JSON)));
        wireMockServer.stubFor(get(urlPathEqualTo("/v1/series"))
                .withQueryParam("type", equalTo("typeB"))
                .willReturn(okJson("""
                        {"series":[{"seriesName":"SERIES_BRAVO_002","seriesType":"typeB","timeZone":5,
                        "lastUpdatedDate":"10/12/2026 6:00:00 PM","earliestDataDate":"1/1/2020 12:00:00 AM",
                        "latestDataDate":"10/13/2026 11:00:00 PM"}]}
                        """)));
        wireMockServer.stubFor(get(urlPathMatching("/v1/series/SERIES_BRAVO_002/values"))
                .willReturn(okJson("""
                        {"seriesName":"SERIES_BRAVO_002","unit":"UNIT_A","granularity":1440,"timeZone":5,
                        "asOfDate":"10/12/2026 6:00:00 PM",
                        "values":[{"startDate":"10/1/2026 12:00:00 AM","repeatedHour":false,"value":5.0}]}
                        """)));
        // Deliberately slow so the first job is still RUNNING when we race it.
        wireMockServer.stubFor(post(urlEqualTo("/api/v2/series/values")).willReturn(
                okJson(LEDGERPOINT_SUCCESS_JSON).withFixedDelay(1500)));

        String url = "/api/series/typeB/sync?startDate=2026-10-01T00:00:00Z&endDate=2026-10-31T23:59:59Z";

        ResponseEntity<JobSummaryResponse> first = restTemplate.postForEntity(url, null, JobSummaryResponse.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        ResponseEntity<String> second = restTemplate.postForEntity(url, null, String.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        assertThat(awaitJobFinished(first.getBody().jobId()).status()).isEqualTo("COMPLETED");
    }
}

