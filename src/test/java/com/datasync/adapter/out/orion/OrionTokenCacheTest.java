package com.datasync.adapter.out.orion;

import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionTransientException;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrionTokenCacheTest {

    private WireMockServer wireMock;
    private OrionTokenCache cache;

    @BeforeEach
    void setUp() {
        wireMock = new WireMockServer(options().dynamicPort());
        wireMock.start();

        OrionProperties properties = new OrionProperties();
        properties.setBaseUrl(wireMock.baseUrl());
        properties.setLogin("svc-data-sync");
        properties.setPassword("secret");

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(5000);
        cache = new OrionTokenCache(RestClient.builder().baseUrl(wireMock.baseUrl()).requestFactory(factory).build(),
                properties);
    }

    @AfterEach
    void tearDown() {
        wireMock.stop();
    }

    @Test
    void manyConcurrentCallersCauseOnlyOneLogin() throws Exception {
        // The delay keeps the first login in flight while every other thread arrives.
        wireMock.stubFor(post(urlEqualTo("/auth/token")).willReturn(
                okJson("{\"accessToken\":\"token-1\",\"expiresInSeconds\":3600}").withFixedDelay(300)));

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                results.add(executor.submit(cache::getToken));
            }
            for (Future<String> result : results) {
                assertThat(result.get()).isEqualTo("token-1");
            }
        }

        wireMock.verify(1, postRequestedFor(urlEqualTo("/auth/token")));
    }

    @Test
    void invalidatingAnAlreadyReplacedTokenKeepsTheFreshOne() {
        wireMock.stubFor(post(urlEqualTo("/auth/token")).inScenario("tokens")
                .whenScenarioStateIs(STARTED)
                .willReturn(okJson("{\"accessToken\":\"token-1\",\"expiresInSeconds\":3600}"))
                .willSetStateTo("second"));
        wireMock.stubFor(post(urlEqualTo("/auth/token")).inScenario("tokens")
                .whenScenarioStateIs("second")
                .willReturn(okJson("{\"accessToken\":\"token-2\",\"expiresInSeconds\":3600}")));

        String stale = cache.getToken();
        cache.invalidate(stale);                      // first task that saw the 401
        assertThat(cache.getToken()).isEqualTo("token-2");
        cache.invalidate(stale);                      // late task still holding the old token

        assertThat(cache.getToken()).isEqualTo("token-2");
        wireMock.verify(2, postRequestedFor(urlEqualTo("/auth/token")));
    }

    @Test
    void aBadCredentialsResponseSurfacesAsAnAuthException() {
        wireMock.stubFor(post(urlEqualTo("/auth/token")).willReturn(aResponse().withStatus(401)));

        assertThatThrownBy(cache::getToken).isInstanceOf(OrionAuthException.class);
    }

    @Test
    void aServerErrorOnTheLoginEndpointIsTransientSoItCanBeRetried() {
        wireMock.stubFor(post(urlEqualTo("/auth/token")).willReturn(aResponse().withStatus(500)));

        assertThatThrownBy(cache::getToken).isInstanceOf(OrionTransientException.class);
    }

    @Test
    void aNetworkFailureOnTheLoginEndpointIsTransient() {
        wireMock.stop(); // nothing listens any more: connection refused

        assertThatThrownBy(cache::getToken).isInstanceOf(OrionTransientException.class);
    }

    @Test
    void aTokenResponseWithoutAnAccessTokenIsRejected() {
        wireMock.stubFor(post(urlEqualTo("/auth/token")).willReturn(okJson("{\"expiresInSeconds\":3600}")));

        assertThatThrownBy(cache::getToken).isInstanceOf(OrionAuthException.class);
    }
}
