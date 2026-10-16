package com.datasync.adapter.out.orion;

import com.datasync.adapter.out.orion.dto.OrionTokenRequest;
import com.datasync.adapter.out.orion.dto.OrionTokenResponse;
import com.datasync.domain.exception.OrionAuthException;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;

/**
 * Caches the Orion bearer token (valid 60 minutes per spec &sect;4) and refreshes it on
 * expiry or on demand ({@link #invalidate()} after a 401). Access is synchronized to avoid a
 * thundering herd of refresh calls from concurrently running series workers (ADR-004).
 */
@Component
public class OrionTokenCache {

    private static final Duration SAFETY_MARGIN = Duration.ofSeconds(60);

    private final RestClient orionRestClient;
    private final OrionProperties properties;

    private volatile String accessToken;
    private volatile Instant expiresAt = Instant.EPOCH;

    public OrionTokenCache(RestClient orionRestClient, OrionProperties properties) {
        this.orionRestClient = orionRestClient;
        this.properties = properties;
    }

    public synchronized String getToken() {
        if (accessToken == null || Instant.now().isAfter(expiresAt.minus(SAFETY_MARGIN))) {
            refresh();
        }
        return accessToken;
    }

    public synchronized void invalidate() {
        accessToken = null;
    }

    private void refresh() {
        OrionTokenResponse response = orionRestClient.post()
                .uri("/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new OrionTokenRequest(properties.getLogin(), properties.getPassword()))
                .retrieve()
                .onStatus(HttpStatusCode::isError, (req, res) -> {
                    throw new OrionAuthException("Failed to obtain Orion token: " + res.getStatusCode());
                })
                .body(OrionTokenResponse.class);

        if (response == null) {
            throw new OrionAuthException("Orion token response was empty");
        }
        this.accessToken = response.accessToken();
        this.expiresAt = Instant.now().plusSeconds(response.expiresInSeconds());
    }
}
