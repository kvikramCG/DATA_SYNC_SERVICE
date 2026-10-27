package com.datasync.adapter.out.orion;

import com.datasync.adapter.out.orion.dto.OrionTokenRequest;
import com.datasync.adapter.out.orion.dto.OrionTokenResponse;
import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionTransientException;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Caches the Orion bearer token (valid 60 minutes per spec &sect;4) and refreshes it on
 * expiry or on demand ({@link #invalidate(String)} after a 401). Only one thread refreshes at a time,
 * so concurrently running series workers (ADR-004) do not stampede the login endpoint.
 */
@Component
public class OrionTokenCache {

    private static final Duration SAFETY_MARGIN = Duration.ofSeconds(60);

    // Token and expiry travel together so a reader can never pair a new token with an old expiry.
    private record CachedToken(String value, Instant expiresAt) {
        boolean usable() {
            return Instant.now().isBefore(expiresAt.minus(SAFETY_MARGIN));
        }
    }

    private final RestClient orionRestClient;
    private final OrionProperties properties;

    // ReentrantLock instead of synchronized: the login call below blocks on the network, and a virtual
    // thread blocked inside synchronized pins its carrier thread (JDK 21), stalling unrelated tasks.
    private final ReentrantLock refreshLock = new ReentrantLock();
    private volatile CachedToken cached;

    public OrionTokenCache(RestClient orionRestClient, OrionProperties properties) {
        this.orionRestClient = orionRestClient;
        this.properties = properties;
    }

    public String getToken() {
        CachedToken current = cached;
        if (current != null && current.usable()) {
            return current.value(); // fast path: no lock while the token is valid
        }
        refreshLock.lock();
        try {
            // Re-check: a thread ahead of us may already have refreshed while we waited for the lock.
            current = cached;
            if (current == null || !current.usable()) {
                current = refresh();
                cached = current;
            }
            return current.value();
        } finally {
            refreshLock.unlock();
        }
    }

    /**
     * Drops the cached token only if it is still {@code rejectedToken}. When many tasks get a 401 for the
     * same stale token, the first one refreshes; the late ones must not throw that fresh token away.
     */
    public void invalidate(String rejectedToken) {
        refreshLock.lock();
        try {
            CachedToken current = cached;
            if (current != null && current.value().equals(rejectedToken)) {
                cached = null;
            }
        } finally {
            refreshLock.unlock();
        }
    }

    private CachedToken refresh() {
        OrionTokenResponse response;
        try {
            response = orionRestClient.post()
                    .uri("/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new OrionTokenRequest(properties.getLogin(), properties.getPassword()))
                    .retrieve()
                    // Spec §4.1 / §8: Orion's own 5xx is transient and retried; bad credentials (401) are not.
                    .onStatus(HttpStatusCode::is5xxServerError, (req, res) -> {
                        throw new OrionTransientException("Orion token endpoint error: " + res.getStatusCode());
                    })
                    .onStatus(HttpStatusCode::isError, (req, res) -> {
                        throw new OrionAuthException("Failed to obtain Orion token: " + res.getStatusCode());
                    })
                    .body(OrionTokenResponse.class);
        } catch (ResourceAccessException networkException) {
            throw new OrionTransientException("Orion network error while obtaining a token: "
                    + networkException.getMessage());
        }

        if (response == null || response.accessToken() == null || response.accessToken().isBlank()) {
            throw new OrionAuthException("Orion token response was empty");
        }
        return new CachedToken(response.accessToken(), Instant.now().plusSeconds(response.expiresInSeconds()));
    }
}
