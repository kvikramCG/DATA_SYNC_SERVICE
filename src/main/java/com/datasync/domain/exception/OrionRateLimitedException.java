package com.datasync.domain.exception;

import java.time.Duration;

/**
 * Orion answered {@code 429 Too Many Requests} (spec &sect;4). It is a transient failure, so it is
 * retried like any other {@link OrionTransientException}, but it additionally carries the server's
 * {@code Retry-After} hint so the adapter can wait at least that long before the next attempt.
 */
public class OrionRateLimitedException extends OrionTransientException {

    private final Duration retryAfter;

    /**
     * @param retryAfter the parsed {@code Retry-After} value, or {@code null} if the header was
     *                   absent or unparseable
     */
    public OrionRateLimitedException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Duration getRetryAfter() {
        return retryAfter;
    }
}
