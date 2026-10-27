package com.datasync.domain.exception;

import java.time.Duration;

/**
 * LedgerPoint answered {@code 429}. A transient failure, so it is retried like a 5xx (spec &sect;8); the
 * optional {@code Retry-After} is honoured before the next attempt.
 */
public class LedgerPointRateLimitedException extends LedgerPointTransientException {

    private final Duration retryAfter;

    public LedgerPointRateLimitedException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    /** @return the server-requested wait, or {@code null} if LedgerPoint sent no usable header */
    public Duration getRetryAfter() {
        return retryAfter;
    }
}
