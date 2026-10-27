package com.datasync.adapter.out.http;

import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;

import java.time.Duration;

/**
 * Shared handling of a {@code 429} response's {@code Retry-After} header (spec &sect;8) for the Orion and
 * LedgerPoint adapters, so both back off the same way.
 */
public final class RetryAfterSupport {

    private RetryAfterSupport() {
    }

    /** Spec &sect;4: {@code Retry-After} is a number of seconds. Anything else is ignored. */
    public static Duration parse(HttpHeaders headers, Logger log) {
        String value = headers.getFirst(HttpHeaders.RETRY_AFTER);
        if (value == null) {
            return null;
        }
        try {
            long seconds = Long.parseLong(value.trim());
            return seconds < 0 ? null : Duration.ofSeconds(seconds);
        } catch (NumberFormatException e) {
            log.warn("Ignoring unparseable Retry-After header");
            return null;
        }
    }

    /** Waits the requested time, capped by {@code cap}, on the calling (virtual) thread. */
    public static void pause(Duration retryAfter, Duration cap, String system, Logger log) {
        if (retryAfter == null || retryAfter.isZero() || retryAfter.isNegative()) {
            return;
        }
        Duration wait = cap != null && retryAfter.compareTo(cap) > 0 ? cap : retryAfter;
        log.info("{} asked us to back off; waiting {} before retrying", system, wait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
