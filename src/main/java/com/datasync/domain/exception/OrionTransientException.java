package com.datasync.domain.exception;

/**
 * Transient Orion failure (429, 5xx, or a network timeout) - eligible for the Resilience4j
 * retry policy configured under {@code resilience4j.retry.instances.orion} (spec &sect;8). If
 * retries are exhausted, this propagates so the Task can be marked {@code SOURCE_UNAVAILABLE}.
 */
public class OrionTransientException extends RuntimeException {

    public OrionTransientException(String message) {
        super(message);
    }
}
