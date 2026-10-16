package com.datasync.domain.exception;

/**
 * Orion rejected the bearer token (401). Handled by a single refresh-and-retry inside the Orion
 * adapter; if it still fails after refresh, this propagates to the caller so the sync pipeline
 * can mark the affected Task {@code SOURCE_UNAVAILABLE} (spec &sect;8).
 */
public class OrionAuthException extends RuntimeException {

    public OrionAuthException(String message) {
        super(message);
    }
}
