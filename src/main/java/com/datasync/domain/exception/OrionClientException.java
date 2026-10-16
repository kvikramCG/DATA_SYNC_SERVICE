package com.datasync.domain.exception;

/**
 * Orion rejected the request as invalid (400/404). Never retried (spec &sect;8); propagates so
 * the Task can be marked {@code SOURCE_UNAVAILABLE}.
 */
public class OrionClientException extends RuntimeException {

    public OrionClientException(String message) {
        super(message);
    }
}
