package com.datasync.domain.exception;

/**
 * Thrown when an Orion {@code granularity} value is outside {15, 60, 1440} (spec &sect;6.3).
 * Corresponds to failure reason {@code UNSUPPORTED_GRANULARITY}.
 */
public class UnsupportedGranularityException extends RuntimeException {

    public UnsupportedGranularityException(String message) {
        super(message);
    }
}
