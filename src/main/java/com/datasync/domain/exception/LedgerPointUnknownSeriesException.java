package com.datasync.domain.exception;

/**
 * LedgerPoint returned 409 - series {@code key} not configured (spec &sect;5.1). Maps to Task
 * failure reason {@code UNKNOWN_SERIES}. Never retried.
 */
public class LedgerPointUnknownSeriesException extends RuntimeException {

    public LedgerPointUnknownSeriesException(String message) {
        super(message);
    }
}
