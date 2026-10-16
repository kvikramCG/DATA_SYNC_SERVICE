package com.datasync.domain.exception;

/**
 * LedgerPoint returned 401/403 (spec &sect;5.1). Per spec &sect;8 this must fail the whole Job
 * immediately, not just the current Task.
 */
public class LedgerPointAuthException extends RuntimeException {

    public LedgerPointAuthException(String message) {
        super(message);
    }
}
