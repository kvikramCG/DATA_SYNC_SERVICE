package com.datasync.domain.exception;

import com.datasync.domain.model.LedgerPointValidationError;

import java.util.List;

/**
 * LedgerPoint returned 400 - the whole batch was rejected (spec &sect;5.1). Maps to Task failure
 * reason {@code TARGET_REJECTED}. Never retried.
 */
public class LedgerPointValidationException extends RuntimeException {

    private final List<LedgerPointValidationError> errors;

    public LedgerPointValidationException(String message, List<LedgerPointValidationError> errors) {
        super(message);
        this.errors = errors;
    }

    public List<LedgerPointValidationError> getErrors() {
        return errors;
    }
}
