package com.datasync.domain.model;

/**
 * One row-level validation error from a LedgerPoint {@code 400} response (spec &sect;5.1).
 */
public record LedgerPointValidationError(int index, String field, String message) {
}
