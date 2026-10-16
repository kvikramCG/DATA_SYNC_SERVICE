package com.datasync.domain.exception;

/**
 * Transient LedgerPoint failure (5xx or a network timeout) - eligible for the Resilience4j
 * retry policy configured under {@code resilience4j.retry.instances.ledgerpoint} (spec &sect;8).
 */
public class LedgerPointTransientException extends RuntimeException {

    public LedgerPointTransientException(String message) {
        super(message);
    }
}
