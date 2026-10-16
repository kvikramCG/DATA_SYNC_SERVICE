package com.datasync.domain.exception;

/**
 * Thrown when a job for the requested series type is already {@code RUNNING} (spec &sect;7.1,
 * 409 response). Backed by the DB unique partial index documented in architecture.md
 * "Concurrency & Job Locking".
 */
public class JobAlreadyRunningException extends RuntimeException {

    public JobAlreadyRunningException(String message) {
        super(message);
    }
}
