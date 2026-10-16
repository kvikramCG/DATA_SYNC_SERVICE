package com.datasync.domain.exception;

/**
 * Thrown when {@code GET /api/jobs/{jobId}} (spec &sect;7.2) references an unknown job id.
 */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(String message) {
        super(message);
    }
}
