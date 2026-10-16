package com.datasync.domain.model;

/**
 * Lifecycle states for a {@code Job} - one sync execution for a series type (spec &sect;7, &sect;9).
 */
public enum JobStatus {
    RUNNING,
    COMPLETED,
    COMPLETED_WITH_FAILURES,
    FAILED
}
