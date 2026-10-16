package com.datasync.domain.model;

/**
 * Lifecycle states for a {@code JobTask} - one series processed within a Job (spec &sect;7, &sect;9).
 */
public enum TaskStatus {
    PENDING,
    SUCCESS,
    SUCCESS_WITH_WARNINGS,
    SKIPPED,
    FAILED
}
