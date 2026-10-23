package com.datasync.domain.model;

/**
 * Lifecycle states for a {@code JobTask} - one series processed within a Job (spec &sect;7, &sect;9).
 */
public enum TaskStatus {
    PENDING,
    /** The worker has picked the Task up; set together with {@code started_at} so duration is measurable. */
    RUNNING,
    SUCCESS,
    SUCCESS_WITH_WARNINGS,
    SKIPPED,
    FAILED
}
