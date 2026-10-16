package com.datasync.domain.model;

/**
 * Lifecycle states for a {@code job_task_chunk} row (one month-chunk within a Task) - spec
 * &sect;3.1 step 2, database-design.md &sect;3.3.
 */
public enum ChunkStatus {
    FETCHED,
    TRANSFORMED,
    PUBLISHED,
    PERSISTED,
    FAILED
}
