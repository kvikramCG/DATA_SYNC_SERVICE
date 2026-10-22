package com.datasync.domain.model;

/**
 * Lifecycle states for a {@code job_task_chunk} row (one month-chunk within a Task) - spec
 * &sect;3.1 step 2, database-design.md &sect;3.3. The happy path walks them in order:
 * {@code PENDING -> FETCHED -> TRANSFORMED -> PUBLISHED -> PERSISTED}; {@code FAILED} ends a chunk
 * that threw. A chunk left in a non-terminal state (for example after a crash) shows exactly how far
 * it got - notably {@code PUBLISHED} means LedgerPoint accepted the batch but the local
 * {@code series_value} rows were not stored yet (safe to re-run: LedgerPoint upserts).
 */
public enum ChunkStatus {
    PENDING,
    FETCHED,
    TRANSFORMED,
    PUBLISHED,
    PERSISTED,
    FAILED
}
