package com.datasync.domain.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Response shape for {@code POST /api/series/{type}/sync} (spec &sect;7.1).
 */
public record JobSummary(
        Long jobId,
        SeriesType seriesType,
        JobStatus status,
        Instant startedAt,
        Instant finishedAt,
        Map<TaskStatus, Integer> taskCounts,
        // Server-generated id that ties together logs, Orion/LedgerPoint calls and the job row.
        UUID correlationId
) {
}
