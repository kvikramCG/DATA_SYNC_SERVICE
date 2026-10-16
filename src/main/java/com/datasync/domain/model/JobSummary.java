package com.datasync.domain.model;

import java.time.Instant;
import java.util.Map;

/**
 * Response shape for {@code POST /api/series/{type}/sync} (spec &sect;7.1).
 */
public record JobSummary(
        Long jobId,
        SeriesType seriesType,
        JobStatus status,
        Instant startedAt,
        Instant finishedAt,
        Map<TaskStatus, Integer> taskCounts
) {
}
