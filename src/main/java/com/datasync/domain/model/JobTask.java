package com.datasync.domain.model;

import java.time.Instant;

/**
 * One series processed within a {@link Job} (spec &sect;7.2, &sect;9).
 */
public record JobTask(
        Long id,
        Long jobId,
        Long seriesId,
        String seriesName,
        TaskStatus status,
        FailureReason failureReason,
        String failureDetail,
        int valuesSent,
        Instant startedAt,
        Instant finishedAt
) {
}
