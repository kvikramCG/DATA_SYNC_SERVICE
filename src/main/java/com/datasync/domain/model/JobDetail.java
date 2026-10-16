package com.datasync.domain.model;

import java.util.List;

/**
 * Response shape for {@code GET /api/jobs/{jobId}} (spec &sect;7.2).
 */
public record JobDetail(
        Long jobId,
        SeriesType seriesType,
        JobStatus status,
        List<JobTask> tasks
) {
}
