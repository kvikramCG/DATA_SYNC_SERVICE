package com.datasync.domain.model;

import java.util.List;
import java.util.UUID;

/**
 * Response shape for {@code GET /api/jobs/{jobId}} (spec &sect;7.2).
 */
public record JobDetail(
        Long jobId,
        SeriesType seriesType,
        JobStatus status,
        List<JobTask> tasks,
        // Lets a caller of GET /api/jobs/{id} find this job's log lines and LedgerPoint requests.
        UUID correlationId
) {
}
