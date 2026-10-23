package com.datasync.adapter.in.web;

import com.datasync.domain.model.JobDetail;

import java.util.List;

/**
 * Inbound adapter DTO for {@code GET /api/jobs/{jobId}} (spec &sect;7.2).
 */
public record JobDetailResponse(
        Long jobId,
        String status,
        List<JobTaskResponse> tasks,
        String correlationId
) {

    public static JobDetailResponse from(JobDetail detail) {
        return new JobDetailResponse(
                detail.jobId(),
                detail.status().name(),
                detail.tasks().stream().map(JobTaskResponse::from).toList(),
                detail.correlationId() == null ? null : detail.correlationId().toString());
    }
}
