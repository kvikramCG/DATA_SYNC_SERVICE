package com.datasync.adapter.in.web;

import com.datasync.domain.model.JobSummary;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Inbound adapter DTO for {@code POST /api/series/{type}/sync} (spec &sect;7.1).
 */
public record JobSummaryResponse(
        Long jobId,
        String seriesType,
        String status,
        String startedAt,
        String finishedAt,
        Map<String, Integer> taskCounts,
        String correlationId
) {

    public static JobSummaryResponse from(JobSummary summary) {
        Map<String, Integer> counts = summary.taskCounts().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().name(), Map.Entry::getValue));
        return new JobSummaryResponse(
                summary.jobId(),
                summary.seriesType().wireValue(),
                summary.status().name(),
                summary.startedAt() == null ? null : summary.startedAt().toString(),
                summary.finishedAt() == null ? null : summary.finishedAt().toString(),
                counts,
                summary.correlationId() == null ? null : summary.correlationId().toString());
    }
}
