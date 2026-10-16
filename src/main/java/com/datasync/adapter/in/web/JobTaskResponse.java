package com.datasync.adapter.in.web;

import com.datasync.domain.model.JobTask;

/**
 * Inbound adapter DTO mirroring one entry of the {@code tasks} array in spec &sect;7.2.
 */
public record JobTaskResponse(
        String seriesName,
        String status,
        int valuesSent,
        String failureReason,
        String failureDetail
) {

    public static JobTaskResponse from(JobTask task) {
        return new JobTaskResponse(
                task.seriesName(),
                task.status().name(),
                task.valuesSent(),
                task.failureReason() == null ? null : task.failureReason().name(),
                task.failureDetail());
    }
}
