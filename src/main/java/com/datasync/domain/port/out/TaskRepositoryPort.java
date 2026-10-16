package com.datasync.domain.port.out;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.TaskStatus;

import java.util.List;

/**
 * Outbound port implemented by the persistence adapter ({@code adapter.out.persistence}).
 */
public interface TaskRepositoryPort {

    JobTask createTask(Long jobId, Long seriesId, String seriesName);

    List<JobTask> findByJobId(Long jobId);

    /**
     * Transitions a Task to a terminal status (spec &sect;7.2/&sect;8.1). Implementations set
     * {@code finishedAt} (and {@code startedAt}, if not already set) using the current clock.
     */
    void updateTaskStatus(Long taskId, TaskStatus status, FailureReason failureReason,
                           String failureDetail, int valuesSent);
}
