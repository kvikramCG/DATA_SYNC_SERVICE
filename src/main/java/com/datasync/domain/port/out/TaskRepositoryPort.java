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
     * Marks a Task {@code RUNNING} and records {@code startedAt} using the current clock. Called once,
     * when a worker actually begins the Task, so {@code finishedAt - startedAt} is the real duration and
     * an in-flight Task is distinguishable from one that is still queued ({@code PENDING}).
     */
    void markTaskStarted(Long taskId);

    /**
     * Transitions a Task to a terminal status (spec &sect;7.2/&sect;8.1). Implementations set
     * {@code finishedAt} (and {@code startedAt}, only if {@link #markTaskStarted} never ran - e.g. a
     * Task that fails before a worker starts it) using the current clock.
     */
    void updateTaskStatus(Long taskId, TaskStatus status, FailureReason failureReason,
                           String failureDetail, int valuesSent);
}
