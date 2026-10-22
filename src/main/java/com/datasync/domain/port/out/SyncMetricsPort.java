package com.datasync.domain.port.out;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;

/**
 * Outbound port through which the application layer reports business events for monitoring. The
 * implementation (Micrometer/Prometheus) lives in an adapter, so the core stays free of any
 * metrics framework.
 */
public interface SyncMetricsPort {

    /** A Job reached a terminal status. */
    void jobFinished(SeriesType type, JobStatus status);

    /** A Task reached its final status; {@code failureReason} is {@code null} unless it failed. */
    void taskFinished(TaskStatus status, FailureReason failureReason);

    /** Result of diffing one chunk of fetched values against what was previously synced. */
    void valuesClassified(int newCount, int updatedCount, int unchangedCount);
}
