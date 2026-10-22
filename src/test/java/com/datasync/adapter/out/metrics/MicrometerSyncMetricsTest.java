package com.datasync.adapter.out.metrics;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerSyncMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerSyncMetrics metrics = new MicrometerSyncMetrics(registry);

    @Test
    void countsFinishedJobsPerTypeAndStatus() {
        metrics.jobFinished(SeriesType.TYPE_A, JobStatus.COMPLETED);
        metrics.jobFinished(SeriesType.TYPE_A, JobStatus.COMPLETED);
        metrics.jobFinished(SeriesType.TYPE_A, JobStatus.FAILED);

        assertThat(registry.get(MicrometerSyncMetrics.JOBS)
                .tag("series_type", SeriesType.TYPE_A.wireValue()).tag("status", "COMPLETED").counter().count())
                .isEqualTo(2);
        assertThat(registry.get(MicrometerSyncMetrics.JOBS)
                .tag("status", "FAILED").counter().count()).isEqualTo(1);
    }

    @Test
    void countsTasksWithAndWithoutFailureReason() {
        metrics.taskFinished(TaskStatus.SUCCESS, null);
        metrics.taskFinished(TaskStatus.FAILED, FailureReason.STALE_SERIES);

        assertThat(registry.get(MicrometerSyncMetrics.TASKS)
                .tag("status", "SUCCESS").tag("failure_reason", "NONE").counter().count()).isEqualTo(1);
        assertThat(registry.get(MicrometerSyncMetrics.TASKS)
                .tag("status", "FAILED").tag("failure_reason", "STALE_SERIES").counter().count()).isEqualTo(1);
    }

    @Test
    void accumulatesClassifiedValueCounts() {
        metrics.valuesClassified(3, 2, 1);
        metrics.valuesClassified(1, 0, 4);

        assertThat(registry.get(MicrometerSyncMetrics.VALUES).tag("change", "new").counter().count()).isEqualTo(4);
        assertThat(registry.get(MicrometerSyncMetrics.VALUES).tag("change", "updated").counter().count()).isEqualTo(2);
        assertThat(registry.get(MicrometerSyncMetrics.VALUES).tag("change", "unchanged").counter().count())
                .isEqualTo(5);
    }
}
