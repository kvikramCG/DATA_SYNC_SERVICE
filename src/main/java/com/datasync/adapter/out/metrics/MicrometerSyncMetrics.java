package com.datasync.adapter.out.metrics;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.port.out.SyncMetricsPort;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Outbound adapter implementing {@link SyncMetricsPort} with Micrometer counters (exposed to
 * Prometheus at {@code /actuator/prometheus}). Tag values are bounded enums, so metric cardinality
 * stays small - series names are deliberately never used as tags.
 */
@Component
public class MicrometerSyncMetrics implements SyncMetricsPort {

    static final String JOBS = "datasync.jobs";
    static final String TASKS = "datasync.sync.tasks";
    static final String VALUES = "datasync.sync.values";

    private final MeterRegistry registry;

    public MicrometerSyncMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void jobFinished(SeriesType type, JobStatus status) {
        registry.counter(JOBS, "series_type", type.wireValue(), "status", status.name()).increment();
    }

    @Override
    public void taskFinished(TaskStatus status, FailureReason failureReason) {
        registry.counter(TASKS,
                "status", status.name(),
                "failure_reason", failureReason == null ? "NONE" : failureReason.name()).increment();
    }

    @Override
    public void valuesClassified(int newCount, int updatedCount, int unchangedCount) {
        registry.counter(VALUES, "change", "new").increment(newCount);
        registry.counter(VALUES, "change", "updated").increment(updatedCount);
        registry.counter(VALUES, "change", "unchanged").increment(unchangedCount);
    }
}
