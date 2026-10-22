package com.datasync.domain.port.in;

import com.datasync.domain.model.JobSummary;
import com.datasync.domain.model.SeriesType;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound port for {@code POST /api/series/{type}/sync} (spec &sect;7.1).
 *
 * <p>Asynchronous: creates the {@code RUNNING} Job, starts the run in the background and returns
 * immediately with a summary whose status is {@code RUNNING} and whose task counts are empty.
 * The outcome is read through {@link GetJobUseCase}. A
 * {@link com.datasync.domain.exception.JobAlreadyRunningException} is still thrown synchronously
 * when a Job of the same series type is already running.
 */
public interface TriggerSyncUseCase {

    JobSummary triggerSync(SeriesType type, String seriesName, Instant startDate, Instant endDate,
                            UUID correlationId);
}
