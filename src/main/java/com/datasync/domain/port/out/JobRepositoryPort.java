package com.datasync.domain.port.out;

import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port implemented by the persistence adapter ({@code adapter.out.persistence}).
 */
public interface JobRepositoryPort {

    /**
     * Creates a new {@code RUNNING} job. Implementations must enforce "one running job per
     * series type" (spec &sect;7.1) and throw
     * {@link com.datasync.domain.exception.JobAlreadyRunningException} if one is already running.
     */
    Job createRunningJob(SeriesType type, String requestedSeriesName, Instant requestedStartDate,
                          Instant requestedEndDate, UUID correlationId);

    void markJobFinished(Long jobId, JobStatus status);

    /**
     * Closes every job still {@code RUNNING} that started before {@code startedBefore} as
     * {@link JobStatus#FAILED}. Orphaned RUNNING rows (process crash, unexpected failure) would
     * otherwise block every later sync of that series type via the unique partial index.
     *
     * @return the number of jobs closed
     */
    int failStaleRunningJobs(Instant startedBefore);

    Optional<Job> findById(Long jobId);
}
