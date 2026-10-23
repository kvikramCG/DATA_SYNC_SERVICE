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

    /**
     * Closes the job with the given terminal status, but only if it is still {@code RUNNING}
     * (first close wins). The stale-job reaper can already have failed a slow job whose threads are
     * still running; when that job later finishes it must not overwrite the reaper's verdict, lose
     * the original {@code finishedAt}, or flip a status the caller may already have acted on.
     *
     * @return {@code true} if this call closed the job; {@code false} if it was already closed
     *         (nothing was changed)
     * @throws com.datasync.domain.exception.JobNotFoundException if no such job exists
     */
    boolean markJobFinished(Long jobId, JobStatus status);

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
