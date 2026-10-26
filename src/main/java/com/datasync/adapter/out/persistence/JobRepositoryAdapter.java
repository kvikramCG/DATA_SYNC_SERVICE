package com.datasync.adapter.out.persistence;

import com.datasync.domain.exception.JobAlreadyRunningException;
import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound adapter implementing {@link JobRepositoryPort}. The one-running-job-per-type rule is
 * enforced by the DB unique partial index {@code uq_job_running_per_type} (see
 * architecture.md "Concurrency & Job Locking") - this adapter only translates the resulting
 * constraint violation into {@link JobAlreadyRunningException}.
 */
@Component
public class JobRepositoryAdapter implements JobRepositoryPort {

    private static final Logger log = LoggerFactory.getLogger(JobRepositoryAdapter.class);

    private final JobJpaRepository repository;
    private final ClockPort clock;

    public JobRepositoryAdapter(JobJpaRepository repository, ClockPort clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Override
    @Transactional
    public Job createRunningJob(SeriesType type, String requestedSeriesName, Instant requestedStartDate,
                                 Instant requestedEndDate, UUID correlationId) {
        JobEntity entity = new JobEntity(
                type.wireValue(),
                JobStatus.RUNNING.name(),
                requestedSeriesName,
                requestedStartDate == null ? null : requestedStartDate.atZone(ZoneOffset.UTC).toLocalDate(),
                requestedEndDate == null ? null : requestedEndDate.atZone(ZoneOffset.UTC).toLocalDate(),
                clock.now(),
                correlationId);
        try {
            // saveAndFlush forces the unique partial index check now, inside this try block,
            // rather than deferring the INSERT to transaction commit.
            JobEntity saved = repository.saveAndFlush(entity);
            return toDomain(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new JobAlreadyRunningException(
                    "A job for series type " + type.wireValue() + " is already running");
        }
    }

    @Override
    @Transactional
    public boolean markJobFinished(Long jobId, JobStatus status) {
        // Previously this was findById + setStatus + save, which overwrote ANY status. That let a job
        // the reaper had already failed flip back to COMPLETED when its threads finally ended.
        // The conditional update below closes the row only if it is still RUNNING (first close wins).
        int updated = repository.closeIfRunning(jobId, status.name(), clock.now());
        if (updated == 1) {
            return true;
        }
        // 0 rows is ambiguous: either the job does not exist or it was already closed. Keep the old
        // contract for the first case (JobNotFoundException) and report the second as "not closed".
        if (!repository.existsById(jobId)) {
            throw new JobNotFoundException("No job found with id " + jobId);
        }
        return false;
    }

    @Override
    @Transactional
    public List<Long> failStaleRunningJobs(Instant startedBefore) {
        // Same predicate as the bulk update below, read in the same transaction, so the logged jobs
        // are the ones being failed. Gives operators the ids/correlation ids to search logs and
        // LedgerPoint for; the previous log only had a count.
        List<JobEntity> stale = repository.findByStatusAndStartedAtBefore(JobStatus.RUNNING.name(), startedBefore);
        stale.forEach(job -> log.warn("Failing stale job {} (type={}, startedAt={}, correlationId={})",
                job.getId(), job.getSeriesType(), job.getStartedAt(), job.getCorrelationId()));
        repository.failRunningJobsStartedBefore(startedBefore, clock.now());
        // The caller needs the ids to stop these jobs' workers and fail their unfinished Tasks.
        return stale.stream().map(JobEntity::getId).toList();
    }

    @Override
    public Optional<Job> findById(Long jobId) {
        return repository.findById(jobId).map(this::toDomain);
    }

    private Job toDomain(JobEntity entity) {
        return new Job(
                entity.getId(),
                SeriesType.fromWireValue(entity.getSeriesType()),
                JobStatus.valueOf(entity.getStatus()),
                entity.getRequestedSeriesName(),
                entity.getRequestedStartDate() == null
                        ? null : entity.getRequestedStartDate().atStartOfDay(ZoneOffset.UTC).toInstant(),
                entity.getRequestedEndDate() == null
                        ? null : entity.getRequestedEndDate().atStartOfDay(ZoneOffset.UTC).toInstant(),
                entity.getStartedAt(),
                entity.getFinishedAt(),
                entity.getCorrelationId());
    }
}
