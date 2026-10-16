package com.datasync.adapter.out.persistence;

import com.datasync.domain.exception.JobAlreadyRunningException;
import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneOffset;
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
    public void markJobFinished(Long jobId, JobStatus status) {
        JobEntity entity = repository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException("No job found with id " + jobId));
        entity.setStatus(status.name());
        entity.setFinishedAt(clock.now());
        repository.save(entity);
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
