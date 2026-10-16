package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Outbound adapter implementing {@link TaskRepositoryPort}. {@code job_task} only stores
 * {@code series_id}, so {@link #findByJobId} resolves series names with a follow-up lookup
 * against {@link SeriesJpaRepository} (acceptable at POC scale - see database-design.md).
 */
@Component
public class TaskRepositoryAdapter implements TaskRepositoryPort {

    private final JobTaskJpaRepository repository;
    private final SeriesJpaRepository seriesRepository;
    private final ClockPort clock;

    public TaskRepositoryAdapter(JobTaskJpaRepository repository, SeriesJpaRepository seriesRepository, ClockPort clock) {
        this.repository = repository;
        this.seriesRepository = seriesRepository;
        this.clock = clock;
    }

    @Override
    @Transactional
    public JobTask createTask(Long jobId, Long seriesId, String seriesName) {
        JobTaskEntity entity = new JobTaskEntity(jobId, seriesId, TaskStatus.PENDING.name());
        JobTaskEntity saved = repository.save(entity);
        return toDomain(saved, seriesName);
    }

    @Override
    public List<JobTask> findByJobId(Long jobId) {
        List<JobTaskEntity> entities = repository.findByJobId(jobId);
        List<Long> seriesIds = entities.stream().map(JobTaskEntity::getSeriesId).distinct().toList();
        Map<Long, String> namesById = seriesRepository.findAllById(seriesIds).stream()
                .collect(Collectors.toMap(SeriesEntity::getId, SeriesEntity::getName));
        return entities.stream()
                .map(entity -> toDomain(entity, namesById.get(entity.getSeriesId())))
                .toList();
    }

    @Override
    @Transactional
    public void updateTaskStatus(Long taskId, TaskStatus status, FailureReason failureReason,
                                  String failureDetail, int valuesSent) {
        JobTaskEntity entity = repository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("No task found with id " + taskId));
        Instant now = clock.now();
        if (entity.getStartedAt() == null) {
            entity.setStartedAt(now);
        }
        entity.setStatus(status.name());
        entity.setFailureReason(failureReason == null ? null : failureReason.name());
        entity.setFailureDetail(failureDetail);
        entity.setValuesSent(valuesSent);
        entity.setFinishedAt(now);
        repository.save(entity);
    }

    private JobTask toDomain(JobTaskEntity entity, String seriesName) {
        return new JobTask(
                entity.getId(),
                entity.getJobId(),
                entity.getSeriesId(),
                seriesName,
                TaskStatus.valueOf(entity.getStatus()),
                entity.getFailureReason() == null ? null : FailureReason.valueOf(entity.getFailureReason()),
                entity.getFailureDetail(),
                entity.getValuesSent(),
                entity.getStartedAt(),
                entity.getFinishedAt());
    }
}
