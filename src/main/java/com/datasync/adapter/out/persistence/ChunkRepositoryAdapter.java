package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.WarningType;
import com.datasync.domain.port.out.ChunkRepositoryPort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Outbound adapter implementing {@link ChunkRepositoryPort} against {@code job_task_chunk} and
 * {@code task_warning}.
 */
@Component
public class ChunkRepositoryAdapter implements ChunkRepositoryPort {

    private final JobTaskChunkJpaRepository chunkRepository;
    private final TaskWarningJpaRepository warningRepository;

    public ChunkRepositoryAdapter(JobTaskChunkJpaRepository chunkRepository, TaskWarningJpaRepository warningRepository) {
        this.chunkRepository = chunkRepository;
        this.warningRepository = warningRepository;
    }

    @Override
    @Transactional
    public Long createChunk(Long taskId, LocalDate chunkStart, LocalDate chunkEnd) {
        // Starting value; SeriesSyncWorker overwrites it with the real HTTP call count (retries and
        // token refreshes included) once the fetch returns.
        JobTaskChunkEntity entity = new JobTaskChunkEntity(taskId, chunkStart, chunkEnd, 1, ChunkStatus.PENDING.name());
        return chunkRepository.save(entity).getId();
    }

    @Override
    @Transactional
    public void markChunkStage(Long chunkId, ChunkStatus stage) {
        if (chunkRepository.updateStatus(chunkId, stage.name()) == 0) {
            throw new IllegalArgumentException("No job_task_chunk found with id " + chunkId);
        }
    }

    @Override
    @Transactional
    public void updateChunkStatus(Long chunkId, ChunkStatus status, int valuesFetched, int valuesNew,
                                   int valuesUpdated, int valuesUnchanged, UUID ledgerPointBatchId) {
        JobTaskChunkEntity entity = chunkRepository.findById(chunkId)
                .orElseThrow(() -> new IllegalArgumentException("No job_task_chunk found with id " + chunkId));
        entity.setStatus(status.name());
        entity.setValuesFetched(valuesFetched);
        entity.setValuesNew(valuesNew);
        entity.setValuesUpdated(valuesUpdated);
        entity.setValuesUnchanged(valuesUnchanged);
        entity.setLedgerPointBatchId(ledgerPointBatchId);
        chunkRepository.save(entity);
    }

    @Override
    @Transactional
    public void updateOrionCallCount(Long chunkId, int orionCallCount) {
        JobTaskChunkEntity entity = chunkRepository.findById(chunkId)
                .orElseThrow(() -> new IllegalArgumentException("No job_task_chunk found with id " + chunkId));
        entity.setOrionCallCount(orionCallCount);
        chunkRepository.save(entity);
    }

    @Override
    @Transactional
    public void recordWarning(Long chunkId, WarningType type, LocalDate intervalDate, String intervalNumber, String detail) {
        warningRepository.save(new TaskWarningEntity(chunkId, type.name(), intervalDate, intervalNumber, detail));
    }
}
