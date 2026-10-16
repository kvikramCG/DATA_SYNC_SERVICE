package com.datasync.domain.port.out;

import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.WarningType;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Outbound port implemented by the persistence adapter ({@code adapter.out.persistence}) for
 * {@code job_task_chunk} and {@code task_warning} - per-month audit trail and gap warnings
 * (spec &sect;6.8, database-design.md &sect;3.3/&sect;3.4).
 */
public interface ChunkRepositoryPort {

    /**
     * Creates the chunk row before processing starts, returning its id for subsequent updates
     * and warnings.
     */
    Long createChunk(Long taskId, LocalDate chunkStart, LocalDate chunkEnd);

    void updateChunkStatus(Long chunkId, ChunkStatus status, int valuesFetched, int valuesNew,
                           int valuesUpdated, int valuesUnchanged, UUID ledgerPointBatchId);

    void recordWarning(Long chunkId, WarningType type, LocalDate intervalDate, String intervalNumber, String detail);
}
