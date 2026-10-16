package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "job_task_chunk")
public class JobTaskChunkEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_task_id", nullable = false)
    private Long jobTaskId;

    @Column(name = "chunk_start_date", nullable = false)
    private LocalDate chunkStartDate;

    @Column(name = "chunk_end_date", nullable = false)
    private LocalDate chunkEndDate;

    @Column(name = "orion_call_count", nullable = false)
    private int orionCallCount;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(name = "values_fetched", nullable = false)
    private int valuesFetched;

    @Column(name = "values_new", nullable = false)
    private int valuesNew;

    @Column(name = "values_updated", nullable = false)
    private int valuesUpdated;

    @Column(name = "values_unchanged", nullable = false)
    private int valuesUnchanged;

    @Column(name = "ledgerpoint_batch_id")
    private UUID ledgerPointBatchId;

    protected JobTaskChunkEntity() {
        // JPA
    }

    public JobTaskChunkEntity(Long jobTaskId, LocalDate chunkStartDate, LocalDate chunkEndDate,
                               int orionCallCount, String status) {
        this.jobTaskId = jobTaskId;
        this.chunkStartDate = chunkStartDate;
        this.chunkEndDate = chunkEndDate;
        this.orionCallCount = orionCallCount;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public Long getJobTaskId() {
        return jobTaskId;
    }

    public LocalDate getChunkStartDate() {
        return chunkStartDate;
    }

    public LocalDate getChunkEndDate() {
        return chunkEndDate;
    }

    public int getOrionCallCount() {
        return orionCallCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getValuesFetched() {
        return valuesFetched;
    }

    public void setValuesFetched(int valuesFetched) {
        this.valuesFetched = valuesFetched;
    }

    public int getValuesNew() {
        return valuesNew;
    }

    public void setValuesNew(int valuesNew) {
        this.valuesNew = valuesNew;
    }

    public int getValuesUpdated() {
        return valuesUpdated;
    }

    public void setValuesUpdated(int valuesUpdated) {
        this.valuesUpdated = valuesUpdated;
    }

    public int getValuesUnchanged() {
        return valuesUnchanged;
    }

    public void setValuesUnchanged(int valuesUnchanged) {
        this.valuesUnchanged = valuesUnchanged;
    }

    public UUID getLedgerPointBatchId() {
        return ledgerPointBatchId;
    }

    public void setLedgerPointBatchId(UUID ledgerPointBatchId) {
        this.ledgerPointBatchId = ledgerPointBatchId;
    }
}
