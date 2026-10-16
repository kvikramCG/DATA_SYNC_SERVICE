package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "job_task")
public class JobTaskEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "series_id", nullable = false)
    private Long seriesId;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(name = "failure_reason", length = 50)
    private String failureReason;

    @Column(name = "failure_detail", length = 2000)
    private String failureDetail;

    @Column(name = "values_sent", nullable = false)
    private int valuesSent;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    protected JobTaskEntity() {
        // JPA
    }

    public JobTaskEntity(Long jobId, Long seriesId, String status) {
        this.jobId = jobId;
        this.seriesId = seriesId;
        this.status = status;
        this.valuesSent = 0;
    }

    public Long getId() {
        return id;
    }

    public Long getJobId() {
        return jobId;
    }

    public Long getSeriesId() {
        return seriesId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public String getFailureDetail() {
        return failureDetail;
    }

    public void setFailureDetail(String failureDetail) {
        this.failureDetail = failureDetail;
    }

    public int getValuesSent() {
        return valuesSent;
    }

    public void setValuesSent(int valuesSent) {
        this.valuesSent = valuesSent;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }
}
