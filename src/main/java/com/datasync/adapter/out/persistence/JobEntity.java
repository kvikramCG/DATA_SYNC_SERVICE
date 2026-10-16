package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "job")
public class JobEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "series_type", nullable = false, length = 20)
    private String seriesType;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(name = "requested_series_name", length = 100)
    private String requestedSeriesName;

    @Column(name = "requested_start_date")
    private LocalDate requestedStartDate;

    @Column(name = "requested_end_date")
    private LocalDate requestedEndDate;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    protected JobEntity() {
        // JPA
    }

    public JobEntity(String seriesType, String status, String requestedSeriesName,
                      LocalDate requestedStartDate, LocalDate requestedEndDate,
                      Instant startedAt, UUID correlationId) {
        this.seriesType = seriesType;
        this.status = status;
        this.requestedSeriesName = requestedSeriesName;
        this.requestedStartDate = requestedStartDate;
        this.requestedEndDate = requestedEndDate;
        this.startedAt = startedAt;
        this.correlationId = correlationId;
    }

    public Long getId() {
        return id;
    }

    public String getSeriesType() {
        return seriesType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRequestedSeriesName() {
        return requestedSeriesName;
    }

    public LocalDate getRequestedStartDate() {
        return requestedStartDate;
    }

    public LocalDate getRequestedEndDate() {
        return requestedEndDate;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }
}
