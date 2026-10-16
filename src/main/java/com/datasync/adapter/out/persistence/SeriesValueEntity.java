package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Entity
@Table(name = "series_value")
@IdClass(SeriesValueId.class)
public class SeriesValueEntity {

    @Id
    @Column(name = "series_id")
    private Long seriesId;

    @Id
    @Column(name = "start_instant")
    private Instant startInstant;

    @Id
    @Column(name = "repeated_hour")
    private boolean repeatedHour;

    @Column(nullable = false, precision = 19, scale = 6)
    private BigDecimal value;

    @Column(name = "interval_number", nullable = false, length = 5)
    private String intervalNumber;

    @Column(name = "local_date", nullable = false)
    private LocalDate localDate;

    @Column(name = "synced_at", nullable = false)
    private Instant syncedAt;

    protected SeriesValueEntity() {
        // JPA
    }

    public SeriesValueEntity(Long seriesId, Instant startInstant, boolean repeatedHour, BigDecimal value,
                              String intervalNumber, LocalDate localDate, Instant syncedAt) {
        this.seriesId = seriesId;
        this.startInstant = startInstant;
        this.repeatedHour = repeatedHour;
        this.value = value;
        this.intervalNumber = intervalNumber;
        this.localDate = localDate;
        this.syncedAt = syncedAt;
    }

    public Long getSeriesId() {
        return seriesId;
    }

    public Instant getStartInstant() {
        return startInstant;
    }

    public boolean isRepeatedHour() {
        return repeatedHour;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public String getIntervalNumber() {
        return intervalNumber;
    }

    public void setIntervalNumber(String intervalNumber) {
        this.intervalNumber = intervalNumber;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }

    public Instant getSyncedAt() {
        return syncedAt;
    }

    public void setSyncedAt(Instant syncedAt) {
        this.syncedAt = syncedAt;
    }
}
