package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.LocalDate;

@Entity
@Table(name = "series")
public class SeriesEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 100)
    private String name;

    @Column(name = "series_type", nullable = false, length = 20)
    private String seriesType;

    @Column(length = 20)
    private String unit;

    @Column(name = "time_zone_code", nullable = false)
    private int timeZoneCode;

    @Column(name = "earliest_data_date")
    private LocalDate earliestDataDate;

    @Column(name = "latest_data_date")
    private LocalDate latestDataDate;

    @Column(name = "last_updated_date")
    private Instant lastUpdatedDate;

    @Column(name = "last_synced_date")
    private LocalDate lastSyncedDate;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected SeriesEntity() {
        // JPA
    }

    public SeriesEntity(String name, String seriesType, int timeZoneCode, Instant createdAt, Instant updatedAt) {
        this.name = name;
        this.seriesType = seriesType;
        this.timeZoneCode = timeZoneCode;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getSeriesType() {
        return seriesType;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public int getTimeZoneCode() {
        return timeZoneCode;
    }

    public LocalDate getEarliestDataDate() {
        return earliestDataDate;
    }

    public void setEarliestDataDate(LocalDate earliestDataDate) {
        this.earliestDataDate = earliestDataDate;
    }

    public LocalDate getLatestDataDate() {
        return latestDataDate;
    }

    public void setLatestDataDate(LocalDate latestDataDate) {
        this.latestDataDate = latestDataDate;
    }

    public Instant getLastUpdatedDate() {
        return lastUpdatedDate;
    }

    public void setLastUpdatedDate(Instant lastUpdatedDate) {
        this.lastUpdatedDate = lastUpdatedDate;
    }

    public LocalDate getLastSyncedDate() {
        return lastSyncedDate;
    }

    public void setLastSyncedDate(LocalDate lastSyncedDate) {
        this.lastSyncedDate = lastSyncedDate;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
