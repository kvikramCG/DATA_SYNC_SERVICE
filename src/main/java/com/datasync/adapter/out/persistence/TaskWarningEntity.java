package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;

@Entity
@Table(name = "task_warning")
public class TaskWarningEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_task_chunk_id", nullable = false)
    private Long jobTaskChunkId;

    @Column(name = "warning_type", nullable = false, length = 50)
    private String warningType;

    @Column(name = "interval_date")
    private LocalDate intervalDate;

    @Column(name = "interval_number", length = 5)
    private String intervalNumber;

    @Column(length = 500)
    private String detail;

    protected TaskWarningEntity() {
        // JPA
    }

    public TaskWarningEntity(Long jobTaskChunkId, String warningType, LocalDate intervalDate,
                              String intervalNumber, String detail) {
        this.jobTaskChunkId = jobTaskChunkId;
        this.warningType = warningType;
        this.intervalDate = intervalDate;
        this.intervalNumber = intervalNumber;
        this.detail = detail;
    }

    public Long getId() {
        return id;
    }

    public Long getJobTaskChunkId() {
        return jobTaskChunkId;
    }

    public String getWarningType() {
        return warningType;
    }

    public LocalDate getIntervalDate() {
        return intervalDate;
    }

    public String getIntervalNumber() {
        return intervalNumber;
    }

    public String getDetail() {
        return detail;
    }
}
