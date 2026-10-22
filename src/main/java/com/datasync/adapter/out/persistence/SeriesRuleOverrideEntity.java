package com.datasync.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * JPA mapping of {@code series_rule_override} (spec &sect;6.6, database-design.md). Rows are
 * maintained by operators (SQL / future admin API); the service only reads them.
 */
@Entity
@Table(name = "series_rule_override")
public class SeriesRuleOverrideEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "orion_series_name", nullable = false, unique = true, length = 100)
    private String orionSeriesName;

    @Column(name = "ledgerpoint_key_override", length = 100)
    private String ledgerPointKeyOverride;

    @Column(nullable = false)
    private boolean skip;

    @Column(nullable = false)
    private boolean active = true;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected SeriesRuleOverrideEntity() {
        // JPA
    }

    public SeriesRuleOverrideEntity(String orionSeriesName, String ledgerPointKeyOverride, boolean skip,
                                    boolean active, Instant updatedAt) {
        this.orionSeriesName = orionSeriesName;
        this.ledgerPointKeyOverride = ledgerPointKeyOverride;
        this.skip = skip;
        this.active = active;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public String getOrionSeriesName() {
        return orionSeriesName;
    }

    public String getLedgerPointKeyOverride() {
        return ledgerPointKeyOverride;
    }

    public boolean isSkip() {
        return skip;
    }

    public boolean isActive() {
        return active;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
