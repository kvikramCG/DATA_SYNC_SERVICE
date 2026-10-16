package com.datasync.domain.port.in;

import com.datasync.domain.model.JobSummary;
import com.datasync.domain.model.SeriesType;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound port for {@code POST /api/series/{type}/sync} (spec &sect;7.1).
 *
 * <p><b>Current scope:</b> creates the Job and one PENDING Task per series; it does not yet
 * fetch/transform/diff/publish values (roadmap items 5-9 in session memory) - so the returned
 * job intentionally stays {@code RUNNING} with tasks {@code PENDING} until that pipeline lands.
 */
public interface TriggerSyncUseCase {

    JobSummary triggerSync(SeriesType type, String seriesName, Instant startDate, Instant endDate,
                            UUID correlationId);
}
