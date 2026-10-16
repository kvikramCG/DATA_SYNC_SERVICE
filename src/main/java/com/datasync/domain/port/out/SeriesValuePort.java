package com.datasync.domain.port.out;

import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Outbound port implemented by the persistence adapter ({@code adapter.out.persistence}) for the
 * {@code series_value} hypertable - the change-detection source of truth (spec &sect;6.7).
 */
public interface SeriesValuePort {

    /**
     * Previously-synced values for one series within a local-date range (typically one month
     * chunk), keyed by {@link SeriesValueKey} - ready to pass straight into
     * {@code ChangeDetector.classify}.
     */
    Map<SeriesValueKey, BigDecimal> findExistingValues(Long seriesId, LocalDate localDateFrom, LocalDate localDateTo);

    /**
     * Upserts values by {@code (seriesId, startInstant, repeatedHour)} - the same key LedgerPoint
     * uses, so re-sending is idempotent (spec &sect;8).
     */
    void upsertAll(Long seriesId, List<NormalizedSeriesValue> values);
}
