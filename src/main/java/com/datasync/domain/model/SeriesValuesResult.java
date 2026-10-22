package com.datasync.domain.model;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Orion's values response for one series/month-chunk (spec &sect;4.3), mapped to domain types.
 *
 * <p>{@code orionCallCount} is the number of HTTP calls the source adapter actually made to obtain
 * this result (1 on a clean fetch; more after a retry or a token refresh).
 */
public record SeriesValuesResult(
        String seriesName,
        String unit,
        int granularity,
        TimeZoneCode timeZone,
        LocalDateTime asOfDate,
        List<RawSeriesValue> values,
        int orionCallCount
) {

    /** Convenience constructor for a result obtained with a single call. */
    public SeriesValuesResult(String seriesName, String unit, int granularity, TimeZoneCode timeZone,
                               LocalDateTime asOfDate, List<RawSeriesValue> values) {
        this(seriesName, unit, granularity, timeZone, asOfDate, values, 1);
    }

    public SeriesValuesResult withOrionCallCount(int count) {
        return new SeriesValuesResult(seriesName, unit, granularity, timeZone, asOfDate, values, count);
    }
}
