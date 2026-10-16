package com.datasync.domain.model;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Orion's values response for one series/month-chunk (spec &sect;4.3), mapped to domain types.
 */
public record SeriesValuesResult(
        String seriesName,
        String unit,
        int granularity,
        TimeZoneCode timeZone,
        LocalDateTime asOfDate,
        List<RawSeriesValue> values
) {
}
