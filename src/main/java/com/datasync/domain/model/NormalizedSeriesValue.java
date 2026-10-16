package com.datasync.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/**
 * A value ready to be diffed/published to LedgerPoint: local date + interval token already
 * computed (spec &sect;6.3/&sect;6.4), plus the UTC instant + repeated-hour flag that make up the
 * change-detection key (spec &sect;6.7, matches the {@code series_value} primary key).
 */
public record NormalizedSeriesValue(
        LocalDate localDate,
        String intervalNumber,
        Instant startInstant,
        boolean repeatedHour,
        BigDecimal value
) {
}
