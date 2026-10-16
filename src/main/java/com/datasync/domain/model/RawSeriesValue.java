package com.datasync.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * One value as returned by Orion's values call (spec &sect;4.3), before interval assignment or
 * DST adjustment - prevailing local time, no timezone applied yet.
 */
public record RawSeriesValue(
        LocalDateTime startDate,
        boolean repeatedHour,
        BigDecimal value
) {
}
