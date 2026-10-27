package com.datasync.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/**
 * A value ready to be diffed/published to LedgerPoint: local date + interval token already
 * computed (spec &sect;6.3/&sect;6.4), plus the UTC instant + repeated-hour flag that make up the
 * change-detection key (spec &sect;6.7, matches the {@code series_value} primary key).
 *
 * <p>DECISION (spring-forward fillers, "option B"): a value Orion did not send but the service
 * derived for the skipped hour (spec &sect;6.4) has {@code filler = true} and names the real interval it
 * was copied from in {@code copiedFromInterval}. A filler is NEVER stored in {@code series_value}: its
 * instant equals the next real interval's instant, so it would collide with that row's primary key.
 * It is re-derived on every run and sent to LedgerPoint only when the interval it copies is sent.
 */
public record NormalizedSeriesValue(
        LocalDate localDate,
        String intervalNumber,
        Instant startInstant,
        boolean repeatedHour,
        BigDecimal value,
        boolean filler,
        String copiedFromInterval
) {

    /** A value received from Orion (not derived). */
    public NormalizedSeriesValue(LocalDate localDate, String intervalNumber, Instant startInstant,
                                  boolean repeatedHour, BigDecimal value) {
        this(localDate, intervalNumber, startInstant, repeatedHour, value, false, null);
    }
}
