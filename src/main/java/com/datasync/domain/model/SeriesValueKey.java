package com.datasync.domain.model;

import java.time.Instant;

/**
 * The change-detection key from spec &sect;6.7: {@code (series, startInstant, repeatedHour)}.
 * {@code series} itself is implicit - callers diff one series' values at a time.
 */
public record SeriesValueKey(Instant startInstant, boolean repeatedHour) {
}
