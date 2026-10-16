package com.datasync.domain.model;

/**
 * A {@link NormalizedSeriesValue} tagged with its change classification (spec &sect;6.7). Only
 * {@code NEW} and {@code UPDATED} are sent to LedgerPoint.
 */
public record ClassifiedSeriesValue(NormalizedSeriesValue value, ChangeType changeType) {
}
