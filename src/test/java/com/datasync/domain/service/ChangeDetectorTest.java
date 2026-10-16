package com.datasync.domain.service;

import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeDetectorTest {

    private static final Instant INSTANT = Instant.parse("2026-11-01T06:00:00Z");

    @Test
    void classifiesAsNewWhenNoStoredValueExists() {
        NormalizedSeriesValue value = value("10.00");

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(List.of(value), Map.of());

        assertThat(result).hasSize(1);
        assertThat(result.get(0).changeType()).isEqualTo(ChangeType.NEW);
    }

    @Test
    void classifiesAsUpdatedWhenStoredValueDiffersBeyondEpsilon() {
        NormalizedSeriesValue value = value("10.00");
        Map<SeriesValueKey, BigDecimal> stored = Map.of(
                new SeriesValueKey(INSTANT, false), new BigDecimal("9.50"));

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(List.of(value), stored);

        assertThat(result.get(0).changeType()).isEqualTo(ChangeType.UPDATED);
    }

    @Test
    void classifiesAsUnchangedWhenStoredValueIsEqual() {
        NormalizedSeriesValue value = value("10.00");
        Map<SeriesValueKey, BigDecimal> stored = Map.of(
                new SeriesValueKey(INSTANT, false), new BigDecimal("10.00"));

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(List.of(value), stored);

        assertThat(result.get(0).changeType()).isEqualTo(ChangeType.UNCHANGED);
    }

    @Test
    void toleratesDifferencesWithinEpsilonAsUnchanged() {
        NormalizedSeriesValue value = value("10.000000001");
        Map<SeriesValueKey, BigDecimal> stored = Map.of(
                new SeriesValueKey(INSTANT, false), new BigDecimal("10.000000000"));

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(List.of(value), stored);

        assertThat(result.get(0).changeType()).isEqualTo(ChangeType.UNCHANGED);
    }

    private static NormalizedSeriesValue value(String amount) {
        return new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", INSTANT, false, new BigDecimal(amount));
    }
}
