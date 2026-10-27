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

    @Test
    void aFillerIsUnchangedWhenItsSourceIntervalIsUnchanged() {
        List<NormalizedSeriesValue> day = springForwardDay("22.10", "23.40");
        Map<SeriesValueKey, BigDecimal> stored = Map.of(
                new SeriesValueKey(day.get(0).startInstant(), false), new BigDecimal("22.10"),
                new SeriesValueKey(day.get(2).startInstant(), false), new BigDecimal("23.40"));

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(day, stored);

        assertThat(result).extracting(ClassifiedSeriesValue::changeType)
                .containsOnly(ChangeType.UNCHANGED);
    }

    @Test
    void aFillerIsUpdatedAlongWithItsSourceInterval() {
        List<NormalizedSeriesValue> day = springForwardDay("25.00", "23.40");
        Map<SeriesValueKey, BigDecimal> stored = Map.of(
                new SeriesValueKey(day.get(0).startInstant(), false), new BigDecimal("22.10"),
                new SeriesValueKey(day.get(2).startInstant(), false), new BigDecimal("23.40"));

        List<ClassifiedSeriesValue> result = ChangeDetector.classify(day, stored);

        assertThat(result).extracting(ClassifiedSeriesValue::changeType)
                .containsExactly(ChangeType.UPDATED, ChangeType.UPDATED, ChangeType.UNCHANGED);
    }

    @Test
    void aFillerIsNewWhenItsSourceIntervalIsNew() {
        List<ClassifiedSeriesValue> result = ChangeDetector.classify(springForwardDay("22.10", "23.40"), Map.of());

        assertThat(result).extracting(ClassifiedSeriesValue::changeType).containsOnly(ChangeType.NEW);
    }

    // Interval 2 (real, 01:00), interval 3 (filler for the skipped 02:00), interval 4 (real, 03:00 - same instant as the filler).
    private static List<NormalizedSeriesValue> springForwardDay(String beforeGap, String afterGap) {
        LocalDate date = LocalDate.of(2026, 3, 8);
        Instant beforeInstant = Instant.parse("2026-03-08T07:00:00Z");
        Instant gapInstant = Instant.parse("2026-03-08T08:00:00Z");
        return List.of(
                new NormalizedSeriesValue(date, "2", beforeInstant, false, new BigDecimal(beforeGap)),
                new NormalizedSeriesValue(date, "3", gapInstant, false, new BigDecimal(beforeGap), true, "2"),
                new NormalizedSeriesValue(date, "4", gapInstant, false, new BigDecimal(afterGap)));
    }

    private static NormalizedSeriesValue value(String amount) {
        return new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", INSTANT, false, new BigDecimal(amount));
    }
}
