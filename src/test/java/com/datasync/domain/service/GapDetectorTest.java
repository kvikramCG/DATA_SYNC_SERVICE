package com.datasync.domain.service;

import com.datasync.domain.model.MissingInterval;
import com.datasync.domain.model.NormalizedSeriesValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GapDetectorTest {

    @Test
    void reportsNoGapsWhenEveryIntervalIsPresent() {
        LocalDate date = LocalDate.of(2026, 6, 1);
        List<NormalizedSeriesValue> values = List.of(
                new NormalizedSeriesValue(date, "1", Instant.parse("2026-06-01T05:00:00Z"), false, BigDecimal.ONE),
                new NormalizedSeriesValue(date, "2", Instant.parse("2026-06-01T06:00:00Z"), false, BigDecimal.ONE));

        // Using daily granularity (expects 1 interval) keeps this fixture minimal but still valid.
        List<MissingInterval> gaps = GapDetector.findMissingIntervals(date, date, values, 1440);

        assertThat(gaps).isEmpty();
    }

    @Test
    void reportsEveryMissingIntervalForAPartiallyPresentDay() {
        LocalDate date = LocalDate.of(2026, 6, 1);
        // Hourly (24 intervals expected) but only interval "1" is present.
        List<NormalizedSeriesValue> values = List.of(
                new NormalizedSeriesValue(date, "1", Instant.parse("2026-06-01T05:00:00Z"), false, BigDecimal.ONE));

        List<MissingInterval> gaps = GapDetector.findMissingIntervals(date, date, values, 60);

        assertThat(gaps).hasSize(23);
        assertThat(gaps).extracting(MissingInterval::intervalNumber).doesNotContain("1");
        assertThat(gaps).allMatch(gap -> gap.date().equals(date));
    }

    @Test
    void reportsAWholeMissingDateWithinTheChunk() {
        LocalDate dayWithData = LocalDate.of(2026, 6, 1);
        LocalDate dayWithoutData = LocalDate.of(2026, 6, 2);
        List<NormalizedSeriesValue> values = List.of(
                new NormalizedSeriesValue(dayWithData, "1", Instant.parse("2026-06-01T05:00:00Z"), false, BigDecimal.ONE));

        List<MissingInterval> gaps = GapDetector.findMissingIntervals(dayWithData, dayWithoutData, values, 1440);

        assertThat(gaps).containsExactly(new MissingInterval(dayWithoutData, "1"));
    }

    @Test
    void repeatedHourEntriesDoNotCountTowardsTheExpectedBaseSet() {
        LocalDate date = LocalDate.of(2026, 11, 1);
        List<NormalizedSeriesValue> values = List.of(
                new NormalizedSeriesValue(date, "2", Instant.parse("2026-11-01T07:00:00Z"), false, BigDecimal.ONE),
                new NormalizedSeriesValue(date, "2x", Instant.parse("2026-11-01T08:00:00Z"), true, BigDecimal.ONE));

        // Only interval "2" (base) is required; "2x" doesn't satisfy some other missing base slot.
        List<MissingInterval> gaps = GapDetector.findMissingIntervals(date, date, values, 1440);

        assertThat(gaps).isEmpty();
    }
}
