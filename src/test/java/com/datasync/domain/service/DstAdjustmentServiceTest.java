package com.datasync.domain.service;

import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.RawSeriesValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the exact fixtures from spec &sect;12 (America/Chicago = Orion {@code TZ_C}).
 */
class DstAdjustmentServiceTest {

    private static final ZoneId CHICAGO = ZoneId.of("America/Chicago");

    @Test
    void springForwardDayFillsTheSkippedHourFromThePreviousIntervalWithNoXSuffix() {
        // 2026-03-08: US DST begins -> 1:00-1:59 AM exists, 2:00-2:59 AM does not.
        List<RawSeriesValue> raw = List.of(
                new RawSeriesValue(LocalDateTime.of(2026, 3, 8, 1, 0), false, new BigDecimal("22.10")),
                new RawSeriesValue(LocalDateTime.of(2026, 3, 8, 3, 0), false, new BigDecimal("23.40")));

        List<NormalizedSeriesValue> result = DstAdjustmentService.normalize(raw, CHICAGO, 60);

        assertThat(result).hasSize(3);
        assertThat(result).extracting(NormalizedSeriesValue::intervalNumber)
                .containsExactly("2", "3", "4");
        assertThat(result).extracting(NormalizedSeriesValue::value)
                .containsExactly(new BigDecimal("22.10"), new BigDecimal("22.10"), new BigDecimal("23.40"));
        assertThat(result).extracting(NormalizedSeriesValue::repeatedHour).containsOnly(false);
        // Only the derived row is a filler, and it records which real interval it was copied from.
        assertThat(result).extracting(NormalizedSeriesValue::filler).containsExactly(false, true, false);
        assertThat(result.get(1).copiedFromInterval()).isEqualTo("2");
    }

    @Test
    void everyFillerOfAFifteenMinuteSpringForwardGapPointsAtTheLastRealInterval() {
        List<RawSeriesValue> raw = List.of(
                new RawSeriesValue(LocalDateTime.of(2026, 3, 8, 1, 45), false, new BigDecimal("8")),
                new RawSeriesValue(LocalDateTime.of(2026, 3, 8, 3, 0), false, new BigDecimal("13")));

        List<NormalizedSeriesValue> result = DstAdjustmentService.normalize(raw, CHICAGO, 15);

        // intervals 8 (real), 9-12 (skipped 02:00-02:59), 13 (real); the list is ordered by instant, so
        // fillers interleave with the real rows that share their instants.
        assertThat(result).extracting(NormalizedSeriesValue::intervalNumber)
                .containsExactlyInAnyOrder("8", "9", "10", "11", "12", "13");
        assertThat(result.stream().filter(NormalizedSeriesValue::filler))
                .hasSize(4)
                .allSatisfy(filler -> {
                    assertThat(filler.copiedFromInterval()).isEqualTo("8");
                    assertThat(filler.value()).isEqualByComparingTo("8");
                });
    }

    @Test
    void fallBackDayAppendsXSuffixToTheRepeatedIntervalOnly() {
        // 2026-11-01: US DST ends -> 1:00-1:59 AM occurs twice.
        List<RawSeriesValue> raw = List.of(
                new RawSeriesValue(LocalDateTime.of(2026, 11, 1, 1, 0), false, new BigDecimal("29.80")),
                new RawSeriesValue(LocalDateTime.of(2026, 11, 1, 1, 0), true, new BigDecimal("29.10")));

        List<NormalizedSeriesValue> result = DstAdjustmentService.normalize(raw, CHICAGO, 60);

        assertThat(result).hasSize(2);
        NormalizedSeriesValue first = result.get(0);
        NormalizedSeriesValue second = result.get(1);

        assertThat(first.intervalNumber()).isEqualTo("2");
        assertThat(first.repeatedHour()).isFalse();
        assertThat(first.value()).isEqualByComparingTo("29.80");

        assertThat(second.intervalNumber()).isEqualTo("2x");
        assertThat(second.repeatedHour()).isTrue();
        assertThat(second.value()).isEqualByComparingTo("29.10");

        // the repeated (later, standard-time) occurrence must be a later UTC instant
        assertThat(second.startInstant()).isAfter(first.startInstant());
    }

    @Test
    void normalDayAssignsIntervalsWithoutAnyAdjustment() {
        List<RawSeriesValue> raw = List.of(
                new RawSeriesValue(LocalDateTime.of(2026, 6, 15, 0, 0), false, new BigDecimal("10.0")),
                new RawSeriesValue(LocalDateTime.of(2026, 6, 15, 23, 0), false, new BigDecimal("20.0")));

        List<NormalizedSeriesValue> result = DstAdjustmentService.normalize(raw, CHICAGO, 60);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(NormalizedSeriesValue::intervalNumber).containsExactly("1", "24");
    }

    @Test
    void dailyGranularityIsAlwaysIntervalOneAndUnaffectedByDst() {
        List<RawSeriesValue> raw = List.of(
                new RawSeriesValue(LocalDateTime.of(2026, 3, 8, 0, 0), false, new BigDecimal("5.0")));

        List<NormalizedSeriesValue> result = DstAdjustmentService.normalize(raw, CHICAGO, 1440);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).intervalNumber()).isEqualTo("1");
    }
}
