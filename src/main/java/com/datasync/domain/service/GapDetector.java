package com.datasync.domain.service;

import com.datasync.domain.model.MissingInterval;
import com.datasync.domain.model.NormalizedSeriesValue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects genuine data gaps (spec &sect;6.8): "if an interval (other than the spring-forward
 * hour) is missing, do not invent data - log a warning". Runs <em>after</em>
 * {@link DstAdjustmentService#normalize}, so the spring-forward hour has already been filled and
 * never shows up here as missing - only Orion's own data gaps do.
 *
 * <p>Repeated-hour ({@code x}-suffixed) entries are intentionally excluded from the expected set:
 * the base interval set {@code 1..N} is what must be complete for every date in the window,
 * regardless of granularity.
 */
public final class GapDetector {

    private GapDetector() {
    }

    public static List<MissingInterval> findMissingIntervals(LocalDate chunkStart, LocalDate chunkEnd,
                                                               List<NormalizedSeriesValue> values, int granularity) {
        IntervalAssigner.validate(granularity);
        int expectedCount = expectedIntervalsPerDay(granularity);

        Map<LocalDate, Set<String>> presentBaseIntervalsByDate = values.stream()
                .filter(value -> !value.repeatedHour())
                .collect(Collectors.groupingBy(NormalizedSeriesValue::localDate,
                        Collectors.mapping(NormalizedSeriesValue::intervalNumber, Collectors.toSet())));

        List<MissingInterval> missing = new ArrayList<>();
        for (LocalDate date = chunkStart; !date.isAfter(chunkEnd); date = date.plusDays(1)) {
            Set<String> present = presentBaseIntervalsByDate.getOrDefault(date, Set.of());
            for (int interval = 1; interval <= expectedCount; interval++) {
                String token = String.valueOf(interval);
                if (!present.contains(token)) {
                    missing.add(new MissingInterval(date, token));
                }
            }
        }
        return missing;
    }

    private static int expectedIntervalsPerDay(int granularity) {
        return switch (granularity) {
            case 1440 -> 1;
            case 60 -> 24;
            case 15 -> 96;
            default -> throw new IllegalStateException("Unreachable - IntervalAssigner.validate already checked this");
        };
    }
}
