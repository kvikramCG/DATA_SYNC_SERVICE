package com.datasync.domain.service;

import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.RawSeriesValue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * DST-aware interval numbering (spec &sect;6.3/&sect;6.4, "dst-engine" per architecture.md
 * &sect;5/&sect;13). Pure, framework-free domain logic - uses only {@code java.time}, whose
 * {@link java.time.zone.ZoneRules} already encode each IANA zone's real DST transition dates, so
 * gap/overlap detection does not need to hardcode "2 AM" or any particular calendar rule.
 *
 * <p><b>Fall-back (25-hour day):</b> Orion sends the repeated hour explicitly flagged
 * {@code repeatedHour=true} (spec &sect;4.3) - this service only needs to append the {@code x}
 * suffix (spec &sect;6.4) and resolve the correct (later) UTC offset for it.
 *
 * <p><b>Spring-forward (23-hour day):</b> Orion sends no value for the skipped local hour - this
 * service detects the gap via {@link java.time.zone.ZoneRules#getTransition} and fills it by
 * copying the immediately preceding interval's value (spec &sect;6.4), never emitting an
 * {@code x} suffix for it.
 *
 * <p>DECISION ("option B"): those copied rows are flagged {@code filler} (see {@link NormalizedSeriesValue})
 * and are never stored in {@code series_value}, because their instant equals the next real interval's
 * primary key. They are re-derived on every run; {@code ChangeDetector} decides whether to send them.
 */
public final class DstAdjustmentService {

    private DstAdjustmentService() {
    }

    public static List<NormalizedSeriesValue> normalize(List<RawSeriesValue> rawValues, ZoneId zoneId, int granularity) {
        IntervalAssigner.validate(granularity);

        Map<LocalDate, List<RawSeriesValue>> byDate = rawValues.stream()
                .collect(Collectors.groupingBy(v -> v.startDate().toLocalDate(), TreeMap::new, Collectors.toList()));

        List<NormalizedSeriesValue> result = new ArrayList<>();
        for (Map.Entry<LocalDate, List<RawSeriesValue>> entry : byDate.entrySet()) {
            result.addAll(normalizeDate(entry.getKey(), entry.getValue(), zoneId, granularity));
        }
        return result;
    }

    private static List<NormalizedSeriesValue> normalizeDate(LocalDate date, List<RawSeriesValue> dayValues,
                                                               ZoneId zoneId, int granularity) {
        SortedMap<Integer, NormalizedSeriesValue> baseByInterval = new TreeMap<>();
        List<NormalizedSeriesValue> repeatedEntries = new ArrayList<>();

        for (RawSeriesValue raw : dayValues) {
            int interval = IntervalAssigner.assign(raw.startDate(), granularity);
            Instant instant = toInstant(raw.startDate(), raw.repeatedHour(), zoneId);
            String token = raw.repeatedHour() ? interval + "x" : String.valueOf(interval);
            NormalizedSeriesValue normalized = new NormalizedSeriesValue(date, token, instant, raw.repeatedHour(), raw.value());
            if (raw.repeatedHour()) {
                repeatedEntries.add(normalized);
            } else {
                baseByInterval.put(interval, normalized);
            }
        }

        fillSpringForwardGap(date, zoneId, granularity, baseByInterval);

        List<NormalizedSeriesValue> dateResult = new ArrayList<>(baseByInterval.values());
        dateResult.addAll(repeatedEntries);
        dateResult.sort(Comparator.comparing(NormalizedSeriesValue::startInstant));
        return dateResult;
    }

    private static void fillSpringForwardGap(LocalDate date, ZoneId zoneId, int granularity,
                                              SortedMap<Integer, NormalizedSeriesValue> baseByInterval) {
        if (granularity == 1440) {
            return; // daily granularity has no sub-day gap concept
        }
        int totalMinutesInDay = 24 * 60;
        for (int minuteOfDay = 0; minuteOfDay < totalMinutesInDay; minuteOfDay += granularity) {
            LocalDateTime candidate = date.atStartOfDay().plusMinutes(minuteOfDay);
            ZoneOffsetTransition transition = zoneId.getRules().getTransition(candidate);
            if (transition == null || !transition.isGap()) {
                continue;
            }
            int missingInterval = IntervalAssigner.assign(candidate, granularity);
            if (baseByInterval.containsKey(missingInterval)) {
                continue;
            }
            NormalizedSeriesValue previous = baseByInterval.get(missingInterval - 1);
            if (previous == null && !baseByInterval.headMap(missingInterval).isEmpty()) {
                previous = baseByInterval.get(baseByInterval.headMap(missingInterval).lastKey());
            }
            if (previous != null) {
                Instant instant = candidate.atZone(zoneId).toInstant();
                // With several skipped intervals (15-minute data) the previous one may itself be a filler;
                // trace back to the real interval so all of them depend on the same source.
                String source = previous.filler() ? previous.copiedFromInterval() : previous.intervalNumber();
                baseByInterval.put(missingInterval,
                        new NormalizedSeriesValue(date, String.valueOf(missingInterval), instant, false,
                                previous.value(), true, source));
            }
            // If there is no preceding interval at all (gap at the very start of the day), it is
            // left unfilled - a later slice's gap-warning logic (spec &sect;6.8) handles that.
        }
    }

    private static Instant toInstant(LocalDateTime localDateTime, boolean repeatedHour, ZoneId zoneId) {
        ZoneOffsetTransition transition = zoneId.getRules().getTransition(localDateTime);
        if (transition != null && transition.isOverlap()) {
            ZoneOffset offset = repeatedHour ? transition.getOffsetAfter() : transition.getOffsetBefore();
            return localDateTime.toInstant(offset);
        }
        return localDateTime.atZone(zoneId).toInstant();
    }
}
