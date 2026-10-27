package com.datasync.domain.service;

import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Change detection (spec &sect;6.7, "diff-engine" per architecture.md &sect;5/&sect;12). Pure,
 * framework-free domain logic - the caller supplies previously-stored values (a persistence
 * concern) as a plain map, so this class has no dependency on any repository port.
 *
 * <p>DECISION (spring-forward fillers, "option B"): fillers are not stored, so they cannot be compared
 * with stored values. Each filler takes the classification of the real interval it was copied from: it
 * is sent when that interval is NEW/UPDATED and skipped when it is UNCHANGED (so a re-run of a
 * spring-forward day sends 0 values, spec AC6).
 */
public final class ChangeDetector {

    private static final BigDecimal EPSILON = new BigDecimal("0.000000001");

    private ChangeDetector() {
    }

    public static List<ClassifiedSeriesValue> classify(List<NormalizedSeriesValue> values,
                                                         Map<SeriesValueKey, BigDecimal> previouslyStored) {
        // First pass: real (Orion-sent) values, keyed by local date + interval token for the fillers.
        Map<String, ChangeType> realByInterval = new HashMap<>();
        for (NormalizedSeriesValue value : values) {
            if (!value.filler()) {
                realByInterval.put(intervalKey(value.localDate(), value.intervalNumber()),
                        classifyReal(value, previouslyStored));
            }
        }

        List<ClassifiedSeriesValue> result = new ArrayList<>(values.size());
        for (NormalizedSeriesValue value : values) {
            ChangeType changeType = value.filler()
                    // A filler whose source is unknown is sent rather than silently dropped.
                    ? realByInterval.getOrDefault(intervalKey(value.localDate(), value.copiedFromInterval()),
                            ChangeType.NEW)
                    : realByInterval.get(intervalKey(value.localDate(), value.intervalNumber()));
            result.add(new ClassifiedSeriesValue(value, changeType));
        }
        return result;
    }

    private static ChangeType classifyReal(NormalizedSeriesValue value,
                                            Map<SeriesValueKey, BigDecimal> previouslyStored) {
        BigDecimal stored = previouslyStored.get(new SeriesValueKey(value.startInstant(), value.repeatedHour()));
        if (stored == null) {
            return ChangeType.NEW;
        }
        return stored.subtract(value.value()).abs().compareTo(EPSILON) > 0 ? ChangeType.UPDATED : ChangeType.UNCHANGED;
    }

    private static String intervalKey(LocalDate date, String intervalNumber) {
        return date + "|" + intervalNumber;
    }
}
