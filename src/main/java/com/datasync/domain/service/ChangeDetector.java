package com.datasync.domain.service;

import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Change detection (spec &sect;6.7, "diff-engine" per architecture.md &sect;5/&sect;12). Pure,
 * framework-free domain logic - the caller supplies previously-stored values (a persistence
 * concern) as a plain map, so this class has no dependency on any repository port.
 */
public final class ChangeDetector {

    private static final BigDecimal EPSILON = new BigDecimal("0.000000001");

    private ChangeDetector() {
    }

    public static List<ClassifiedSeriesValue> classify(List<NormalizedSeriesValue> values,
                                                         Map<SeriesValueKey, BigDecimal> previouslyStored) {
        List<ClassifiedSeriesValue> result = new ArrayList<>(values.size());
        for (NormalizedSeriesValue value : values) {
            SeriesValueKey key = new SeriesValueKey(value.startInstant(), value.repeatedHour());
            BigDecimal stored = previouslyStored.get(key);
            ChangeType changeType;
            if (stored == null) {
                changeType = ChangeType.NEW;
            } else if (stored.subtract(value.value()).abs().compareTo(EPSILON) > 0) {
                changeType = ChangeType.UPDATED;
            } else {
                changeType = ChangeType.UNCHANGED;
            }
            result.add(new ClassifiedSeriesValue(value, changeType));
        }
        return result;
    }
}
