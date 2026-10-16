package com.datasync.domain.service;

import com.datasync.domain.exception.UnsupportedGranularityException;

import java.time.LocalDateTime;

/**
 * Interval assignment (spec &sect;6.3, "interval-engine" per architecture.md &sect;5). Pure,
 * framework-free domain logic.
 */
public final class IntervalAssigner {

    private IntervalAssigner() {
    }

    /**
     * @throws UnsupportedGranularityException if {@code granularity} is not one of 15, 60, 1440.
     */
    public static int assign(LocalDateTime localDateTime, int granularity) {
        return switch (granularity) {
            case 1440 -> 1;
            case 60 -> localDateTime.getHour() + 1;
            case 15 -> localDateTime.getHour() * 4 + localDateTime.getMinute() / 15 + 1;
            default -> throw new UnsupportedGranularityException("Unsupported granularity: " + granularity);
        };
    }

    /**
     * @throws UnsupportedGranularityException if {@code granularity} is not one of 15, 60, 1440.
     */
    public static void validate(int granularity) {
        assign(LocalDateTime.MIN, granularity);
    }
}
