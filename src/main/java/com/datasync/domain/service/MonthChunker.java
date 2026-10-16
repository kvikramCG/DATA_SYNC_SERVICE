package com.datasync.domain.service;

import com.datasync.domain.model.DateRange;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a sync window into calendar-month chunks (spec &sect;3.1 steps 1-2): "Call Orion once
 * per calendar month in the window (keeps payloads small and failures isolated)". Pure,
 * framework-free domain logic.
 */
public final class MonthChunker {

    private MonthChunker() {
    }

    /**
     * Spec &sect;3.1: "Normalize {@code end} to the last day of its month."
     */
    public static LocalDate normalizeEndToLastDayOfMonth(LocalDate end) {
        return end.withDayOfMonth(end.lengthOfMonth());
    }

    /**
     * @throws IllegalArgumentException if {@code start} is after {@code end}.
     */
    public static List<DateRange> chunkByMonth(LocalDate start, LocalDate end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("start date " + start + " is after end date " + end);
        }
        List<DateRange> chunks = new ArrayList<>();
        LocalDate chunkStart = start;
        while (!chunkStart.isAfter(end)) {
            LocalDate monthEnd = chunkStart.withDayOfMonth(chunkStart.lengthOfMonth());
            LocalDate chunkEnd = monthEnd.isAfter(end) ? end : monthEnd;
            chunks.add(new DateRange(chunkStart, chunkEnd));
            chunkStart = chunkEnd.plusDays(1);
        }
        return chunks;
    }
}
