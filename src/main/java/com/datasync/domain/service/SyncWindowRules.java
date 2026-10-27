package com.datasync.domain.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Rules about the sync window and the per-series "last synced date" watermark (spec &sect;3.1). Pure domain
 * logic shared by window resolution and the worker, so both agree on the one-year default.
 */
public final class SyncWindowRules {

    private SyncWindowRules() {
    }

    /** Spec &sect;3.1: a series that was never synced starts 1 year prior to today. */
    public static LocalDate defaultStart(Instant now) {
        return now.atZone(ZoneOffset.UTC).toLocalDate().minusYears(1);
    }

    /**
     * Whether a successful run may move the watermark forward. Only when the run's window is contiguous
     * with what is already synced: it must start on or before the watermark - or, for a never-synced
     * series, on or before the default start. Otherwise the dates in between were never synced, and moving
     * the watermark would make every later default run skip them.
     */
    public static boolean mayAdvanceWatermark(LocalDate windowStart, LocalDate lastSyncedDate, Instant now) {
        LocalDate baseline = lastSyncedDate != null ? lastSyncedDate : defaultStart(now);
        return !windowStart.isAfter(baseline);
    }
}
