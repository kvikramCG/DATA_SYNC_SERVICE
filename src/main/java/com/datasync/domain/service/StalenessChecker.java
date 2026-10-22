package com.datasync.domain.service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Spec &sect;6.9: a series whose Orion {@code lastUpdatedDate} is older than N days is stale and
 * must not be synced (it fails with {@code STALE_SERIES}).
 */
public final class StalenessChecker {

    private StalenessChecker() {
    }

    /**
     * @param lastUpdated series last-update timestamp in the series' own time zone; {@code null}
     *                    means "unknown" and is never treated as stale
     * @return {@code true} when {@code lastUpdated} lies strictly before {@code now - staleAfterDays}
     */
    public static boolean isStale(LocalDateTime lastUpdated, ZoneId seriesZone, Instant now, int staleAfterDays) {
        if (lastUpdated == null) {
            return false;
        }
        Instant cutoff = now.minus(Duration.ofDays(staleAfterDays));
        return lastUpdated.atZone(seriesZone).toInstant().isBefore(cutoff);
    }
}
