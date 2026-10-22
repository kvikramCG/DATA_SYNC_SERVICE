package com.datasync.domain.service;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class StalenessCheckerTest {

    private static final ZoneId CHICAGO = ZoneId.of("America/Chicago");
    // 2026-10-12 18:00 Chicago (CDT, UTC-5) = 2026-10-12T23:00Z
    private static final LocalDateTime LAST_UPDATED = LocalDateTime.of(2026, 10, 12, 18, 0);

    @Test
    void notStaleWhenWithinTheWindow() {
        assertThat(StalenessChecker.isStale(LAST_UPDATED, CHICAGO, Instant.parse("2026-10-14T12:00:00Z"), 5))
                .isFalse();
    }

    @Test
    void notStaleExactlyAtTheBoundary() {
        assertThat(StalenessChecker.isStale(LAST_UPDATED, CHICAGO, Instant.parse("2026-10-17T23:00:00Z"), 5))
                .isFalse();
    }

    @Test
    void staleOneSecondPastTheBoundary() {
        assertThat(StalenessChecker.isStale(LAST_UPDATED, CHICAGO, Instant.parse("2026-10-17T23:00:01Z"), 5))
                .isTrue();
    }

    @Test
    void unknownLastUpdateIsNeverStale() {
        assertThat(StalenessChecker.isStale(null, CHICAGO, Instant.parse("2030-01-01T00:00:00Z"), 5)).isFalse();
    }
}
