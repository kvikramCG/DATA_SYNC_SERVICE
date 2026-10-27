package com.datasync.domain.service;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class SyncWindowRulesTest {

    private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");

    @Test
    void defaultStartIsOneYearBeforeToday() {
        assertThat(SyncWindowRules.defaultStart(NOW)).isEqualTo(LocalDate.of(2025, 10, 14));
    }

    @Test
    void aNeverSyncedSeriesMayAdvanceOnlyWhenTheWindowStartsAtOrBeforeTheDefaultStart() {
        assertThat(SyncWindowRules.mayAdvanceWatermark(LocalDate.of(2025, 10, 14), null, NOW)).isTrue();
        assertThat(SyncWindowRules.mayAdvanceWatermark(LocalDate.of(2025, 1, 1), null, NOW)).isTrue();
        assertThat(SyncWindowRules.mayAdvanceWatermark(LocalDate.of(2025, 10, 15), null, NOW)).isFalse();
    }

    @Test
    void aSyncedSeriesMayAdvanceOnlyWhenTheWindowStartsAtOrBeforeTheWatermark() {
        LocalDate watermark = LocalDate.of(2026, 10, 1);

        assertThat(SyncWindowRules.mayAdvanceWatermark(watermark, watermark, NOW)).isTrue();
        assertThat(SyncWindowRules.mayAdvanceWatermark(LocalDate.of(2026, 9, 1), watermark, NOW)).isTrue();
        assertThat(SyncWindowRules.mayAdvanceWatermark(LocalDate.of(2026, 10, 2), watermark, NOW)).isFalse();
    }
}
