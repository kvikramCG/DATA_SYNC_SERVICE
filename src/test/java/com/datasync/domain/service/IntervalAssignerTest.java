package com.datasync.domain.service;

import com.datasync.domain.exception.UnsupportedGranularityException;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntervalAssignerTest {

    @Test
    void dailyGranularityAlwaysAssignsIntervalOne() {
        assertThat(IntervalAssigner.assign(LocalDateTime.of(2026, 1, 1, 13, 45), 1440)).isEqualTo(1);
    }

    @Test
    void hourlyGranularityIsPeriodEnding() {
        assertThat(IntervalAssigner.assign(LocalDateTime.of(2026, 1, 1, 0, 0), 60)).isEqualTo(1);
        assertThat(IntervalAssigner.assign(LocalDateTime.of(2026, 1, 1, 23, 0), 60)).isEqualTo(24);
    }

    @Test
    void quarterHourGranularityProducesNinetySixIntervals() {
        assertThat(IntervalAssigner.assign(LocalDateTime.of(2026, 1, 1, 0, 0), 15)).isEqualTo(1);
        assertThat(IntervalAssigner.assign(LocalDateTime.of(2026, 1, 1, 23, 45), 15)).isEqualTo(96);
    }

    @Test
    void unsupportedGranularityThrows() {
        assertThatThrownBy(() -> IntervalAssigner.assign(LocalDateTime.now(), 30))
                .isInstanceOf(UnsupportedGranularityException.class);
    }
}
