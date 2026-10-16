package com.datasync.domain.service;

import com.datasync.domain.model.DateRange;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MonthChunkerTest {

    @Test
    void normalizesEndToLastDayOfMonth() {
        assertThat(MonthChunker.normalizeEndToLastDayOfMonth(LocalDate.of(2026, 2, 5)))
                .isEqualTo(LocalDate.of(2026, 2, 28));
    }

    @Test
    void threeMonthWindowProducesExactlyThreeChunks() {
        // Spec acceptance criterion #2: a window spanning 3 months results in exactly 3 Orion calls.
        List<DateRange> chunks = MonthChunker.chunkByMonth(
                LocalDate.of(2026, 1, 15), LocalDate.of(2026, 3, 31));

        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0)).isEqualTo(new DateRange(LocalDate.of(2026, 1, 15), LocalDate.of(2026, 1, 31)));
        assertThat(chunks.get(1)).isEqualTo(new DateRange(LocalDate.of(2026, 2, 1), LocalDate.of(2026, 2, 28)));
        assertThat(chunks.get(2)).isEqualTo(new DateRange(LocalDate.of(2026, 3, 1), LocalDate.of(2026, 3, 31)));
    }

    @Test
    void singleDayWindowProducesOneChunk() {
        List<DateRange> chunks = MonthChunker.chunkByMonth(LocalDate.of(2026, 5, 10), LocalDate.of(2026, 5, 10));

        assertThat(chunks).containsExactly(new DateRange(LocalDate.of(2026, 5, 10), LocalDate.of(2026, 5, 10)));
    }

    @Test
    void startAfterEndThrows() {
        assertThatThrownBy(() -> MonthChunker.chunkByMonth(LocalDate.of(2026, 5, 10), LocalDate.of(2026, 5, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
