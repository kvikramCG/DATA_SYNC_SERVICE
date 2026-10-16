package com.datasync.domain.model;

import java.time.LocalDate;

/**
 * An inclusive {@code [start, end]} calendar-date window, as produced by
 * {@code com.datasync.domain.service.MonthChunker}.
 */
public record DateRange(LocalDate start, LocalDate end) {
}
