package com.datasync.adapter.out.orion.dto;

/**
 * One entry of {@code GET /v1/series} (spec &sect;4.2).
 */
public record OrionSeriesDto(
        String seriesName,
        String seriesType,
        int timeZone,
        String lastUpdatedDate,
        String earliestDataDate,
        String latestDataDate
) {
}
