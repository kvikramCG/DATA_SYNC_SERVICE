package com.datasync.adapter.out.orion.dto;

import java.util.List;

/**
 * Response body for {@code GET /v1/series/{name}/values} (spec &sect;4.3).
 */
public record OrionSeriesValuesResponse(
        String seriesName,
        String unit,
        int granularity,
        int timeZone,
        String asOfDate,
        List<OrionValueDto> values
) {
}
