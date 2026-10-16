package com.datasync.adapter.out.orion.dto;

import java.util.List;

/**
 * Response body for {@code GET /v1/series} (spec &sect;4.2).
 */
public record OrionSeriesListResponse(List<OrionSeriesDto> series) {
}
