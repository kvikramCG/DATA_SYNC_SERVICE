package com.datasync.adapter.out.orion.dto;

import java.math.BigDecimal;

/**
 * One entry of {@code GET /v1/series/{name}/values} (spec &sect;4.3).
 */
public record OrionValueDto(String startDate, boolean repeatedHour, BigDecimal value) {
}
