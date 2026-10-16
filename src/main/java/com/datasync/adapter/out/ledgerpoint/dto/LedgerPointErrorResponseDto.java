package com.datasync.adapter.out.ledgerpoint.dto;

import java.util.List;

/**
 * 400 response body for {@code POST /api/v2/series/values} (spec &sect;5.1).
 */
public record LedgerPointErrorResponseDto(String batchId, List<LedgerPointErrorDto> errors) {
}
