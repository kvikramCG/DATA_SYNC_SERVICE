package com.datasync.adapter.out.ledgerpoint.dto;

/**
 * Success response body for {@code POST /api/v2/series/values} (spec &sect;5.1).
 */
public record LedgerPointBatchResponseDto(String batchId, int received, int inserted, int updated) {
}
