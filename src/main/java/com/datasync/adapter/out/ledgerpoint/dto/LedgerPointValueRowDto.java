package com.datasync.adapter.out.ledgerpoint.dto;

/**
 * One row of the {@code POST /api/v2/series/values} request body (spec &sect;5.1).
 */
public record LedgerPointValueRowDto(String date, String interval, String key, String value) {
}
