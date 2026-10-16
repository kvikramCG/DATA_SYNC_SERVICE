package com.datasync.adapter.out.ledgerpoint.dto;

/**
 * One entry of the {@code errors} array in a LedgerPoint 400 response (spec &sect;5.1).
 */
public record LedgerPointErrorDto(int index, String field, String message) {
}
