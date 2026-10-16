package com.datasync.adapter.out.orion.dto;

/**
 * Request body for {@code POST /auth/token} (spec &sect;4.1).
 */
public record OrionTokenRequest(String login, String password) {
}
