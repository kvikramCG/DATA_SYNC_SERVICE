package com.datasync.adapter.out.orion.dto;

/**
 * Response body for {@code POST /auth/token} (spec &sect;4.1).
 */
public record OrionTokenResponse(String accessToken, long expiresInSeconds) {
}
