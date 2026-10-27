package com.datasync.domain.exception;

/**
 * Orion answered successfully but with data this service cannot interpret (for example an unreadable
 * value {@code startDate}). Not retried - asking again returns the same payload. The Task fails with
 * {@code INTERNAL_ERROR} and this message, since spec &sect;8.1 has no dedicated data-quality code.
 */
public class OrionDataException extends RuntimeException {

    public OrionDataException(String message) {
        super(message);
    }
}
