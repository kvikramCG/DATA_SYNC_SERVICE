package com.datasync.domain.exception;

/**
 * Thrown when an Orion {@code timeZone} code (spec &sect;6.2) has no known mapping.
 * In the full job/task model this corresponds to failure reason {@code UNSUPPORTED_TIMEZONE}.
 */
public class UnsupportedTimeZoneException extends RuntimeException {

    public UnsupportedTimeZoneException(String message) {
        super(message);
    }
}
