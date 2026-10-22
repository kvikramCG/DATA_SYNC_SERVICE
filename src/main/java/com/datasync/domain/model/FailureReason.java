package com.datasync.domain.model;

/**
 * Task failure reason codes (spec &sect;8.1). Stored as {@code varchar} rather than a native DB
 * enum (see database-design.md) so a new reason only requires an application-side change.
 * {@link #INTERNAL_ERROR} is an addition to the spec list: an unexpected bug in this service, so
 * the Task is never left {@code PENDING}.
 */
public enum FailureReason {
    SOURCE_UNAVAILABLE,
    NO_DATA,
    STALE_SERIES,
    UNSUPPORTED_GRANULARITY,
    UNSUPPORTED_TIMEZONE,
    TARGET_REJECTED,
    UNKNOWN_SERIES,
    TARGET_UNAVAILABLE,
    INTERNAL_ERROR
}
