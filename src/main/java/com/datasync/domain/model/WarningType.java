package com.datasync.domain.model;

/**
 * {@code task_warning.warning_type} (database-design.md &sect;3.4). Only {@code GAP_MISSING} is
 * currently emitted (spec &sect;6.8) - the DST-related constants are reserved for future
 * audit-trail detail if ever needed, since DST adjustments themselves are not warnings (spec
 * &sect;6.4 treats them as the expected, successful outcome).
 */
public enum WarningType {
    GAP_MISSING,
    DST_SPRING_FORWARD_FILLED,
    DST_FALL_BACK_REPEATED
}
