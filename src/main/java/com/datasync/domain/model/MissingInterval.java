package com.datasync.domain.model;

import java.time.LocalDate;

/**
 * One interval that Orion never returned a value for, within the requested window (spec
 * &sect;6.8) - excludes the spring-forward hour, which {@code DstAdjustmentService} already
 * fills before gap detection runs.
 */
public record MissingInterval(LocalDate date, String intervalNumber) {
}
