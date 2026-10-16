package com.datasync.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One sync execution for a series type (spec &sect;7, &sect;9). {@code requestedStartDate}/
 * {@code requestedEndDate} are the caller's optional window override (spec &sect;7.1); they are
 * kept as {@link Instant} in the domain even though the database column is a plain {@code DATE}
 * (see database-design.md) - the UTC-day truncation is a persistence-adapter concern, not a
 * domain one.
 */
public record Job(
        Long id,
        SeriesType seriesType,
        JobStatus status,
        String requestedSeriesName,
        Instant requestedStartDate,
        Instant requestedEndDate,
        Instant startedAt,
        Instant finishedAt,
        UUID correlationId
) {
}
