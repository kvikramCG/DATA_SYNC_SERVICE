package com.datasync.domain.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * A series as known to the Data Sync Service, as returned by Orion's series listing call
 * (spec &sect;4.2). Dates are kept as prevailing local time ({@link LocalDateTime}, no zone
 * applied) - converting to a UTC instant is the responsibility of the DST/interval engine
 * (a later slice), not this plain descriptor.
 *
 * <p>{@code id} is the persisted surrogate key once the descriptor has been upserted via
 * {@link com.datasync.domain.port.out.SeriesRepositoryPort}; it is {@code null} for a descriptor
 * freshly mapped from Orion that has not been persisted yet. {@code Job}/{@code JobTask} link to
 * a series by this id.
 *
 * <p>{@code lastSyncedDate} is the service's own high-watermark (spec &sect;3.1) - {@code null}
 * means the series has never been successfully synced, in which case window resolution falls
 * back to "1 year prior to today".
 */
public record SeriesDescriptor(
        Long id,
        String name,
        SeriesType type,
        TimeZoneCode timeZone,
        LocalDateTime lastUpdatedDate,
        LocalDateTime earliestDataDate,
        LocalDateTime latestDataDate,
        LocalDate lastSyncedDate
) {
}
