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
 *
 * <p>{@code timeZone} is {@code null} when Orion reported a timezone code this service does not
 * support (spec &sect;6.2); {@code orionTimeZoneCode} then still carries the raw code so the Task
 * can fail with {@code UNSUPPORTED_TIMEZONE} and a useful detail message.
 *
 * <p>{@code catalogIssue} is non-null when Orion's list entry for this series had a missing or
 * unreadable date (spec &sect;4.2 lists all three as always present). The series is still kept (a Task
 * needs a series row) so that Task can fail with {@code INTERNAL_ERROR} and this text, instead of the
 * bad entry failing the whole job. It is transient: it is not stored, only carried for the current run.
 */
public record SeriesDescriptor(
        Long id,
        String name,
        SeriesType type,
        TimeZoneCode timeZone,
        LocalDateTime lastUpdatedDate,
        LocalDateTime earliestDataDate,
        LocalDateTime latestDataDate,
        LocalDate lastSyncedDate,
        int orionTimeZoneCode,
        String catalogIssue
) {

    /** Convenience constructor for series with a supported timezone. */
    public SeriesDescriptor(Long id, String name, SeriesType type, TimeZoneCode timeZone,
                             LocalDateTime lastUpdatedDate, LocalDateTime earliestDataDate,
                             LocalDateTime latestDataDate, LocalDate lastSyncedDate) {
        this(id, name, type, timeZone, lastUpdatedDate, earliestDataDate, latestDataDate, lastSyncedDate,
                timeZone == null ? 0 : timeZone.orionCode(), null);
    }

    /** A series with a valid catalog entry. */
    public SeriesDescriptor(Long id, String name, SeriesType type, TimeZoneCode timeZone,
                             LocalDateTime lastUpdatedDate, LocalDateTime earliestDataDate,
                             LocalDateTime latestDataDate, LocalDate lastSyncedDate, int orionTimeZoneCode) {
        this(id, name, type, timeZone, lastUpdatedDate, earliestDataDate, latestDataDate, lastSyncedDate,
                orionTimeZoneCode, null);
    }

    public SeriesDescriptor withCatalogIssue(String issue) {
        return new SeriesDescriptor(id, name, type, timeZone, lastUpdatedDate, earliestDataDate, latestDataDate,
                lastSyncedDate, orionTimeZoneCode, issue);
    }

    public SeriesDescriptor withLastSyncedDate(LocalDate newLastSyncedDate) {
        return new SeriesDescriptor(id, name, type, timeZone, lastUpdatedDate, earliestDataDate, latestDataDate,
                newLastSyncedDate, orionTimeZoneCode, catalogIssue);
    }
}
