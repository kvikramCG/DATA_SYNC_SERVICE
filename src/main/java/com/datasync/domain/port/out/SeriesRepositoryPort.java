package com.datasync.domain.port.out;

import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;

import java.time.LocalDate;
import java.util.List;

/**
 * Outbound port implemented by the persistence adapter ({@code adapter.out.persistence}).
 */
public interface SeriesRepositoryPort {

    /**
     * Upserts each descriptor by {@code name} and returns the persisted descriptors with their
     * database id populated (needed by {@code JobTask} to reference a series).
     */
    List<SeriesDescriptor> upsertAll(List<SeriesDescriptor> descriptors);

    List<SeriesDescriptor> findByType(SeriesType type);

    /**
     * Updates the service's sync high-watermark for a series (spec &sect;3.1) after a Task
     * successfully finishes processing its whole window.
     */
    void updateLastSyncedDate(Long seriesId, LocalDate lastSyncedDate);

    /**
     * Stores the unit Orion last reported for the series (database-design.md &sect;3.5: refreshed on
     * every values fetch). A no-op when the stored unit is already equal.
     */
    void updateUnit(Long seriesId, String unit);
}
