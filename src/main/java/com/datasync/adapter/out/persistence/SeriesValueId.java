package com.datasync.adapter.out.persistence;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Composite id for {@link SeriesValueEntity}, matching the {@code series_value} primary key
 * {@code (series_id, start_instant, repeated_hour)} (database-design.md &sect;3.7).
 */
public class SeriesValueId implements Serializable {

    private Long seriesId;
    private Instant startInstant;
    private boolean repeatedHour;

    public SeriesValueId() {
        // JPA
    }

    public SeriesValueId(Long seriesId, Instant startInstant, boolean repeatedHour) {
        this.seriesId = seriesId;
        this.startInstant = startInstant;
        this.repeatedHour = repeatedHour;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SeriesValueId other)) {
            return false;
        }
        return repeatedHour == other.repeatedHour
                && Objects.equals(seriesId, other.seriesId)
                && Objects.equals(startInstant, other.startInstant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seriesId, startInstant, repeatedHour);
    }
}
