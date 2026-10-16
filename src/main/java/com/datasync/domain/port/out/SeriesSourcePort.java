package com.datasync.domain.port.out;

import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValuesResult;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Outbound port implemented by the Orion adapter ({@code adapter.out.orion}).
 */
public interface SeriesSourcePort {

    List<SeriesDescriptor> listSeries(SeriesType type);

    /**
     * Fetches one series' values for a single month-chunk window (spec &sect;4.3). Callers are
     * expected to pre-split wider windows via {@code MonthChunker} - one call here is one Orion
     * request.
     */
    SeriesValuesResult fetchValues(String seriesName, LocalDateTime startPrevailing, LocalDateTime endPrevailing);
}
