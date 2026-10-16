package com.datasync.domain.port.in;

import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;

import java.util.List;

/**
 * Inbound port: refresh the locally cached series catalog for a given series type by pulling
 * the latest list from Orion. First vertical slice of the eventual sync job flow (spec &sect;3,
 * step "b. list series").
 */
public interface RefreshSeriesCatalogUseCase {

    List<SeriesDescriptor> refreshCatalog(SeriesType type);
}
