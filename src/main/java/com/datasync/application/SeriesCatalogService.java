package com.datasync.application;

import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.port.in.RefreshSeriesCatalogUseCase;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import com.datasync.domain.port.out.SeriesSourcePort;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestration layer (job-engine / sync-engine per architecture.md &sect;5): coordinates the
 * outbound ports. Contains no HTTP/JPA details itself.
 */
@Service
public class SeriesCatalogService implements RefreshSeriesCatalogUseCase {

    private final SeriesSourcePort seriesSourcePort;
    private final SeriesRepositoryPort seriesRepositoryPort;

    public SeriesCatalogService(SeriesSourcePort seriesSourcePort, SeriesRepositoryPort seriesRepositoryPort) {
        this.seriesSourcePort = seriesSourcePort;
        this.seriesRepositoryPort = seriesRepositoryPort;
    }

    @Override
    public List<SeriesDescriptor> refreshCatalog(SeriesType type) {
        List<SeriesDescriptor> fetched = seriesSourcePort.listSeries(type);
        return seriesRepositoryPort.upsertAll(fetched);
    }
}
