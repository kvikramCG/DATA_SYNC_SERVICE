package com.datasync.adapter.in.web;

import com.datasync.domain.model.JobSummary;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.port.in.TriggerSyncUseCase;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound adapter for {@code POST /api/series/{seriesType}/sync} (spec &sect;7.1).
 */
@RestController
@RequestMapping("/api/series")
public class SyncController {

    private final TriggerSyncUseCase triggerSyncUseCase;

    public SyncController(TriggerSyncUseCase triggerSyncUseCase) {
        this.triggerSyncUseCase = triggerSyncUseCase;
    }

    @PostMapping("/{seriesType}/sync")
    public JobSummaryResponse sync(@PathVariable String seriesType,
                                    @RequestParam(required = false) String seriesName,
                                    @RequestParam(required = false) Instant startDate,
                                    @RequestParam(required = false) Instant endDate) {
        SeriesType type = SeriesType.fromWireValue(seriesType);
        JobSummary summary = triggerSyncUseCase.triggerSync(type, seriesName, startDate, endDate,
                resolveCorrelationId());
        return JobSummaryResponse.from(summary);
    }

    /**
     * Reuses the correlation id {@link CorrelationIdFilter} already resolved/generated for this
     * request (spec &sect;8 Logging), falling back to a fresh one if the client supplied a
     * non-UUID {@code X-Correlation-Id} value.
     */
    private UUID resolveCorrelationId() {
        String mdcValue = MDC.get(CorrelationIdFilter.MDC_KEY);
        if (mdcValue != null) {
            try {
                return UUID.fromString(mdcValue);
            } catch (IllegalArgumentException ignored) {
                // fall through to a generated id
            }
        }
        return UUID.randomUUID();
    }
}
