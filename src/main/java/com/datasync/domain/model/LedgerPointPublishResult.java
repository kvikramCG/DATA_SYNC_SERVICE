package com.datasync.domain.model;

import java.util.List;
import java.util.UUID;

/**
 * Aggregated result of {@code SeriesPublisherPort.publish} across one or more LedgerPoint
 * batches (spec &sect;5.1) - a publish call may be split into several batches when it exceeds
 * the 10,000-row limit.
 */
public record LedgerPointPublishResult(int received, int inserted, int updated, List<UUID> batchIds) {
}
