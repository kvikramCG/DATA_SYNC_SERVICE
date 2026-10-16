package com.datasync.domain.port.out;

import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.LedgerPointPublishResult;

import java.util.List;
import java.util.UUID;

/**
 * Outbound port implemented by the LedgerPoint adapter ({@code adapter.out.ledgerpoint}).
 * Callers are expected to have already filtered {@code values} down to {@code NEW}/
 * {@code UPDATED} entries (spec &sect;6.7) - {@code UNCHANGED} values are never sent.
 */
public interface SeriesPublisherPort {

    LedgerPointPublishResult publish(String key, String unitCode, String timeZoneCode,
                                      List<ClassifiedSeriesValue> values, UUID correlationId);
}
