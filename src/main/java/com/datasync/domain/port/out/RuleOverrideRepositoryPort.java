package com.datasync.domain.port.out;

import com.datasync.domain.model.SeriesRuleOverride;

import java.util.Optional;

/**
 * Outbound port for the naming / skip rules table (spec &sect;6.6). Only <em>active</em> rules are
 * ever returned.
 */
public interface RuleOverrideRepositoryPort {

    Optional<SeriesRuleOverride> findActiveByOrionSeriesName(String orionSeriesName);
}
