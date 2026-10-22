package com.datasync.domain.model;

/**
 * A naming / skip rule for one Orion series (spec &sect;6.6). {@code ledgerPointKeyOverride} replaces
 * the default LedgerPoint key (the Orion series name) when it is not blank; {@code skip} makes the
 * Task finish {@code SKIPPED} without contacting Orion or LedgerPoint.
 */
public record SeriesRuleOverride(String orionSeriesName, String ledgerPointKeyOverride, boolean skip) {

    /**
     * @return the LedgerPoint key to publish under: the override if one is set, otherwise the
     *         Orion series name.
     */
    public String effectiveKey() {
        return ledgerPointKeyOverride == null || ledgerPointKeyOverride.isBlank()
                ? orionSeriesName
                : ledgerPointKeyOverride;
    }
}
