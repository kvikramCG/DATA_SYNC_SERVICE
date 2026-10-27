package com.datasync.domain.service;

import java.util.Map;

/**
 * Orion unit code to LedgerPoint unit code mapping (spec &sect;6.5).
 */
public final class UnitMapper {

    private static final Map<String, String> MAPPING = Map.of(
            "UNIT_A", "U1",
            "UNIT_B", "U1",
            "UNIT_C", "U2",
            "UNIT_D", "U3"
    );

    private UnitMapper() {
    }

    /**
     * Unknown Orion unit codes pass through unchanged (spec &sect;6.5 "anything else"). A missing unit
     * stays missing: it must never fail the Task, because the unit only feeds the log/audit label.
     */
    public static String map(String orionUnit) {
        if (orionUnit == null) {
            return null;
        }
        return MAPPING.getOrDefault(orionUnit, orionUnit);
    }
}
