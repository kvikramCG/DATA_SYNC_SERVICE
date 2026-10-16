package com.datasync.domain.model;

import com.datasync.domain.exception.UnsupportedTimeZoneException;

/**
 * Orion timezone code mapping (spec &sect;6.2). Pure domain value object - no infrastructure
 * dependency.
 */
public enum TimeZoneCode {

    EASTERN(5, "America/New_York", "TZ_E"),
    CENTRAL(6, "America/Chicago", "TZ_C"),
    MOUNTAIN(7, "America/Denver", "TZ_M"),
    PACIFIC(8, "America/Los_Angeles", "TZ_P");

    private final int orionCode;
    private final String ianaZoneId;
    private final String ledgerPointCode;

    TimeZoneCode(int orionCode, String ianaZoneId, String ledgerPointCode) {
        this.orionCode = orionCode;
        this.ianaZoneId = ianaZoneId;
        this.ledgerPointCode = ledgerPointCode;
    }

    public int orionCode() {
        return orionCode;
    }

    public String ianaZoneId() {
        return ianaZoneId;
    }

    public String ledgerPointCode() {
        return ledgerPointCode;
    }

    public static TimeZoneCode fromOrionCode(int code) {
        for (TimeZoneCode zone : values()) {
            if (zone.orionCode == code) {
                return zone;
            }
        }
        throw new UnsupportedTimeZoneException("Unsupported Orion timezone code: " + code);
    }
}
