package com.datasync.domain.model;

/**
 * Orion series type. The POC processes {@code typeA} and {@code typeB} identically.
 */
public enum SeriesType {

    TYPE_A("typeA"),
    TYPE_B("typeB");

    private final String wireValue;

    SeriesType(String wireValue) {
        this.wireValue = wireValue;
    }

    public String wireValue() {
        return wireValue;
    }

    public static SeriesType fromWireValue(String value) {
        for (SeriesType type : values()) {
            if (type.wireValue.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown series type: " + value);
    }
}
