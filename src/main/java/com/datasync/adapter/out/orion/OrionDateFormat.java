package com.datasync.adapter.out.orion;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Parses/formats Orion's custom date pattern {@code M/d/yyyy h:mm:ss a} (spec &sect;4, US locale).
 */
public final class OrionDateFormat {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a", Locale.US);

    private OrionDateFormat() {
    }

    public static LocalDateTime parse(String value) {
        return LocalDateTime.parse(value, FORMATTER);
    }

    public static String format(LocalDateTime value) {
        return value.format(FORMATTER);
    }
}
