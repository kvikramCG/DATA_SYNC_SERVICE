package com.datasync.application;

/**
 * Makes caller-supplied text (for example the {@code seriesName} request parameter) safe to write to a log
 * line: control characters such as newlines are replaced, so it cannot forge extra log lines, and the
 * text is bounded.
 */
final class LogText {

    private static final int MAX_LENGTH = 120;

    private LogText() {
    }

    static String clean(String text) {
        if (text == null) {
            return null;
        }
        String clean = text.replaceAll("\\p{Cntrl}", " ");
        return clean.length() <= MAX_LENGTH ? clean : clean.substring(0, MAX_LENGTH) + "...";
    }
}
