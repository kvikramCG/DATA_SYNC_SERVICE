package com.datasync.domain.model;

/**
 * Change-detection classification (spec &sect;6.7).
 */
public enum ChangeType {
    NEW,
    UPDATED,
    UNCHANGED
}
