package com.datasync.domain.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic list partitioning - used to respect LedgerPoint's 10,000-row batch limit (spec
 * &sect;5): "split larger payloads".
 */
public final class BatchSplitter {

    private BatchSplitter() {
    }

    public static <T> List<List<T>> split(List<T> items, int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += maxSize) {
            batches.add(items.subList(i, Math.min(i + maxSize, items.size())));
        }
        return batches;
    }
}
