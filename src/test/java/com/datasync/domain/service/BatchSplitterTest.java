package com.datasync.domain.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchSplitterTest {

    @Test
    void splitsEvenlyWhenSizeIsAMultipleOfMaxSize() {
        List<Integer> items = List.of(1, 2, 3, 4);

        List<List<Integer>> batches = BatchSplitter.split(items, 2);

        assertThat(batches).containsExactly(List.of(1, 2), List.of(3, 4));
    }

    @Test
    void lastBatchIsSmallerWhenNotEvenlyDivisible() {
        List<Integer> items = List.of(1, 2, 3, 4, 5);

        List<List<Integer>> batches = BatchSplitter.split(items, 2);

        assertThat(batches).containsExactly(List.of(1, 2), List.of(3, 4), List.of(5));
    }

    @Test
    void returnsASingleBatchWhenUnderTheLimit() {
        List<List<Integer>> batches = BatchSplitter.split(List.of(1, 2, 3), 10_000);

        assertThat(batches).containsExactly(List.of(1, 2, 3));
    }

    @Test
    void emptyInputProducesNoBatches() {
        assertThat(BatchSplitter.split(List.of(), 10_000)).isEmpty();
    }

    @Test
    void nonPositiveMaxSizeThrows() {
        assertThatThrownBy(() -> BatchSplitter.split(List.of(1), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
