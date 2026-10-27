package com.datasync.domain.service;

import com.datasync.domain.model.ChangeType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BatchLabelBuilderTest {

    @Test
    void labelsAsInsertWhenEveryValueIsNew() {
        String label = BatchLabelBuilder.build(List.of(ChangeType.NEW, ChangeType.NEW),
                "SERIES_ALPHA_001", "U1", "TZ_C");

        assertThat(label).isEqualTo("INS_SERIES_ALPHA_001_U1_TZ_C");
    }

    @Test
    void labelsAsUpdateWhenAnyValueIsUpdated() {
        String label = BatchLabelBuilder.build(List.of(ChangeType.NEW, ChangeType.UPDATED),
                "SERIES_ALPHA_001", "U1", "TZ_C");

        assertThat(label).isEqualTo("UPD_SERIES_ALPHA_001_U1_TZ_C");
    }

    @Test
    void usesAPlaceholderWhenTheUnitIsMissing() {
        assertThat(BatchLabelBuilder.build(List.of(ChangeType.NEW), "S", null, "TZ_C")).isEqualTo("INS_S_NA_TZ_C");
        assertThat(BatchLabelBuilder.build(List.of(ChangeType.NEW), "S", " ", "TZ_C")).isEqualTo("INS_S_NA_TZ_C");
    }
}
