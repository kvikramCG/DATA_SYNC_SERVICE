package com.datasync.domain.service;

import com.datasync.domain.model.ChangeType;

import java.util.List;

/**
 * Builds the LedgerPoint audit/log batch label (spec &sect;6.6):
 * {@code {INS|UPD}_{key}_{unitCode}_{timeZone}}. A batch is labeled {@code INS} only when every
 * value in it is {@link ChangeType#NEW}; any {@code UPDATED} value makes it {@code UPD}.
 */
public final class BatchLabelBuilder {

    private BatchLabelBuilder() {
    }

    public static String build(List<ChangeType> changeTypes, String key, String unitCode, String timeZoneCode) {
        String prefix = changeTypes.stream().allMatch(type -> type == ChangeType.NEW) ? "INS" : "UPD";
        return prefix + "_" + key + "_" + unitCode + "_" + timeZoneCode;
    }
}
