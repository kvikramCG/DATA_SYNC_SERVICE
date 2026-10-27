package com.datasync.domain.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UnitMapperTest {

    @Test
    void mapsKnownUnits() {
        assertThat(UnitMapper.map("UNIT_A")).isEqualTo("U1");
        assertThat(UnitMapper.map("UNIT_B")).isEqualTo("U1");
        assertThat(UnitMapper.map("UNIT_C")).isEqualTo("U2");
        assertThat(UnitMapper.map("UNIT_D")).isEqualTo("U3");
    }

    @Test
    void passesThroughUnknownUnits() {
        assertThat(UnitMapper.map("SOMETHING_ELSE")).isEqualTo("SOMETHING_ELSE");
    }

    @Test
    void aMissingUnitDoesNotFailAndStaysMissing() {
        assertThat(UnitMapper.map(null)).isNull();
        assertThat(UnitMapper.map("")).isEmpty();
    }
}
