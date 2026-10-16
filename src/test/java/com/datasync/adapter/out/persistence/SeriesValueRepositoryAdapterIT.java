package com.datasync.adapter.out.persistence;

import com.datasync.adapter.out.clock.SystemClockAdapter;
import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.service.ChangeDetector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving {@code series_value} persistence against real TimescaleDB, and that
 * its output wires directly into {@link ChangeDetector} (spec &sect;6.7 acceptance criteria
 * #6/#7: re-running an unchanged sync sends nothing, changing one value sends exactly one).
 */
@Testcontainers
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({SeriesValueRepositoryAdapter.class, SeriesRepositoryAdapter.class, SystemClockAdapter.class})
class SeriesValueRepositoryAdapterIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg16").asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void dataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private SeriesValueRepositoryAdapter valueAdapter;

    @Autowired
    private SeriesRepositoryAdapter seriesRepositoryAdapter;

    @Test
    void upsertIsIdempotentAndFeedsChangeDetectorCorrectly() {
        Long seriesId = persistParentSeries();

        Instant instant = Instant.parse("2026-11-01T07:00:00Z");
        NormalizedSeriesValue original = new NormalizedSeriesValue(
                LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("29.80"));

        valueAdapter.upsertAll(seriesId, List.of(original));
        valueAdapter.upsertAll(seriesId, List.of(original)); // re-run: must not duplicate the row

        Map<SeriesValueKey, BigDecimal> stored =
                valueAdapter.findExistingValues(seriesId, LocalDate.of(2026, 11, 1), LocalDate.of(2026, 11, 30));

        assertThat(stored).hasSize(1);
        assertThat(stored.get(new SeriesValueKey(instant, false))).isEqualByComparingTo("29.80");

        // acceptance criterion #6: re-classifying the same value against what's stored -> UNCHANGED
        List<ClassifiedSeriesValue> unchangedRun = ChangeDetector.classify(List.of(original), stored);
        assertThat(unchangedRun.get(0).changeType()).isEqualTo(ChangeType.UNCHANGED);

        // acceptance criterion #7: changing the value -> UPDATED
        NormalizedSeriesValue changed = new NormalizedSeriesValue(
                LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("30.00"));
        List<ClassifiedSeriesValue> changedRun = ChangeDetector.classify(List.of(changed), stored);
        assertThat(changedRun.get(0).changeType()).isEqualTo(ChangeType.UPDATED);
    }

    private Long persistParentSeries() {
        SeriesDescriptor descriptor = new SeriesDescriptor(
                null,
                "SERIES_ALPHA_001",
                SeriesType.TYPE_A,
                TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                null);
        return seriesRepositoryAdapter.upsertAll(List.of(descriptor)).get(0).id();
    }
}
