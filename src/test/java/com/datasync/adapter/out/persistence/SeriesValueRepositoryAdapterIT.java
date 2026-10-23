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
import org.springframework.jdbc.core.JdbcTemplate;
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

    // Assertions read through plain SQL on purpose: the adapter now writes with native SQL, and a JPA
    // read inside the same test transaction could return stale cached entities instead of the rows.
    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void existingRowIsUpdatedInPlaceAndKeepsItsLocalDate() {
        Long seriesId = persistParentSeries();
        Instant instant = Instant.parse("2026-11-01T07:00:00Z");
        valueAdapter.upsertAll(seriesId, List.of(new NormalizedSeriesValue(
                LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("29.80"))));

        valueAdapter.upsertAll(seriesId, List.of(new NormalizedSeriesValue(
                LocalDate.of(2026, 11, 1), "3", instant, false, new BigDecimal("30.10"))));

        assertThat(countRows(seriesId)).isEqualTo(1);
        Map<String, Object> row = jdbc.queryForMap(
                "select value, interval_number, local_date from series_value where series_id = ?", seriesId);
        assertThat((BigDecimal) row.get("value")).isEqualByComparingTo("30.10");
        assertThat(row.get("interval_number")).isEqualTo("3");
        assertThat(row.get("local_date").toString()).isEqualTo("2026-11-01");
    }

    @Test
    void repeatedHourIsStoredAsASeparateRowFromTheSameInstant() {
        Long seriesId = persistParentSeries();
        Instant instant = Instant.parse("2026-11-01T07:00:00Z");

        valueAdapter.upsertAll(seriesId, List.of(
                new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("29.80")),
                new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", instant, true, new BigDecimal("29.10"))));

        assertThat(countRows(seriesId)).isEqualTo(2);
    }

    @Test
    void duplicateKeysInOneCallDoNotFailAndTheLastValueWins() {
        Long seriesId = persistParentSeries();
        Instant instant = Instant.parse("2026-11-01T07:00:00Z");

        // Postgres would reject two rows with the same key in one INSERT ... ON CONFLICT statement.
        valueAdapter.upsertAll(seriesId, List.of(
                new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("1.00")),
                new NormalizedSeriesValue(LocalDate.of(2026, 11, 1), "2", instant, false, new BigDecimal("2.00"))));

        assertThat(countRows(seriesId)).isEqualTo(1);
        BigDecimal stored = jdbc.queryForObject(
                "select value from series_value where series_id = ?", BigDecimal.class, seriesId);
        assertThat(stored).isEqualByComparingTo("2.00");
    }

    @Test
    void largeCallSpanningSeveralJdbcBatchesIsInsertedThenRewrittenWithoutDuplicates() {
        Long seriesId = persistParentSeries();
        // 2,500 hourly values = 3 JDBC batches (1,000 + 1,000 + 500).
        Instant first = Instant.parse("2026-01-01T00:00:00Z");
        List<NormalizedSeriesValue> values = new java.util.ArrayList<>();
        for (int i = 0; i < 2_500; i++) {
            Instant instant = first.plusSeconds(3600L * i);
            values.add(new NormalizedSeriesValue(
                    LocalDate.ofInstant(instant, java.time.ZoneOffset.UTC), "1", instant, false, BigDecimal.valueOf(i)));
        }

        valueAdapter.upsertAll(seriesId, values);
        valueAdapter.upsertAll(seriesId, values); // idempotent re-run

        assertThat(countRows(seriesId)).isEqualTo(2_500);
    }

    @Test
    void emptyInputIsANoOp() {
        Long seriesId = persistParentSeries();

        valueAdapter.upsertAll(seriesId, List.of());

        assertThat(countRows(seriesId)).isZero();
    }

    private int countRows(Long seriesId) {
        Integer count = jdbc.queryForObject("select count(*) from series_value where series_id = ?",
                Integer.class, seriesId);
        return count == null ? 0 : count;
    }

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
