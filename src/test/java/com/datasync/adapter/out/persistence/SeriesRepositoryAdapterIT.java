package com.datasync.adapter.out.persistence;

import com.datasync.adapter.out.clock.SystemClockAdapter;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TimeZoneCode;
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

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving the persistence adapter against a real TimescaleDB-flavored Postgres
 * (Flyway migrations included), not H2 - required since {@code V1__init_schema.sql} uses the
 * {@code timescaledb} extension and {@code create_hypertable}.
 */
@Testcontainers
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({SeriesRepositoryAdapter.class, SystemClockAdapter.class})
class SeriesRepositoryAdapterIT {

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
    private SeriesRepositoryAdapter adapter;

    @Test
    void upsertIsIdempotentAndFindableByType() {
        SeriesDescriptor descriptor = new SeriesDescriptor(
                null,
                "SERIES_ALPHA_001",
                SeriesType.TYPE_A,
                TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                null);

        adapter.upsertAll(List.of(descriptor));
        adapter.upsertAll(List.of(descriptor));

        List<SeriesDescriptor> found = adapter.findByType(SeriesType.TYPE_A);

        assertThat(found).hasSize(1);
        assertThat(found.get(0).name()).isEqualTo("SERIES_ALPHA_001");
        assertThat(found.get(0).timeZone()).isEqualTo(TimeZoneCode.CENTRAL);
    }
}
