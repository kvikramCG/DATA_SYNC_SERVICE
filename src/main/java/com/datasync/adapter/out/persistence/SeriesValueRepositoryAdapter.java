package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.SeriesValuePort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Outbound adapter implementing {@link SeriesValuePort} against the {@code series_value}
 * hypertable.
 */
@Component
public class SeriesValueRepositoryAdapter implements SeriesValuePort {

    // DECISION: the primary key is (series_id, start_instant, repeated_hour), so a single
    // INSERT ... ON CONFLICT DO UPDATE is the whole upsert. local_date is deliberately NOT updated on
    // conflict: the old JPA code only changed value, interval_number and synced_at on an existing row,
    // and the same key always maps to the same local date.
    private static final String UPSERT_SQL =
            "INSERT INTO series_value "
                    + "(series_id, start_instant, repeated_hour, value, interval_number, local_date, synced_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (series_id, start_instant, repeated_hour) DO UPDATE SET "
                    + "value = EXCLUDED.value, "
                    + "interval_number = EXCLUDED.interval_number, "
                    + "synced_at = EXCLUDED.synced_at";

    // DECISION: 1,000 rows per JDBC batch. Big enough to cut network round trips by ~3 orders of
    // magnitude versus one statement per row, small enough to keep each statement and its memory
    // footprint modest (a 15-minute series has ~35,000 values per year). The whole call still runs in
    // ONE transaction, so a failure rolls back the entire chunk exactly as before.
    private static final int JDBC_BATCH_SIZE = 1_000;

    private final SeriesValueJpaRepository repository;
    // DECISION: native batched SQL for the write path only. The previous loop (findById + save on a
    // composite-id entity without @Version) cost ~2 SELECTs + 1 INSERT per NEW row (save() merges), and
    // a first-time backfill is ~9k-35k rows per series-year. That held a pooled connection for tens of
    // seconds per chunk and exhausted the pool. Reads stay on JPA; this is adapter-internal, so the
    // port and the domain are unchanged.
    private final JdbcTemplate jdbcTemplate;
    private final ClockPort clock;

    public SeriesValueRepositoryAdapter(SeriesValueJpaRepository repository, JdbcTemplate jdbcTemplate,
                                         ClockPort clock) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    @Override
    public Map<SeriesValueKey, BigDecimal> findExistingValues(Long seriesId, LocalDate localDateFrom, LocalDate localDateTo) {
        return repository.findBySeriesIdAndLocalDateBetween(seriesId, localDateFrom, localDateTo).stream()
                .collect(Collectors.toMap(
                        entity -> new SeriesValueKey(entity.getStartInstant(), entity.isRepeatedHour()),
                        SeriesValueEntity::getValue));
    }

    @Override
    @Transactional
    public void upsertAll(Long seriesId, List<NormalizedSeriesValue> values) {
        if (values.isEmpty()) {
            return;
        }
        Instant now = clock.now();

        // DECISION: collapse duplicate keys in the input (last one wins, matching the old loop where a
        // later save simply overwrote an earlier one). Postgres rejects a single INSERT that would
        // affect the same row twice ("ON CONFLICT DO UPDATE command cannot affect row a second
        // time"), and the driver may merge a batch into multi-row INSERTs (reWriteBatchedInserts), so
        // duplicates must be removed before sending. LinkedHashMap keeps insertion order stable.
        Map<SeriesValueKey, NormalizedSeriesValue> byKey = new LinkedHashMap<>();
        for (NormalizedSeriesValue value : values) {
            byKey.put(new SeriesValueKey(value.startInstant(), value.repeatedHour()), value);
        }
        List<NormalizedSeriesValue> rows = new ArrayList<>(byKey.values());

        // @Transactional makes JdbcTemplate join the same transaction/connection as JPA (the JPA
        // transaction manager exposes its JDBC connection), so every batch commits or rolls back
        // together.
        jdbcTemplate.batchUpdate(UPSERT_SQL, rows, JDBC_BATCH_SIZE, (ps, value) -> {
            ps.setLong(1, seriesId);
            // OffsetDateTime (UTC) is the unambiguous way to bind a timestamptz with the PostgreSQL
            // driver; java.sql.Timestamp would depend on the JVM default time zone.
            ps.setObject(2, OffsetDateTime.ofInstant(value.startInstant(), ZoneOffset.UTC));
            ps.setBoolean(3, value.repeatedHour());
            ps.setBigDecimal(4, value.value());
            ps.setString(5, value.intervalNumber());
            ps.setObject(6, value.localDate());
            ps.setObject(7, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
        });
    }
}
