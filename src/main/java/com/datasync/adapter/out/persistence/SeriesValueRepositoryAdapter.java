package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.SeriesValuePort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Outbound adapter implementing {@link SeriesValuePort} against the {@code series_value}
 * hypertable.
 */
@Component
public class SeriesValueRepositoryAdapter implements SeriesValuePort {

    private final SeriesValueJpaRepository repository;
    private final ClockPort clock;

    public SeriesValueRepositoryAdapter(SeriesValueJpaRepository repository, ClockPort clock) {
        this.repository = repository;
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
        Instant now = clock.now();
        for (NormalizedSeriesValue value : values) {
            SeriesValueId id = new SeriesValueId(seriesId, value.startInstant(), value.repeatedHour());
            SeriesValueEntity entity = repository.findById(id)
                    .orElseGet(() -> new SeriesValueEntity(
                            seriesId, value.startInstant(), value.repeatedHour(),
                            value.value(), value.intervalNumber(), value.localDate(), now));
            entity.setValue(value.value());
            entity.setIntervalNumber(value.intervalNumber());
            entity.setSyncedAt(now);
            repository.save(entity);
        }
    }
}
