package com.datasync.adapter.out.persistence;

import com.datasync.domain.exception.UnsupportedTimeZoneException;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Outbound adapter implementing {@link SeriesRepositoryPort} on top of Spring Data JPA.
 */
@Component
public class SeriesRepositoryAdapter implements SeriesRepositoryPort {

    private final SeriesJpaRepository repository;
    private final ClockPort clock;

    public SeriesRepositoryAdapter(SeriesJpaRepository repository, ClockPort clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Override
    @Transactional
    public List<SeriesDescriptor> upsertAll(List<SeriesDescriptor> descriptors) {
        Instant now = clock.now();
        List<SeriesDescriptor> persisted = new ArrayList<>(descriptors.size());
        for (SeriesDescriptor descriptor : descriptors) {
            SeriesEntity entity = repository.findByName(descriptor.name())
                    .orElseGet(() -> new SeriesEntity(
                            descriptor.name(),
                            descriptor.type().wireValue(),
                            descriptor.orionTimeZoneCode(),
                            now,
                            now));

            entity.setTimeZoneCode(descriptor.orionTimeZoneCode());
            entity.setEarliestDataDate(descriptor.earliestDataDate() == null
                    ? null : descriptor.earliestDataDate().toLocalDate());
            entity.setLatestDataDate(descriptor.latestDataDate() == null
                    ? null : descriptor.latestDataDate().toLocalDate());
            // Without a supported timezone the local timestamp cannot be turned into an instant.
            entity.setLastUpdatedDate(descriptor.lastUpdatedDate() == null || descriptor.timeZone() == null
                    ? null : descriptor.lastUpdatedDate().atZone(ZoneId.of(descriptor.timeZone().ianaZoneId())).toInstant());
            entity.setUpdatedAt(now);

            SeriesEntity saved = repository.save(entity);
            persisted.add(toDomain(saved));
        }
        return persisted;
    }

    @Override
    public List<SeriesDescriptor> findByType(SeriesType type) {
        return repository.findBySeriesType(type.wireValue()).stream()
                .map(this::toDomain)
                .toList();
    }

    @Override
    @Transactional
    public void updateLastSyncedDate(Long seriesId, LocalDate lastSyncedDate) {
        SeriesEntity entity = repository.findById(seriesId)
                .orElseThrow(() -> new IllegalArgumentException("No series found with id " + seriesId));
        entity.setLastSyncedDate(lastSyncedDate);
        entity.setUpdatedAt(clock.now());
        repository.save(entity);
    }

    @Override
    @Transactional
    public void updateUnit(Long seriesId, String unit) {
        SeriesEntity entity = repository.findById(seriesId)
                .orElseThrow(() -> new IllegalArgumentException("No series found with id " + seriesId));
        if (java.util.Objects.equals(entity.getUnit(), unit)) {
            return;
        }
        entity.setUnit(unit);
        entity.setUpdatedAt(clock.now());
        repository.save(entity);
    }

    private SeriesDescriptor toDomain(SeriesEntity entity) {
        TimeZoneCode timeZone;
        try {
            timeZone = TimeZoneCode.fromOrionCode(entity.getTimeZoneCode());
        } catch (UnsupportedTimeZoneException ex) {
            timeZone = null; // surfaces later as a Task failure with UNSUPPORTED_TIMEZONE (spec 6.2)
        }
        LocalDateTime lastUpdated = entity.getLastUpdatedDate() == null || timeZone == null
                ? null
                : LocalDateTime.ofInstant(entity.getLastUpdatedDate(), ZoneId.of(timeZone.ianaZoneId()));
        return new SeriesDescriptor(
                entity.getId(),
                entity.getName(),
                SeriesType.fromWireValue(entity.getSeriesType()),
                timeZone,
                lastUpdated,
                entity.getEarliestDataDate() == null ? null : entity.getEarliestDataDate().atStartOfDay(),
                entity.getLatestDataDate() == null ? null : entity.getLatestDataDate().atStartOfDay(),
                entity.getLastSyncedDate(),
                entity.getTimeZoneCode());
    }
}
