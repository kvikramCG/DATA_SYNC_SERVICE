package com.datasync.adapter.out.persistence;

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
                            descriptor.timeZone().orionCode(),
                            now,
                            now));

            entity.setEarliestDataDate(descriptor.earliestDataDate() == null
                    ? null : descriptor.earliestDataDate().toLocalDate());
            entity.setLatestDataDate(descriptor.latestDataDate() == null
                    ? null : descriptor.latestDataDate().toLocalDate());
            entity.setLastUpdatedDate(descriptor.lastUpdatedDate() == null
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

    private SeriesDescriptor toDomain(SeriesEntity entity) {
        TimeZoneCode timeZone = TimeZoneCode.fromOrionCode(entity.getTimeZoneCode());
        LocalDateTime lastUpdated = entity.getLastUpdatedDate() == null
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
                entity.getLastSyncedDate());
    }
}
