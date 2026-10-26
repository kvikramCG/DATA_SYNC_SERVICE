package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.port.out.ClockPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SeriesRepositoryAdapterTest {

    private static final Instant NOW = Instant.parse("2026-10-26T10:00:00Z");

    @Mock
    private SeriesJpaRepository repository;

    @Test
    void oneSeriesThatCannotBeSavedDoesNotStopTheOthers() {
        ClockPort clock = () -> NOW;
        SeriesRepositoryAdapter adapter = new SeriesRepositoryAdapter(repository, clock);
        when(repository.findByName(any())).thenReturn(Optional.empty());
        // The second save is rejected (e.g. value too long for the column); the first and third succeed.
        when(repository.save(any(SeriesEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0))
                .thenThrow(new DataIntegrityViolationException("value too long for type character varying(100)"))
                .thenAnswer(invocation -> invocation.getArgument(0));

        List<SeriesDescriptor> persisted = adapter.upsertAll(List.of(
                descriptor("SERIES_ALPHA_001"), descriptor("SERIES_TOO_LONG"), descriptor("SERIES_GAMMA_003")));

        assertThat(persisted).extracting(SeriesDescriptor::name)
                .containsExactly("SERIES_ALPHA_001", "SERIES_GAMMA_003");
    }

    private static SeriesDescriptor descriptor(String name) {
        return new SeriesDescriptor(null, name, SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                null);
    }
}
