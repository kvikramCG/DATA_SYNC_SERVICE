package com.datasync.application;

import com.datasync.domain.exception.LedgerPointAuthException;
import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.LedgerPointPublishResult;
import com.datasync.domain.model.RawSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.model.WarningType;
import com.datasync.domain.port.out.ChunkRepositoryPort;
import com.datasync.domain.port.out.SeriesPublisherPort;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import com.datasync.domain.port.out.SeriesSourcePort;
import com.datasync.domain.port.out.SeriesValuePort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SeriesSyncWorkerTest {

    @Mock
    private SeriesSourcePort seriesSourcePort;
    @Mock
    private SeriesValuePort seriesValuePort;
    @Mock
    private SeriesPublisherPort seriesPublisherPort;
    @Mock
    private TaskRepositoryPort taskRepositoryPort;
    @Mock
    private SeriesRepositoryPort seriesRepositoryPort;
    @Mock
    private ChunkRepositoryPort chunkRepositoryPort;

    private SeriesSyncWorker worker;

    private static final Long TASK_ID = 100L;
    private static final Long CHUNK_ID = 999L;
    private static final LocalDate WINDOW_START = LocalDate.of(2026, 10, 1);
    private static final LocalDate WINDOW_END = LocalDate.of(2026, 10, 31);
    private static final LocalDate SINGLE_DAY = LocalDate.of(2026, 10, 15);

    private final SeriesDescriptor series = new SeriesDescriptor(
            1L, "SERIES_ALPHA_001", SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
            LocalDateTime.of(2026, 10, 12, 18, 0),
            LocalDateTime.of(2020, 1, 1, 0, 0),
            LocalDateTime.of(2026, 10, 13, 23, 0),
            null);

    @BeforeEach
    void setUp() {
        worker = new SeriesSyncWorker(seriesSourcePort, seriesValuePort, seriesPublisherPort,
                taskRepositoryPort, seriesRepositoryPort, chunkRepositoryPort);
        when(chunkRepositoryPort.createChunk(any(), any(), any())).thenReturn(CHUNK_ID);
    }

    private SeriesValuesResult oneHourlyValue(BigDecimal value) {
        return new SeriesValuesResult(
                "SERIES_ALPHA_001", "UNIT_A", 60, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                List.of(new RawSeriesValue(LocalDateTime.of(2026, 10, 15, 3, 0), false, value)));
    }

    private SeriesValuesResult oneDailyValue(BigDecimal value) {
        return new SeriesValuesResult(
                "SERIES_ALPHA_001", "UNIT_A", 1440, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                List.of(new RawSeriesValue(SINGLE_DAY.atStartOfDay(), false, value)));
    }

    @Test
    void successfulSyncPublishesAndPersistsNewValuesWithNoGaps() {
        // Single-day window + daily granularity (1 interval expected) -> no gaps, plain SUCCESS.
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCESS);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(seriesValuePort).upsertAll(eq(1L), anyList());
        verify(seriesRepositoryPort).updateLastSyncedDate(1L, SINGLE_DAY);
        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.SUCCESS, null, null, 1);
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.PERSISTED), eq(1), eq(1), eq(0),
                eq(0), any());
        verify(chunkRepositoryPort, never()).recordWarning(any(), any(), any(), any(), any());
    }

    @Test
    void sparseHourlyDataProducesGapWarningsAndSuccessWithWarnings() {
        // Hourly granularity over a single day expects 24 intervals; only 1 is provided.
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneHourlyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCESS_WITH_WARNINGS);
        verify(chunkRepositoryPort, org.mockito.Mockito.times(23))
                .recordWarning(eq(CHUNK_ID), eq(WarningType.GAP_MISSING), eq(SINGLE_DAY), any(), any());
        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.SUCCESS_WITH_WARNINGS, null, null, 1);
    }

    @Test
    void unchangedValuesAreNotPublishedButTaskStillSucceeds() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        // Existing value equals the fetched one -> UNCHANGED.
        java.time.Instant sameInstant = SINGLE_DAY.atStartOfDay()
                .atZone(java.time.ZoneId.of("America/Chicago")).toInstant();
        when(seriesValuePort.findExistingValues(eq(1L), any(), any()))
                .thenReturn(Map.of(new com.datasync.domain.model.SeriesValueKey(sameInstant, false),
                        new BigDecimal("31.25")));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCESS);
        verify(seriesPublisherPort, never()).publish(any(), any(), any(), anyList(), any());
        verify(seriesValuePort, never()).upsertAll(any(), anyList());
        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.SUCCESS, null, null, 0);
    }

    @Test
    void noDataAcrossWholeWindowFailsTaskWithNoData() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(new SeriesValuesResult("SERIES_ALPHA_001", "UNIT_A", 60, TimeZoneCode.CENTRAL,
                        LocalDateTime.now(), List.of()));

        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED), eq(FailureReason.NO_DATA),
                any(), eq(0));
    }

    @Test
    void unsupportedGranularityFailsOnlyTheTask() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(new SeriesValuesResult("SERIES_ALPHA_001", "UNIT_A", 30, TimeZoneCode.CENTRAL,
                        LocalDateTime.now(),
                        List.of(new RawSeriesValue(LocalDateTime.of(2026, 10, 15, 3, 0), false, BigDecimal.ONE))));

        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.UNSUPPORTED_GRANULARITY), any(), eq(0));
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.FAILED), eq(0), eq(0), eq(0),
                eq(0), any());
    }

    @Test
    void ledgerPointAuthFailureEscalatesToFailTheWholeJob() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenThrow(new LedgerPointAuthException("LedgerPoint rejected the API key"));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isTrue();
        verify(seriesValuePort, never()).upsertAll(any(), anyList());
    }
}

