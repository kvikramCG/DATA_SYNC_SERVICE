package com.datasync.application;

import com.datasync.domain.exception.LedgerPointAuthException;
import com.datasync.domain.exception.LedgerPointValidationException;
import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.LedgerPointPublishResult;
import com.datasync.domain.model.LedgerPointValidationError;
import com.datasync.domain.model.RawSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesRuleOverride;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.model.WarningType;
import com.datasync.domain.port.out.ChunkRepositoryPort;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.RuleOverrideRepositoryPort;
import com.datasync.domain.port.out.SeriesPublisherPort;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import com.datasync.domain.port.out.SeriesSourcePort;
import com.datasync.domain.port.out.SeriesValuePort;
import com.datasync.domain.port.out.SyncMetricsPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
    @Mock
    private RuleOverrideRepositoryPort ruleOverrideRepositoryPort;
    @Mock
    private ClockPort clock;
    @Mock
    private SyncMetricsPort metrics;

    private SeriesSyncWorker worker;

    // Two days after the fixture series' lastUpdatedDate (2026-10-12 18:00 Central), well inside 5 days.
    private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");
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
                taskRepositoryPort, seriesRepositoryPort, chunkRepositoryPort,
                ruleOverrideRepositoryPort, clock, metrics, 5);
        org.mockito.Mockito.lenient().when(chunkRepositoryPort.createChunk(any(), any(), any())).thenReturn(CHUNK_ID);
        org.mockito.Mockito.lenient().when(clock.now()).thenReturn(NOW);
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
    void recordsTheUnitAndTheRealOrionCallCountFromTheFetch() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")).withOrionCallCount(3));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(seriesRepositoryPort).updateUnit(1L, "UNIT_A");
        verify(chunkRepositoryPort).updateOrionCallCount(CHUNK_ID, 3);
    }

    @Test
    void successfulSyncPublishesAndPersistsNewValuesWithNoGaps() {
        // Single-day window + daily granularity (1 interval expected) -> no gaps, plain SUCCESS.
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

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

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCESS_WITH_WARNINGS);
        verify(chunkRepositoryPort, org.mockito.Mockito.times(23))
                .recordWarning(eq(CHUNK_ID), eq(WarningType.GAP_MISSING), eq(SINGLE_DAY), any(), any());
        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.SUCCESS_WITH_WARNINGS, null, null, 1);
    }

    @Test
    void daysAfterTheLastReturnedDataAreNotReportedAsGaps() {
        // Whole-month window, daily granularity, data only on Oct 15: Oct 1-14 are gaps, Oct 16-31 are not.
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCESS_WITH_WARNINGS);
        verify(chunkRepositoryPort, org.mockito.Mockito.times(14))
                .recordWarning(eq(CHUNK_ID), eq(WarningType.GAP_MISSING), any(LocalDate.class), any(), any());
        verify(chunkRepositoryPort, never())
                .recordWarning(any(), any(), eq(LocalDate.of(2026, 10, 16)), any(), any());
    }

    @Test
    void daysBeforeTheSeriesEarliestDataDateAreNotReportedAsGaps() {
        SeriesDescriptor startsOct10 = new SeriesDescriptor(
                1L, "SERIES_ALPHA_001", SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2026, 10, 10, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                null);
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, startsOct10, WINDOW_START, WINDOW_END, UUID.randomUUID(), new AtomicBoolean());

        // Only Oct 10-14 are before the data and after the series started.
        verify(chunkRepositoryPort, org.mockito.Mockito.times(5))
                .recordWarning(eq(CHUNK_ID), eq(WarningType.GAP_MISSING), any(LocalDate.class), any(), any());
        verify(chunkRepositoryPort, never())
                .recordWarning(any(), any(), eq(LocalDate.of(2026, 10, 9)), any(), any());
    }

    @Test
    void seriesWithAnUnsupportedTimezoneFailsTheTaskWithUnsupportedTimezone() {
        SeriesDescriptor badTz = new SeriesDescriptor(
                1L, "SERIES_ALPHA_001", SeriesType.TYPE_A, null,
                null, LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2026, 10, 13, 23, 0), null, 99);

        TaskOutcome outcome = worker.process(TASK_ID, badTz, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.UNSUPPORTED_TIMEZONE), eq("Unsupported Orion timezone code: 99"), eq(0));
        verifyNoInteractions(seriesSourcePort, seriesPublisherPort);
    }

    @Test
    void reversedWindowFailsTheTaskWithNoData() {
        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_END, WINDOW_START, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED), eq(FailureReason.NO_DATA),
                any(), eq(0));
        verifyNoInteractions(seriesSourcePort, seriesPublisherPort);
    }

    @Test
    void unexpectedExceptionFailsTheTaskWithInternalErrorInsteadOfLeavingItPending() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenThrow(new IllegalStateException("bug"));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.INTERNAL_ERROR), any(), eq(0));
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.FAILED), eq(0), eq(0), eq(0),
                eq(0), any());
    }

    @Test
    void taskIsMarkedStartedBeforeItsFinalStatusIsWritten() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        InOrder order = inOrder(taskRepositoryPort);
        order.verify(taskRepositoryPort).markTaskStarted(TASK_ID);
        order.verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.SUCCESS), any(), any(), eq(1));
    }

    @Test
    void taskThatIsAbortedBeforeStartingIsNeverMarkedStarted() {
        AtomicBoolean aborted = new AtomicBoolean(true);

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), aborted);

        verify(taskRepositoryPort, never()).markTaskStarted(any());
    }

    @Test
    void rejectedBatchFailureDetailListsTheRowErrorsFromLedgerPoint() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        List<LedgerPointValidationError> rowErrors = new java.util.ArrayList<>();
        for (int i = 0; i < 7; i++) {
            rowErrors.add(new LedgerPointValidationError(i, "value", "not a number"));
        }
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenThrow(new LedgerPointValidationException("LedgerPoint rejected the batch", rowErrors));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        org.mockito.ArgumentCaptor<String> detail = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.TARGET_REJECTED), detail.capture(), eq(0));
        assertThat(detail.getValue())
                .startsWith("LedgerPoint rejected the batch: [batch row 0 value: not a number")
                .contains("batch row 4 value: not a number")
                .doesNotContain("batch row 5")
                .endsWith("(+2 more)");
    }

    @Test
    void rejectedBatchWithNoParsableRowErrorsKeepsTheGenericMessage() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenThrow(new LedgerPointValidationException("LedgerPoint rejected the batch", List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.FAILED, FailureReason.TARGET_REJECTED,
                "LedgerPoint rejected the batch", 0);
    }

    @Test
    void failureDetailLongerThanTheColumnIsTruncatedAndStripsControlCharacters() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenThrow(new IllegalStateException("line1\nline2 " + "x".repeat(5_000)));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        org.mockito.ArgumentCaptor<String> detail = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.INTERNAL_ERROR), detail.capture(), eq(0));
        assertThat(detail.getValue()).hasSize(2000).endsWith("...").doesNotContain("\n");
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

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

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

        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID(), new AtomicBoolean());

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

        TaskOutcome outcome = worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID(), new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.UNSUPPORTED_GRANULARITY), any(), eq(0));
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.FAILED), eq(0), eq(0), eq(0),
                eq(0), any());
    }

    @Test
    void orionRateLimiterTimeoutFailsTheTaskInsteadOfLeavingItPending() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenThrow(RequestNotPermitted.createRequestNotPermitted(RateLimiter.ofDefaults("orion")));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.SOURCE_UNAVAILABLE), any(), eq(0));
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.FAILED), eq(0), eq(0), eq(0),
                eq(0), any());
    }

    @Test
    void lastSyncedDateIsTheLastDateActuallyReceivedNotTheMonthEnd() {
        // Window is the whole month, but Orion only has data up to Oct 15.
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, WINDOW_START, WINDOW_END, UUID.randomUUID(), new AtomicBoolean());

        verify(seriesRepositoryPort).updateLastSyncedDate(1L, SINGLE_DAY);
        verify(seriesRepositoryPort, never()).updateLastSyncedDate(1L, WINDOW_END);
    }

    @Test
    void lastSyncedDateNeverMovesBackwardsWhenAnOlderWindowIsReprocessed() {
        LocalDate alreadySyncedUpTo = LocalDate.of(2026, 10, 20);
        SeriesDescriptor alreadySynced = new SeriesDescriptor(
                1L, "SERIES_ALPHA_001", SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                alreadySyncedUpTo);
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, alreadySynced, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(seriesRepositoryPort).updateLastSyncedDate(1L, alreadySyncedUpTo);
    }

    @Test
    void ledgerPointAuthFailureEscalatesToFailTheWholeJob() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenThrow(new LedgerPointAuthException("LedgerPoint rejected the API key"));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isTrue();
        verify(seriesValuePort, never()).upsertAll(any(), anyList());
    }

    @Test
    void ledgerPointAuthFailureRaisesTheSharedAbortFlag() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenThrow(new LedgerPointAuthException("LedgerPoint rejected the API key"));
        AtomicBoolean aborted = new AtomicBoolean(false);

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), aborted);

        assertThat(aborted).isTrue();
    }

    @Test
    void taskStartedAfterTheJobWasAbortedFailsWithoutCallingAnyone() {
        AtomicBoolean aborted = new AtomicBoolean(true);

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), aborted);

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.TARGET_UNAVAILABLE), any(), eq(0));
        verifyNoInteractions(seriesSourcePort, seriesPublisherPort);
    }

    @Test
    void abortRaisedWhilePublishingStopsBeforeTheLedgerPointCall() {
        AtomicBoolean aborted = new AtomicBoolean(false);
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any())).thenAnswer(inv -> {
            aborted.set(true); // a sibling task failed authentication while this one was fetching
            return oneDailyValue(new BigDecimal("31.25"));
        });
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), aborted);

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        verify(seriesPublisherPort, never()).publish(any(), any(), any(), anyList(), any());
        verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.FAILED), eq(0), eq(0), eq(0),
                eq(0), any());
    }

    @Test
    void activeSkipOverrideSkipsTheTaskWithoutTouchingOrionOrLedgerPoint() {
        when(ruleOverrideRepositoryPort.findActiveByOrionSeriesName("SERIES_ALPHA_001"))
                .thenReturn(Optional.of(new SeriesRuleOverride("SERIES_ALPHA_001", null, true)));

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.SKIPPED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(TASK_ID, TaskStatus.SKIPPED, null, null, 0);
        verifyNoInteractions(seriesSourcePort, seriesPublisherPort, chunkRepositoryPort);
    }

    @Test
    void keyOverrideIsUsedAsTheLedgerPointKeyButOrionIsQueriedByItsOwnName() {
        when(ruleOverrideRepositoryPort.findActiveByOrionSeriesName("SERIES_ALPHA_001"))
                .thenReturn(Optional.of(new SeriesRuleOverride("SERIES_ALPHA_001", "LP_ALPHA", false)));
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(seriesPublisherPort).publish(eq("LP_ALPHA"), any(), any(), anyList(), any());
    }

    @Test
    void seriesNotUpdatedForMoreThanFiveDaysFailsAsStaleWithoutFetchingOrPosting() {
        when(clock.now()).thenReturn(Instant.parse("2026-10-18T12:00:00Z")); // 5d 18h after the last update

        TaskOutcome outcome = worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(),
                new AtomicBoolean());

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.jobShouldFail()).isFalse();
        verify(taskRepositoryPort).updateTaskStatus(eq(TASK_ID), eq(TaskStatus.FAILED),
                eq(FailureReason.STALE_SERIES), any(), eq(0));
        verifyNoInteractions(seriesSourcePort, seriesPublisherPort, chunkRepositoryPort);
    }

    @Test
    void chunkAdvancesThroughFetchedTransformedPublishedBeforeBeingPersisted() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        InOrder order = inOrder(chunkRepositoryPort);
        order.verify(chunkRepositoryPort).markChunkStage(CHUNK_ID, ChunkStatus.FETCHED);
        order.verify(chunkRepositoryPort).markChunkStage(CHUNK_ID, ChunkStatus.TRANSFORMED);
        order.verify(chunkRepositoryPort).markChunkStage(CHUNK_ID, ChunkStatus.PUBLISHED);
        order.verify(chunkRepositoryPort).updateChunkStatus(eq(CHUNK_ID), eq(ChunkStatus.PERSISTED), eq(1), eq(1),
                eq(0), eq(0), any());
    }

    @Test
    void unchangedChunkIsNeverMarkedPublished() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        Instant sameInstant = SINGLE_DAY.atStartOfDay().atZone(java.time.ZoneId.of("America/Chicago")).toInstant();
        when(seriesValuePort.findExistingValues(eq(1L), any(), any()))
                .thenReturn(Map.of(new com.datasync.domain.model.SeriesValueKey(sameInstant, false),
                        new BigDecimal("31.25")));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(chunkRepositoryPort, never()).markChunkStage(CHUNK_ID, ChunkStatus.PUBLISHED);
        verify(metrics).valuesClassified(0, 0, 1);
    }

    @Test
    void classifiedValueCountsAreReportedToMetrics() {
        when(seriesSourcePort.fetchValues(eq("SERIES_ALPHA_001"), any(), any()))
                .thenReturn(oneDailyValue(new BigDecimal("31.25")));
        when(seriesValuePort.findExistingValues(eq(1L), any(), any())).thenReturn(Map.of());
        when(seriesPublisherPort.publish(any(), any(), any(), anyList(), any()))
                .thenReturn(new LedgerPointPublishResult(1, 1, 0, List.of()));

        worker.process(TASK_ID, series, SINGLE_DAY, SINGLE_DAY, UUID.randomUUID(), new AtomicBoolean());

        verify(metrics).valuesClassified(1, 0, 0);
    }
}

