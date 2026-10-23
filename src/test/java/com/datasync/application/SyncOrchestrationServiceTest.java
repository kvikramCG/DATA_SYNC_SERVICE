package com.datasync.application;

import com.datasync.domain.exception.JobAlreadyRunningException;
import com.datasync.domain.exception.OrionTransientException;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.TimeZoneCode;
import com.datasync.domain.port.in.RefreshSeriesCatalogUseCase;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import com.datasync.domain.port.out.SyncMetricsPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the Job is never left {@code RUNNING} when something fails after it was created
 * (a RUNNING row blocks every later sync of that series type through the unique partial index).
 */
@ExtendWith(MockitoExtension.class)
class SyncOrchestrationServiceTest {

    private static final Long JOB_ID = 42L;
    private static final long WAIT_MS = 5000;
    private static final Instant NOW = Instant.parse("2026-10-16T10:00:00Z");

    @Mock
    private RefreshSeriesCatalogUseCase refreshSeriesCatalogUseCase;
    @Mock
    private JobRepositoryPort jobRepositoryPort;
    @Mock
    private TaskRepositoryPort taskRepositoryPort;
    @Mock
    private SeriesSyncWorker seriesSyncWorker;
    @Mock
    private ClockPort clock;
    @Mock
    private SyncMetricsPort metrics;

    private ExecutorService executor;
    private SyncOrchestrationService service;

    private final Job runningJob = new Job(JOB_ID, SeriesType.TYPE_A, JobStatus.RUNNING, null, null, null,
            NOW, null, UUID.randomUUID());

    private final SeriesDescriptor series = new SeriesDescriptor(
            1L, "SERIES_ALPHA_001", SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
            LocalDateTime.of(2026, 10, 12, 18, 0),
            LocalDateTime.of(2020, 1, 1, 0, 0),
            LocalDateTime.of(2026, 10, 13, 23, 0),
            null);

    @BeforeEach
    void setUp() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
        service = new SyncOrchestrationService(refreshSeriesCatalogUseCase, jobRepositoryPort, taskRepositoryPort,
                seriesSyncWorker, executor, clock, metrics);
        lenient().when(jobRepositoryPort.createRunningJob(any(), any(), any(), any(), any())).thenReturn(runningJob);
        // markJobFinished now returns whether this call closed the job; a bare mock would return false
        // ("already closed") and every test would hit the rejected path, so default to the normal case.
        lenient().when(jobRepositoryPort.markJobFinished(any(), any())).thenReturn(true);
        lenient().when(clock.now()).thenReturn(NOW);
    }

    @Test
    void catalogRefreshFailureMarksTheJobFailed() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A))
                .thenThrow(new OrionTransientException("Orion is down"));

        var summary = service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        assertThat(summary.status()).isEqualTo(JobStatus.RUNNING);
        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.FAILED);
    }

    @Test
    void taskCreationFailureMarksTheJobFailed() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series));
        when(taskRepositoryPort.createTask(anyLong(), any(), any()))
                .thenThrow(new IllegalStateException("database unavailable"));

        service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.FAILED);
    }

    @Test
    void failureToMarkTheJobFailedDoesNotEscapeTheBackgroundRun() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A))
                .thenThrow(new OrionTransientException("Orion is down"));
        org.mockito.Mockito.doThrow(new IllegalStateException("database unavailable too"))
                .when(jobRepositoryPort).markJobFinished(JOB_ID, JobStatus.FAILED);

        var summary = service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        assertThat(summary.status()).isEqualTo(JobStatus.RUNNING);
        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.FAILED);
    }

    @Test
    void triggerReturnsARunningSummaryWithoutWaitingForTheTasks() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series));
        JobTask task = new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.PENDING, null, null, 0, null, null);
        when(taskRepositoryPort.createTask(JOB_ID, 1L, "SERIES_ALPHA_001")).thenReturn(task);
        when(seriesSyncWorker.process(eq(7L), eq(series), any(), any(), any(), any())).thenAnswer(inv -> {
            release.await();
            return new TaskOutcome(TaskStatus.SUCCESS, false);
        });
        when(taskRepositoryPort.findByJobId(JOB_ID)).thenReturn(List.of(
                new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.SUCCESS, null, null, 1, NOW, NOW)));

        var summary = service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        assertThat(summary.jobId()).isEqualTo(JOB_ID);
        assertThat(summary.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(summary.finishedAt()).isNull();
        assertThat(summary.taskCounts()).isEmpty();
        verify(jobRepositoryPort, never()).markJobFinished(any(), any());

        release.countDown();
        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.COMPLETED);
    }

    @Test
    void executorRejectionMarksTheJobFailedAndRethrows() {
        executor.shutdown();

        assertThatThrownBy(() -> service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID()))
                .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);

        verify(jobRepositoryPort).markJobFinished(JOB_ID, JobStatus.FAILED);
    }

    @Test
    void startDateAfterEndDateIsRejectedBeforeAnyJobIsCreated() {
        Instant start = Instant.parse("2026-11-01T00:00:00Z");
        Instant end = Instant.parse("2026-10-01T00:00:00Z");

        assertThatThrownBy(() -> service.triggerSync(SeriesType.TYPE_A, null, start, end, UUID.randomUUID()))
                .isInstanceOf(IllegalArgumentException.class);

        verify(jobRepositoryPort, never()).createRunningJob(any(), any(), any(), any(), any());
    }

    @Test
    void workerRunsWithJobIdAndSeriesNameInTheLoggingContext() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series));
        JobTask task = new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.PENDING, null, null, 0, null, null);
        when(taskRepositoryPort.createTask(JOB_ID, 1L, "SERIES_ALPHA_001")).thenReturn(task);
        java.util.Map<String, String> seen = new java.util.concurrent.ConcurrentHashMap<>();
        when(seriesSyncWorker.process(eq(7L), eq(series), any(), any(), any(), any())).thenAnswer(inv -> {
            seen.put("jobId", String.valueOf(org.slf4j.MDC.get("jobId")));
            seen.put("seriesName", String.valueOf(org.slf4j.MDC.get("seriesName")));
            seen.put("correlationId", String.valueOf(org.slf4j.MDC.get("correlationId")));
            return new TaskOutcome(TaskStatus.SUCCESS, false);
        });
        when(taskRepositoryPort.findByJobId(JOB_ID)).thenReturn(List.of(
                new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.SUCCESS, null, null, 1, NOW, NOW)));
        UUID correlationId = UUID.randomUUID();

        service.triggerSync(SeriesType.TYPE_A, null, null, null, correlationId);

        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.COMPLETED);
        assertThat(seen).containsEntry("jobId", String.valueOf(JOB_ID))
                .containsEntry("seriesName", "SERIES_ALPHA_001")
                .containsEntry("correlationId", correlationId.toString());
    }

    @Test
    void unexpectedWorkerFailureMarksTheTaskInternalErrorInsteadOfLeavingItPending() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series));
        JobTask task = new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.PENDING, null, null, 0, null, null);
        when(taskRepositoryPort.createTask(JOB_ID, 1L, "SERIES_ALPHA_001")).thenReturn(task);
        when(seriesSyncWorker.process(eq(7L), eq(series), any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("boom"));
        when(taskRepositoryPort.findByJobId(JOB_ID)).thenReturn(List.of(
                new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.FAILED, FailureReason.INTERNAL_ERROR,
                        "boom", 0, NOW, NOW)));

        service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        verify(taskRepositoryPort, timeout(WAIT_MS)).updateTaskStatus(eq(7L), eq(TaskStatus.FAILED),
                eq(FailureReason.INTERNAL_ERROR), any(), eq(0));
        verify(jobRepositoryPort, timeout(WAIT_MS)).markJobFinished(JOB_ID, JobStatus.COMPLETED_WITH_FAILURES);
    }

    @Test
    void jobAlreadyRunningDoesNotTouchTheExistingRunningJob() {
        when(jobRepositoryPort.createRunningJob(any(), any(), any(), any(), any()))
                .thenThrow(new JobAlreadyRunningException("already running"));

        assertThatThrownBy(() -> service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID()))
                .isInstanceOf(JobAlreadyRunningException.class);

        verify(jobRepositoryPort, never()).markJobFinished(any(), any());
    }

    @Test
    void successfulRunStillClosesTheJobWithTheComputedStatus() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series));
        JobTask task = new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.PENDING, null, null, 0, null, null);
        JobTask done = new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.SUCCESS, null, null, 1, NOW, NOW);
        when(taskRepositoryPort.createTask(JOB_ID, 1L, "SERIES_ALPHA_001")).thenReturn(task);
        when(seriesSyncWorker.process(eq(7L), eq(series), any(), any(), any(), any()))
                .thenReturn(new TaskOutcome(TaskStatus.SUCCESS, false));
        when(taskRepositoryPort.findByJobId(JOB_ID)).thenReturn(List.of(done));

        var summary = service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        assertThat(summary.status()).isEqualTo(JobStatus.RUNNING);
        verify(metrics, timeout(WAIT_MS)).jobFinished(SeriesType.TYPE_A, JobStatus.COMPLETED);
        verify(jobRepositoryPort).markJobFinished(JOB_ID, JobStatus.COMPLETED);
        verify(jobRepositoryPort, never()).markJobFinished(JOB_ID, JobStatus.FAILED);
        verify(metrics).taskFinished(TaskStatus.SUCCESS, null);
    }

    @Test
    void ledgerPointAuthFailureFailsTheJobAndAllWorkersShareOneAbortFlag() {
        SeriesDescriptor second = new SeriesDescriptor(
                2L, "SERIES_BETA_002", SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0), LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0), null);
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of(series, second));
        when(taskRepositoryPort.createTask(JOB_ID, 1L, "SERIES_ALPHA_001"))
                .thenReturn(new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.PENDING, null, null, 0, null, null));
        when(taskRepositoryPort.createTask(JOB_ID, 2L, "SERIES_BETA_002"))
                .thenReturn(new JobTask(8L, JOB_ID, 2L, "SERIES_BETA_002", TaskStatus.PENDING, null, null, 0, null, null));
        when(seriesSyncWorker.process(eq(7L), any(), any(), any(), any(), any()))
                .thenReturn(new TaskOutcome(TaskStatus.FAILED, true));
        when(seriesSyncWorker.process(eq(8L), any(), any(), any(), any(), any()))
                .thenReturn(new TaskOutcome(TaskStatus.FAILED, false));
        when(taskRepositoryPort.findByJobId(JOB_ID)).thenReturn(List.of(
                new JobTask(7L, JOB_ID, 1L, "SERIES_ALPHA_001", TaskStatus.FAILED,
                        FailureReason.TARGET_UNAVAILABLE, "auth", 0, NOW, NOW),
                new JobTask(8L, JOB_ID, 2L, "SERIES_BETA_002", TaskStatus.FAILED,
                        FailureReason.TARGET_UNAVAILABLE, "aborted", 0, NOW, NOW)));

        service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        verify(metrics, timeout(WAIT_MS)).jobFinished(SeriesType.TYPE_A, JobStatus.FAILED);
        verify(jobRepositoryPort).markJobFinished(JOB_ID, JobStatus.FAILED);
        ArgumentCaptor<AtomicBoolean> flags = ArgumentCaptor.forClass(AtomicBoolean.class);
        verify(seriesSyncWorker, org.mockito.Mockito.times(2)).process(any(), any(), any(), any(), any(), flags.capture());
        List<AtomicBoolean> captured = new ArrayList<>(flags.getAllValues());
        assertThat(captured.get(0)).isSameAs(captured.get(1));
    }

    @Test
    void unexpectedFailureIsCountedAsAFailedJobInMetrics() {
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A))
                .thenThrow(new OrionTransientException("Orion is down"));

        service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        verify(metrics, timeout(WAIT_MS)).jobFinished(SeriesType.TYPE_A, JobStatus.FAILED);
    }

    @Test
    void alreadyClosedJobIsNotReportedInMetricsWhenTheCloseIsRejected() throws Exception {
        // An empty catalog closes the job straight away (no tasks), so nothing else is submitted to
        // the executor afterwards and we can wait for the background run to finish deterministically.
        when(refreshSeriesCatalogUseCase.refreshCatalog(SeriesType.TYPE_A)).thenReturn(List.of());
        // The reaper already closed this job: the port reports that nothing was changed.
        when(jobRepositoryPort.markJobFinished(JOB_ID, JobStatus.COMPLETED)).thenReturn(false);

        service.triggerSync(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());
        executor.shutdown();
        assertThat(executor.awaitTermination(WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS)).isTrue();

        verify(jobRepositoryPort).markJobFinished(JOB_ID, JobStatus.COMPLETED);
        // The stored status was not changed, so no job outcome may be reported.
        verify(metrics, never()).jobFinished(any(), any());
    }
}
