package com.datasync.application;

import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.model.DateRange;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobDetail;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.JobSummary;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.port.in.GetJobUseCase;
import com.datasync.domain.port.in.RefreshSeriesCatalogUseCase;
import com.datasync.domain.port.in.TriggerSyncUseCase;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import com.datasync.domain.port.out.SyncMetricsPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import com.datasync.domain.service.MonthChunker;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestration layer (job-engine per architecture.md &sect;5) for the Job/Task lifecycle.
 * Composes {@link RefreshSeriesCatalogUseCase} for series discovery and {@link SeriesSyncWorker}
 * (sync-engine) for the per-series fetch/transform/diff/publish/persist pipeline.
 *
 * <p>Every target series' Task runs concurrently on its own virtual thread (ADR-004), globally
 * throttled by the shared Orion rate limiter inside the Orion adapter. The trigger is
 * asynchronous: {@link #triggerSync} only creates the {@code RUNNING} Job row (so a concurrent
 * trigger still fails fast with {@code JobAlreadyRunningException}) and hands the rest of the
 * run to a background virtual thread, which blocks on {@link ExecutorService#invokeAll} until
 * every Task finishes. Callers poll {@link #getJob} for the outcome.
 *
 * <p>A LedgerPoint authentication failure in any Task raises a per-Job abort flag shared with
 * every worker, so sibling Tasks stop at their next checkpoint and the Job ends {@code FAILED}
 * (spec &sect;8). Abort is cooperative: a Task already blocked inside an HTTP call finishes that
 * call first.
 */
@Service
public class SyncOrchestrationService implements TriggerSyncUseCase, GetJobUseCase {

    private static final Logger log = LoggerFactory.getLogger(SyncOrchestrationService.class);
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";
    private static final String JOB_ID_MDC_KEY = "jobId";
    private static final String SERIES_NAME_MDC_KEY = "seriesName";
    // Kept under the 30s ECS stop timeout; running jobs are told to stop and given this long to close.
    private static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(20);

    private final RefreshSeriesCatalogUseCase refreshSeriesCatalogUseCase;
    private final JobRepositoryPort jobRepositoryPort;
    private final TaskRepositoryPort taskRepositoryPort;
    private final SeriesSyncWorker seriesSyncWorker;
    private final ExecutorService virtualThreadExecutor;
    private final ClockPort clock;
    private final SyncMetricsPort metrics;
    private final JobAbortRegistry abortRegistry;

    public SyncOrchestrationService(RefreshSeriesCatalogUseCase refreshSeriesCatalogUseCase,
                                     JobRepositoryPort jobRepositoryPort,
                                     TaskRepositoryPort taskRepositoryPort,
                                     SeriesSyncWorker seriesSyncWorker,
                                     ExecutorService virtualThreadExecutor,
                                     ClockPort clock,
                                     SyncMetricsPort metrics,
                                     JobAbortRegistry abortRegistry) {
        this.refreshSeriesCatalogUseCase = refreshSeriesCatalogUseCase;
        this.jobRepositoryPort = jobRepositoryPort;
        this.taskRepositoryPort = taskRepositoryPort;
        this.seriesSyncWorker = seriesSyncWorker;
        this.virtualThreadExecutor = virtualThreadExecutor;
        this.clock = clock;
        this.metrics = metrics;
        this.abortRegistry = abortRegistry;
    }

    /**
     * Tells every job running here to stop and waits briefly for them to close. Without this a deploy or
     * restart leaves the job RUNNING, and the unique partial index then rejects every new sync of that
     * series type until the reaper fires. Lives on this bean (not the registry) so it runs before the
     * repositories and the connection pool, which the closing jobs still need, are destroyed.
     */
    @PreDestroy
    void shutdown() {
        abortRegistry.abortAll();
        if (!abortRegistry.awaitIdle(SHUTDOWN_GRACE)) {
            log.warn("Some jobs were still running after {}; the stale-job reaper will close them", SHUTDOWN_GRACE);
        }
    }

    @Override
    public JobSummary triggerSync(SeriesType type, String seriesName, Instant startDate, Instant endDate,
                                   UUID correlationId) {
        if (startDate != null && endDate != null && startDate.isAfter(endDate)) {
            // Rejected before any Job row exists (mapped to HTTP 400 by the web adapter).
            throw new IllegalArgumentException("startDate must not be after endDate");
        }

        // Must stay OUTSIDE the try block: a JobAlreadyRunningException means another job owns the
        // RUNNING row, which must not be touched.
        Job job = jobRepositoryPort.createRunningJob(type, seriesName, startDate, endDate, correlationId);

        try {
            virtualThreadExecutor.execute(
                    () -> runJobInBackground(job, type, seriesName, startDate, endDate, correlationId));
        } catch (RuntimeException ex) {
            // The run was never started (e.g. executor shut down); a RUNNING row left behind would
            // block every later sync of this series type (unique partial index).
            failJobQuietly(job.id(), type, ex);
            throw ex;
        }
        // The id is returned to the caller so it can be quoted when asking about this job.
        return new JobSummary(job.id(), type, JobStatus.RUNNING, job.startedAt(), null, Map.of(), correlationId);
    }

    private void runJobInBackground(Job job, SeriesType type, String seriesName, Instant startDate,
                                     Instant endDate, UUID correlationId) {
        MDC.put(CORRELATION_ID_MDC_KEY, correlationId.toString());
        MDC.put(JOB_ID_MDC_KEY, String.valueOf(job.id()));
        // Registered before any work so the reaper or shutdown can reach the job's workers from the start.
        AtomicBoolean jobAborted = abortRegistry.register(job.id());
        try {
            runJob(job, type, seriesName, startDate, endDate, correlationId, jobAborted);
        } catch (RuntimeException ex) {
            // Nobody is waiting on this thread, so the failure is recorded on the Job row (and in
            // the log) rather than propagated.
            failJobQuietly(job.id(), type, ex);
        } finally {
            abortRegistry.unregister(job.id());
            MDC.remove(CORRELATION_ID_MDC_KEY);
            MDC.remove(JOB_ID_MDC_KEY);
        }
    }

    private void runJob(Job job, SeriesType type, String seriesName, Instant startDate, Instant endDate,
                         UUID correlationId, AtomicBoolean jobAborted) {
        List<SeriesDescriptor> persisted = refreshSeriesCatalogUseCase.refreshCatalog(type);
        List<SeriesDescriptor> targets = seriesName == null
                ? persisted
                : persisted.stream().filter(d -> d.name().equals(seriesName)).toList();

        if (targets.isEmpty()) {
            // Nothing to process (unknown seriesName filter, or Orion returned no series for this
            // type - spec &sect;4.2 notes an empty list is valid) - the job can legitimately close
            // immediately rather than sit RUNNING forever.
            finishJob(job.id(), type, JobStatus.COMPLETED);
            return;
        }

        // Lifecycle log: start of the job (cid/job are added by the log pattern from MDC).
        log.info("Sync job {} started: type={}, seriesFilter={}, targetSeries={}",
                job.id(), type.wireValue(), seriesName, targets.size());

        List<JobTask> tasks = targets.stream()
                .map(d -> taskRepositoryPort.createTask(job.id(), d.id(), d.name()))
                .toList();

        boolean jobShouldFail = runTasksConcurrently(job.id(), targets, tasks, startDate, endDate, correlationId,
                jobAborted);

        // Item 1: any Task still PENDING/RUNNING here never reported back (e.g. its own status write failed).
        // Failing it now keeps it from sitting "waiting" forever and makes the job counts below honest.
        failUnfinishedTasks(job.id(), "Task did not finish before the job ended");

        List<JobTask> finalTasks = taskRepositoryPort.findByJobId(job.id());
        finalTasks.forEach(t -> metrics.taskFinished(t.status(), t.failureReason()));
        Map<TaskStatus, Integer> taskCounts = countByStatus(finalTasks);
        // Lifecycle log: per-status task totals, so a job's outcome is visible without querying the DB.
        log.info("Sync job {} tasks finished: {}", job.id(), taskCounts);
        // An abort raised from outside (reaper, shutdown) must also end the job FAILED, not "with failures".
        JobStatus finalStatus = determineFinalStatus(jobShouldFail || jobAborted.get(), taskCounts);
        finishJob(job.id(), type, finalStatus);
    }

    private void failUnfinishedTasks(Long jobId, String detail) {
        try {
            int failed = taskRepositoryPort.failUnfinishedTasks(jobId, FailureReason.INTERNAL_ERROR, detail);
            if (failed > 0) {
                log.warn("Sync job {}: failed {} task(s) that never reached a final status", jobId, failed);
            }
        } catch (RuntimeException ex) {
            // Cleanup must not hide the failure that got us here or stop the job from being closed.
            log.error("Could not fail the unfinished tasks of job {}", jobId, ex);
        }
    }

    private void finishJob(Long jobId, SeriesType type, JobStatus status) {
        boolean closed = jobRepositoryPort.markJobFinished(jobId, status);
        if (!closed) {
            // The job was already closed - in practice by the stale-job reaper while this (slow but
            // still running) job kept working. First close wins, so the stored status is left alone.
            // We also skip the job metric and the "finished" log: the stored status differs from the
            // one computed here, and the reaper path records no job metric, so emitting one would
            // report an outcome that is not what the API returns. The WARN keeps the discrepancy
            // visible (cid/job are added to the line from MDC).
            log.warn("Sync job {} not closed as {}: it was already closed (likely reaped as stale); "
                    + "keeping the stored status", jobId, status);
            return;
        }
        metrics.jobFinished(type, status);
        // Lifecycle log: final job status (also reached via the failure and empty-job paths).
        log.info("Sync job {} finished: type={}, status={}", jobId, type.wireValue(), status);
    }

    private void failJobQuietly(Long jobId, SeriesType type, RuntimeException cause) {
        log.error("Sync job {} failed unexpectedly, marking it FAILED", jobId, cause);
        // Item 1: Tasks created before the failure would otherwise stay PENDING on a job that is already over.
        failUnfinishedTasks(jobId, "Job failed before this task finished: " + cause.getClass().getSimpleName());
        try {
            finishJob(jobId, type, JobStatus.FAILED);
        } catch (RuntimeException markFailure) {
            // Do not mask the original failure. The stale-job reaper will close the row later.
            log.error("Could not mark job {} FAILED; the stale-job reaper will close it", jobId, markFailure);
        }
    }

    /**
     * @return {@code true} if any Task hit a failure that must fail the whole Job immediately
     *         (a LedgerPoint auth failure - spec &sect;8).
     */
    private boolean runTasksConcurrently(Long jobId, List<SeriesDescriptor> targets, List<JobTask> tasks,
                                          Instant startDate, Instant endDate, UUID correlationId,
                                          AtomicBoolean jobAborted) {
        List<Callable<TaskOutcome>> jobsToRun = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            SeriesDescriptor series = targets.get(i);
            Long taskId = tasks.get(i).id();
            DateRange window = resolveWindow(series, startDate, endDate);
            jobsToRun.add(() -> {
                // MDC is thread-local and each Task runs on its own virtual thread, so the context
                // is set (and cleared) here rather than inherited.
                MDC.put(CORRELATION_ID_MDC_KEY, correlationId.toString());
                MDC.put(JOB_ID_MDC_KEY, String.valueOf(jobId));
                MDC.put(SERIES_NAME_MDC_KEY, series.name());
                try {
                    return seriesSyncWorker.process(
                            taskId, series, window.start(), window.end(), correlationId, jobAborted);
                } finally {
                    MDC.remove(CORRELATION_ID_MDC_KEY);
                    MDC.remove(JOB_ID_MDC_KEY);
                    MDC.remove(SERIES_NAME_MDC_KEY);
                }
            });
        }

        boolean jobShouldFail = false;
        try {
            List<Future<TaskOutcome>> futures = virtualThreadExecutor.invokeAll(jobsToRun);
            for (int i = 0; i < futures.size(); i++) {
                try {
                    TaskOutcome outcome = futures.get(i).get();
                    jobShouldFail = jobShouldFail || outcome.jobShouldFail();
                } catch (ExecutionException e) {
                    // SeriesSyncWorker.process catches every failure itself - this is only a
                    // defensive fallback (e.g. an Error) so the Task is never left PENDING.
                    log.error("Unexpected failure processing a series task", e.getCause());
                    markTaskInternalError(tasks.get(i).id(), e.getCause());
                } catch (CancellationException e) {
                    // Unchecked, so it would otherwise escape and abort the loop, leaving the remaining Tasks uncollected.
                    log.error("A series task was cancelled before it finished", e);
                    markTaskInternalError(tasks.get(i).id(), e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Sync job interrupted", e);
        }
        return jobShouldFail;
    }

    private void markTaskInternalError(Long taskId, Throwable cause) {
        try {
            taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.FAILED, FailureReason.INTERNAL_ERROR,
                    cause.getClass().getSimpleName() + (cause.getMessage() == null ? "" : ": " + cause.getMessage()), 0);
        } catch (RuntimeException ex) {
            log.error("Could not mark task {} FAILED after an unexpected error", taskId, ex);
        }
    }

    /**
     * Spec &sect;3.1 step 1: caller overrides win; otherwise start = last successfully synced
     * date (or 1 year prior to today if never synced), end = the series' latest data date,
     * normalized to the last day of its month.
     */
    private DateRange resolveWindow(SeriesDescriptor series, Instant callerStart, Instant callerEnd) {
        LocalDate start = callerStart != null
                ? callerStart.atZone(ZoneOffset.UTC).toLocalDate()
                : series.lastSyncedDate() != null
                        ? series.lastSyncedDate()
                        : clock.now().atZone(ZoneOffset.UTC).toLocalDate().minusYears(1);

        LocalDate end = callerEnd != null
                ? callerEnd.atZone(ZoneOffset.UTC).toLocalDate()
                : series.latestDataDate() != null
                        ? series.latestDataDate().toLocalDate()
                        : clock.now().atZone(ZoneOffset.UTC).toLocalDate();

        return new DateRange(start, MonthChunker.normalizeEndToLastDayOfMonth(end));
    }

    private JobStatus determineFinalStatus(boolean jobShouldFail, Map<TaskStatus, Integer> taskCounts) {
        if (jobShouldFail) {
            return JobStatus.FAILED;
        }
        int failedCount = taskCounts.getOrDefault(TaskStatus.FAILED, 0);
        return failedCount == 0 ? JobStatus.COMPLETED : JobStatus.COMPLETED_WITH_FAILURES;
    }

    @Override
    public JobDetail getJob(Long jobId) {
        Job job = jobRepositoryPort.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException("No job found with id " + jobId));
        List<JobTask> tasks = taskRepositoryPort.findByJobId(jobId);
        // job.correlationId() is the value persisted at creation, so GET reports exactly what was logged/sent.
        return new JobDetail(job.id(), job.seriesType(), job.status(), tasks, job.correlationId());
    }

    private Map<TaskStatus, Integer> countByStatus(List<JobTask> tasks) {
        Map<TaskStatus, Integer> counts = new EnumMap<>(TaskStatus.class);
        for (JobTask task : tasks) {
            counts.merge(task.status(), 1, Integer::sum);
        }
        return counts;
    }
}

