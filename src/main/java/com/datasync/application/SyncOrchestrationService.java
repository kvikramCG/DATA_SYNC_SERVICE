package com.datasync.application;

import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.model.DateRange;
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
import com.datasync.domain.port.out.TaskRepositoryPort;
import com.datasync.domain.service.MonthChunker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Orchestration layer (job-engine per architecture.md &sect;5) for the Job/Task lifecycle.
 * Composes {@link RefreshSeriesCatalogUseCase} for series discovery and {@link SeriesSyncWorker}
 * (sync-engine) for the per-series fetch/transform/diff/publish/persist pipeline.
 *
 * <p>Every target series' Task runs concurrently on its own virtual thread (ADR-004), globally
 * throttled by the shared Orion rate limiter inside the Orion adapter - the trigger endpoint
 * itself stays synchronous (spec &sect;7.1: "Job ran (check status/taskCounts)"), blocking until
 * every Task finishes via {@link ExecutorService#invokeAll}.
 *
 * <p><b>Scope boundary (see session TODO roadmap):</b> {@code series_rule_override} (item 11) and
 * the staleness check (item 12) are not applied yet - see {@link SeriesSyncWorker}'s Javadoc.
 */
@Service
public class SyncOrchestrationService implements TriggerSyncUseCase, GetJobUseCase {

    private static final Logger log = LoggerFactory.getLogger(SyncOrchestrationService.class);

    private final RefreshSeriesCatalogUseCase refreshSeriesCatalogUseCase;
    private final JobRepositoryPort jobRepositoryPort;
    private final TaskRepositoryPort taskRepositoryPort;
    private final SeriesSyncWorker seriesSyncWorker;
    private final ExecutorService virtualThreadExecutor;
    private final ClockPort clock;

    public SyncOrchestrationService(RefreshSeriesCatalogUseCase refreshSeriesCatalogUseCase,
                                     JobRepositoryPort jobRepositoryPort,
                                     TaskRepositoryPort taskRepositoryPort,
                                     SeriesSyncWorker seriesSyncWorker,
                                     ExecutorService virtualThreadExecutor,
                                     ClockPort clock) {
        this.refreshSeriesCatalogUseCase = refreshSeriesCatalogUseCase;
        this.jobRepositoryPort = jobRepositoryPort;
        this.taskRepositoryPort = taskRepositoryPort;
        this.seriesSyncWorker = seriesSyncWorker;
        this.virtualThreadExecutor = virtualThreadExecutor;
        this.clock = clock;
    }

    @Override
    public JobSummary triggerSync(SeriesType type, String seriesName, Instant startDate, Instant endDate,
                                   UUID correlationId) {
        Job job = jobRepositoryPort.createRunningJob(type, seriesName, startDate, endDate, correlationId);

        List<SeriesDescriptor> persisted = refreshSeriesCatalogUseCase.refreshCatalog(type);
        List<SeriesDescriptor> targets = seriesName == null
                ? persisted
                : persisted.stream().filter(d -> d.name().equals(seriesName)).toList();

        if (targets.isEmpty()) {
            // Nothing to process (unknown seriesName filter, or Orion returned no series for this
            // type - spec &sect;4.2 notes an empty list is valid) - the job can legitimately close
            // immediately rather than sit RUNNING forever.
            jobRepositoryPort.markJobFinished(job.id(), JobStatus.COMPLETED);
            return new JobSummary(job.id(), type, JobStatus.COMPLETED, job.startedAt(), clock.now(), Map.of());
        }

        List<JobTask> tasks = targets.stream()
                .map(d -> taskRepositoryPort.createTask(job.id(), d.id(), d.name()))
                .toList();

        boolean jobShouldFail = runTasksConcurrently(targets, tasks, startDate, endDate, correlationId);

        List<JobTask> finalTasks = taskRepositoryPort.findByJobId(job.id());
        Map<TaskStatus, Integer> taskCounts = countByStatus(finalTasks);
        JobStatus finalStatus = determineFinalStatus(jobShouldFail, taskCounts);
        jobRepositoryPort.markJobFinished(job.id(), finalStatus);

        return new JobSummary(job.id(), type, finalStatus, job.startedAt(), clock.now(), taskCounts);
    }

    /**
     * @return {@code true} if any Task hit a failure that must fail the whole Job immediately
     *         (a LedgerPoint auth failure - spec &sect;8).
     */
    private boolean runTasksConcurrently(List<SeriesDescriptor> targets, List<JobTask> tasks,
                                          Instant startDate, Instant endDate, UUID correlationId) {
        List<Callable<TaskOutcome>> jobsToRun = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            SeriesDescriptor series = targets.get(i);
            Long taskId = tasks.get(i).id();
            DateRange window = resolveWindow(series, startDate, endDate);
            jobsToRun.add(() -> seriesSyncWorker.process(taskId, series, window.start(), window.end(), correlationId));
        }

        boolean jobShouldFail = false;
        try {
            List<Future<TaskOutcome>> futures = virtualThreadExecutor.invokeAll(jobsToRun);
            for (Future<TaskOutcome> future : futures) {
                try {
                    TaskOutcome outcome = future.get();
                    jobShouldFail = jobShouldFail || outcome.jobShouldFail();
                } catch (ExecutionException e) {
                    // SeriesSyncWorker.process catches every expected failure itself - this is
                    // only a defensive fallback for a genuinely unexpected bug.
                    log.error("Unexpected failure processing a series task", e.getCause());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Sync job interrupted", e);
        }
        return jobShouldFail;
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
        return new JobDetail(job.id(), job.seriesType(), job.status(), tasks);
    }

    private Map<TaskStatus, Integer> countByStatus(List<JobTask> tasks) {
        Map<TaskStatus, Integer> counts = new EnumMap<>(TaskStatus.class);
        for (JobTask task : tasks) {
            counts.merge(task.status(), 1, Integer::sum);
        }
        return counts;
    }
}

