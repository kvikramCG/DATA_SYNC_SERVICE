package com.datasync.application;

import com.datasync.domain.exception.LedgerPointAuthException;
import com.datasync.domain.exception.LedgerPointTransientException;
import com.datasync.domain.exception.LedgerPointUnknownSeriesException;
import com.datasync.domain.exception.LedgerPointValidationException;
import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionClientException;
import com.datasync.domain.exception.OrionTransientException;
import com.datasync.domain.exception.UnsupportedGranularityException;
import com.datasync.domain.exception.UnsupportedTimeZoneException;
import com.datasync.domain.model.ChangeType;
import com.datasync.domain.model.ChunkStatus;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.DateRange;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.LedgerPointPublishResult;
import com.datasync.domain.model.MissingInterval;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesRuleOverride;
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TaskStatus;
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
import com.datasync.domain.service.ChangeDetector;
import com.datasync.domain.service.DstAdjustmentService;
import com.datasync.domain.service.GapDetector;
import com.datasync.domain.service.IntervalAssigner;
import com.datasync.domain.service.MonthChunker;
import com.datasync.domain.service.StalenessChecker;
import com.datasync.domain.service.UnitMapper;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes one series/Task end-to-end (spec &sect;3.1 steps 1-8, architecture.md "sync-engine"):
 * apply the rule override (&sect;6.6) and staleness check (&sect;6.9) -&gt; chunk by month -&gt; fetch -&gt;
 * transform (DST + interval + unit) -&gt; diff -&gt; publish -&gt; persist -&gt; update Task status,
 * recording a {@code job_task_chunk} audit row (with its stage) and any {@code task_warning}s
 * (spec &sect;6.8) per chunk.
 * Runs on its own virtual thread, one per Task (ADR-004) - all failure types are caught here so
 * one series failing never stops the Job (Isolation Principle, architecture.md &sect;8). The one
 * exception is a LedgerPoint authentication failure, which raises the shared {@code jobAborted}
 * flag so every sibling Task stops at its next checkpoint (spec &sect;5.1: "Fail Job immediately").
 */
@Component
public class SeriesSyncWorker {

    private static final Logger log = LoggerFactory.getLogger(SeriesSyncWorker.class);

    private final SeriesSourcePort seriesSourcePort;
    private final SeriesValuePort seriesValuePort;
    private final SeriesPublisherPort seriesPublisherPort;
    private final TaskRepositoryPort taskRepositoryPort;
    private final SeriesRepositoryPort seriesRepositoryPort;
    private final ChunkRepositoryPort chunkRepositoryPort;
    private final RuleOverrideRepositoryPort ruleOverrideRepositoryPort;
    private final ClockPort clock;
    private final SyncMetricsPort metrics;
    private final int staleAfterDays;

    public SeriesSyncWorker(SeriesSourcePort seriesSourcePort,
                             SeriesValuePort seriesValuePort,
                             SeriesPublisherPort seriesPublisherPort,
                             TaskRepositoryPort taskRepositoryPort,
                             SeriesRepositoryPort seriesRepositoryPort,
                             ChunkRepositoryPort chunkRepositoryPort,
                             RuleOverrideRepositoryPort ruleOverrideRepositoryPort,
                             ClockPort clock,
                             SyncMetricsPort metrics,
                             @Value("${datasync.sync.stale-after-days:5}") int staleAfterDays) {
        this.seriesSourcePort = seriesSourcePort;
        this.seriesValuePort = seriesValuePort;
        this.seriesPublisherPort = seriesPublisherPort;
        this.taskRepositoryPort = taskRepositoryPort;
        this.seriesRepositoryPort = seriesRepositoryPort;
        this.chunkRepositoryPort = chunkRepositoryPort;
        this.ruleOverrideRepositoryPort = ruleOverrideRepositoryPort;
        this.clock = clock;
        this.metrics = metrics;
        this.staleAfterDays = staleAfterDays;
    }

    TaskOutcome process(Long taskId, SeriesDescriptor series, LocalDate windowStart, LocalDate windowEnd,
                         UUID correlationId, AtomicBoolean jobAborted) {
        try {
            return processInternal(taskId, series, windowStart, windowEnd, correlationId, jobAborted);
        } catch (JobAbortedException ex) {
            return fail(taskId, FailureReason.TARGET_UNAVAILABLE, ex.getMessage());
        } catch (UnsupportedGranularityException ex) {
            return fail(taskId, FailureReason.UNSUPPORTED_GRANULARITY, ex.getMessage());
        } catch (UnsupportedTimeZoneException ex) {
            return fail(taskId, FailureReason.UNSUPPORTED_TIMEZONE, ex.getMessage());
        } catch (LedgerPointValidationException ex) {
            return fail(taskId, FailureReason.TARGET_REJECTED, ex.getMessage());
        } catch (LedgerPointUnknownSeriesException ex) {
            return fail(taskId, FailureReason.UNKNOWN_SERIES, ex.getMessage());
        } catch (LedgerPointTransientException ex) {
            return fail(taskId, FailureReason.TARGET_UNAVAILABLE, ex.getMessage());
        } catch (LedgerPointAuthException ex) {
            // Spec §8: a LedgerPoint auth failure must fail the whole Job immediately, not just
            // this Task. Raise the shared flag so sibling Tasks stop at their next checkpoint; the
            // returned flag lets the caller mark the Job FAILED.
            jobAborted.set(true);
            taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.FAILED, FailureReason.TARGET_UNAVAILABLE,
                    ex.getMessage(), 0);
            return new TaskOutcome(TaskStatus.FAILED, true);
        } catch (OrionAuthException | OrionTransientException | OrionClientException ex) {
            return fail(taskId, FailureReason.SOURCE_UNAVAILABLE, ex.getMessage());
        } catch (RequestNotPermitted ex) {
            // The shared Orion rate limiter (5 req/s, ADR-004) could not hand out a permit within
            // its wait timeout. Without this the Task would stay PENDING forever.
            return fail(taskId, FailureReason.SOURCE_UNAVAILABLE,
                    "Timed out waiting for an Orion rate-limiter permit: " + ex.getMessage());
        } catch (RuntimeException ex) {
            // Last line of defence: an unexpected bug must not leave the Task PENDING (spec 8.1).
            log.error("Task {} failed unexpectedly", taskId, ex);
            return fail(taskId, FailureReason.INTERNAL_ERROR,
                    ex.getClass().getSimpleName() + ": " + ex.getMessage());
        }
    }

    private TaskOutcome processInternal(Long taskId, SeriesDescriptor series, LocalDate windowStart,
                                         LocalDate windowEnd, UUID correlationId, AtomicBoolean jobAborted) {
        throwIfAborted(jobAborted);

        Optional<SeriesRuleOverride> override =
                ruleOverrideRepositoryPort.findActiveByOrionSeriesName(series.name());
        if (override.isPresent() && override.get().skip()) {
            log.info("Task {} SKIPPED: series '{}' has an active skip rule override", taskId, series.name());
            taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.SKIPPED, null, null, 0);
            return new TaskOutcome(TaskStatus.SKIPPED, false);
        }

        if (series.timeZone() == null) {
            return fail(taskId, FailureReason.UNSUPPORTED_TIMEZONE,
                    "Unsupported Orion timezone code: " + series.orionTimeZoneCode());
        }

        if (windowStart.isAfter(windowEnd)) {
            return fail(taskId, FailureReason.NO_DATA, "Requested window is empty: " + windowStart
                    + " is after " + windowEnd);
        }

        if (StalenessChecker.isStale(series.lastUpdatedDate(), ZoneId.of(series.timeZone().ianaZoneId()),
                clock.now(), staleAfterDays)) {
            return fail(taskId, FailureReason.STALE_SERIES, "Series last updated "
                    + series.lastUpdatedDate().toLocalDate() + " is older than " + staleAfterDays + " days");
        }

        String ledgerPointKey = override.map(SeriesRuleOverride::effectiveKey).orElse(series.name());
        List<DateRange> chunks = MonthChunker.chunkByMonth(windowStart, windowEnd);

        int totalRawValuesSeen = 0;
        int totalValuesSent = 0;
        boolean anyWarnings = false;
        LocalDate latestReceivedDate = null;

        for (DateRange chunk : chunks) {
            throwIfAborted(jobAborted);
            ChunkResult result = processChunk(taskId, series, ledgerPointKey, chunk, correlationId, jobAborted);
            totalRawValuesSeen += result.rawValuesSeen();
            totalValuesSent += result.valuesSent();
            anyWarnings = anyWarnings || result.hasWarnings();
            if (result.latestLocalDate() != null
                    && (latestReceivedDate == null || result.latestLocalDate().isAfter(latestReceivedDate))) {
                latestReceivedDate = result.latestLocalDate();
            }
        }

        if (totalRawValuesSeen == 0) {
            return fail(taskId, FailureReason.NO_DATA, "Orion returned no values in the requested window");
        }

        // The high-watermark is the last date Orion actually returned data for - NOT windowEnd, which
        // is normalized to the end of the month and may lie in the future. It never moves backwards
        // (e.g. when a caller re-processes an older window).
        LocalDate previous = series.lastSyncedDate();
        LocalDate newLastSynced = previous != null && previous.isAfter(latestReceivedDate)
                ? previous : latestReceivedDate;
        seriesRepositoryPort.updateLastSyncedDate(series.id(), newLastSynced);
        TaskStatus finalStatus = anyWarnings ? TaskStatus.SUCCESS_WITH_WARNINGS : TaskStatus.SUCCESS;
        taskRepositoryPort.updateTaskStatus(taskId, finalStatus, null, null, totalValuesSent);
        // Lifecycle log: task outcome (cid/job/series are added by the log pattern from MDC).
        log.info("Task {} finished {}: valuesSent={}, window={}..{}",
                taskId, finalStatus, totalValuesSent, windowStart, windowEnd);
        return new TaskOutcome(finalStatus, false);
    }

    /**
     * Processes a single month-chunk, persisting a {@code job_task_chunk} row with the outcome
     * (success or failure) either way - on failure the original exception is rethrown so
     * {@link #process} still maps it to the correct Task failure reason.
     */
    private ChunkResult processChunk(Long taskId, SeriesDescriptor series, String ledgerPointKey, DateRange chunk,
                                      UUID correlationId, AtomicBoolean jobAborted) {
        Long chunkId = chunkRepositoryPort.createChunk(taskId, chunk.start(), chunk.end());
        try {
            SeriesValuesResult fetched = seriesSourcePort.fetchValues(
                    series.name(), chunk.start().atStartOfDay(), chunk.end().atTime(23, 59, 59));
            chunkRepositoryPort.updateOrionCallCount(chunkId, fetched.orionCallCount());

            if (fetched.values().isEmpty()) {
                chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.PERSISTED, 0, 0, 0, 0, null);
                return new ChunkResult(0, 0, false, null);
            }
            chunkRepositoryPort.markChunkStage(chunkId, ChunkStatus.FETCHED);
            if (fetched.unit() != null && !fetched.unit().isBlank()) {
                seriesRepositoryPort.updateUnit(series.id(), fetched.unit());
            }

            IntervalAssigner.validate(fetched.granularity());
            ZoneId zoneId = ZoneId.of(fetched.timeZone().ianaZoneId());
            List<NormalizedSeriesValue> normalized =
                    DstAdjustmentService.normalize(fetched.values(), zoneId, fetched.granularity());

            LocalDate latestLocalDate =
                    normalized.stream().map(NormalizedSeriesValue::localDate).max(LocalDate::compareTo).orElse(null);

            // Dates outside what Orion can have data for are not "missing": nothing before the series'
            // earliestDataDate, nothing after the last date actually returned (e.g. the rest of the
            // current month, or the tail of a window that extends past latestDataDate).
            LocalDate gapStart = series.earliestDataDate() != null
                    && series.earliestDataDate().toLocalDate().isAfter(chunk.start())
                    ? series.earliestDataDate().toLocalDate() : chunk.start();
            LocalDate gapEnd = latestLocalDate != null && latestLocalDate.isBefore(chunk.end())
                    ? latestLocalDate : chunk.end();
            List<MissingInterval> gaps = gapStart.isAfter(gapEnd)
                    ? List.of()
                    : GapDetector.findMissingIntervals(gapStart, gapEnd, normalized, fetched.granularity());
            for (MissingInterval gap : gaps) {
                chunkRepositoryPort.recordWarning(chunkId, WarningType.GAP_MISSING, gap.date(), gap.intervalNumber(),
                        "Orion returned no value for this interval");
            }

            Map<SeriesValueKey, BigDecimal> existing =
                    seriesValuePort.findExistingValues(series.id(), chunk.start(), chunk.end());
            List<ClassifiedSeriesValue> classified = ChangeDetector.classify(normalized, existing);
            int newCount = countByType(classified, ChangeType.NEW);
            int updatedCount = countByType(classified, ChangeType.UPDATED);
            int unchangedCount = countByType(classified, ChangeType.UNCHANGED);
            metrics.valuesClassified(newCount, updatedCount, unchangedCount);
            chunkRepositoryPort.markChunkStage(chunkId, ChunkStatus.TRANSFORMED);

            List<ClassifiedSeriesValue> toSend = classified.stream()
                    .filter(c -> c.changeType() != ChangeType.UNCHANGED)
                    .toList();

            UUID ledgerPointBatchId = null;
            if (!toSend.isEmpty()) {
                throwIfAborted(jobAborted);
                String unitCode = UnitMapper.map(fetched.unit());
                String timeZoneCode = fetched.timeZone().ledgerPointCode();
                LedgerPointPublishResult publishResult =
                        seriesPublisherPort.publish(ledgerPointKey, unitCode, timeZoneCode, toSend, correlationId);
                if (!publishResult.batchIds().isEmpty()) {
                    ledgerPointBatchId = publishResult.batchIds().get(0);
                }
                chunkRepositoryPort.markChunkStage(chunkId, ChunkStatus.PUBLISHED);

                // Persist only after LedgerPoint accepted the batch (spec §8 "Partial months").
                List<NormalizedSeriesValue> toPersist = toSend.stream().map(ClassifiedSeriesValue::value).toList();
                seriesValuePort.upsertAll(series.id(), toPersist);
            }

            chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.PERSISTED,
                    fetched.values().size(), newCount, updatedCount, unchangedCount, ledgerPointBatchId);
            // Lifecycle log: one line per month-chunk with the counts and the LedgerPoint batch id,
            // so a chunk can be matched to the LedgerPoint request carrying the same correlation id.
            log.info("Task {} chunk {}..{} persisted: fetched={}, new={}, updated={}, unchanged={}, sent={}, batchId={}",
                    taskId, chunk.start(), chunk.end(), fetched.values().size(), newCount, updatedCount,
                    unchangedCount, toSend.size(), ledgerPointBatchId);
            return new ChunkResult(fetched.values().size(), toSend.size(), !gaps.isEmpty(), latestLocalDate);
        } catch (RuntimeException ex) {
            chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.FAILED, 0, 0, 0, 0, null);
            throw ex;
        }
    }

    private int countByType(List<ClassifiedSeriesValue> classified, ChangeType type) {
        return (int) classified.stream().filter(c -> c.changeType() == type).count();
    }

    private static void throwIfAborted(AtomicBoolean jobAborted) {
        if (jobAborted.get()) {
            throw new JobAbortedException();
        }
    }

    /** Raised at a checkpoint once another Task has hit a LedgerPoint authentication failure. */
    private static final class JobAbortedException extends RuntimeException {
        JobAbortedException() {
            super("Job aborted: LedgerPoint authentication failed in another task", null, false, false);
        }
    }

    private TaskOutcome fail(Long taskId, FailureReason reason, String detail) {
        log.warn("Task {} failed: {} - {}", taskId, reason, detail);
        taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.FAILED, reason, detail, 0);
        return new TaskOutcome(TaskStatus.FAILED, false);
    }

    private record ChunkResult(int rawValuesSeen, int valuesSent, boolean hasWarnings, LocalDate latestLocalDate) {
    }
}
