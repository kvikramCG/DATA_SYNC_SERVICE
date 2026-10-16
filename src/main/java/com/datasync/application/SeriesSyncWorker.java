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
import com.datasync.domain.model.SeriesValueKey;
import com.datasync.domain.model.SeriesValuesResult;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.WarningType;
import com.datasync.domain.port.out.ChunkRepositoryPort;
import com.datasync.domain.port.out.SeriesPublisherPort;
import com.datasync.domain.port.out.SeriesRepositoryPort;
import com.datasync.domain.port.out.SeriesSourcePort;
import com.datasync.domain.port.out.SeriesValuePort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import com.datasync.domain.service.ChangeDetector;
import com.datasync.domain.service.DstAdjustmentService;
import com.datasync.domain.service.GapDetector;
import com.datasync.domain.service.IntervalAssigner;
import com.datasync.domain.service.MonthChunker;
import com.datasync.domain.service.UnitMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Processes one series/Task end-to-end (spec &sect;3.1 steps 1-8, architecture.md "sync-engine"):
 * resolve window (done by the caller) -&gt; chunk by month -&gt; fetch -&gt; transform (DST +
 * interval + unit) -&gt; diff -&gt; publish -&gt; persist -&gt; update Task status, recording a
 * {@code job_task_chunk} audit row and any {@code task_warning}s (spec &sect;6.8) per chunk.
 * Runs on its own virtual thread, one per Task (ADR-004) - all failure types are caught here so
 * one series failing never stops the Job (Isolation Principle, architecture.md &sect;8).
 *
 * <p><b>Scope boundary:</b> {@code series_rule_override} lookup (roadmap item 11) and the
 * staleness check from spec &sect;6.9 (roadmap item 12) are deliberately not implemented yet -
 * the LedgerPoint {@code key} always defaults to the Orion series name, and a Task never finishes
 * {@code SKIPPED} until item 11 lands.
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

    public SeriesSyncWorker(SeriesSourcePort seriesSourcePort,
                             SeriesValuePort seriesValuePort,
                             SeriesPublisherPort seriesPublisherPort,
                             TaskRepositoryPort taskRepositoryPort,
                             SeriesRepositoryPort seriesRepositoryPort,
                             ChunkRepositoryPort chunkRepositoryPort) {
        this.seriesSourcePort = seriesSourcePort;
        this.seriesValuePort = seriesValuePort;
        this.seriesPublisherPort = seriesPublisherPort;
        this.taskRepositoryPort = taskRepositoryPort;
        this.seriesRepositoryPort = seriesRepositoryPort;
        this.chunkRepositoryPort = chunkRepositoryPort;
    }

    TaskOutcome process(Long taskId, SeriesDescriptor series, LocalDate windowStart, LocalDate windowEnd,
                         UUID correlationId) {
        try {
            return processInternal(taskId, series, windowStart, windowEnd, correlationId);
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
            // this Task - the caller escalates based on the returned flag.
            taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.FAILED, FailureReason.TARGET_UNAVAILABLE,
                    ex.getMessage(), 0);
            return new TaskOutcome(TaskStatus.FAILED, true);
        } catch (OrionAuthException | OrionTransientException | OrionClientException ex) {
            return fail(taskId, FailureReason.SOURCE_UNAVAILABLE, ex.getMessage());
        }
    }

    private TaskOutcome processInternal(Long taskId, SeriesDescriptor series, LocalDate windowStart,
                                         LocalDate windowEnd, UUID correlationId) {
        List<DateRange> chunks = MonthChunker.chunkByMonth(windowStart, windowEnd);

        int totalRawValuesSeen = 0;
        int totalValuesSent = 0;
        boolean anyWarnings = false;

        for (DateRange chunk : chunks) {
            ChunkResult result = processChunk(taskId, series, chunk, correlationId);
            totalRawValuesSeen += result.rawValuesSeen();
            totalValuesSent += result.valuesSent();
            anyWarnings = anyWarnings || result.hasWarnings();
        }

        if (totalRawValuesSeen == 0) {
            return fail(taskId, FailureReason.NO_DATA, "Orion returned no values in the requested window");
        }

        seriesRepositoryPort.updateLastSyncedDate(series.id(), windowEnd);
        TaskStatus finalStatus = anyWarnings ? TaskStatus.SUCCESS_WITH_WARNINGS : TaskStatus.SUCCESS;
        taskRepositoryPort.updateTaskStatus(taskId, finalStatus, null, null, totalValuesSent);
        return new TaskOutcome(finalStatus, false);
    }

    /**
     * Processes a single month-chunk, persisting a {@code job_task_chunk} row with the outcome
     * (success or failure) either way - on failure the original exception is rethrown so
     * {@link #process} still maps it to the correct Task failure reason.
     */
    private ChunkResult processChunk(Long taskId, SeriesDescriptor series, DateRange chunk, UUID correlationId) {
        Long chunkId = chunkRepositoryPort.createChunk(taskId, chunk.start(), chunk.end());
        try {
            SeriesValuesResult fetched = seriesSourcePort.fetchValues(
                    series.name(), chunk.start().atStartOfDay(), chunk.end().atTime(23, 59, 59));

            if (fetched.values().isEmpty()) {
                chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.PERSISTED, 0, 0, 0, 0, null);
                return new ChunkResult(0, 0, false);
            }

            IntervalAssigner.validate(fetched.granularity());
            ZoneId zoneId = ZoneId.of(fetched.timeZone().ianaZoneId());
            List<NormalizedSeriesValue> normalized =
                    DstAdjustmentService.normalize(fetched.values(), zoneId, fetched.granularity());

            List<MissingInterval> gaps =
                    GapDetector.findMissingIntervals(chunk.start(), chunk.end(), normalized, fetched.granularity());
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

            List<ClassifiedSeriesValue> toSend = classified.stream()
                    .filter(c -> c.changeType() != ChangeType.UNCHANGED)
                    .toList();

            UUID ledgerPointBatchId = null;
            if (!toSend.isEmpty()) {
                String unitCode = UnitMapper.map(fetched.unit());
                String timeZoneCode = fetched.timeZone().ledgerPointCode();
                LedgerPointPublishResult publishResult =
                        seriesPublisherPort.publish(series.name(), unitCode, timeZoneCode, toSend, correlationId);
                if (!publishResult.batchIds().isEmpty()) {
                    ledgerPointBatchId = publishResult.batchIds().get(0);
                }

                // Persist only after LedgerPoint accepted the batch (spec §8 "Partial months").
                List<NormalizedSeriesValue> toPersist = toSend.stream().map(ClassifiedSeriesValue::value).toList();
                seriesValuePort.upsertAll(series.id(), toPersist);
            }

            chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.PERSISTED,
                    fetched.values().size(), newCount, updatedCount, unchangedCount, ledgerPointBatchId);
            return new ChunkResult(fetched.values().size(), toSend.size(), !gaps.isEmpty());
        } catch (RuntimeException ex) {
            chunkRepositoryPort.updateChunkStatus(chunkId, ChunkStatus.FAILED, 0, 0, 0, 0, null);
            throw ex;
        }
    }

    private int countByType(List<ClassifiedSeriesValue> classified, ChangeType type) {
        return (int) classified.stream().filter(c -> c.changeType() == type).count();
    }

    private TaskOutcome fail(Long taskId, FailureReason reason, String detail) {
        log.warn("Task {} failed: {} - {}", taskId, reason, detail);
        taskRepositoryPort.updateTaskStatus(taskId, TaskStatus.FAILED, reason, detail, 0);
        return new TaskOutcome(TaskStatus.FAILED, false);
    }

    private record ChunkResult(int rawValuesSeen, int valuesSent, boolean hasWarnings) {
    }
}
