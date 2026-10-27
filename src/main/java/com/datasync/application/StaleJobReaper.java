package com.datasync.application;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Closes jobs left {@code RUNNING} for longer than {@code datasync.job.max-running-duration}
 * (for example after a process crash). A stuck RUNNING row would otherwise block every later sync
 * of that series type, because of the unique partial index {@code uq_job_running_per_type}
 * (architecture.md "Concurrency &amp; Job Locking").
 *
 * <p>The max duration must be comfortably longer than the longest legitimate job. Closing the row does
 * not stop a job that is still working, so the reaper also raises that job's abort flag (workers stop at
 * their next checkpoint) and fails its unfinished Tasks.
 */
@Component
public class StaleJobReaper {

    private static final Logger log = LoggerFactory.getLogger(StaleJobReaper.class);

    private final JobRepositoryPort jobRepositoryPort;
    private final TaskRepositoryPort taskRepositoryPort;
    private final JobAbortRegistry abortRegistry;
    private final ClockPort clock;
    private final Duration maxRunningDuration;

    public StaleJobReaper(JobRepositoryPort jobRepositoryPort,
                          TaskRepositoryPort taskRepositoryPort,
                          JobAbortRegistry abortRegistry,
                          ClockPort clock,
                          @Value("${datasync.job.max-running-duration:PT6H}") Duration maxRunningDuration) {
        this.jobRepositoryPort = jobRepositoryPort;
        this.taskRepositoryPort = taskRepositoryPort;
        this.abortRegistry = abortRegistry;
        this.clock = clock;
        this.maxRunningDuration = maxRunningDuration;
    }

    @Scheduled(fixedDelayString = "${datasync.job.reaper-interval:PT5M}",
            initialDelayString = "${datasync.job.reaper-initial-delay:PT1M}")
    public void reap() {
        Instant cutoff = clock.now().minus(maxRunningDuration);
        List<Long> failedJobIds = jobRepositoryPort.failStaleRunningJobs(cutoff);
        if (failedJobIds.isEmpty()) {
            return;
        }
        log.warn("Marked {} job(s) FAILED because they were RUNNING since before {}: {}",
                failedJobIds.size(), cutoff, failedJobIds);
        for (Long jobId : failedJobIds) {
            // One job's cleanup failing must not leave the remaining reaped jobs half-cleaned.
            try {
                boolean stopped = abortRegistry.abort(jobId);
                int failedTasks = taskRepositoryPort.failUnfinishedTasks(jobId, FailureReason.INTERNAL_ERROR,
                        "Job " + jobId + " was closed as stale before this task finished");
                log.warn("Reaped job {}: workers in this instance {}, {} unfinished task(s) failed",
                        jobId, stopped ? "told to stop" : "not running here", failedTasks);
            } catch (RuntimeException ex) {
                log.error("Could not clean up reaped job {}", jobId, ex);
            }
        }
    }
}
