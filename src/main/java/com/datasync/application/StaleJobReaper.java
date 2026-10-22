package com.datasync.application;

import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Closes jobs left {@code RUNNING} for longer than {@code datasync.job.max-running-duration}
 * (for example after a process crash). A stuck RUNNING row would otherwise block every later sync
 * of that series type, because of the unique partial index {@code uq_job_running_per_type}
 * (architecture.md "Concurrency &amp; Job Locking").
 *
 * <p>The max duration must be comfortably longer than the longest legitimate job.
 */
@Component
public class StaleJobReaper {

    private static final Logger log = LoggerFactory.getLogger(StaleJobReaper.class);

    private final JobRepositoryPort jobRepositoryPort;
    private final ClockPort clock;
    private final Duration maxRunningDuration;

    public StaleJobReaper(JobRepositoryPort jobRepositoryPort,
                          ClockPort clock,
                          @Value("${datasync.job.max-running-duration:PT2H}") Duration maxRunningDuration) {
        this.jobRepositoryPort = jobRepositoryPort;
        this.clock = clock;
        this.maxRunningDuration = maxRunningDuration;
    }

    @Scheduled(fixedDelayString = "${datasync.job.reaper-interval:PT5M}",
            initialDelayString = "${datasync.job.reaper-initial-delay:PT1M}")
    public void reap() {
        Instant cutoff = clock.now().minus(maxRunningDuration);
        int failed = jobRepositoryPort.failStaleRunningJobs(cutoff);
        if (failed > 0) {
            log.warn("Marked {} job(s) FAILED because they were RUNNING since before {}", failed, cutoff);
        }
    }
}
