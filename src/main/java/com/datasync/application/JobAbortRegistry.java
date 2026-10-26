package com.datasync.application;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-job abort flags for the jobs running in this JVM. Workers poll the flag at their checkpoints, so
 * anything that closes a job from the outside (stale-job reaper, shutdown) can also stop its workers,
 * which a database status change alone cannot do.
 *
 * <p>In-process only: a reaper on another instance cannot reach a job running here. The conditional
 * close in {@code JobRepositoryPort#markJobFinished} still stops such a job from overwriting its status.
 */
@Component
public class JobAbortRegistry {

    private final ConcurrentMap<Long, AtomicBoolean> flags = new ConcurrentHashMap<>();

    AtomicBoolean register(Long jobId) {
        AtomicBoolean flag = new AtomicBoolean(false);
        flags.put(jobId, flag);
        return flag;
    }

    void unregister(Long jobId) {
        flags.remove(jobId);
    }

    /** @return {@code true} if the job is running in this JVM and its flag was raised. */
    boolean abort(Long jobId) {
        AtomicBoolean flag = flags.get(jobId);
        if (flag == null) {
            return false;
        }
        flag.set(true);
        return true;
    }

    void abortAll() {
        flags.values().forEach(flag -> flag.set(true));
    }

    /** @return {@code true} if every registered job finished within {@code timeout}. */
    boolean awaitIdle(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!flags.isEmpty()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return flags.isEmpty();
            }
        }
        return true;
    }
}
