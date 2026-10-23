package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface JobJpaRepository extends JpaRepository<JobEntity, Long> {

    // Lets the reaper log WHICH jobs it is about to fail (id + correlation id), not just a count.
    List<JobEntity> findByStatusAndStartedAtBefore(String status, Instant startedBefore);

    // DECISION (first close wins): a job is closed only while it is still RUNNING. The reaper and the
    // job's own thread can both try to close the same row (a slow but healthy job gets reaped, then
    // finishes later). Doing the check inside the UPDATE's WHERE clause makes it atomic in the database -
    // a read-then-write in Java could let both writers pass the check. A terminal status is therefore
    // final: the second closer updates 0 rows and the caller decides what to do (log, skip metrics).
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update JobEntity j set j.status = :status, j.finishedAt = :finishedAt "
            + "where j.id = :id and j.status = 'RUNNING'")
    int closeIfRunning(@Param("id") Long id,
                       @Param("status") String status,
                       @Param("finishedAt") Instant finishedAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update JobEntity j set j.status = 'FAILED', j.finishedAt = :finishedAt "
            + "where j.status = 'RUNNING' and j.startedAt < :startedBefore")
    int failRunningJobsStartedBefore(@Param("startedBefore") Instant startedBefore,
                                     @Param("finishedAt") Instant finishedAt);
}
