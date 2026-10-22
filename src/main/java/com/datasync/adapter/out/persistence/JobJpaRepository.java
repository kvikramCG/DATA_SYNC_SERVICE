package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface JobJpaRepository extends JpaRepository<JobEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update JobEntity j set j.status = 'FAILED', j.finishedAt = :finishedAt "
            + "where j.status = 'RUNNING' and j.startedAt < :startedBefore")
    int failRunningJobsStartedBefore(@Param("startedBefore") Instant startedBefore,
                                     @Param("finishedAt") Instant finishedAt);
}
