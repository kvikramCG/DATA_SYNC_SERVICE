package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface JobTaskJpaRepository extends JpaRepository<JobTaskEntity, Long> {

    List<JobTaskEntity> findByJobId(Long jobId);

    // One conditional UPDATE: a Task that already reached a terminal status is never touched.
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update JobTaskEntity t set t.status = 'FAILED', t.failureReason = :reason, "
            + "t.failureDetail = :detail, t.finishedAt = :finishedAt "
            + "where t.jobId = :jobId and t.status in ('PENDING', 'RUNNING')")
    int failUnfinishedByJobId(@Param("jobId") Long jobId,
                              @Param("reason") String reason,
                              @Param("detail") String detail,
                              @Param("finishedAt") Instant finishedAt);
}
