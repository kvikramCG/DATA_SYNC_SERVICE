package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JobTaskChunkJpaRepository extends JpaRepository<JobTaskChunkEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update JobTaskChunkEntity c set c.status = :status where c.id = :chunkId")
    int updateStatus(@Param("chunkId") Long chunkId, @Param("status") String status);
}
