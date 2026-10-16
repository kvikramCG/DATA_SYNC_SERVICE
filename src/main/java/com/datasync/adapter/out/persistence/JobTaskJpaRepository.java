package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobTaskJpaRepository extends JpaRepository<JobTaskEntity, Long> {

    List<JobTaskEntity> findByJobId(Long jobId);
}
