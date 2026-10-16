package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface JobJpaRepository extends JpaRepository<JobEntity, Long> {
}
