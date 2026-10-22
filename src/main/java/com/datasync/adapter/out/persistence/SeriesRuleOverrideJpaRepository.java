package com.datasync.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SeriesRuleOverrideJpaRepository extends JpaRepository<SeriesRuleOverrideEntity, Long> {

    Optional<SeriesRuleOverrideEntity> findByOrionSeriesNameAndActiveTrue(String orionSeriesName);
}
