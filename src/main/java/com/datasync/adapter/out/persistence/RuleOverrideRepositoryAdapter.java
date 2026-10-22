package com.datasync.adapter.out.persistence;

import com.datasync.domain.model.SeriesRuleOverride;
import com.datasync.domain.port.out.RuleOverrideRepositoryPort;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Outbound adapter implementing {@link RuleOverrideRepositoryPort} on top of Spring Data JPA.
 */
@Component
public class RuleOverrideRepositoryAdapter implements RuleOverrideRepositoryPort {

    private final SeriesRuleOverrideJpaRepository repository;

    public RuleOverrideRepositoryAdapter(SeriesRuleOverrideJpaRepository repository) {
        this.repository = repository;
    }

    @Override
    public Optional<SeriesRuleOverride> findActiveByOrionSeriesName(String orionSeriesName) {
        return repository.findByOrionSeriesNameAndActiveTrue(orionSeriesName)
                .map(entity -> new SeriesRuleOverride(
                        entity.getOrionSeriesName(), entity.getLedgerPointKeyOverride(), entity.isSkip()));
    }
}
