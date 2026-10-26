package com.datasync.adapter.out.persistence;

import com.datasync.adapter.out.clock.SystemClockAdapter;
import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobStatus;
import com.datasync.domain.model.SeriesType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the stale-job reaper query against real Postgres: an orphaned RUNNING row is closed as
 * FAILED and no longer blocks a new job of the same series type (unique partial index).
 */
@Testcontainers
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JobRepositoryAdapter.class, SystemClockAdapter.class})
class JobRepositoryAdapterIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg16").asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void dataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private JobRepositoryAdapter adapter;

    @Test
    void recentRunningJobIsLeftAlone() {
        Job job = adapter.createRunningJob(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        List<Long> failed = adapter.failStaleRunningJobs(Instant.now().minusSeconds(3600));

        assertThat(failed).isEmpty();
        assertThat(adapter.findById(job.id()).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void staleRunningJobIsFailedAndNoLongerBlocksTheSeriesType() {
        Job stale = adapter.createRunningJob(SeriesType.TYPE_B, null, null, null, UUID.randomUUID());

        List<Long> failed = adapter.failStaleRunningJobs(Instant.now().plusSeconds(60));

        assertThat(failed).containsExactly(stale.id());
        Job reloaded = adapter.findById(stale.id()).orElseThrow();
        assertThat(reloaded.status()).isEqualTo(JobStatus.FAILED);
        assertThat(reloaded.finishedAt()).isNotNull();

        // The unique partial index no longer blocks a new RUNNING job for the same type.
        Job next = adapter.createRunningJob(SeriesType.TYPE_B, null, null, null, UUID.randomUUID());
        assertThat(next.status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void runningJobIsClosedWithTheGivenStatus() {
        Job job = adapter.createRunningJob(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());

        boolean closed = adapter.markJobFinished(job.id(), JobStatus.COMPLETED);

        assertThat(closed).isTrue();
        Job reloaded = adapter.findById(job.id()).orElseThrow();
        assertThat(reloaded.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(reloaded.finishedAt()).isNotNull();
    }

    @Test
    void jobFailedByTheReaperIsNotOverwrittenWhenItsThreadsLaterFinish() {
        // The scenario behind first-close-wins: a slow job is reaped as FAILED, keeps running, and
        // then tries to close itself as COMPLETED.
        Job job = adapter.createRunningJob(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());
        adapter.failStaleRunningJobs(Instant.now().plusSeconds(60));
        Job reaped = adapter.findById(job.id()).orElseThrow();

        boolean closed = adapter.markJobFinished(job.id(), JobStatus.COMPLETED);

        assertThat(closed).isFalse();
        Job reloaded = adapter.findById(job.id()).orElseThrow();
        assertThat(reloaded.status()).isEqualTo(JobStatus.FAILED);
        // The reaper's finishedAt is kept as well, so the audit trail is not rewritten.
        assertThat(reloaded.finishedAt()).isEqualTo(reaped.finishedAt());
    }

    @Test
    void closingAnUnknownJobStillThrowsNotFound() {
        // 0 updated rows must not be mistaken for "already closed" when the job does not exist.
        assertThatThrownBy(() -> adapter.markJobFinished(-1L, JobStatus.COMPLETED))
                .isInstanceOf(JobNotFoundException.class);
    }
}
