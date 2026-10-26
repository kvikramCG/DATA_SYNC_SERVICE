package com.datasync.adapter.out.persistence;

import com.datasync.adapter.out.clock.SystemClockAdapter;
import com.datasync.domain.model.FailureReason;
import com.datasync.domain.model.Job;
import com.datasync.domain.model.JobTask;
import com.datasync.domain.model.SeriesDescriptor;
import com.datasync.domain.model.SeriesType;
import com.datasync.domain.model.TaskStatus;
import com.datasync.domain.model.TimeZoneCode;
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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves the "fail unfinished tasks" UPDATE against real Postgres. */
@Testcontainers
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({TaskRepositoryAdapter.class, JobRepositoryAdapter.class, SeriesRepositoryAdapter.class,
        SystemClockAdapter.class})
class TaskRepositoryAdapterIT {

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
    private TaskRepositoryAdapter taskAdapter;
    @Autowired
    private JobRepositoryAdapter jobAdapter;
    @Autowired
    private SeriesRepositoryAdapter seriesAdapter;

    @Test
    void pendingAndRunningTasksAreFailedAndFinishedTasksAreLeftAlone() {
        Job job = jobAdapter.createRunningJob(SeriesType.TYPE_A, null, null, null, UUID.randomUUID());
        List<SeriesDescriptor> series = seriesAdapter.upsertAll(List.of(
                descriptor("SERIES_PENDING"), descriptor("SERIES_RUNNING"), descriptor("SERIES_DONE")));

        JobTask pending = taskAdapter.createTask(job.id(), series.get(0).id(), "SERIES_PENDING");
        JobTask running = taskAdapter.createTask(job.id(), series.get(1).id(), "SERIES_RUNNING");
        JobTask done = taskAdapter.createTask(job.id(), series.get(2).id(), "SERIES_DONE");
        taskAdapter.markTaskStarted(running.id());
        taskAdapter.updateTaskStatus(done.id(), TaskStatus.SUCCESS, null, null, 5);

        int changed = taskAdapter.failUnfinishedTasks(job.id(), FailureReason.INTERNAL_ERROR, "job ended early");

        assertThat(changed).isEqualTo(2);
        Map<Long, JobTask> byId = taskAdapter.findByJobId(job.id()).stream()
                .collect(Collectors.toMap(JobTask::id, Function.identity()));
        assertThat(byId.get(pending.id()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(byId.get(pending.id()).failureReason()).isEqualTo(FailureReason.INTERNAL_ERROR);
        assertThat(byId.get(pending.id()).failureDetail()).isEqualTo("job ended early");
        assertThat(byId.get(running.id()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(byId.get(done.id()).status()).isEqualTo(TaskStatus.SUCCESS);
        assertThat(byId.get(done.id()).valuesSent()).isEqualTo(5);
    }

    private static SeriesDescriptor descriptor(String name) {
        return new SeriesDescriptor(null, name, SeriesType.TYPE_A, TimeZoneCode.CENTRAL,
                LocalDateTime.of(2026, 10, 12, 18, 0),
                LocalDateTime.of(2020, 1, 1, 0, 0),
                LocalDateTime.of(2026, 10, 13, 23, 0),
                null);
    }
}
