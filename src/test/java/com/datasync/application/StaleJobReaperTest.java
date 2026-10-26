package com.datasync.application;

import com.datasync.domain.model.FailureReason;
import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import com.datasync.domain.port.out.TaskRepositoryPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaleJobReaperTest {

    private static final Instant CUTOFF = Instant.parse("2026-10-16T08:00:00Z");

    @Mock
    private JobRepositoryPort jobRepositoryPort;
    @Mock
    private TaskRepositoryPort taskRepositoryPort;
    @Mock
    private ClockPort clock;

    private JobAbortRegistry abortRegistry;
    private StaleJobReaper reaper;

    @BeforeEach
    void setUp() {
        abortRegistry = new JobAbortRegistry();
        when(clock.now()).thenReturn(Instant.parse("2026-10-16T10:00:00Z"));
        reaper = new StaleJobReaper(jobRepositoryPort, taskRepositoryPort, abortRegistry, clock, Duration.ofHours(2));
    }

    @Test
    void failsJobsThatStartedBeforeNowMinusTheMaxRunningDuration() {
        when(jobRepositoryPort.failStaleRunningJobs(CUTOFF)).thenReturn(List.of(1L, 2L));

        reaper.reap();

        verify(jobRepositoryPort).failStaleRunningJobs(CUTOFF);
    }

    @Test
    void stopsTheWorkersOfEveryReapedJobAndFailsTheirUnfinishedTasks() {
        AtomicBoolean reapedFlag = abortRegistry.register(7L);
        AtomicBoolean healthyFlag = abortRegistry.register(8L);
        when(jobRepositoryPort.failStaleRunningJobs(CUTOFF)).thenReturn(List.of(7L));

        reaper.reap();

        assertThat(reapedFlag.get()).isTrue();
        assertThat(healthyFlag.get()).isFalse();
        verify(taskRepositoryPort).failUnfinishedTasks(eq(7L), eq(FailureReason.INTERNAL_ERROR), any());
    }

    @Test
    void doesNothingWhenNoJobIsStale() {
        when(jobRepositoryPort.failStaleRunningJobs(CUTOFF)).thenReturn(List.of());

        reaper.reap();

        verify(taskRepositoryPort, never()).failUnfinishedTasks(any(), any(), any());
    }

    @Test
    void oneJobFailingToCleanUpDoesNotStopTheOthers() {
        when(jobRepositoryPort.failStaleRunningJobs(CUTOFF)).thenReturn(List.of(7L, 8L));
        when(taskRepositoryPort.failUnfinishedTasks(eq(7L), any(), any()))
                .thenThrow(new IllegalStateException("database unavailable"));

        reaper.reap();

        verify(taskRepositoryPort).failUnfinishedTasks(eq(8L), eq(FailureReason.INTERNAL_ERROR), any());
    }
}
