package com.datasync.application;

import com.datasync.domain.port.out.ClockPort;
import com.datasync.domain.port.out.JobRepositoryPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaleJobReaperTest {

    @Mock
    private JobRepositoryPort jobRepositoryPort;
    @Mock
    private ClockPort clock;

    @Test
    void failsJobsThatStartedBeforeNowMinusTheMaxRunningDuration() {
        Instant now = Instant.parse("2026-10-16T10:00:00Z");
        when(clock.now()).thenReturn(now);
        when(jobRepositoryPort.failStaleRunningJobs(Instant.parse("2026-10-16T08:00:00Z"))).thenReturn(2);

        new StaleJobReaper(jobRepositoryPort, clock, Duration.ofHours(2)).reap();

        verify(jobRepositoryPort).failStaleRunningJobs(Instant.parse("2026-10-16T08:00:00Z"));
    }
}
