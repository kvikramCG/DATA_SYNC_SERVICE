package com.datasync.application;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class JobAbortRegistryTest {

    private final JobAbortRegistry registry = new JobAbortRegistry();

    @Test
    void abortRaisesOnlyTheNamedJobsFlag() {
        AtomicBoolean first = registry.register(1L);
        AtomicBoolean second = registry.register(2L);

        assertThat(registry.abort(1L)).isTrue();

        assertThat(first.get()).isTrue();
        assertThat(second.get()).isFalse();
    }

    @Test
    void abortingAJobThatIsNotRunningHereReportsFalse() {
        assertThat(registry.abort(99L)).isFalse();
    }

    @Test
    void abortAllRaisesEveryFlag() {
        AtomicBoolean first = registry.register(1L);
        AtomicBoolean second = registry.register(2L);

        registry.abortAll();

        assertThat(first.get()).isTrue();
        assertThat(second.get()).isTrue();
    }

    @Test
    void awaitIdleReturnsOnceEveryJobHasUnregistered() throws Exception {
        registry.register(1L);
        Thread finisher = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            registry.unregister(1L);
        });

        assertThat(registry.awaitIdle(Duration.ofSeconds(5))).isTrue();
        finisher.join();
    }

    @Test
    void awaitIdleGivesUpAfterTheTimeoutWhileAJobIsStillRegistered() {
        registry.register(1L);

        assertThat(registry.awaitIdle(Duration.ofMillis(150))).isFalse();
    }
}
