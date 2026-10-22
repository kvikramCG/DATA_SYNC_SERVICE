package com.datasync.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ADR-004: series-level parallelism runs on Java 21 virtual threads rather than a reactive
 * stack - one virtual thread per series Task, globally throttled by the shared Orion rate
 * limiter (see {@code resilience4j.ratelimiter.instances.orion}).
 */
@Configuration
public class VirtualThreadConfig {

    @Bean
    public AsyncTaskExecutor applicationTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Raw executor used by {@code SyncOrchestrationService} to run each job in the background
     * and, via {@code invokeAll}, every series Task of that job concurrently (the HTTP trigger
     * is asynchronous: 202 Accepted, then poll {@code GET /api/jobs/{id}}).
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
