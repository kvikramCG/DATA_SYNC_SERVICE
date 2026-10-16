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
     * Raw executor used by {@code SyncOrchestrationService} via {@code invokeAll} to run every
     * series Task of a job concurrently and block until all finish (the HTTP trigger is
     * synchronous per spec &sect;7.1).
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
