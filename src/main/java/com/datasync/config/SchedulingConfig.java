package com.datasync.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables {@code @Scheduled} support (used by the stale-job reaper).
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
