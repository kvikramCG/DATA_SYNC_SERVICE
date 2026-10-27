package com.datasync.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards settings that are silently ineffective when missing (they only show up in production). */
class ApplicationYamlTest {

    private static Properties yaml() {
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource("application.yml"));
        return factory.getObject();
    }

    @Test
    void retryInstancesTurnExponentialBackoffOnExplicitly() {
        Properties properties = yaml();

        // Without the flag Resilience4j ignores the multiplier and retries at a fixed interval.
        assertThat(properties.getProperty("resilience4j.retry.instances.orion.enable-exponential-backoff"))
                .isEqualTo("true");
        assertThat(properties.getProperty("resilience4j.retry.instances.ledgerpoint.enable-exponential-backoff"))
                .isEqualTo("true");
    }

    @Test
    void shutdownIsGraceful() {
        assertThat(yaml().getProperty("server.shutdown")).isEqualTo("graceful");
    }

    @Test
    void theStaleJobTimeoutIsSizedForAFullFirstRun() {
        // PT2H reaped a healthy first run of ~3,000 series (about 2 h 10 min at Orion's 5 requests/s).
        assertThat(java.time.Duration.parse(yaml().getProperty("datasync.job.max-running-duration")))
                .isGreaterThanOrEqualTo(java.time.Duration.ofHours(6));
    }

    @Test
    void ledgerPointHonorsRetryAfterUpToAConfiguredCap() {
        assertThat(yaml().getProperty("ledgerpoint.max-retry-after")).isEqualTo("60s");
    }
}
