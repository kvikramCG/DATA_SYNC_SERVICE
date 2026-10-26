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
}
