package com.datasync.adapter.out.orion;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Configuration for the Orion outbound adapter (spec &sect;4).
 */
@ConfigurationProperties(prefix = "orion")
// A blank value (e.g. ORION_PASSWORD unset, which application.yml defaults to "") must stop startup.
@Validated
public class OrionProperties {

    @NotBlank
    private String baseUrl;
    @NotBlank
    private String login;
    @NotBlank
    private String password;
    private long connectTimeoutMs = 5000;
    private long readTimeoutMs = 60000;

    /** Upper bound on how long a single {@code Retry-After} hint from a 429 is honored. */
    private Duration maxRetryAfter = Duration.ofSeconds(60);

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public long getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(long readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public Duration getMaxRetryAfter() {
        return maxRetryAfter;
    }

    public void setMaxRetryAfter(Duration maxRetryAfter) {
        this.maxRetryAfter = maxRetryAfter;
    }
}
