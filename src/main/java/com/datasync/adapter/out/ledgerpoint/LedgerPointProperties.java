package com.datasync.adapter.out.ledgerpoint;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration for the LedgerPoint outbound adapter (spec &sect;5).
 */
@ConfigurationProperties(prefix = "ledgerpoint")
// A blank value (e.g. LEDGERPOINT_API_KEY unset, which application.yml defaults to "") must stop startup.
@Validated
public class LedgerPointProperties {

    @NotBlank
    private String baseUrl;
    @NotBlank
    private String apiKey;
    private long connectTimeoutMs = 5000;
    private long readTimeoutMs = 60000;
    private int maxBatchSize = 10_000;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
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

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
