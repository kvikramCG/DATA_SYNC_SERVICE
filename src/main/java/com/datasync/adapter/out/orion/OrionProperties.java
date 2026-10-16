package com.datasync.adapter.out.orion;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Orion outbound adapter (spec &sect;4).
 */
@ConfigurationProperties(prefix = "orion")
public class OrionProperties {

    private String baseUrl;
    private String login;
    private String password;
    private long connectTimeoutMs = 5000;
    private long readTimeoutMs = 60000;

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
}
