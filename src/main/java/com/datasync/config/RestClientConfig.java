package com.datasync.config;

import com.datasync.adapter.out.ledgerpoint.LedgerPointProperties;
import com.datasync.adapter.out.orion.OrionProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Builds the outbound {@link RestClient}s used by the Orion and LedgerPoint adapters, with the
 * connect/read timeouts mandated by spec &sect;8 (connect 5s, read 60s).
 */
@Configuration
public class RestClientConfig {

    @Bean
    public RestClient orionRestClient(OrionProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) properties.getConnectTimeoutMs());
        factory.setReadTimeout((int) properties.getReadTimeoutMs());

        return RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(factory)
                .build();
    }

    @Bean
    public RestClient ledgerPointRestClient(LedgerPointProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) properties.getConnectTimeoutMs());
        factory.setReadTimeout((int) properties.getReadTimeoutMs());

        return RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(factory)
                .build();
    }
}
