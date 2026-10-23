package com.datasync.config;

import com.datasync.adapter.out.ledgerpoint.LedgerPointProperties;
import com.datasync.adapter.out.orion.OrionProperties;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Builds the outbound {@link RestClient}s used by the Orion and LedgerPoint adapters, with the
 * connect/read timeouts mandated by spec &sect;8 (connect 5s, read 60s).
 */
@Configuration
public class RestClientConfig {

    // Same literals as CorrelationIdFilter / the orchestrator's MDC key; kept local so this config
    // class does not depend on the web adapter package.
    private static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    /**
     * Copies the current thread's correlation id (MDC) onto every outbound Orion request - token
     * login, series list and values. This works because those calls run synchronously on the same
     * (virtual) thread that the orchestrator already tagged with the id, so the Orion ports need no
     * extra parameter. LedgerPoint keeps its explicit parameter and is not touched.
     */
    private static final ClientHttpRequestInterceptor CORRELATION_ID_INTERCEPTOR = (request, body, execution) -> {
        String correlationId = MDC.get(CORRELATION_ID_MDC_KEY);
        // Skip when there is no context (e.g. a call outside a job) or the header is already set.
        if (correlationId != null && !request.getHeaders().containsKey(CORRELATION_ID_HEADER)) {
            request.getHeaders().set(CORRELATION_ID_HEADER, correlationId);
        }
        return execution.execute(request, body);
    };

    @Bean
    public RestClient orionRestClient(OrionProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) properties.getConnectTimeoutMs());
        factory.setReadTimeout((int) properties.getReadTimeoutMs());

        return RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(factory)
                .requestInterceptor(CORRELATION_ID_INTERCEPTOR)
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
