package com.datasync.adapter.in.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Generates a fresh, server-side correlation id for every request (spec &sect;8 Logging) and makes
 * it available to every log line for the duration of the request via MDC. Any inbound
 * {@code X-Correlation-Id} header is deliberately ignored: the id is owned by this service, so it
 * is always a well-formed UUID and can never be forged or used for log injection.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    /** Request attribute the controller reads the typed {@link UUID} from (no MDC parsing needed). */
    // Must be a compile-time constant (used in an annotation), so a literal rather than Class.getName().
    public static final String REQUEST_ATTRIBUTE = "com.datasync.adapter.in.web.CorrelationIdFilter.correlationId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        // Always generate - never trust the client-supplied header.
        UUID correlationId = UUID.randomUUID();
        // Typed copy for the controller, so it passes exactly this id down to the job (one source of truth).
        request.setAttribute(REQUEST_ATTRIBUTE, correlationId);
        // MDC covers every log line of this request thread, including the exception handlers.
        MDC.put(MDC_KEY, correlationId.toString());
        // Echo the server-generated id so callers can quote it to support.
        response.setHeader(HEADER, correlationId.toString());
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }
}
