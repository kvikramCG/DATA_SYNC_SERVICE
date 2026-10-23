package com.datasync.adapter.in.web;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void generatesAServerSideUuidAndExposesItEverywhere() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdcDuringChain = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> mdcDuringChain.set(MDC.get(CorrelationIdFilter.MDC_KEY)));

        UUID attribute = (UUID) request.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE);
        assertThat(attribute).isNotNull();
        assertThat(mdcDuringChain.get()).isEqualTo(attribute.toString());
        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo(attribute.toString());
    }

    @Test
    void ignoresAnyInboundCorrelationIdHeader() throws ServletException, IOException {
        for (String inbound : new String[] {
                UUID.randomUUID().toString(), "not-a-uuid", "abc\r\nX-Injected: 1", "x".repeat(5000)}) {
            MockHttpServletRequest request = new MockHttpServletRequest();
            request.addHeader(CorrelationIdFilter.HEADER, inbound);
            MockHttpServletResponse response = new MockHttpServletResponse();

            filter.doFilter(request, response, (req, res) -> { });

            String echoed = response.getHeader(CorrelationIdFilter.HEADER);
            assertThat(echoed).isNotEqualTo(inbound);
            // Always a well-formed UUID (throws if not).
            assertThat(UUID.fromString(echoed)).isEqualTo(request.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE));
        }
    }

    @Test
    void generatesADifferentIdForEveryRequest() throws ServletException, IOException {
        MockHttpServletRequest first = new MockHttpServletRequest();
        MockHttpServletRequest second = new MockHttpServletRequest();

        filter.doFilter(first, new MockHttpServletResponse(), (req, res) -> { });
        filter.doFilter(second, new MockHttpServletResponse(), (req, res) -> { });

        assertThat(first.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE))
                .isNotEqualTo(second.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE));
    }

    @Test
    void clearsMdcAfterTheChainEvenWhenItThrows() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(() -> filter.doFilter(request, response, (req, res) -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(MDC.get(CorrelationIdFilter.MDC_KEY)).isNull();
    }
}
