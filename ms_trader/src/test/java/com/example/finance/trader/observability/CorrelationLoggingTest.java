package com.example.finance.trader.observability;

import com.example.finance.trader.api.CorrelationFilter;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CorrelationLoggingTest {
    @Test
    void requestCorrelationIsAvailableDuringHandlingAndRestoredAfterFailure() {
        var request = new MockHttpServletRequest();
        var response = new MockHttpServletResponse();
        String correlation = "550e8400-e29b-41d4-a716-446655440000";
        request.addHeader("X-Correlation-ID", correlation);
        MDC.put("correlationId", "previous-context");
        try {
            assertThrows(IllegalStateException.class, () -> new CorrelationFilter().doFilter(request, response, (req, res) -> {
                assertEquals(correlation, MDC.get("correlationId"));
                throw new IllegalStateException("handler failed");
            }));
            assertEquals("previous-context", MDC.get("correlationId"));
        } finally {
            MDC.remove("correlationId");
        }
    }
}
