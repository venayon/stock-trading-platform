package com.example.finance.trader.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
public class CorrelationFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String value = request.getHeader("X-Correlation-ID");
        if (value == null || !value.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
            value = UUID.randomUUID().toString();
        }
        request.setAttribute("correlationId", value);
        response.setHeader("X-Correlation-ID", value);
        String previous = MDC.get("correlationId");
        MDC.put("correlationId", value);
        try {
            chain.doFilter(request, response);
        } finally {
            if (previous == null) MDC.remove("correlationId");
            else MDC.put("correlationId", previous);
        }
    }
}
