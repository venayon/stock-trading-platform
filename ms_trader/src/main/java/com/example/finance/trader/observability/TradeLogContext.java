package com.example.finance.trader.observability;

import org.slf4j.MDC;

/**
 * Scoped identity fields, restoring an enclosing HTTP/relay context on close.
 */
public final class TradeLogContext implements AutoCloseable {
    private final String previousTrade = MDC.get("tradeId");
    private final String previousCorrelation = MDC.get("correlationId");

    public TradeLogContext(String tradeId, String correlationId) {
        set("tradeId", tradeId);
        set("correlationId", correlationId);
    }

    private static void set(String key, String value) {
        if (value == null) MDC.remove(key);
        else MDC.put(key, value);
    }

    public void close() {
        set("tradeId", previousTrade);
        set("correlationId", previousCorrelation);
    }
}
