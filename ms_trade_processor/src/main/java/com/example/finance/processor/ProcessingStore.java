package com.example.finance.processor;

import java.time.Instant;

public interface ProcessingStore {
    Claim claim(TradeData input, ProcessedTrade.Point source, String correlationId, Instant now);

    void complete(Claim claim, ProcessedTrade.Point output, Instant now);

    void release(Claim claim);

    record Claim(ProcessedTrade trade, String token, boolean complete) {
    }
}
