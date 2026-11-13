package com.example.finance.trader.application;

import java.time.Instant;

public record StoredTrade(String tradeId, String idempotencyKey, String requestHash,
                          CreateTradeCommand trade, String publicationStatus,
                          Instant createdAt, String correlationId, EventMetadata event) {
    public StoredTrade(String tradeId, String idempotencyKey, String requestHash, CreateTradeCommand trade,
                       String publicationStatus, Instant createdAt, String correlationId) {
        this(tradeId, idempotencyKey, requestHash, trade, publicationStatus, createdAt, correlationId, null);
    }
}
