package com.example.finance.processor;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document("processed_trades")
public record ProcessedTrade(@Id String tradeId, String inputHash, TradeData trade, String processingStatus,
                             Point source, Point output, int targetPartition, int routeVersion, String correlationId,
                             Instant createdAt, Instant updatedAt, String leaseToken, Instant leaseUntil) {
    public record Point(String topic, int partition, long offset) {
    }
}
