package com.example.finance.trader.persistence;

import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Document("trades")
public record TradeDocument(@Id String tradeId,
                            String idempotencyKey,
                            String requestHash,
                            String tradeDate,
                            String side,
                            String symbol,
                            Long quantity,
                            @Field(targetType = FieldType.DECIMAL128) BigDecimal price,
                            String currency,
                            String counterparty,
                            String publicationStatus,
                            Instant createdAt,
                            String correlationId,
                            EventMetadata event,
                            Integer publicationAttempts,
                            Instant nextAttemptAt,
                            Instant leaseUntil,
                            String leaseToken,
                            String lastPublicationError) {
    static TradeDocument from(StoredTrade stored) {
        CreateTradeCommand trade = stored.trade();

        return new TradeDocument(stored.tradeId(),
            stored.idempotencyKey(),
            stored.requestHash(),
            trade.tradeDate().toString(),
            trade.side(),
            trade.symbol(),
            trade.quantity(),
            trade.price(),
            trade.currency(),
            trade.counterparty(),
            stored.publicationStatus(),
            stored.createdAt(),
            stored.correlationId(),
            stored.event(),
            0,
            stored.createdAt(),
            null,
            null,
            null);
    }

    StoredTrade toTrade() {
        return new StoredTrade(tradeId, idempotencyKey, requestHash,
            new CreateTradeCommand(LocalDate.parse(tradeDate),
                side,
                symbol,
                quantity, price, currency, counterparty),
            publicationStatus, createdAt, correlationId, event);
    }
}
