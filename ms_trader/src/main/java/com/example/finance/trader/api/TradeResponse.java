package com.example.finance.trader.api;

import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.time.LocalDate;

public record TradeResponse(String tradeId,
                            @Schema(allowableValues = {"PENDING", "PUBLISHED"}, description = "PUBLISHED includes acknowledged Kafka metadata; acceptance returns PENDING first.")
                            String publicationStatus,
                            LocalDate tradeDate, String side, String symbol, Long quantity,
                            @Schema(type = "string", example = "123.450000") String price,
                            String currency, String counterparty, Instant createdAt,
                            @JsonInclude(JsonInclude.Include.NON_NULL) EventMetadata event) {
    static TradeResponse from(StoredTrade stored) {
        var trade = stored.trade();
        return new TradeResponse(stored.tradeId(), stored.publicationStatus(), trade.tradeDate(),
            trade.side(), trade.symbol(), trade.quantity(), trade.price().toPlainString(),
            trade.currency(), trade.counterparty(), stored.createdAt(), stored.event());
    }
}
