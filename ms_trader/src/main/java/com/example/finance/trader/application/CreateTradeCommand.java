package com.example.finance.trader.application;

import java.math.BigDecimal;
import java.time.LocalDate;

public record CreateTradeCommand(
    LocalDate tradeDate,
    String side,
    String symbol,
    Long quantity,
    BigDecimal price,
    String currency,
    String counterparty) {
}
