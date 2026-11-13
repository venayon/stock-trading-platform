package com.example.finance.trader.application;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TradeValidator {

    public static final Set<String> CURRENCIES = Set.of("USD", "GBP", "EUR");

    public CreateTradeCommand validate(CreateTradeCommand command) {
        Map<String, String> errors = new LinkedHashMap<>();
        if (command == null) {
            throw new TradeValidationException(Map.of("request", "trade is required"));
        }
        if (command.quantity() == null) {
            errors.put("quantity", "quantity is required");
        } else if (command.quantity() <= 0) {
            errors.put("quantity", "quantity must be greater than zero");
        }

        BigDecimal price = command.price();
        if (price == null) {
            errors.put("price", "price is required");
        } else if (price.signum() <= 0) {
            errors.put("price", "price must be greater than zero");
        } else if (price.compareTo(new BigDecimal("999999999999.999999")) > 0) {
            errors.put("price", "price must not exceed 999999999999.999999");
        } else {
            try {
                price = price.setScale(6, RoundingMode.UNNECESSARY);
            } catch (ArithmeticException error) {
                errors.put("price", "price must have at most six decimal places after removing trailing zeros");
            }
        }
        if (command.tradeDate() == null) {
            errors.put("tradeDate", "tradeDate is required");
        } else if (command.tradeDate().toEpochDay() < Integer.MIN_VALUE
            || command.tradeDate().toEpochDay() > Integer.MAX_VALUE) {
            errors.put("tradeDate", "tradeDate must fit the Avro epoch-day range");
        }
        if (!"BUY".equals(command.side()) && !"SELL".equals(command.side())) {
            errors.put("side", "side must be BUY or SELL");
        }

        String symbol = command.symbol() == null ? null : command.symbol().trim().toUpperCase(Locale.ROOT);
        if (symbol == null || !symbol.matches("[A-Z0-9][A-Z0-9.-]{0,15}")) {
            errors.put("symbol", "symbol must contain 1 to 16 letters, digits, dots or hyphens, starting with a letter or digit");
        }
        if (command.currency() == null || !CURRENCIES.contains(command.currency())) {
            errors.put("currency", "currency must be USD, GBP or EUR");
        }
        String counterparty = command.counterparty() == null ? null : command.counterparty().trim();
        if (counterparty != null && counterparty.isEmpty()) {
            counterparty = null;
        }
        if (counterparty != null && counterparty.length() > 128) {
            errors.put("counterparty", "counterparty must not exceed 128 characters");
        }
        if (!errors.isEmpty()) {
            throw new TradeValidationException(errors);
        }
        return new CreateTradeCommand(command.tradeDate(), command.side(), symbol, command.quantity(),
            price, command.currency(), counterparty);
    }
}
