package com.example.finance.processor;

import com.example.finance.avro.Side;
import com.example.finance.avro.StockTrade;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Set;
import java.util.UUID;

public record TradeData(String tradeId, String tradeDate, String side, String symbol, long quantity,
                        @Field(targetType = FieldType.DECIMAL128) BigDecimal price, String currency,
                        String counterparty) {
    public static TradeData validate(StockTrade value, String key) {

        if (value == null)
            throw new InvalidTradeException("Null trade is not supported");

        try {
            if (!UUID.fromString(value.getTradeId()).toString().equals(value.getTradeId()))
                throw new IllegalArgumentException();

            if (value.getTradeDate() == null
                || value.getTradeDate().toEpochDay() < Integer.MIN_VALUE
                || value.getTradeDate().toEpochDay() > Integer.MAX_VALUE)
                throw new IllegalArgumentException();

            if (value.getSide() == null || value.getSymbol() == null
                || !value.getSymbol().matches("[A-Z0-9][A-Z0-9.-]{0,15}")
                || !value.getSymbol().equals(key))
                throw new IllegalArgumentException();

            if (value.getQuantity() <= 0 || value.getPrice() == null
                || value.getPrice().signum() <= 0
                || value.getPrice().compareTo(new BigDecimal("999999999999.999999")) > 0)
                throw new IllegalArgumentException();

            if (!Set.of("USD", "GBP", "EUR").contains(value.getCurrency()))
                throw new IllegalArgumentException();

            if (value.getCounterparty() != null && (value.getCounterparty().length() > 128
                || value.getCounterparty().isEmpty()
                || !value.getCounterparty().equals(value.getCounterparty().trim())))
                throw new IllegalArgumentException();

            return new TradeData(value.getTradeId(),
                value.getTradeDate().toString(),
                value.getSide().name(),
                value.getSymbol(),
                value.getQuantity(),
                value.getPrice().setScale(6, RoundingMode.UNNECESSARY),
                value.getCurrency(), value.getCounterparty()
            );

        } catch (RuntimeException error) {
            throw new InvalidTradeException("Invalid immutable trade or mismatched Kafka key");
        }
    }

    public StockTrade event() {
        return StockTrade.newBuilder().setTradeId(tradeId).setTradeDate(LocalDate.parse(tradeDate)).setSide(Side.valueOf(side))
            .setSymbol(symbol).setQuantity(quantity).setPrice(price).setCurrency(currency).setCounterparty(counterparty).build();
    }

    public String digest() {
        try {
            var bytes = event().toByteBuffer();
            byte[] copy = new byte[bytes.remaining()];
            bytes.get(copy);
            return "avro-v1:" + java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(copy));
        } catch (java.io.IOException | java.security.NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }
}
