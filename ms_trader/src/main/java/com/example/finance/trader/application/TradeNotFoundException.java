package com.example.finance.trader.application;

public class TradeNotFoundException extends RuntimeException {
    public TradeNotFoundException() {
        super("Trade was not found");
    }
}
