package com.example.finance.trader.application;

public class TradeStorageUnavailableException extends RuntimeException {
    public TradeStorageUnavailableException(Throwable cause) {
        super("Trade storage is unavailable; retry with the same Idempotency-Key", cause);
    }
}
