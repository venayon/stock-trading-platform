package com.example.finance.trader.application;

public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException() {
        super("Idempotency-Key is already associated with a different trade request");
    }
}
