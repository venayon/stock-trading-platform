package com.example.finance.processor;

public class InvalidTradeException extends IllegalArgumentException {
    public InvalidTradeException(String message) {
        super(message);
    }
}
