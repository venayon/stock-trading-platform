package com.example.finance.trader.application;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class TradeValidationException extends IllegalArgumentException {
    private final Map<String, String> errors;

    public TradeValidationException(Map<String, String> errors) {
        super(errors.values().iterator().next());
        this.errors = Collections.unmodifiableMap(new LinkedHashMap<>(errors));
    }

    public Map<String, String> errors() {
        return errors;
    }
}
