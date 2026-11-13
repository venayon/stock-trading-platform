package com.example.finance.trader.api;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.math.BigDecimal;

public class DecimalInputDeserializer extends JsonDeserializer<BigDecimal> {

    @Override
    public BigDecimal deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (parser.currentToken().isNumeric()) {
            return parser.getDecimalValue();
        }
        if (parser.hasToken(JsonToken.VALUE_STRING)
            && parser.getText().matches("[+-]?[0-9]+(?:\\.[0-9]+)?")) {
            return new BigDecimal(parser.getText());
        }
        return (BigDecimal) context.handleUnexpectedToken(BigDecimal.class, parser);
    }
}
