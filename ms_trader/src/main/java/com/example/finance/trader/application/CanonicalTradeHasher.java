package com.example.finance.trader.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Versioned, ordered canonical JSON; called only after TradeValidator normalization.
 */
public class CanonicalTradeHasher {
    private final ObjectMapper json = new ObjectMapper();

    public String hash(CreateTradeCommand trade) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("tradeDate", trade.tradeDate().toString());
        fields.put("side", trade.side());
        fields.put("symbol", trade.symbol());
        fields.put("quantity", trade.quantity());
        fields.put("price", trade.price().toPlainString());
        fields.put("currency", trade.currency());
        fields.put("counterparty", trade.counterparty());
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("trade-request-v1\n".getBytes(StandardCharsets.UTF_8));
            return "v1:" + HexFormat.of().formatHex(digest.digest(json.writeValueAsBytes(fields)));
        } catch (NoSuchAlgorithmException | JsonProcessingException error) {
            throw new IllegalStateException("Unable to hash canonical trade", error);
        }
    }
}
