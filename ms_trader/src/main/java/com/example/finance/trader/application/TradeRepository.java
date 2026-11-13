package com.example.finance.trader.application;

import java.util.Optional;

public interface TradeRepository {
    /**
     * Atomically insert the candidate, or return the document owning its idempotency key.
     */
    TradeAcceptance insertOrGet(StoredTrade candidate);

    Optional<StoredTrade> findById(String tradeId);
}
