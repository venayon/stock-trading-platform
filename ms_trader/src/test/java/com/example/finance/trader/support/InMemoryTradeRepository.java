package com.example.finance.trader.support;

import com.example.finance.trader.application.StoredTrade;
import com.example.finance.trader.application.TradeAcceptance;
import com.example.finance.trader.application.TradeRepository;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double only. Real concurrency/uniqueness guarantees are tested against MongoDB.
 */
public class InMemoryTradeRepository implements TradeRepository {
    public final AtomicInteger attempts = new AtomicInteger();
    private final ConcurrentHashMap<String, StoredTrade> trades = new ConcurrentHashMap<>();

    public TradeAcceptance insertOrGet(StoredTrade candidate) {
        attempts.incrementAndGet();
        StoredTrade previous = trades.putIfAbsent(candidate.idempotencyKey(), candidate);
        return new TradeAcceptance(previous == null ? candidate : previous, previous == null);
    }

    public Optional<StoredTrade> findById(String id) {
        return trades.values().stream().filter(t -> t.tradeId().equals(id)).findFirst();
    }

    public int size() {
        return trades.size();
    }
}
