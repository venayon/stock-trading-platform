package com.example.finance.trader.observability;

import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.IdempotencyConflictException;
import com.example.finance.trader.application.TradeService;
import com.example.finance.trader.support.InMemoryTradeRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TradeMetricsTest {
    @Test
    void acceptanceSeparatesFreshReplayedAndConflictingRequestsWithoutIdentityLabels() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeMetrics(registry);
        var service = new TradeService(new InMemoryTradeRepository(), Clock.systemUTC(), UUID::randomUUID, metrics);
        var trade = new CreateTradeCommand(LocalDate.of(2026, 10, 24), "BUY", "AAPL", 10L, new BigDecimal("1"), "USD", null);
        service.accept("first-key", trade, "correlation");
        service.accept("first-key", trade, "other-correlation");
        assertThrows(IdempotencyConflictException.class, () -> service.accept("first-key",
            new CreateTradeCommand(trade.tradeDate(), "SELL", "AAPL", 10L, trade.price(), "USD", null), "correlation"));
        assertEquals(1, registry.get("trades.accepted").counter().count());
        assertEquals(1, registry.get("trades.idempotent.replays").counter().count());
        assertEquals(1, registry.get("trades.errors").tags("stage", "acceptance", "code", "conflict").counter().count());
        assertTrue(registry.getMeters().stream().flatMap(m -> m.getId().getTags().stream())
            .noneMatch(t -> java.util.Set.of("tradeId", "correlationId", "symbol", "idempotencyKey").contains(t.getKey())));
    }

    @Test
    void publicationAcknowledgmentAndCompletionAreDistinctEvents() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeMetrics(registry);
        metrics.acknowledged();
        metrics.retried();
        metrics.error(TradeMetrics.Stage.COMPLETION, TradeMetrics.Code.STATE);
        assertEquals(1, registry.get("trades.published").counter().count());
        assertEquals(0, registry.get("trades.publication.completed").counter().count());
        assertEquals(1, registry.get("trades.publication.retries").counter().count());
    }
}
