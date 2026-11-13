package com.example.finance.trader.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BacklogSamplerTest {
    @Test
    void failedPollingPreservesLastKnownBacklogAndExposesFailureRatherThanZero() {
        var registry = new SimpleMeterRegistry();
        Instant now = Instant.parse("2026-10-24T12:00:00Z");
        var fail = new AtomicBoolean();
        var sampler = new BacklogSampler(() -> {
            if (fail.get()) throw new RuntimeException("Mongo unavailable");
            return new BacklogSnapshot(3, now.minusSeconds(30));
        }, registry, Clock.fixed(now, ZoneOffset.UTC), new TradeMetrics(registry));
        assertTrue(Double.isNaN(registry.get("trades.pending.publications").gauge().value()));
        sampler.poll();
        assertEquals(3, registry.get("trades.pending.publications").gauge().value());
        assertEquals(30, registry.get("trades.pending.oldest.age.seconds").gauge().value());
        assertEquals(1, registry.get("trades.backlog.poll.success").gauge().value());
        fail.set(true);
        sampler.poll();
        assertEquals(3, registry.get("trades.pending.publications").gauge().value());
        assertEquals(0, registry.get("trades.backlog.poll.success").gauge().value());
        assertEquals(now.getEpochSecond(), registry.get("trades.backlog.last.success.timestamp.seconds").gauge().value());
    }

    @Test
    void emptyBacklogHasZeroAgeAfterSuccessfulSampling() {
        var registry = new SimpleMeterRegistry();
        var sampler = new BacklogSampler(() -> new BacklogSnapshot(0, null), registry, Clock.systemUTC(), new TradeMetrics(registry));
        sampler.poll();
        assertEquals(0, registry.get("trades.pending.oldest.age.seconds").gauge().value());
    }
}
