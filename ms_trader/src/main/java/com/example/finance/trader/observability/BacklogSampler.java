package com.example.finance.trader.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class BacklogSampler {
    private static final Logger LOG = LoggerFactory.getLogger(BacklogSampler.class);
    private final Supplier<BacklogSnapshot> reader;
    private final Clock clock;
    private final TradeMetrics metrics;
    private final AtomicReference<BacklogSnapshot> snapshot = new AtomicReference<>();
    private final AtomicInteger success = new AtomicInteger();
    private final AtomicLong lastSuccess = new AtomicLong();

    public BacklogSampler(Supplier<BacklogSnapshot> reader, MeterRegistry registry, Clock clock, TradeMetrics metrics) {
        this.reader = reader;
        this.clock = clock;
        this.metrics = metrics;
        Gauge.builder("trades.pending.publications", snapshot, ref -> ref.get() == null ? Double.NaN : ref.get().pending()).register(registry);
        Gauge.builder("trades.pending.oldest.age.seconds", this, BacklogSampler::oldestAge).register(registry);
        registry.gauge("trades.backlog.poll.success", success);
        registry.gauge("trades.backlog.last.success.timestamp.seconds", lastSuccess);
    }

    private double oldestAge() {
        BacklogSnapshot current = snapshot.get();
        if (current == null) return Double.NaN;
        return current.oldest() == null ? 0 : Math.max(0, clock.instant().getEpochSecond() - current.oldest().getEpochSecond());
    }

    @Scheduled(fixedDelayString = "${trader.observability.backlog.interval-ms:5000}")
    public void poll() {
        try {
            snapshot.set(reader.get());
            lastSuccess.set(clock.instant().getEpochSecond());
            success.set(1);
        } catch (Exception error) {
            success.set(0);
            metrics.error(TradeMetrics.Stage.BACKLOG, TradeMetrics.Code.SAMPLING);
            LOG.warn("backlog poll outcome=FAILED errorType={}", error.getClass().getSimpleName());
        }
    }
}
