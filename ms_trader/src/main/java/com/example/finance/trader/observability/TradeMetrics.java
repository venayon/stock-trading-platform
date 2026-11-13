package com.example.finance.trader.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Locale;

public class TradeMetrics {
    private static final TradeMetrics NOOP = new TradeMetrics(new CompositeMeterRegistry());
    private final MeterRegistry registry;
    private final Counter accepted, replayed, acknowledged, completed, retries, stale;

    public TradeMetrics(MeterRegistry registry) {
        this.registry = registry;
        accepted = registry.counter("trades.accepted");
        replayed = registry.counter("trades.idempotent.replays");
        acknowledged = registry.counter("trades.published");
        completed = registry.counter("trades.publication.completed");
        retries = registry.counter("trades.publication.retries");
        stale = registry.counter("trades.publication.stale");
        for (Outcome outcome : Outcome.values()) timer(outcome);
    }

    public static TradeMetrics noop() {
        return NOOP;
    }

    public void accepted() {
        accepted.increment();
    }

    public void replayed() {
        replayed.increment();
    }

    public void acknowledged() {
        acknowledged.increment();
    }

    public void completed() {
        completed.increment();
    }

    public void retried() {
        retries.increment();
    }

    public void stale() {
        stale.increment();
    }

    public void error(Stage stage, Code code) {
        registry.counter("trades.errors", "stage", lower(stage), "code", lower(code)).increment();
    }

    private String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }

    private Timer timer(Outcome outcome) {
        return Timer.builder("trades.publication").tag("outcome", lower(outcome))
            .publishPercentileHistogram().register(registry);
    }

    public Timer.Sample startPublication() {
        return Timer.start(registry);
    }

    public void stopPublication(Timer.Sample sample, Outcome outcome) {
        sample.stop(timer(outcome));
    }

    public enum Stage {ACCEPTANCE, QUERY, PUBLICATION, COMPLETION, POLL, BACKLOG}

    public enum Code {VALIDATION, CONFLICT, STORAGE, SEND, STATE, SAMPLING}

    public enum Outcome {SUCCESS, FAILURE, STALE}
}
