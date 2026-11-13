package com.example.finance.trader.publication;

import com.example.finance.trader.observability.TradeLogContext;
import com.example.finance.trader.observability.TradeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.UUID;

public class PublicationWorker {
    private static final Logger LOG = LoggerFactory.getLogger(PublicationWorker.class);
    private final PublicationRepository repository;
    private final TradeEventPublisher publisher;
    private final Clock clock;
    private final TradeMetrics metrics;

    public PublicationWorker(PublicationRepository repository, TradeEventPublisher publisher, Clock clock) {
        this(repository, publisher, clock, TradeMetrics.noop());
    }

    public PublicationWorker(PublicationRepository repository, TradeEventPublisher publisher, Clock clock, TradeMetrics metrics) {
        this.repository = repository;
        this.publisher = publisher;
        this.clock = clock;
        this.metrics = metrics;
    }

    public boolean runOnce() {
        var now = clock.instant();
        var next = repository.claimNext(now, now.plusSeconds(60), UUID.randomUUID().toString());
        if (next.isEmpty()) return false;
        PublicationTask task = next.get();
        var sample = metrics.startPublication();
        var outcome = TradeMetrics.Outcome.FAILURE;
        var stage = TradeMetrics.Stage.PUBLICATION;
        try (var context = new TradeLogContext(task.trade().tradeId(), task.trade().correlationId())) {
            var event = publisher.publish(task.trade());
            metrics.acknowledged();
            stage = TradeMetrics.Stage.COMPLETION;
            boolean completed = repository.markPublished(task, event);
            if (completed) metrics.completed();
            else metrics.stale();
            outcome = completed ? TradeMetrics.Outcome.SUCCESS : TradeMetrics.Outcome.STALE;
            LOG.info("publication outcome={} tradeId={} correlationId={} topic={} partition={} offset={}",
                completed ? "PUBLISHED" : "STALE_LEASE", task.trade().tradeId(), task.trade().correlationId(),
                event.topic(), event.partition(), event.offset());
        } catch (Exception error) {
            metrics.retried();
            metrics.error(stage, stage == TradeMetrics.Stage.PUBLICATION ? TradeMetrics.Code.SEND : TradeMetrics.Code.STATE);
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            LOG.warn("publication outcome=RETRY tradeId={} correlationId={} attempt={} errorType={}",
                task.trade().tradeId(), task.trade().correlationId(), task.attempt(), error.getClass().getSimpleName());
            // If Mongo is unavailable this update can fail; the durable lease will expire instead.
            repository.retryLater(task, clock.instant().plusSeconds(Math.min(60L, 1L << Math.min(task.attempt(), 6))));
        } finally {
            metrics.stopPublication(sample, outcome);
        }
        return true;
    }
}
