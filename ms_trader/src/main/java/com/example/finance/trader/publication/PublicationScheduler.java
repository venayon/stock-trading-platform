package com.example.finance.trader.publication;

import com.example.finance.trader.observability.TradeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class PublicationScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(PublicationScheduler.class);
    private final PublicationWorker worker;
    private final TradeMetrics metrics;

    public PublicationScheduler(PublicationWorker worker, TradeMetrics metrics) {
        this.worker = worker;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${trader.publication.poll-interval-ms:1000}",
        initialDelayString = "${trader.publication.initial-delay-ms:1000}")
    public void poll() {
        try {
            for (int i = 0; i < 20 && !Thread.currentThread().isInterrupted(); i++) {
                if (!worker.runOnce()) break;
            }
        } catch (Exception error) {
            metrics.error(TradeMetrics.Stage.POLL, TradeMetrics.Code.STORAGE);
            LOG.warn("publication poll outcome=RETRY errorType={}", error.getClass().getSimpleName());
        }
    }
}
