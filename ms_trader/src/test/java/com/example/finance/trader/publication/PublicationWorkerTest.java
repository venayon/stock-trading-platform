package com.example.finance.trader.publication;

import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PublicationWorkerTest {
    static final Instant NOW = Instant.parse("2026-10-24T12:00:00Z");
    static final StoredTrade TRADE = new StoredTrade("trade-id", "key", "hash",
        new CreateTradeCommand(LocalDate.of(2026, 10, 24), "BUY", "AAPL", 10L,
            new BigDecimal("123.450000"), "USD", null), "PENDING", NOW, "correlation");
    static final EventMetadata ACK = new EventMetadata("stock-trades", 2, 37);

    @Test
    void marksPublishedOnlyAfterAcknowledgmentAndNeverRepublishesCompletedWork() {
        Store store = new Store();
        AtomicInteger sends = new AtomicInteger();
        PublicationWorker worker = new PublicationWorker(store, trade -> {
            assertTrue(store.pending);
            assertNull(store.completed);
            assertEquals(TRADE, trade);
            sends.incrementAndGet();
            return ACK;
        }, Clock.fixed(NOW, ZoneOffset.UTC));
        assertTrue(worker.runOnce());
        assertEquals(ACK, store.completed);
        assertFalse(worker.runOnce());
        assertEquals(1, sends.get());
        assertEquals(NOW.plusSeconds(60), store.leaseUntil);
    }

    @Test
    void failedSendRemainsPendingAndSchedulesRetryWithoutMetadata() {
        Store store = new Store();
        PublicationWorker worker = new PublicationWorker(store, trade -> {
            throw new Exception("broker down");
        },
            Clock.fixed(NOW, ZoneOffset.UTC));
        assertTrue(worker.runOnce());
        assertTrue(store.pending);
        assertNull(store.completed);
        assertEquals(NOW.plusSeconds(2), store.retryAt);
    }

    @Test
    void failedCompletionRemainsRecoverableAndBackoffIsBounded() {
        Store store = new Store();
        store.failCompletion = true;
        store.attempt = 100;
        PublicationWorker worker = new PublicationWorker(store, trade -> ACK, Clock.fixed(NOW, ZoneOffset.UTC));
        assertTrue(worker.runOnce());
        assertTrue(store.pending);
        assertEquals(NOW.plusSeconds(60), store.retryAt);
    }

    @Test
    void failedCompletionRecordsAcknowledgmentButNotCompletionAndTimesTheFailure() {
        Store store = new Store();
        store.failCompletion = true;
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var metrics = new com.example.finance.trader.observability.TradeMetrics(registry);
        assertTrue(new PublicationWorker(store, trade -> ACK, Clock.fixed(NOW, ZoneOffset.UTC), metrics).runOnce());
        assertEquals(1, registry.get("trades.published").counter().count());
        assertEquals(0, registry.get("trades.publication.completed").counter().count());
        assertEquals(1, registry.get("trades.publication").tag("outcome", "failure").timer().count());
    }

    @Test
    void emptyQueueDoesNotCallPublisher() {
        Store store = new Store();
        store.pending = false;
        PublicationWorker worker = new PublicationWorker(store, trade -> {
            fail("unexpected send");
            return ACK;
        }, Clock.systemUTC());
        assertFalse(worker.runOnce());
    }

    static class Store implements PublicationRepository {
        boolean pending = true, claimed, failCompletion;
        EventMetadata completed;
        Instant retryAt, leaseUntil;
        int attempt = 1;

        public Optional<PublicationTask> claimNext(Instant now, Instant leaseUntil, String token) {
            this.leaseUntil = leaseUntil;
            if (!pending || claimed) return Optional.empty();
            claimed = true;
            return Optional.of(new PublicationTask(TRADE, token, attempt));
        }

        public boolean markPublished(PublicationTask task, EventMetadata event) {
            if (failCompletion) throw new RuntimeException("Mongo acknowledgment lost");
            completed = event;
            pending = false;
            return true;
        }

        public void retryLater(PublicationTask task, Instant at) {
            retryAt = at;
        }
    }
}
