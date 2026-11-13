package com.example.finance.trader.application;

import com.example.finance.trader.support.InMemoryTradeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.stream.Stream;

import static com.example.finance.trader.application.TradeValidationTest.trade;
import static org.junit.jupiter.api.Assertions.*;

class TradeAcceptanceTest {
    private static final Instant NOW = Instant.parse("2026-10-24T12:00:00Z");
    private static final CreateTradeCommand VALID = trade(java.time.LocalDate.of(2026, 10, 24),
        "BUY", "AAPL", 10L, "123.45", "USD", null);
    private final InMemoryTradeRepository repository = new InMemoryTradeRepository();
    private final TradeService service = new TradeService(repository,
        Clock.fixed(NOW, ZoneOffset.UTC), UUID::randomUUID);

    static Stream<CreateTradeCommand> changedPayloads() {
        return Stream.of(
            trade(VALID.tradeDate().plusDays(1), "BUY", "AAPL", 10L, "123.45", "USD", null),
            trade(VALID.tradeDate(), "SELL", "AAPL", 10L, "123.45", "USD", null),
            trade(VALID.tradeDate(), "BUY", "MSFT", 10L, "123.45", "USD", null),
            trade(VALID.tradeDate(), "BUY", "AAPL", 11L, "123.45", "USD", null),
            trade(VALID.tradeDate(), "BUY", "AAPL", 10L, "123.46", "USD", null),
            trade(VALID.tradeDate(), "BUY", "AAPL", 10L, "123.45", "GBP", null),
            trade(VALID.tradeDate(), "BUY", "AAPL", 10L, "123.45", "USD", "demo"));
    }

    static Stream<String> invalidKeys() {
        return Stream.of(null, "", "   ", "x".repeat(129), "a\nb", "café", "a\u007fb");
    }

    @Test
    void acceptsOneDurablePendingTradeWithGeneratedId() {
        TradeAcceptance result = service.accept("request-1", VALID, "correlation-1");
        assertTrue(result.created());
        UUID.fromString(result.trade().tradeId());
        assertEquals("PENDING", result.trade().publicationStatus());
        assertEquals(NOW, result.trade().createdAt());
        assertEquals("correlation-1", result.trade().correlationId());
        assertEquals(result.trade(), repository.findById(result.trade().tradeId()).orElseThrow());
        assertEquals(1, repository.size());
    }

    @Test
    void retryReusesTradeEvenWhenCanonicalInputsDifferOrServiceRestarts() {
        TradeAcceptance first = service.accept("request-1", VALID, "first-correlation");
        CreateTradeCommand normalizedEquivalent = trade(VALID.tradeDate(), "BUY", " aapl ", 10L,
            "123.4500000", "USD", "   ");
        TradeAcceptance retry = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID)
            .accept("request-1", normalizedEquivalent, "new-correlation");
        assertFalse(retry.created());
        assertEquals(first.trade(), retry.trade());
        assertEquals(1, repository.size());
    }

    @Test
    void samePayloadWithDifferentKeysCreatesDifferentTrades() {
        var first = service.accept("request-1", VALID, "correlation");
        var second = service.accept("request-2", VALID, "correlation");
        assertNotEquals(first.trade().tradeId(), second.trade().tradeId());
        assertEquals(2, repository.size());
    }

    @ParameterizedTest
    @MethodSource("changedPayloads")
    void changedPayloadConflictsWithoutChangingStoredTrade(CreateTradeCommand changed) {
        var first = service.accept("request-1", VALID, "correlation");
        assertThrows(IdempotencyConflictException.class, () -> service.accept("request-1", changed, "other"));
        assertEquals(first.trade(), repository.findById(first.trade().tradeId()).orElseThrow());
        assertEquals(1, repository.size());
    }

    @ParameterizedTest
    @MethodSource("invalidKeys")
    void invalidKeysNeverReachPersistence(String key) {
        assertThrows(TradeValidationException.class, () -> service.accept(key, VALID, "correlation"));
        assertEquals(0, repository.attempts.get());
    }

    @Test
    void retryResolvesAnAcknowledgmentLostAfterTheDocumentWasPersisted() {
        TradeRepository uncertain = new TradeRepository() {
            private boolean first = true;

            public TradeAcceptance insertOrGet(StoredTrade candidate) {
                TradeAcceptance result = repository.insertOrGet(candidate);
                if (first) {
                    first = false;
                    throw new TradeStorageUnavailableException(new RuntimeException("lost acknowledgment"));
                }
                return result;
            }

            public java.util.Optional<StoredTrade> findById(String id) {
                return repository.findById(id);
            }
        };
        TradeService client = new TradeService(uncertain, Clock.systemUTC(), UUID::randomUUID);
        assertThrows(TradeStorageUnavailableException.class, () -> client.accept("uncertain", VALID, "first"));
        var retry = client.accept("uncertain", VALID, "retry");
        assertFalse(retry.created());
        assertEquals(1, repository.size());
        assertEquals("first", retry.trade().correlationId());
    }

    @Test
    void boundaryKeysAreNotTrimmedOrCaseFolded() {
        assertTrue(service.accept("x".repeat(128), VALID, "correlation").created());
        assertTrue(service.accept("key", VALID, "correlation").created());
        assertTrue(service.accept("KEY", VALID, "correlation").created());
        assertTrue(service.accept(" key ", VALID, "correlation").created());
        assertEquals(4, repository.size());
    }
}
