package com.example.finance.trader.application;

import com.example.finance.trader.support.InMemoryTradeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TradeValidationTest {

    private static final LocalDate DATE = LocalDate.of(2026, 10, 24);

    static Stream<CreateTradeCommand> invalidTrades() {
        return Stream.of(
            null,
            trade(null, "BUY", "AAPL", 1L, "1", "USD", null),
            trade(LocalDate.MAX, "BUY", "AAPL", 1L, "1", "USD", null),
            trade(LocalDate.MIN, "BUY", "AAPL", 1L, "1", "USD", null),
            trade(DATE, null, "AAPL", 1L, "1", "USD", null),
            trade(DATE, "buy", "AAPL", 1L, "1", "USD", null),
            trade(DATE, "HOLD", "AAPL", 1L, "1", "USD", null),
            trade(DATE, "BUY", null, 1L, "1", "USD", null),
            trade(DATE, "BUY", " ", 1L, "1", "USD", null),
            trade(DATE, "BUY", "AAPL!", 1L, "1", "USD", null),
            trade(DATE, "BUY", "A".repeat(17), 1L, "1", "USD", null),
            trade(DATE, "BUY", ".AAPL", 1L, "1", "USD", null),
            trade(DATE, "BUY", "AAPL", null, "1", "USD", null),
            trade(DATE, "BUY", "AAPL", 0L, "1", "USD", null),
            trade(DATE, "BUY", "AAPL", -1L, "1", "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, null, "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, "0", "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, "-0.000001", "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, "0.0000001", "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, "1000000000000", "USD", null),
            trade(DATE, "BUY", "AAPL", 1L, "1", null, null),
            trade(DATE, "BUY", "AAPL", 1L, "1", "usd", null),
            trade(DATE, "BUY", "AAPL", 1L, "1", "JPY", null),
            trade(DATE, "BUY", "AAPL", 1L, "1", "US", null),
            trade(DATE, "BUY", "AAPL", 1L, "1", "USD", "x".repeat(129)));
    }

    static CreateTradeCommand trade(LocalDate date, String side, String symbol, Long quantity,
                                    String price, String currency, String counterparty) {
        return new CreateTradeCommand(date, side, symbol, quantity,
            price == null ? null : new BigDecimal(price), currency, counterparty);
    }

    @ParameterizedTest
    @MethodSource("invalidTrades")
    void rejectsInvalidTradeWithoutSideEffects(CreateTradeCommand command) {
        InMemoryTradeRepository repository = new InMemoryTradeRepository();
        TradeService service = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID);
        assertAll(
            () -> assertThrows(IllegalArgumentException.class, () -> service.accept("test-key", command, "correlation")),
            () -> assertEquals(0, repository.attempts.get()));
    }

    @Test
    void canonicalizesBeforePersistence() {
        CreateTradeCommand input = trade(DATE, "SELL", " brk.b ", Long.MAX_VALUE,
            "1.2300000", "GBP", "  demo  ");
        CreateTradeCommand expected = trade(DATE, "SELL", "BRK.B", Long.MAX_VALUE,
            "1.230000", "GBP", "demo");
        InMemoryTradeRepository repository = new InMemoryTradeRepository();
        TradeAcceptance result = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID)
            .accept("test-key", input, "correlation");
        assertEquals(expected, result.trade().trade());
        assertEquals(expected, repository.findById(result.trade().tradeId()).orElseThrow().trade());
    }

    @Test
    void acceptsDecimalAndDateBoundariesAndBlankCounterparty() {
        for (long day : new long[]{Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            CreateTradeCommand actual = new TradeValidator().validate(trade(
                LocalDate.ofEpochDay(day), "BUY", "A".repeat(16), 1L,
                "999999999999.999999", "EUR", "  "));
            assertNull(actual.counterparty());
            assertEquals(new BigDecimal("999999999999.999999"), actual.price());
        }
        assertEquals(new BigDecimal("0.000001"), new TradeValidator().validate(
            trade(DATE, "BUY", "AAPL", 1L, "0.000001", "USD", "x".repeat(128))).price());
    }
}
