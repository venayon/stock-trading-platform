package com.example.finance.trader.application;

import com.example.finance.trader.support.InMemoryTradeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TradeServiceTest {
    @Test
    void rejectsMissingPriceWithoutPersistenceOrPublication() {
        InMemoryTradeRepository repository = new InMemoryTradeRepository();
        TradeService service = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID);
        CreateTradeCommand command = new CreateTradeCommand(
            LocalDate.of(2026, 10, 24), "BUY", "AAPL", 10L,
            null, "USD", null);

        assertAll(
            () -> {
                IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> service.accept("test-key", command, "correlation"));
                assertEquals("price is required", error.getMessage());
            },
            () -> assertEquals(0, repository.attempts.get(), "persistence calls"));
    }

    @ParameterizedTest(name = "price {0} is rejected without side effects")
    @ValueSource(strings = {"0", "0.000000", "-1", "-123.450000", "-0.000001"})
    void rejectsNonPositivePriceWithoutPersistenceOrPublication(String price) {
        InMemoryTradeRepository repository = new InMemoryTradeRepository();
        TradeService service = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID);
        CreateTradeCommand command = new CreateTradeCommand(
            LocalDate.of(2026, 10, 24), "BUY", "AAPL", 10L,
            new BigDecimal(price), "USD", null);

        assertAll(
            () -> {
                IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> service.accept("test-key", command, "correlation"));
                assertEquals("price must be greater than zero", error.getMessage());
            },
            () -> assertEquals(0, repository.attempts.get(), "persistence calls"));
    }

    @ParameterizedTest(name = "quantity {0} is rejected without side effects")
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonPositiveQuantityWithoutPersistenceOrPublication(long quantity) {
        InMemoryTradeRepository repository = new InMemoryTradeRepository();
        TradeService service = new TradeService(repository, Clock.systemUTC(), UUID::randomUUID);
        CreateTradeCommand command = new CreateTradeCommand(
            LocalDate.of(2026, 10, 24), "BUY", "AAPL", quantity,
            new BigDecimal("123.450000"), "USD", null);

        assertAll(
            () -> {
                IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> service.accept("test-key", command, "correlation"));
                assertEquals("quantity must be greater than zero", error.getMessage());
            },
            () -> assertEquals(0, repository.attempts.get(), "persistence calls"));
    }
}
