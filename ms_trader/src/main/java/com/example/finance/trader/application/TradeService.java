package com.example.finance.trader.application;

import com.example.finance.trader.observability.TradeLogContext;
import com.example.finance.trader.observability.TradeMetrics;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public class TradeService {
    private final TradeRepository repository;
    private final Clock clock;
    private final TradeMetrics metrics;
    private final Supplier<UUID> ids;
    private final TradeValidator validator = new TradeValidator();
    private final CanonicalTradeHasher hasher = new CanonicalTradeHasher();

    public TradeService(TradeRepository repository, Clock clock, Supplier<UUID> ids) {
        this(repository, clock, ids, TradeMetrics.noop());
    }

    public TradeService(TradeRepository repository, Clock clock, Supplier<UUID> ids, TradeMetrics metrics) {
        this.repository = repository;
        this.clock = clock;
        this.ids = ids;
        this.metrics = metrics;
    }

    public TradeAcceptance accept(String key, CreateTradeCommand command, String correlationId) {
        try {
            return acceptValidated(key, command, correlationId);
        } catch (TradeValidationException error) {
            metrics.error(TradeMetrics.Stage.ACCEPTANCE, TradeMetrics.Code.VALIDATION);
            throw error;
        } catch (IdempotencyConflictException error) {
            metrics.error(TradeMetrics.Stage.ACCEPTANCE, TradeMetrics.Code.CONFLICT);
            throw error;
        } catch (TradeStorageUnavailableException error) {
            metrics.error(TradeMetrics.Stage.ACCEPTANCE, TradeMetrics.Code.STORAGE);
            throw error;
        }
    }

    private TradeAcceptance acceptValidated(String key, CreateTradeCommand command, String correlationId) {
        if (key == null || key.isBlank() || !key.matches("[\\x20-\\x7E]{1,128}")) {
            throw new TradeValidationException(Map.of("idempotencyKey",
                "Idempotency-Key must contain 1 to 128 printable ASCII characters and must not be blank"));
        }
        CreateTradeCommand canonical = validator.validate(command);
        StoredTrade candidate = new StoredTrade(ids.get().toString(), key, hasher.hash(canonical), canonical,
            "PENDING", clock.instant().truncatedTo(ChronoUnit.MILLIS), correlationId);
        TradeAcceptance result = repository.insertOrGet(candidate);
        if (!result.trade().requestHash().equals(candidate.requestHash())) {
            throw new IdempotencyConflictException();
        }
        if (result.created()) metrics.accepted();
        else metrics.replayed();
        try (var context = new TradeLogContext(result.trade().tradeId(), correlationId)) {
            org.slf4j.LoggerFactory.getLogger(TradeService.class).atInfo()
                .addKeyValue("event", "trade.acceptance")
                .addKeyValue("outcome", result.created() ? "ACCEPTED" : "REPLAYED").log("Trade acceptance");
        }
        return result;
    }

    public Optional<StoredTrade> findById(String tradeId) {
        try {
            return repository.findById(tradeId);
        } catch (TradeStorageUnavailableException error) {
            metrics.error(TradeMetrics.Stage.QUERY, TradeMetrics.Code.STORAGE);
            throw error;
        }
    }
}
