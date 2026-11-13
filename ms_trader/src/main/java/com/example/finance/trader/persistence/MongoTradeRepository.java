package com.example.finance.trader.persistence;

import com.example.finance.trader.application.StoredTrade;
import com.example.finance.trader.application.TradeAcceptance;
import com.example.finance.trader.application.TradeRepository;
import com.example.finance.trader.application.TradeStorageUnavailableException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class MongoTradeRepository implements TradeRepository {
    private final MongoTemplate mongo;

    public MongoTradeRepository(MongoTemplate mongo) {
        this.mongo = mongo;
        // Explicit durability; do not let an unacknowledged URI weaken acceptance semantics.
        mongo.setWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(3, TimeUnit.SECONDS));
        mongo.setReadPreference(ReadPreference.primary());
        // Fail startup if uniqueness cannot be established. No writes before this returns.
        mongo.indexOps(TradeDocument.class).createIndex(new Index()
            .on("idempotencyKey", Sort.Direction.ASC).unique()
            .named("uk_trade_idempotency").collation(Collation.simple()));
        mongo.indexOps(TradeDocument.class).createIndex(new Index()
            .on("createdAt", Sort.Direction.DESC).on("tradeId", Sort.Direction.DESC)
            .named("trade_created_at"));
    }

    @Override
    public TradeAcceptance insertOrGet(StoredTrade candidate) {
        try {
            try {
                TradeDocument inserted = mongo.insert(TradeDocument.from(candidate));
                return new TradeAcceptance(inserted.toTrade(), true);
            } catch (DuplicateKeyException race) {
                TradeDocument winner = mongo.findOne(Query.query(Criteria.where("idempotencyKey")
                    .is(candidate.idempotencyKey())).collation(Collation.simple()), TradeDocument.class);
                if (winner == null) throw race; // A different unique constraint failed; never fabricate a retry.
                return new TradeAcceptance(winner.toTrade(), false);
            }
        } catch (DataAccessException unavailable) {
            throw new TradeStorageUnavailableException(unavailable);
        }
    }

    @Override
    public Optional<StoredTrade> findById(String tradeId) {
        try {
            return Optional.ofNullable(mongo.findById(tradeId, TradeDocument.class)).map(TradeDocument::toTrade);
        } catch (DataAccessException unavailable) {
            throw new TradeStorageUnavailableException(unavailable);
        }
    }
}
