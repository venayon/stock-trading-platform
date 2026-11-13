package com.example.finance.processor;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

@Repository
public class MongoProcessingStore implements ProcessingStore {
    private final MongoTemplate mongo;

    public MongoProcessingStore(MongoTemplate mongo) {
        this.mongo = mongo;
        mongo.setWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(3, java.util.concurrent.TimeUnit.SECONDS));
        mongo.setReadPreference(ReadPreference.primary());
        mongo.indexOps(ProcessedTrade.class).createIndex(new Index().on("createdAt", Sort.Direction.DESC).on("tradeId", Sort.Direction.DESC));
        mongo.indexOps(ProcessedTrade.class).createIndex(new Index().on("trade.symbol", Sort.Direction.ASC).on("createdAt", Sort.Direction.DESC));
    }

    public Claim claim(TradeData input, ProcessedTrade.Point source, String correlationId, Instant now) {
        var candidate = new ProcessedTrade(input.tradeId(), input.digest(), input, "PENDING", source, null,
            new PartitionRouter().route(input.side(), input.symbol()), 1, correlationId, now, now, null, null);
        ProcessedTrade stored;
        try {
            stored = mongo.insert(candidate);
        } catch (DuplicateKeyException race) {
            stored = mongo.findById(input.tradeId(), ProcessedTrade.class);
        }
        if (stored == null) throw new IllegalStateException("Concurrent document unavailable");
        if (!stored.inputHash().equals(candidate.inputHash()))
            throw new InvalidTradeException("Trade ID payload conflict");
        if (stored.processingStatus().equals("COMPLETE")) return new Claim(stored, null, true);
        String token = UUID.randomUUID().toString();
        Criteria available = new Criteria().orOperator(Criteria.where("leaseUntil").lte(now), Criteria.where("leaseUntil").is(null));
        var query = Query.query(new Criteria().andOperator(Criteria.where("tradeId").is(input.tradeId()).and("processingStatus").is("PENDING"), available));
        var claimed = mongo.findAndModify(query, new Update().set("leaseToken", token).set("leaseUntil", now.plusSeconds(60)).set("updatedAt", now), FindAndModifyOptions.options().returnNew(true), ProcessedTrade.class);
        if (claimed == null) throw new IllegalStateException("Trade is currently leased; retry");
        return new Claim(claimed, token, false);
    }

    private Query owned(Claim claim) {
        return Query.query(Criteria.where("tradeId").is(claim.trade().tradeId()).and("leaseToken").is(claim.token()).and("processingStatus").is("PENDING"));
    }

    public void complete(Claim claim, ProcessedTrade.Point output, Instant now) {
        if (mongo.updateFirst(owned(claim), new Update().set("processingStatus", "COMPLETE").set("output", output).set("updatedAt", now).unset("leaseToken").unset("leaseUntil"), ProcessedTrade.class).getModifiedCount() != 1)
            throw new IllegalStateException("Stale processing lease");
    }

    public void release(Claim claim) {
        mongo.updateFirst(owned(claim), new Update().unset("leaseToken").unset("leaseUntil"), ProcessedTrade.class);
    }
}
