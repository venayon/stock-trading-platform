package com.example.finance.trader.persistence;

import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.publication.PublicationRepository;
import com.example.finance.trader.publication.PublicationTask;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Instant;
import java.util.Optional;

public class MongoPublicationRepository implements PublicationRepository {
    private final MongoTemplate mongo;

    public MongoPublicationRepository(MongoTemplate mongo) {
        this.mongo = mongo;
        mongo.indexOps(TradeDocument.class).createIndex(new Index()
            .on("publicationStatus", Sort.Direction.ASC).on("nextAttemptAt", Sort.Direction.ASC)
            .on("leaseUntil", Sort.Direction.ASC).named("pending_publication"));
    }

    @Override
    public Optional<PublicationTask> claimNext(Instant now, Instant leaseUntil, String token) {
        Criteria due = new Criteria().orOperator(Criteria.where("nextAttemptAt").lte(now),
            Criteria.where("nextAttemptAt").exists(false), Criteria.where("nextAttemptAt").is(null));
        Criteria unleased = new Criteria().orOperator(Criteria.where("leaseUntil").lte(now),
            Criteria.where("leaseUntil").exists(false), Criteria.where("leaseUntil").is(null));
        Query query = Query.query(new Criteria().andOperator(Criteria.where("publicationStatus").is("PENDING"), due, unleased))
            .with(Sort.by(Sort.Order.asc("createdAt"), Sort.Order.asc("tradeId")));
        TradeDocument document = mongo.findAndModify(query, new Update().set("leaseToken", token)
                .set("leaseUntil", leaseUntil).inc("publicationAttempts", 1),
            FindAndModifyOptions.options().returnNew(true), TradeDocument.class);
        return Optional.ofNullable(document).map(d -> new PublicationTask(d.toTrade(), token, d.publicationAttempts()));
    }

    private Query owned(PublicationTask task) {
        return Query.query(Criteria.where("tradeId").is(task.trade().tradeId())
            .and("publicationStatus").is("PENDING").and("leaseToken").is(task.leaseToken()));
    }

    @Override
    public boolean markPublished(PublicationTask task, EventMetadata event) {
        return mongo.updateFirst(owned(task), new Update().set("publicationStatus", "PUBLISHED").set("event", event)
                .unset("leaseToken").unset("leaseUntil").unset("nextAttemptAt").unset("lastPublicationError"),
            TradeDocument.class).getModifiedCount() == 1;
    }

    @Override
    public void retryLater(PublicationTask task, Instant nextAttemptAt) {
        mongo.updateFirst(owned(task), new Update().set("nextAttemptAt", nextAttemptAt)
            .set("lastPublicationError", "PUBLICATION_FAILED").unset("leaseToken").unset("leaseUntil"), TradeDocument.class);
    }
}
