package com.example.finance.trader.observability;

import com.example.finance.trader.persistence.TradeDocument;
import io.micrometer.core.instrument.MeterRegistry;
import org.bson.Document;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration
@EnableScheduling
public class ObservabilityConfiguration {
    @Bean
    TradeMetrics tradeMetrics(MeterRegistry registry) {
        return new TradeMetrics(registry);
    }

    @Bean
    @ConditionalOnProperty(name = "trader.observability.backlog.enabled", havingValue = "true", matchIfMissing = true)
    BacklogSampler backlogSampler(MongoTemplate mongo, MeterRegistry registry, TradeMetrics metrics) {
        return new BacklogSampler(() -> {
            Document result = mongo.aggregate(Aggregation.newAggregation(
                    Aggregation.match(Criteria.where("publicationStatus").is("PENDING")),
                    Aggregation.group().count().as("pending").min("createdAt").as("oldest")),
                TradeDocument.class, Document.class).getUniqueMappedResult();
            return result == null ? new BacklogSnapshot(0, null) : new BacklogSnapshot(
                ((Number) result.get("pending")).longValue(), result.getDate("oldest").toInstant());
        }, registry, Clock.systemUTC(), metrics);
    }
}
