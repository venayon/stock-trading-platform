package com.example.finance.trader.publication;

import com.example.finance.avro.StockTrade;
import com.example.finance.trader.application.TradeRepository;
import com.example.finance.trader.observability.TradeMetrics;
import com.example.finance.trader.persistence.MongoPublicationRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "trader.publication.enabled", havingValue = "true", matchIfMissing = true)
public class PublicationConfiguration {
    @Bean
    PublicationRepository publicationRepository(MongoTemplate mongo, TradeRepository acceptedTrades) {
        // acceptedTrades initializes durability/read preference and acceptance indexes first.
        return new MongoPublicationRepository(mongo);
    }

    @Bean
    TradeEventPublisher tradeEventPublisher(KafkaTemplate<String, StockTrade> kafka,
                                            @Value("${trader.publication.topic}") String topic) {
        return new KafkaTradePublisher(kafka, topic);
    }

    @Bean
    PublicationWorker publicationWorker(PublicationRepository repository, TradeEventPublisher publisher, TradeMetrics metrics) {
        return new PublicationWorker(repository, publisher, Clock.systemUTC(), metrics);
    }

    @Bean
    PublicationScheduler publicationScheduler(PublicationWorker worker, TradeMetrics metrics) {
        return new PublicationScheduler(worker, metrics);
    }
}
