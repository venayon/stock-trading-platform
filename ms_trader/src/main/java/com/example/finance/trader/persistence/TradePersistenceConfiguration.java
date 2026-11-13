package com.example.finance.trader.persistence;

import com.example.finance.trader.application.TradeRepository;
import com.example.finance.trader.application.TradeService;
import com.example.finance.trader.observability.TradeMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Clock;
import java.util.UUID;

@Configuration
public class TradePersistenceConfiguration {
    @Bean
    TradeRepository tradeRepository(MongoTemplate mongo) {
        return new MongoTradeRepository(mongo);
    }

    @Bean
    TradeService tradeService(TradeRepository repository, TradeMetrics metrics) {
        return new TradeService(repository, Clock.systemUTC(), UUID::randomUUID, metrics);
    }
}
