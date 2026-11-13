package com.example.finance.feed;

import com.example.finance.trader.observability.TradeMetrics;
import com.example.finance.trader.persistence.TradePersistenceConfiguration;
import com.example.finance.trader.publication.PublicationConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableConfigurationProperties(FeedProperties.class)
@Import({TradePersistenceConfiguration.class, PublicationConfiguration.class})
public class FeedApplication {
    public static void main(String[] args) {
        SpringApplication.run(FeedApplication.class, args);
    }

    @Bean
    TradeMetrics tradeMetrics(MeterRegistry registry) {
        return new TradeMetrics(registry);
    }
}
