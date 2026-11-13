package com.example.finance.processor;

import com.example.finance.avro.StockTrade;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    @Bean
    ProducerFactory<Object, Object> producerFactory(KafkaProperties properties) {
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(),
            () -> new DelegatingByTypeSerializer(Map.of(String.class, new StringSerializer(), byte[].class, new ByteArraySerializer())),
            () -> new DelegatingByTypeSerializer(Map.of(StockTrade.class, new KafkaAvroSerializer(), byte[].class, new ByteArraySerializer())));
    }

    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> factory) {
        return new KafkaTemplate<>(factory);
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template, MeterRegistry meters) {
        var recoverer = new DeadLetterPublishingRecoverer(template, (record, error) -> new TopicPartition("stock-trades-dlt", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(15));
        recoverer.excludeHeader(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE,
            DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_MSG);
        recoverer.setStripPreviousExceptionHeaders(true);
        var handler = new DefaultErrorHandler((record, error) -> {
            recoverer.accept(record, error); // Throwing here retains the original offset for recovery.
            meters.counter("trades.dlt").increment();
        }, new FixedBackOff(1000L, 3L));
        handler.addNotRetryableExceptions(InvalidTradeException.class);
        handler.setAckAfterHandle(true); // RECORD mode commits only after successful recovery.
        return handler;
    }
}
