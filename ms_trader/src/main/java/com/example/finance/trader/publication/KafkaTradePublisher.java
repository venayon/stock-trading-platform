package com.example.finance.trader.publication;

import com.example.finance.avro.StockTrade;
import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class KafkaTradePublisher implements TradeEventPublisher {
    private final KafkaTemplate<String, StockTrade> template;
    private final String topic;
    private final StockTradeMapper mapper = new StockTradeMapper();

    public KafkaTradePublisher(KafkaTemplate<String, StockTrade> template, String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public EventMetadata publish(StoredTrade stored) throws Exception {
        if (template.partitionsFor(topic).size() != 6) {
            throw new IllegalStateException("stock-trades requires exactly six partitions");
        }
        ProducerRecord<String, StockTrade> record = new ProducerRecord<>(topic, stored.trade().symbol(), mapper.map(stored));
        record.headers().add("tradeId", stored.tradeId().getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Correlation-ID", stored.correlationId().getBytes(StandardCharsets.UTF_8));
        record.headers().add("schemaVersion", "1".getBytes(StandardCharsets.UTF_8));
        var result = template.send(record).get(11, TimeUnit.SECONDS);
        var metadata = result.getRecordMetadata();
        return new EventMetadata(metadata.topic(), metadata.partition(), metadata.offset());
    }
}
