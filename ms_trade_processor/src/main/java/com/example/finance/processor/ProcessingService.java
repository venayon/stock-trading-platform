package com.example.finance.processor;

import com.example.finance.avro.StockTrade;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class ProcessingService {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingService.class);
    private final ProcessingStore store;
    private final KafkaTemplate<Object, Object> producer;
    private final MeterRegistry meters;

    public ProcessingService(ProcessingStore store, KafkaTemplate<Object, Object> producer, MeterRegistry meters) {
        this.store = store;
        this.producer = producer;
        this.meters = meters;
        for (String name : new String[]{"trades.consumed", "trades.processed", "trades.deduplicated", "trades.processor.errors", "trades.dlt"})
            meters.counter(name);
    }

    @KafkaListener(id = "stock-trade-processor-v1", topics = "stock-trades", concurrency = "${processor.concurrency:3}", autoStartup = "${processor.listener.enabled:true}")
    public void consume(ConsumerRecord<String, StockTrade> record) throws Exception {
        meters.counter("trades.consumed").increment();
        Timer.Sample sample = Timer.start(meters);
        String outcome = "failure";
        ProcessingStore.Claim claim = null;
        String oldId = MDC.get("tradeId"), oldCorrelation = MDC.get("correlationId");
        try {
            TradeData data = TradeData.validate(record.value(), record.key());
            var header = record.headers().lastHeader("X-Correlation-ID");
            String correlation = header == null || header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
            if (!correlation.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
                correlation = UUID.randomUUID().toString();
            MDC.put("tradeId", data.tradeId());
            MDC.put("correlationId", correlation);
            claim = store.claim(data, new ProcessedTrade.Point(record.topic(), record.partition(), record.offset()), correlation, Instant.now());
            if (claim.complete()) {
                meters.counter("trades.deduplicated").increment();
                outcome = "duplicate";
                return;
            }
            if (producer.partitionsFor("stock-trades-processed").size() != 6)
                throw new IllegalStateException("Processed topic requires six partitions");
            var output = new ProducerRecord<Object, Object>("stock-trades-processed", claim.trade().targetPartition(), data.symbol(), data.event());
            output.headers().add("X-Correlation-ID", correlation.getBytes(StandardCharsets.UTF_8));
            output.headers().add("tradeId", data.tradeId().getBytes(StandardCharsets.UTF_8));
            output.headers().add("source-topic", record.topic().getBytes(StandardCharsets.UTF_8));
            output.headers().add("source-partition", Integer.toString(record.partition()).getBytes(StandardCharsets.UTF_8));
            output.headers().add("source-offset", Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
            output.headers().add("routeVersion", "1".getBytes(StandardCharsets.UTF_8));
            var ack = producer.send(output).get(11, TimeUnit.SECONDS).getRecordMetadata();
            store.complete(claim, new ProcessedTrade.Point(ack.topic(), ack.partition(), ack.offset()), Instant.now());
            meters.counter("trades.processed").increment();
            outcome = "success";
            LOG.atInfo().addKeyValue("event", "trade.processed").addKeyValue("partition", ack.partition()).addKeyValue("offset", ack.offset()).log("Trade processed");
        } catch (Exception failure) {
            meters.counter("trades.processor.errors").increment();
            if (claim != null && !claim.complete()) {
                try {
                    store.release(claim);
                } catch (RuntimeException ignored) { /* lease expiry remains recovery */ }
            }
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            throw failure;
        } finally {
            sample.stop(Timer.builder("trades.processing").tag("outcome", outcome).publishPercentileHistogram().register(meters));
            if (oldId == null) MDC.remove("tradeId");
            else MDC.put("tradeId", oldId);
            if (oldCorrelation == null) MDC.remove("correlationId");
            else MDC.put("correlationId", oldCorrelation);
        }
    }
}
