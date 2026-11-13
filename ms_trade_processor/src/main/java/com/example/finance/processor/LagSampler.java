package com.example.finance.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class LagSampler {
    private final Admin admin;
    private final MeterRegistry meters;
    private final Map<TopicPartition, AtomicLong> lag = new HashMap<>();
    private final AtomicInteger success = new AtomicInteger();
    private final AtomicLong last = new AtomicLong();

    public LagSampler(KafkaProperties properties, MeterRegistry meters) {
        this.meters = meters;
        var config = new HashMap<String, Object>(properties.buildAdminProperties(null));
        config.put("request.timeout.ms", 3000);
        config.put("default.api.timeout.ms", 4000);
        admin = Admin.create(config);
        meters.gauge("trades.lag.poll.success", success);
        meters.gauge("trades.lag.last.success.timestamp.seconds", last);
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void poll() {
        try {
            var description = admin.describeTopics(List.of("stock-trades")).allTopicNames().get(4, TimeUnit.SECONDS).get("stock-trades");
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>(), earliest = new HashMap<>();
            for (var partition : description.partitions()) {
                var key = new TopicPartition("stock-trades", partition.partition());
                latest.put(key, OffsetSpec.latest());
                earliest.put(key, OffsetSpec.earliest());
            }
            var ends = admin.listOffsets(latest).all().get(4, TimeUnit.SECONDS);
            var starts = admin.listOffsets(earliest).all().get(4, TimeUnit.SECONDS);
            var committed = admin.listConsumerGroupOffsets("stock-trade-processor-v1").partitionsToOffsetAndMetadata().get(4, TimeUnit.SECONDS);
            Map<TopicPartition, Long> values = new HashMap<>();
            for (var key : ends.keySet())
                values.put(key, Math.max(0, ends.get(key).offset() - (committed.get(key) == null ? starts.get(key).offset() : committed.get(key).offset())));
            values.forEach((key, value) -> lag.computeIfAbsent(key, k -> {
                var gauge = new AtomicLong();
                meters.gauge("trades.consumer.lag", Tags.of("topic", k.topic(), "partition", Integer.toString(k.partition()), "group", "stock-trade-processor-v1"), gauge);
                return gauge;
            }).set(value));
            last.set(java.time.Instant.now().getEpochSecond());
            success.set(1);
        } catch (Exception error) {
            success.set(0);
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void close() {
        admin.close(java.time.Duration.ofSeconds(2));
    }
}
