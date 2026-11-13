package com.example.finance.trader.publication;

import com.example.finance.avro.StockTrade;
import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;
import com.example.finance.trader.application.TradeService;
import com.example.finance.trader.persistence.TradeDocument;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties = "trader.publication.initial-delay-ms=3600000")
class KafkaPublicationIT {
    static final String TOPIC = "stock-trades";
    static final Network NETWORK = Network.newNetwork();
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.9.4")
        .withNetwork(NETWORK).withListener("kafka:19092")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .withEnv("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m");
    @Container
    static final GenericContainer<?> REGISTRY = new GenericContainer<>(
        DockerImageName.parse("confluentinc/cp-schema-registry:7.9.4"))
        .withNetwork(NETWORK).withExposedPorts(8081).dependsOn(KAFKA)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "registry")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("SCHEMA_REGISTRY_HEAP_OPTS", "-Xms64m -Xmx256m")
        .waitingFor(Wait.forHttp("/subjects").forPort(8081).withStartupTimeout(Duration.ofSeconds(120)));
    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0.16");
    @Autowired
    TradeService trades;
    @Autowired
    PublicationRepository pending;
    @Autowired
    TradeEventPublisher publisher;
    @Autowired
    PublicationWorker worker;
    @Autowired
    MongoTemplate mongo;

    static String registryUrl() {
        return "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(8081);
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 6, (short) 1).configs(Map.of(
                    "retention.ms", "604800000", "cleanup.policy", "delete", "min.insync.replicas", "1"))))
                .all().get(20, TimeUnit.SECONDS);
        }
        try (var source = KafkaPublicationIT.class.getResourceAsStream("/StockTrade.avsc");
             var schemas = new CachedSchemaRegistryClient(registryUrl(), 10)) {
            schemas.updateCompatibility(TOPIC + "-value", "BACKWARD_TRANSITIVE");
            schemas.register(TOPIC + "-value", new AvroSchema(new Schema.Parser().parse(source)));
        }
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.kafka.producer.properties.schema.registry.url", KafkaPublicationIT::registryUrl);
        properties.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("trader"));
    }

    @BeforeEach
    void cleanMongo() {
        mongo.remove(new Query(), TradeDocument.class);
    }

    StoredTrade accept() {
        return trades.accept(UUID.randomUUID().toString(), new CreateTradeCommand(LocalDate.of(2026, 10, 24),
                "BUY", "AAPL", Long.MAX_VALUE, new BigDecimal("999999999999.999999"), "USD", null),
            "550e8400-e29b-41d4-a716-446655440000").trade();
    }

    @Test
    void publishesSpecificAvroWithHeadersKeyAndAcknowledgedCoordinates() throws Exception {
        StoredTrade accepted = accept();
        assertEquals("PENDING", accepted.publicationStatus());
        assertTrue(worker.runOnce());
        StoredTrade published = trades.findById(accepted.tradeId()).orElseThrow();
        assertEquals("PUBLISHED", published.publicationStatus());
        var records = recordsFor(accepted.tradeId(), 1);
        var record = records.getFirst();
        assertEquals("AAPL", record.key());
        assertEquals(accepted.trade().price(), record.value().getPrice());
        assertEquals(accepted.trade().tradeDate(), record.value().getTradeDate());
        assertEquals(Long.MAX_VALUE, record.value().getQuantity());
        assertEquals(new EventMetadata(TOPIC, record.partition(), record.offset()), published.event());
        assertEquals(accepted.correlationId(), new String(record.headers().lastHeader("X-Correlation-ID").value(), StandardCharsets.UTF_8));
        assertEquals(accepted.tradeId(), new String(record.headers().lastHeader("tradeId").value(), StandardCharsets.UTF_8));
        var before = endOffsets();
        assertFalse(trades.accept(accepted.idempotencyKey(), accepted.trade(), "retry-correlation").created());
        assertFalse(worker.runOnce());
        assertEquals(before, endOffsets());
    }

    @Test
    void aFailedAttemptRemainsPendingThenPublishesOnRetry() throws Exception {
        StoredTrade accepted = accept();
        Instant now = Instant.now();
        new PublicationWorker(pending, trade -> {
            throw new TimeoutException("broker unavailable");
        },
            Clock.fixed(now, ZoneOffset.UTC)).runOnce();
        assertEquals("PENDING", trades.findById(accepted.tradeId()).orElseThrow().publicationStatus());
        var document = mongo.getCollection("trades").find(new org.bson.Document("_id", accepted.tradeId())).first();
        assertEquals("PUBLICATION_FAILED", document.getString("lastPublicationError"));
        assertNull(document.get("event"));
        assertTrue(new PublicationWorker(pending, publisher, Clock.fixed(now.plusSeconds(3), ZoneOffset.UTC)).runOnce());
        assertEquals("PUBLISHED", trades.findById(accepted.tradeId()).orElseThrow().publicationStatus());
        assertEquals(1, recordsFor(accepted.tradeId(), 1).size());
    }

    @Test
    void expiredLeaseRecoversAckBeforeCrashAndFencesTheOldCompletion() throws Exception {
        StoredTrade accepted = accept();
        Instant now = Instant.now();
        PublicationTask old = pending.claimNext(now, now.plusSeconds(60), "old-owner").orElseThrow();
        EventMetadata firstAck = publisher.publish(old.trade());
        assertTrue(pending.claimNext(now.plusSeconds(30), now.plusSeconds(90), "too-early").isEmpty());
        var restarted = new PublicationWorker(pending, publisher, Clock.fixed(now.plusSeconds(61), ZoneOffset.UTC));
        assertTrue(restarted.runOnce());
        assertFalse(pending.markPublished(old, firstAck));
        var records = recordsFor(accepted.tradeId(), 2);
        assertEquals(2, records.size()); // Application retries can duplicate a previously acknowledged event.
        assertEquals("PUBLISHED", trades.findById(accepted.tradeId()).orElseThrow().publicationStatus());
        assertNotEquals(firstAck.offset(), trades.findById(accepted.tradeId()).orElseThrow().event().offset());
    }

    @Test
    void concurrentRelaysCannotClaimTheSameLiveLease() throws Exception {
        accept();
        Instant now = Instant.now();
        CyclicBarrier ready = new CyclicBarrier(8);
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            List<Future<Optional<PublicationTask>>> attempts = new ArrayList<>();
            for (int i = 0; i < 8; i++)
                attempts.add(pool.submit(() -> {
                    ready.await(10, TimeUnit.SECONDS);
                    return pending.claimNext(now, now.plusSeconds(60), UUID.randomUUID().toString());
                }));
            int claims = 0;
            for (var attempt : attempts) if (attempt.get(20, TimeUnit.SECONDS).isPresent()) claims++;
            assertEquals(1, claims);
        }
    }

    @Test
    void oldPendingDocumentsWithoutRelayFieldsAreClaimedAndPublished() throws Exception {
        StoredTrade accepted = accept();
        mongo.getCollection("trades").updateOne(new org.bson.Document("_id", accepted.tradeId()),
            new org.bson.Document("$unset", new org.bson.Document("nextAttemptAt", "")
                .append("publicationAttempts", "").append("leaseUntil", "").append("leaseToken", "")));
        assertTrue(worker.runOnce());
        assertEquals("PUBLISHED", trades.findById(accepted.tradeId()).orElseThrow().publicationStatus());
        assertEquals(1, recordsFor(accepted.tradeId(), 1).size());
    }

    List<ConsumerRecord<String, StockTrade>> recordsFor(String tradeId, int count) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("group.id", "verification-" + UUID.randomUUID());
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", false);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", registryUrl());
        properties.put("specific.avro.reader", true);
        try (KafkaConsumer<String, StockTrade> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            List<ConsumerRecord<String, StockTrade>> found = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (found.size() < count && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.value().getTradeId().equals(tradeId)) found.add(record);
                }
            }
            assertEquals(count, found.size(), "matching Avro records before timeout");
            return found;
        }
    }

    Map<TopicPartition, Long> endOffsets() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (int i = 0; i < 6; i++) request.put(new TopicPartition(TOPIC, i), OffsetSpec.latest());
            Map<TopicPartition, Long> result = new HashMap<>();
            admin.listOffsets(request).all().get(10, TimeUnit.SECONDS).forEach((key, value) -> result.put(key, value.offset()));
            return result;
        }
    }
}
