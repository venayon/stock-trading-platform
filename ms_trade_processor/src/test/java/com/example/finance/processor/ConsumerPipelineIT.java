package com.example.finance.processor;

import com.example.finance.avro.Side;
import com.example.finance.avro.StockTrade;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class ConsumerPipelineIT {
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
    KafkaTemplate<Object, Object> producer;
    @Autowired
    MongoTemplate mongo;
    @Autowired
    MockMvc mvc;
    @Autowired
    LagSampler lag;
    @Autowired
    io.micrometer.core.instrument.MeterRegistry meters;
    @Autowired
    ProcessingStore processingStore;

    static String registryUrl() {
        return "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(8081);
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 6, (short) 1), new NewTopic("stock-trades-processed", 6, (short) 1), new NewTopic("stock-trades-dlt", 6, (short) 1)))
                .all().get(20, TimeUnit.SECONDS);
        }
        try (var source = ConsumerPipelineIT.class.getResourceAsStream("/StockTrade.avsc");
             var schemas = new CachedSchemaRegistryClient(registryUrl(), 10)) {
            var schema = new AvroSchema(new Schema.Parser().parse(source));
            for (String subject : List.of(TOPIC + "-value", "stock-trades-processed-value", "stock-trades-dlt-value")) {
                schemas.updateCompatibility(subject, "BACKWARD_TRANSITIVE");
                schemas.register(subject, schema);
            }
        }
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.kafka.producer.properties.schema.registry.url", ConsumerPipelineIT::registryUrl);
        properties.add("spring.kafka.consumer.properties.schema.registry.url", ConsumerPipelineIT::registryUrl);
        properties.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("processor"));
    }

    static void await(BooleanSupplier done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
        while (System.nanoTime() < deadline) {
            if (done.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("Condition not reached before timeout");
    }

    RecordMetadata send(StockTrade value, int partition) throws Exception {
        return producer.send(new ProducerRecord<>(TOPIC, partition, value.getSymbol(), value)).get(15, TimeUnit.SECONDS).getRecordMetadata();
    }

    ProcessedTrade complete(String id) throws Exception {
        await(() -> {
            var doc = mongo.findById(id, ProcessedTrade.class);
            return doc != null && doc.processingStatus().equals("COMPLETE");
        });
        return mongo.findById(id, ProcessedTrade.class);
    }

    long committed(int partition) {
        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets("stock-trade-processor-v1").partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            var offset = offsets.get(new TopicPartition(TOPIC, partition));
            return offset == null ? 0 : offset.offset();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    long end(String topic, int partition) throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            return admin.listOffsets(Map.of(new TopicPartition(topic, partition), OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS).values().iterator().next().offset();
        }
    }

    List<ConsumerRecord<byte[], byte[]>> read(String topic, int partition, long start) {
        try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", UUID.randomUUID().toString(), "enable.auto.commit", false), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var key = new TopicPartition(topic, partition);
            consumer.assign(List.of(key));
            consumer.seek(key, start);
            var out = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (out.isEmpty() && System.nanoTime() < deadline)
                consumer.poll(Duration.ofMillis(300)).forEach(out::add);
            assertFalse(out.isEmpty());
            return out;
        }
    }

    RecordMetadata poison(int partition, byte[] bytes) throws Exception {
        try (var raw = new KafkaProducer<byte[], byte[]>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all"), new ByteArraySerializer(), new ByteArraySerializer())) {
            return raw.send(new ProducerRecord<>(TOPIC, partition, "AAPL".getBytes(java.nio.charset.StandardCharsets.UTF_8), bytes)).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void validEventIsPersistedRoutedAndCommittedWithExactValues() throws Exception {
        var event = ProcessingServiceTest.event();
        event.setSide(Side.SELL);
        var input = send(event, 0);
        var result = complete(event.getTradeId());
        assertEquals(4, result.targetPartition());
        assertEquals(event.getPrice(), result.trade().price());
        assertEquals(Long.MAX_VALUE, result.trade().quantity());
        assertEquals(input.offset(), result.source().offset());
        var output = read("stock-trades-processed", 4, result.output().offset()).getFirst();
        try (var decoder = new io.confluent.kafka.serializers.KafkaAvroDeserializer()) {
            decoder.configure(Map.of("schema.registry.url", registryUrl(), "specific.avro.reader", true), false);
            StockTrade decoded = (StockTrade) decoder.deserialize(output.topic(), output.value());
            assertEquals(event, decoded);
        }
        assertEquals(Long.toString(input.offset()), new String(output.headers().lastHeader("source-offset").value(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(result.correlationId(), new String(output.headers().lastHeader("X-Correlation-ID").value(), java.nio.charset.StandardCharsets.UTF_8));
        await(() -> committed(0) > input.offset());
        mvc.perform(get("/api/processed/" + event.getTradeId())).andExpect(status().isOk()).andExpect(jsonPath("$.processingStatus").value("COMPLETE"));
    }

    @Test
    void completedDuplicateDoesNotProduceAnotherOutput() throws Exception {
        var event = ProcessingServiceTest.event();
        long before = end("stock-trades-processed", 1);
        send(event, 1);
        complete(event.getTradeId());
        var duplicate = send(event, 1);
        await(() -> committed(1) > duplicate.offset());
        assertEquals(before + 1, end("stock-trades-processed", 1));
        assertEquals(1, mongo.count(org.springframework.data.mongodb.core.query.Query.query(org.springframework.data.mongodb.core.query.Criteria.where("tradeId").is(event.getTradeId())), ProcessedTrade.class));
        mvc.perform(post("/api/process").contentType("application/json").content("{\"tradeId\":\"" + event.getTradeId() + "\"}")).andExpect(status().isOk());
    }

    @Test
    void malformedAvroPreservesBytesInSamePartitionDltAndAllowsProgress() throws Exception {
        byte[] bytes = {7, 8, 9};
        long before = end("stock-trades-dlt", 2);
        var input = poison(2, bytes);
        await(() -> committed(2) > input.offset());
        var output = read("stock-trades-dlt", 2, before).getFirst();
        assertArrayEquals(bytes, output.value());
        assertNotNull(output.headers().lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET));
    }

    @Test
    void conflictingTradeIdentityGoesToDltWithoutOverwritingState() throws Exception {
        var event = ProcessingServiceTest.event();
        send(event, 3);
        complete(event.getTradeId());
        long before = end("stock-trades-dlt", 3);
        event.setQuantity(1L);
        var conflict = send(event, 3);
        await(() -> committed(3) > conflict.offset());
        assertEquals(Long.MAX_VALUE, mongo.findById(event.getTradeId(), ProcessedTrade.class).trade().quantity());
        assertFalse(read("stock-trades-dlt", 3, before).isEmpty());
    }

    @Test
    void failedDltPublicationDoesNotAdvanceSourceOffsetUntilDltReturns() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.deleteTopics(List.of("stock-trades-dlt")).all().get(10, TimeUnit.SECONDS);
            RecordMetadata input;
            try {
                input = poison(5, new byte[]{3, 2, 1});
                Thread.sleep(6000);
                assertTrue(committed(5) <= input.offset(), "failed DLT publication must not commit source");
            } finally {
                admin.createTopics(List.of(new NewTopic("stock-trades-dlt", 6, (short) 1))).all().get(10, TimeUnit.SECONDS);
            }
            await(() -> committed(5) > input.offset());
        }
    }

    @Test
    void boundedQueriesAndReplayEnqueueUseRealSourceDocuments() throws Exception {
        var event = ProcessingServiceTest.event();
        mongo.getMongoDatabaseFactory().getMongoDatabase("trader").getCollection("trades").insertOne(new org.bson.Document("_id", event.getTradeId())
            .append("publicationStatus", "PUBLISHED").append("tradeDate", event.getTradeDate().toString()).append("side", "BUY").append("symbol", "AAPL")
            .append("quantity", event.getQuantity()).append("price", new org.bson.types.Decimal128(event.getPrice())).append("currency", "USD"));
        mvc.perform(post("/api/process").contentType("application/json").content("{\"tradeId\":\"" + event.getTradeId() + "\"}"))
            .andExpect(status().isAccepted()).andExpect(jsonPath("$.replayPublication.topic").value(TOPIC));
        complete(event.getTradeId());
        mvc.perform(get("/api/processed?size=1&symbol=aapl&side=BUY&processingStatus=COMPLETE")).andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1));
        mvc.perform(get("/api/processed?size=101")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/processed/invalid")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/processed/" + UUID.randomUUID())).andExpect(status().isNotFound());
    }

    @Test
    void expiredProcessingLeaseFencesOldCompletionAndPreservesIdentity() {
        var data = TradeData.validate(ProcessingServiceTest.event(), "AAPL");
        var now = Instant.now();
        var source = new ProcessedTrade.Point(TOPIC, 0, 0);
        var old = processingStore.claim(data, source, "correlation", now);
        assertThrows(IllegalStateException.class, () -> processingStore.claim(data, source, "other", now.plusSeconds(30)));
        var next = processingStore.claim(data, source, "other", now.plusSeconds(61));
        var output = new ProcessedTrade.Point("stock-trades-processed", 1, 1);
        assertThrows(IllegalStateException.class, () -> processingStore.complete(old, output, now.plusSeconds(62)));
        processingStore.complete(next, output, now.plusSeconds(62));
        assertTrue(processingStore.claim(data, source, "retry", now.plusSeconds(63)).complete());
    }

    @Test
    void samplesCommittedGroupLag() throws Exception {
        var event = ProcessingServiceTest.event();
        var input = send(event, 4);
        complete(event.getTradeId());
        await(() -> committed(4) > input.offset());
        lag.poll();
        assertEquals(1, meters.get("trades.lag.poll.success").gauge().value());
        assertFalse(meters.find("trades.consumer.lag").gauges().isEmpty());
    }
}
