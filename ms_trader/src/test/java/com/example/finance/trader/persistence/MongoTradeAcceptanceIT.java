package com.example.finance.trader.persistence;

import com.example.finance.trader.application.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@Testcontainers
@SpringBootTest(properties = "trader.publication.enabled=false")
@AutoConfigureMockMvc
class MongoTradeAcceptanceIT {
    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0.16");
    static final CreateTradeCommand VALID = new CreateTradeCommand(LocalDate.of(2026, 10, 24),
        "BUY", "AAPL", 10L, new BigDecimal("123.450000"), "USD", null);
    @Autowired
    TradeService service;
    @Autowired
    MongoTemplate mongo;
    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper json;
    @Autowired
    com.example.finance.trader.observability.BacklogSampler sampler;
    @Autowired
    io.micrometer.core.instrument.MeterRegistry meters;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("trader"));
    }

    @BeforeEach
    void clearTrades() {
        mongo.remove(new Query(), TradeDocument.class);
    }

    @Test
    void realHttpAcceptanceAndRetrySurviveNewClientAndRepository() throws Exception {
        String body = """
            {"tradeDate":"2026-10-24","side":"BUY","symbol":" aapl ","quantity":10,"price":"123.45"}
            """;
        String first = mvc.perform(post("/api/trades").header("Idempotency-Key", "restart-key")
                .contentType("application/json").content(body))
            .andExpect(status().isAccepted()).andExpect(jsonPath("$.publicationStatus").value("PENDING"))
            .andReturn().getResponse().getContentAsString();
        String id = json.readTree(first).get("tradeId").asText();
        try (var newClient = MongoClients.create(MONGO.getReplicaSetUrl("trader"))) {
            TradeService restarted = new TradeService(
                new MongoTradeRepository(new MongoTemplate(newClient, "trader")), Clock.systemUTC(), UUID::randomUUID);
            var retry = restarted.accept("restart-key", VALID, "new-correlation");
            assertFalse(retry.created());
            assertEquals(id, retry.trade().tradeId());
        }
        mvc.perform(get("/api/trades/" + id)).andExpect(status().isOk()).andExpect(content().json(first));
        assertEquals(1, mongo.count(new Query(), TradeDocument.class));
    }

    @Test
    void simultaneousSameKeySubmissionsInsertExactlyOneDocument() throws Exception {
        int callers = 12;
        CyclicBarrier ready = new CyclicBarrier(callers);
        try (ExecutorService pool = Executors.newFixedThreadPool(callers)) {
            List<Future<TradeAcceptance>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(pool.submit(() -> {
                    ready.await(10, TimeUnit.SECONDS);
                    return service.accept("concurrent-key", VALID, "correlation");
                }));
            }
            List<TradeAcceptance> results = new ArrayList<>();
            for (var future : futures) results.add(future.get(20, TimeUnit.SECONDS));
            assertEquals(1, results.stream().filter(TradeAcceptance::created).count());
            assertEquals(1, results.stream().map(r -> r.trade().tradeId()).distinct().count());
            assertEquals(1, mongo.count(new Query(), TradeDocument.class));
        }
    }

    @Test
    void competingDifferentPayloadsProduceOneWinnerAndOneConflict() throws Exception {
        CreateTradeCommand changed = new CreateTradeCommand(VALID.tradeDate(), "SELL", "AAPL", 10L,
            VALID.price(), "USD", null);
        CyclicBarrier ready = new CyclicBarrier(2);
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            List<Callable<String>> jobs = new ArrayList<>();
            for (CreateTradeCommand trade : List.of(VALID, changed)) {
                jobs.add(() -> {
                    ready.await(10, TimeUnit.SECONDS);
                    try {
                        service.accept("race-key", trade, "correlation");
                        return "accepted";
                    } catch (IdempotencyConflictException expected) {
                        return "conflict";
                    }
                });
            }
            List<String> outcomes = new ArrayList<>();
            for (var future : pool.invokeAll(jobs, 20, TimeUnit.SECONDS)) outcomes.add(future.get());
            assertEquals(Set.of("accepted", "conflict"), new HashSet<>(outcomes));
            assertEquals(1, mongo.count(new Query(), TradeDocument.class));
        }
    }

    @Test
    void backlogGaugeSamplesRealPendingDocumentsAndReadinessIncludesMongo() throws Exception {
        service.accept("metric-1", VALID, "correlation");
        service.accept("metric-2", VALID, "correlation");
        sampler.poll();
        assertEquals(2, meters.get("trades.pending.publications").gauge().value());
        assertEquals(1, meters.get("trades.backlog.poll.success").gauge().value());
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
        mongo.updateFirst(new Query(), new org.springframework.data.mongodb.core.query.Update()
            .set("publicationStatus", "PUBLISHED"), TradeDocument.class);
        sampler.poll();
        assertEquals(1, meters.get("trades.pending.publications").gauge().value());
    }

    @Test
    void primaryKeyCollisionDoesNotMasqueradeAsAnIdempotentRetry() {
        UUID fixed = UUID.randomUUID();
        TradeService collision = new TradeService(new MongoTradeRepository(mongo), Clock.systemUTC(), () -> fixed);
        assertTrue(collision.accept("original", VALID, "correlation").created());
        assertThrows(TradeStorageUnavailableException.class, () -> collision.accept("different-key", VALID, "other"));
        assertEquals(1, mongo.count(new Query(), TradeDocument.class));
    }

    @Test
    void repositoryInitializationFailsWhenUniqueIndexCannotBeEstablished() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl("trader"))) {
            MongoTemplate incompatible = new MongoTemplate(client, "bad_index_" + UUID.randomUUID().toString().replace("-", ""));
            incompatible.indexOps(TradeDocument.class).createIndex(new org.springframework.data.mongodb.core.index.Index()
                .on("idempotencyKey", org.springframework.data.domain.Sort.Direction.ASC).named("uk_trade_idempotency"));
            assertThrows(org.springframework.dao.DataAccessException.class, () -> new MongoTradeRepository(incompatible));
        }
    }

    @Test
    void usesUniqueBinaryKeyIndexAndExactBsonTypes() {
        CreateTradeCommand boundary = new CreateTradeCommand(LocalDate.ofEpochDay(Integer.MAX_VALUE), "SELL",
            "BRK.B", Long.MAX_VALUE, new BigDecimal("999999999999.999999"), "GBP", null);
        var first = service.accept("CaseSensitive", boundary, "correlation");
        assertTrue(service.accept("casesensitive", boundary, "correlation").created());
        Document stored = mongo.getCollection("trades").find(new Document("_id", first.trade().tradeId())).first();
        assertNotNull(stored);
        assertEquals(boundary.price(), stored.get("price", Decimal128.class).bigDecimalValue());
        assertEquals(Long.MAX_VALUE, stored.getLong("quantity"));
        assertEquals(boundary.tradeDate().toString(), stored.getString("tradeDate"));
        assertEquals(boundary, service.findById(first.trade().tradeId()).orElseThrow().trade());
        assertTrue(mongo.indexOps(TradeDocument.class).getIndexInfo().stream()
            .anyMatch(index -> index.getName().equals("uk_trade_idempotency") && index.isUnique()));
    }
}
