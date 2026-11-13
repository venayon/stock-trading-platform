package com.example.finance.trader.api;

import com.example.finance.trader.application.TradeRepository;
import com.example.finance.trader.application.TradeStorageUnavailableException;
import com.example.finance.trader.support.InMemoryTradeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {"trader.publication.enabled=false", "trader.observability.backlog.enabled=false", "management.endpoint.health.group.readiness.include=readinessState", "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration,org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@AutoConfigureMockMvc
class TradeAcceptanceApiTest {
    static final String VALID = """
        {"tradeDate":"2026-10-24","side":"BUY","symbol":" aapl ",
         "quantity":10,"price":"123.450000"}
        """;
    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper json;
    @MockitoBean
    TradeRepository repository;
    InMemoryTradeRepository stored;

    @BeforeEach
    void store() {
        stored = new InMemoryTradeRepository();
        when(repository.insertOrGet(any())).thenAnswer(call -> stored.insertOrGet(call.getArgument(0)));
        when(repository.findById(anyString())).thenAnswer(call -> stored.findById(call.getArgument(0)));
    }

    @Test
    void acceptsThenReplaysAndMakesLocationQueryable() throws Exception {
        String first = mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID))
            .andExpect(status().isAccepted())
            .andExpect(header().string("Location", startsWith("/api/trades/")))
            .andExpect(jsonPath("$.publicationStatus").value("PENDING"))
            .andExpect(jsonPath("$.event").doesNotExist())
            .andExpect(jsonPath("$.idempotencyKey").doesNotExist())
            .andExpect(jsonPath("$.requestHash").doesNotExist())
            .andReturn().getResponse().getContentAsString();
        String id = json.readTree(first).get("tradeId").asText();
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID.replace(" aapl ", "AAPL")))
            .andExpect(status().isOk())
            .andExpect(header().string("Idempotent-Replay", "true"))
            .andExpect(content().json(first));
        mvc.perform(get("/api/trades/" + id))
            .andExpect(status().isOk()).andExpect(content().json(first));
        org.junit.jupiter.api.Assertions.assertEquals(1, stored.size());
    }

    @Test
    void differentPayloadReturnsConflictAndDoesNotReplaceTrade() throws Exception {
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
            .contentType("application/json").content(VALID)).andExpect(status().isAccepted());
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID.replace("10,", "11,")))
            .andExpect(status().isConflict())
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(jsonPath("$.errorCode").value("IDEMPOTENCY_CONFLICT"));
        org.junit.jupiter.api.Assertions.assertEquals(1, stored.size());
    }

    @Test
    void invalidTradeOrKeyDoesNotReachStorage() throws Exception {
        mvc.perform(post("/api/trades").contentType("application/json").content(VALID))
            .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.idempotencyKey").exists());
        mvc.perform(post("/api/trades").header("Idempotency-Key", "a", "b")
                .contentType("application/json").content(VALID))
            .andExpect(status().isBadRequest());
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID.replace("10,", "0,")))
            .andExpect(status().isBadRequest());
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID.replace("\"price\":", "\"typo\":1,\"price\":")))
            .andExpect(status().isBadRequest());
        verifyNoInteractions(repository);
    }

    @Test
    void distinguishesInvalidAndUnknownTradeIds() throws Exception {
        mvc.perform(get("/api/trades/not-a-uuid")).andExpect(status().isBadRequest());
        verifyNoInteractions(repository);
        mvc.perform(get("/api/trades/" + UUID.randomUUID())).andExpect(status().isNotFound())
            .andExpect(jsonPath("$.errorCode").value("TRADE_NOT_FOUND"));
    }

    @Test
    void failedStorageReturnsRetryable503WithoutLeakingCause() throws Exception {
        doThrow(new TradeStorageUnavailableException(
            new RuntimeException("mongodb://secret-password@internal-host"))).when(repository).insertOrGet(any());
        mvc.perform(post("/api/trades").header("Idempotency-Key", "api-1")
                .contentType("application/json").content(VALID))
            .andExpect(status().isServiceUnavailable())
            .andExpect(header().string("Retry-After", "2"))
            .andExpect(jsonPath("$.errorCode").value("TRADE_STORAGE_UNAVAILABLE"))
            .andExpect(jsonPath("$.correlationId").exists())
            .andExpect(content().string(not(containsString("secret-password"))));
    }

    @Test
    void documentsAcceptanceStatusHeaderAndQueryContract() throws Exception {
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
            .andExpect(jsonPath("$.paths['/api/trades'].post.responses['202']").exists())
            .andExpect(jsonPath("$.paths['/api/trades'].post.responses['409']").exists())
            .andExpect(jsonPath("$.paths['/api/trades/{id}'].get").exists());
    }
}
