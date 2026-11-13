package com.example.finance.trader.observability;

import com.example.finance.trader.application.TradeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {"trader.publication.enabled=false", "trader.observability.backlog.enabled=false",
    "management.endpoint.health.group.readiness.include=readinessState",
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration,org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@AutoConfigureMockMvc
@AutoConfigureObservability
@ExtendWith(OutputCaptureExtension.class)
class ObservabilityApiTest {
    @Autowired
    MockMvc mvc;
    @MockitoBean
    TradeRepository repository;

    @Test
    void exposesPrometheusWithBusinessAndJvmMeters() throws Exception {
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk())
            .andExpect(content().string(containsString("trades_accepted_total")))
            .andExpect(content().string(containsString("trades_publication_seconds_bucket")))
            .andExpect(content().string(containsString("jvm_memory_used_bytes")))
            .andExpect(content().string(containsString("application=\"ms-trader\"")));
    }

    @Test
    void acceptanceWritesStructuredIdentityOnceWithoutBreakingTheAppender(CapturedOutput output) throws Exception {
        org.mockito.Mockito.when(repository.insertOrGet(org.mockito.ArgumentMatchers.any())).thenAnswer(call ->
            new com.example.finance.trader.application.TradeAcceptance(call.getArgument(0), true));
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/trades")
                .header("Idempotency-Key", "logging-regression")
                .header("X-Correlation-ID", "550e8400-e29b-41d4-a716-446655440000")
                .contentType("application/json")
                .content("{\"tradeDate\":\"2026-10-24\",\"side\":\"BUY\",\"symbol\":\"AAPL\",\"quantity\":1,\"price\":\"1\"}"))
            .andExpect(status().isAccepted());
        org.junit.jupiter.api.Assertions.assertTrue(output.getOut().contains("\"event\":\"trade.acceptance\""));
        org.junit.jupiter.api.Assertions.assertTrue(output.getOut().contains("\"correlationId\":\"550e8400-e29b-41d4-a716-446655440000\""));
        org.junit.jupiter.api.Assertions.assertFalse(output.getAll().contains("Duplicate nested pairs"));
    }

    @Test
    void healthProbesAreExposedButAdministrativeEndpointsAreNot() throws Exception {
        mvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
        mvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
    }
}
