package com.example.finance.trader.observability;

import com.example.finance.trader.application.TradeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"trader.publication.enabled=false", "trader.observability.backlog.enabled=false",
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration,org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@AutoConfigureMockMvc
@Import(DependencyHealthTest.HealthConfiguration.class)
class DependencyHealthTest {
    @MockitoBean
    TradeRepository repository;
    @Autowired
    MockMvc mvc;
    @Autowired
    MongoHealth mongo;

    @Test
    void dependencyFailureChangesReadinessWithoutFailingLiveness() throws Exception {
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
        mongo.up.set(false);
        try {
            mvc.perform(get("/actuator/health/readiness")).andExpect(status().isServiceUnavailable());
            mvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        } finally {
            mongo.up.set(true);
        }
    }

    static class MongoHealth implements HealthIndicator {
        AtomicBoolean up = new AtomicBoolean(true);

        public Health health() {
            return up.get() ? Health.up().build() : Health.down().build();
        }
    }

    @TestConfiguration
    static class HealthConfiguration {
        @Bean
        MongoHealth mongoHealthIndicator() {
            return new MongoHealth();
        }
    }
}
