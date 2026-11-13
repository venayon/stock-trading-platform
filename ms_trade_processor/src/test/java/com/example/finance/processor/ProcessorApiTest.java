package com.example.finance.processor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ProcessorApi.class)
class ProcessorApiTest {
    @Autowired
    MockMvc mvc;
    @MockitoBean
    MongoTemplate mongo;
    @MockitoBean
    KafkaTemplate<Object, Object> producer;

    @org.junit.jupiter.api.BeforeEach
    void ignoreFrameworkLifecycleCalls() {
        clearInvocations(mongo, producer);
    }

    @Test
    void rawSourceDatabaseFailureIsRetryableAndDoesNotLeakConnectionDetails() throws Exception {
        when(mongo.getMongoDatabaseFactory()).thenThrow(new com.mongodb.MongoTimeoutException("private-host-secret"));
        mvc.perform(post("/api/process").contentType("application/json").content("{\"tradeId\":\"" + UUID.randomUUID() + "\"}"))
            .andExpect(status().isServiceUnavailable()).andExpect(header().string("Retry-After", "2"))
            .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("private-host-secret"))));
        verifyNoInteractions(producer);
    }

    @Test
    void invalidPagingOrIdentityDoesNotAccessStorage() throws Exception {
        mvc.perform(get("/api/processed?size=101")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/processed/not-an-id")).andExpect(status().isBadRequest());
        verifyNoInteractions(mongo, producer);
    }
}
