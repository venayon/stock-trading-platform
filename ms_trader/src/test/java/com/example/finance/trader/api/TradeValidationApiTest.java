package com.example.finance.trader.api;

import com.example.finance.trader.application.TradeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {"trader.publication.enabled=false", "trader.observability.backlog.enabled=false", "management.endpoint.health.group.readiness.include=readinessState", "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration,org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@AutoConfigureMockMvc
class TradeValidationApiTest {

    private static final String VALID = """
        {"tradeDate":"2026-10-24","side":"BUY","symbol":" aapl ",
         "quantity":10,"price":"123.450000"}
        """;
    @Autowired
    MockMvc mvc;
    @MockitoBean
    TradeRepository repository;

    static Stream<String> malformedRequests() {
        return Stream.of(
            "null", "{}", "[]", "{", VALID + "{}",
            VALID.replace("\"price\":", "\"quantity\":2,\"price\":"),
            VALID.replace("10,", "1.5,"),
            VALID.replace("10,", "9223372036854775808,"),
            VALID.replace("10,", "\"10\","),
            VALID.replace("2026-10-24", "2026-02-30"),
            VALID.replace("2026-10-24", "2026-1-1"),
            VALID.replace("\"123.450000\"", "\"1e2\""),
            VALID.replace("\"123.450000\"", "true"),
            VALID.replace("\"123.450000\"", "\" 1 \""),
            VALID.replace("\"123.450000\"", "\"NaN\""),
            VALID.replace("10,", "0,"),
            VALID.replace("\"123.450000\"", "null"),
            VALID.replace("\"123.450000\"", "\"0.0000001\""),
            VALID.replace("\"123.450000\"", "\"1000000000000\""),
            VALID.replace("\"BUY\"", "\"HOLD\""),
            VALID.replace("\" aapl \"", "\"!\""),
            VALID.replace("\"price\":", "\"currency\":null,\"price\":"),
            VALID.replace("\"price\":", "\"currency\":\"JPY\",\"price\":"),
            VALID.replace("\"price\":", "\"typo\":1,\"price\":"));
    }

    @ParameterizedTest
    @MethodSource("malformedRequests")
    void rejectsInvalidJsonOrTradeWithProblemDetail(String body) throws Exception {
        mvc.perform(post("/api/trades/validate").contentType("application/json").content(body))
            .andExpect(status().isBadRequest())
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(jsonPath("$.errorCode").exists())
            .andExpect(jsonPath("$.correlationId").exists());
    }

    @Test
    void returnsCanonicalPreviewWithCurrencyDefaultAndCorrelationId() throws Exception {
        mvc.perform(post("/api/trades/validate").contentType("application/json").content(VALID)
                .header("X-Correlation-ID", "550e8400-e29b-41d4-a716-446655440000"))
            .andExpect(status().isOk())
            .andExpect(header().string("X-Correlation-ID", "550e8400-e29b-41d4-a716-446655440000"))
            .andExpect(jsonPath("$.symbol").value("AAPL"))
            .andExpect(jsonPath("$.price").value("123.450000"))
            .andExpect(jsonPath("$.currency").value("USD"));
    }

    @Test
    void preservesNumericPriceAndMaximumQuantityExactly() throws Exception {
        mvc.perform(post("/api/trades/validate").contentType("application/json")
                .content(VALID.replace("10,", "9223372036854775807,")
                    .replace("\"123.450000\"", "999999999999.999999")))
            .andExpect(status().isOk())
            .andExpect(content().string(org.hamcrest.Matchers.containsString("9223372036854775807")))
            .andExpect(jsonPath("$.price").value("999999999999.999999"));
    }

    @Test
    void returnsFieldErrorsAndReplacesUntrustedCorrelationId() throws Exception {
        mvc.perform(post("/api/trades/validate").contentType("application/json")
                .header("X-Correlation-ID", "untrusted").content("{}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors.quantity").exists())
            .andExpect(jsonPath("$.errors.price").exists())
            .andExpect(header().string("X-Correlation-ID", org.hamcrest.Matchers.matchesPattern(
                "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")));
    }

    @Test
    void servesOpenApiSwaggerAndClientForm() throws Exception {
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
            .andExpect(jsonPath("$.paths['/api/trades/validate'].post").exists())
            .andExpect(jsonPath("$.paths['/api/trades/validate'].post.responses['400'].content['application/problem+json'].schema['$ref']")
                .value("#/components/schemas/ValidationProblem"))
            .andExpect(jsonPath("$.components.schemas.TradeValidationRequest.additionalProperties").value(false))
            .andExpect(jsonPath("$.components.schemas.TradeValidationRequest.required").isArray())
            .andExpect(jsonPath("$.components.schemas.TradeValidationRequest.properties.quantity.maximum")
                .value(9223372036854775807L));
        mvc.perform(get("/swagger-ui/index.html")).andExpect(status().isOk());
        mvc.perform(get("/index.html")).andExpect(status().isOk());
    }
}
