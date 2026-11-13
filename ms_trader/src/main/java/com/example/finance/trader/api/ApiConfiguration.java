package com.example.finance.trader.api;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

@Configuration
public class ApiConfiguration {
    @Bean
    Jackson2ObjectMapperBuilderCustomizer strictJson() {
        return builder -> builder.featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature())
            .featuresToDisable(DeserializationFeature.ACCEPT_FLOAT_AS_INT,
                MapperFeature.ALLOW_COERCION_OF_SCALARS);
    }

    @Bean
    OpenAPI tradeOpenApi() {
        return new OpenAPI().info(new Info().title("Stock Trader API").version("0.0.3")
            .description("Durable MongoDB trade acceptance and side-effect-free validation preview. Pending trades publish asynchronously as Avro events to Kafka."));
    }

    @Bean
    OpenApiCustomizer exactDecimalContract() {
        return api -> {
            Schema<?> request = api.getComponents().getSchemas().get("TradeValidationRequest");
            if (request == null) return;
            request.setAdditionalProperties(false);
            StringSchema text = new StringSchema();
            text.setPattern("^\\+?(?:0*[1-9][0-9]{0,11}(?:\\.[0-9]{1,6}0*)?|0+\\.(?=[0-9]{0,5}[1-9])[0-9]{1,6}0*)$");
            NumberSchema number = new NumberSchema();
            number.setMinimum(new BigDecimal("0.000001"));
            number.setMaximum(new BigDecimal("999999999999.999999"));
            number.setMultipleOf(new BigDecimal("0.000001"));
            Schema<Object> price = new Schema<>();
            price.setOneOf(List.of(text, number));
            price.setDescription("Exact positive decimal <= 999999999999.999999 with at most six decimal places "
                + "after removing trailing zeros. Decimal strings preserve precision in JavaScript. Exponent strings are rejected.");
            price.setExample("123.450000");
            request.getProperties().put("price", price);
        };
    }
}
