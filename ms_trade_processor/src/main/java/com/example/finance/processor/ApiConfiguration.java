package com.example.finance.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApiConfiguration {
    @Bean
    OpenAPI openApi() {
        return new OpenAPI().info(new Info()
            .title("Trade Processor API")
            .version("0.0.1")
            .description("Processed trade queries and canonical source replay. " +
                "Processing is at least once; output records may be duplicated after a crash."));
    }

    @Bean
    Jackson2ObjectMapperBuilderCustomizer strictJson() {
        return builder -> builder.featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_TRAILING_TOKENS).featuresToDisable(DeserializationFeature.ACCEPT_FLOAT_AS_INT, MapperFeature.ALLOW_COERCION_OF_SCALARS);
    }
}
