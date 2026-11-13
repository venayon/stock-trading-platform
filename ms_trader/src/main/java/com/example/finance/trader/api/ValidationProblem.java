package com.example.finance.trader.api;

import java.net.URI;
import java.util.Map;

/**
 * OpenAPI description of the ProblemDetail extension fields returned by the
 * advice.
 */
public record ValidationProblem(URI type, String title, int status, String detail, URI instance,
                                String errorCode, Map<String, String> errors, String correlationId) {
}
