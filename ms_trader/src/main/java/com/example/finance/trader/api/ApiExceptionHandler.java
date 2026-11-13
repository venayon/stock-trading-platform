package com.example.finance.trader.api;

import com.example.finance.trader.application.IdempotencyConflictException;
import com.example.finance.trader.application.TradeNotFoundException;
import com.example.finance.trader.application.TradeStorageUnavailableException;
import com.example.finance.trader.application.TradeValidationException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(TradeValidationException.class)
    public ProblemDetail invalidTrade(TradeValidationException error, HttpServletRequest request) {
        return problem("INVALID_TRADE", "Trade validation failed", error.errors(), request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail invalidJson(HttpMessageNotReadableException error, HttpServletRequest request) {
        return problem("INVALID_JSON", "A JSON trade object with correctly typed fields is required",
            Map.of("request", "Check field names, value types, dates and decimal format"), request);
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail conflict(IdempotencyConflictException error, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", error.getMessage(), Map.of(), request);
    }

    @ExceptionHandler(TradeNotFoundException.class)
    public ProblemDetail notFound(TradeNotFoundException error, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "TRADE_NOT_FOUND", error.getMessage(), Map.of(), request);
    }

    @ExceptionHandler(TradeStorageUnavailableException.class)
    public ResponseEntity<ProblemDetail> unavailable(TradeStorageUnavailableException error, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "2").body(
            problem(HttpStatus.SERVICE_UNAVAILABLE, "TRADE_STORAGE_UNAVAILABLE", error.getMessage(), Map.of(), request));
    }

    private ProblemDetail problem(String code, String detail, Map<String, String> errors, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, code, detail, errors, request);
    }

    private ProblemDetail problem(HttpStatus status, String code, String detail, Map<String, String> errors, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(status.getReasonPhrase());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", code);
        problem.setProperty("errors", errors);
        problem.setProperty("correlationId", request.getAttribute("correlationId"));
        return problem;
    }
}
