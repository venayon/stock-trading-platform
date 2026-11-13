package com.example.finance.processor;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ProblemDetail invalid(Exception error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Invalid trade ID, query or request");
    }

    @ExceptionHandler({DataAccessException.class, com.mongodb.MongoException.class, ExecutionException.class, TimeoutException.class, org.apache.kafka.common.KafkaException.class})
    public ResponseEntity<ProblemDetail> unavailable(Exception error) {
        return ResponseEntity.status(503).header("Retry-After", "2").body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Dependency unavailable; retry later"));
    }
}
