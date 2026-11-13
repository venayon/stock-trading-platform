package com.example.finance.trader.api;

import com.example.finance.trader.application.TradeAcceptance;
import com.example.finance.trader.application.TradeNotFoundException;
import com.example.finance.trader.application.TradeService;
import com.example.finance.trader.application.TradeValidationException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/trades")
public class TradeController {
    private final TradeService service;

    public TradeController(TradeService service) {
        this.service = service;
    }

    @PostMapping(consumes = "application/json", produces = "application/json")
    @Operation(summary = "Durably accept an immutable PENDING trade",
        description = "Requires a client idempotency key. A matching retry returns the stored trade without creating a duplicate. "
            + "Acceptance is acknowledged by MongoDB; the durable relay publishes asynchronously. Poll the Location for PUBLISHED and event metadata.")
    @ApiResponse(responseCode = "202", description = "New PENDING trade accepted",
        content = @Content(mediaType = "application/json", schema = @Schema(implementation = TradeResponse.class)))
    @ApiResponse(responseCode = "200", description = "Same-key retry; Idempotent-Replay: true",
        content = @Content(mediaType = "application/json", schema = @Schema(implementation = TradeResponse.class)))
    @ApiResponse(responseCode = "400", description = "Invalid key, JSON or trade",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    @ApiResponse(responseCode = "409", description = "Key already belongs to a different canonical payload",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    @ApiResponse(responseCode = "503", description = "Storage unavailable or write outcome uncertain; retry the same key",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    public ResponseEntity<TradeResponse> accept(
        @Parameter(required = true, description = "1–128 printable ASCII characters; not blank. Exact and case-sensitive. Retain the same key on retries.")
        @RequestHeader(value = "Idempotency-Key", required = false) String key,
        @RequestBody TradeValidationRequest input, HttpServletRequest request) {
        if (Collections.list(request.getHeaders("Idempotency-Key")).size() != 1) {
            throw new TradeValidationException(Map.of("idempotencyKey", "Exactly one Idempotency-Key header is required"));
        }
        TradeAcceptance result = service.accept(key, input.toCommand(), (String) request.getAttribute("correlationId"));
        var response = ResponseEntity.status(result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK)
            .location(URI.create("/api/trades/" + result.trade().tradeId()));
        if (!result.created()) response.header("Idempotent-Replay", "true");
        return response.body(TradeResponse.from(result.trade()));
    }

    @GetMapping(value = "/{id}", produces = "application/json")
    @Operation(summary = "Read an accepted trade and its publication state")
    @ApiResponse(responseCode = "200", description = "Stored trade",
        content = @Content(mediaType = "application/json", schema = @Schema(implementation = TradeResponse.class)))
    @ApiResponse(responseCode = "400", description = "Malformed trade ID",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    @ApiResponse(responseCode = "404", description = "Trade ID not found",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    @ApiResponse(responseCode = "503", description = "Storage unavailable",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    public TradeResponse find(@PathVariable String id) {
        if (!id.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
            throw new TradeValidationException(Map.of("tradeId", "tradeId must be a UUID"));
        }
        return TradeResponse.from(service.findById(UUID.fromString(id).toString()).orElseThrow(TradeNotFoundException::new));
    }
}
