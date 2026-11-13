package com.example.finance.trader.api;

import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.TradeValidator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/trades")
public class TradeValidationController {

    private final TradeValidator validator = new TradeValidator();

    @PostMapping(value = "/validate", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Validate and preview a canonical trade",
        description = "Checks every field and returns normalized input. Does not persist or publish a trade.")
    @ApiResponse(responseCode = "200", description = "Valid canonical preview",
        content = @Content(mediaType = "application/json", schema = @Schema(implementation = TradePreview.class)))
    @ApiResponse(responseCode = "400", description = "Invalid JSON or trade; field errors where available",
        content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ValidationProblem.class)))
    public TradePreview validate(@RequestBody TradeValidationRequest request) {
        CreateTradeCommand canonical = validator.validate(request.toCommand());
        return new TradePreview(canonical.tradeDate(), canonical.side(), canonical.symbol(), canonical.quantity(),
            canonical.price().toPlainString(), canonical.currency(), canonical.counterparty());
    }

    public record TradePreview(LocalDate tradeDate, String side, String symbol, Long quantity,
                               String price, String currency, String counterparty) {
    }
}
