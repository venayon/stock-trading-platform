package com.example.finance.trader.api;

import com.example.finance.trader.application.CreateTradeCommand;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.OptBoolean;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.LocalDate;

@Schema(description = "Trade input shared by acceptance and validation preview. Unknown fields are rejected.")
public class TradeValidationRequest {

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "2026-10-24",
        description = "Strict ISO date, within signed 32-bit epoch-day range. No market-calendar restriction.")
    @JsonFormat(pattern = "uuuu-MM-dd", lenient = OptBoolean.FALSE)
    public LocalDate tradeDate;

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, allowableValues = {"BUY", "SELL"})
    public String side;

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "AAPL",
        description = "Trimmed and uppercased using Locale.ROOT before matching [A-Z0-9][A-Z0-9.-]{0,15}.")
    public String symbol;

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, minimum = "1", maximum = "9223372036854775807",
        description = "JSON integer; strings and fractional values are rejected.", example = "10")
    public Long quantity;

    @JsonDeserialize(using = DecimalInputDeserializer.class)
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED,
        description = "Exact positive decimal, maximum 999999999999.999999, scale six without rounding. "
            + "A plain decimal string is preferred; a JSON number is also accepted. Extra trailing zeros are allowed.",
        type = "string", example = "123.450000")
    public BigDecimal price;

    @Schema(allowableValues = {"USD", "GBP", "EUR"}, defaultValue = "USD",
        description = "Omitted defaults to USD; explicit null is invalid.")
    public String currency = "USD";

    @Schema(nullable = true, description = "Trimmed; blank becomes null; maximum 128 UTF-16 code units after trimming.")
    public String counterparty;

    public CreateTradeCommand toCommand() {
        return new CreateTradeCommand(tradeDate, side, symbol, quantity, price, currency, counterparty);
    }
}
