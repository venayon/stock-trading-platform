package com.example.finance.trader.publication;

import com.example.finance.avro.Side;
import com.example.finance.avro.StockTrade;
import com.example.finance.trader.application.CreateTradeCommand;
import com.example.finance.trader.application.StoredTrade;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class StockTradeMapperTest {
    @Test
    void generatedContractPreservesExactLogicalTypesAndSchemaIdentity() throws Exception {
        for (long epoch : new long[]{Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            var input = new StoredTrade("id", "key", "hash", new CreateTradeCommand(LocalDate.ofEpochDay(epoch),
                "SELL", "BRK.B", Long.MAX_VALUE, new BigDecimal("999999999999.999999"), "GBP", null),
                "PENDING", PublicationWorkerTest.NOW, "correlation");
            StockTrade event = new StockTradeMapper().map(input);
            assertEquals(input.trade().tradeDate(), event.getTradeDate());
            assertEquals(input.trade().price(), event.getPrice());
            assertEquals(Long.MAX_VALUE, event.getQuantity());
            assertEquals(Side.SELL, event.getSide());
            assertNull(event.getCounterparty());
        }
        try (var source = getClass().getResourceAsStream("/StockTrade.avsc")) {
            assertNotNull(source);
            Schema canonical = new Schema.Parser().parse(source);
            assertEquals("com.example.finance.avro.StockTrade", canonical.getFullName());
            assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
                SchemaCompatibility.checkReaderWriterCompatibility(StockTrade.getClassSchema(), canonical).getType());
        }
    }
}
