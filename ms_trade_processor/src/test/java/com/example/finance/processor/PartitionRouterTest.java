package com.example.finance.processor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionRouterTest {
    @Test
    void routesSidesToTheirOwnDeterministicSymbolBuckets() {
        var router = new PartitionRouter();
        assertEquals(1, router.route("BUY", "AAPL"));
        assertEquals(4, router.route("SELL", "AAPL"));
        assertEquals(2, router.route("BUY", "MSFT"));
        assertEquals(5, router.route("SELL", "MSFT"));
        for (String symbol : new String[]{"AAPL", "MSFT", "ZZZZZZZZZZ", "BRK.B"}) {
            int buy = router.route("BUY", symbol), sell = router.route("SELL", symbol);
            assertTrue(buy >= 0 && buy < 3);
            assertEquals(buy + 3, sell);
        }
        assertThrows(IllegalArgumentException.class, () -> router.route("HOLD", "AAPL"));
    }
}
