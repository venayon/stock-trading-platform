package com.example.finance.feed;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.batch.item.ExecutionContext;

class FeedCsvTest {
    private static final String HEADER_FOR_IDENTITY = FeedCsv.HEADER + "\n";
    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "", "x", "1.1234567", "1000000000000", "1e2"})
    void rejectsInvalidPrices(String price) {
        assertNotNull(FeedCsv.parse(2, "2026-10-24,BUY,AAPL,1," + price + ",USD,").error());
    }

    @Test
    void canonicalizesAndHandlesQuotedCommas() {
        var row = FeedCsv.parse(2, "2026-10-24,SELL,aapl,1,10.10,,\"Dealer, Inc\"");
        assertNull(row.error());
        assertEquals("AAPL", row.trade().symbol());
        assertEquals("USD", row.trade().currency());
        assertEquals("Dealer, Inc", row.trade().counterparty());
    }

    @Test
    void resumesAtByteCheckpointIncludingUtf8() throws Exception {
        Path path = dir.resolve("f.csv");
        Files.writeString(
            path,
            "\uFEFF"
                + FeedCsv.HEADER
                + "\r\n2026-10-24,BUY,AAPL,1,1,USD,café\r\n2026-10-24,SELL,MSFT,2,2,GBP,\n");
        ExecutionContext ec = new ExecutionContext();
        var first = new FeedCsv(path, 1024);
        first.open(ec);
        assertEquals(2, first.read().line());
        first.update(ec);
        first.close();
        var resumed = new FeedCsv(path, 1024);
        resumed.open(ec);
        assertEquals(3, resumed.read().line());
        assertNull(resumed.read());
        resumed.close();
    }

    @Test
    void oversizedAndMalformedUtf8DoNotConsumeNextRecord() throws Exception {
        Path path = dir.resolve("f.csv");
        try (var out = Files.newOutputStream(path)) {
            out.write(
                (FeedCsv.HEADER + "\n" + "x".repeat(100000) + "\n").getBytes(StandardCharsets.UTF_8));
            out.write(new byte[]{(byte) 0xC3, 10});
            out.write("2026-10-24,BUY,AAPL,1,1,USD,\n".getBytes(StandardCharsets.UTF_8));
        }
        var reader = new FeedCsv(path, 128);
        reader.open(new ExecutionContext());
        assertEquals("RECORD_TOO_LARGE", reader.read().error());
        assertEquals("INVALID_UTF8", reader.read().error());
        assertNull(reader.read().error());
        assertNull(reader.read());
        reader.close();
    }

    @Test
    void rejectsWrongHeader() throws Exception {
        Path p = dir.resolve("f.csv");
        Files.writeString(p, "wrong\n");
        var reader = new FeedCsv(p, 128);
        try {
            assertThrows(FeedCsv.Rejected.class, () -> reader.open(new ExecutionContext()));
        } finally {
            reader.close();
        }
    }

    @Test
    void claimedFileRetainsOriginalPolicyAcrossScans() throws Exception {
        Path file = dir.resolve("claimed.csv");
        Files.writeString(file, HEADER_FOR_IDENTITY);
        var original = FeedFiles.identify(file, "v1");
        assertEquals(original, FeedFiles.identify(file, "v2"));
    }

    @Test
    void modifiedClaimedFileCannotBecomeANewFeedOnAutomaticRetry() throws Exception {
        Path file = dir.resolve("claimed.csv");
        Files.writeString(file, HEADER_FOR_IDENTITY);
        FeedFiles.identify(file, "v1");
        Files.writeString(file, HEADER_FOR_IDENTITY + "changed");
        assertThrows(java.io.IOException.class, () -> FeedFiles.identify(file, "v1"));
    }
}
