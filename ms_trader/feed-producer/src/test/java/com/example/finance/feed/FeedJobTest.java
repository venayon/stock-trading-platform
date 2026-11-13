package com.example.finance.feed;

import static org.junit.jupiter.api.Assertions.*;

import com.example.finance.trader.application.*;

import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import javax.sql.DataSource;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.datasource.embedded.*;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(FeedJobTest.Config.class)
class FeedJobTest {
    @Autowired
    @Qualifier("feedJobLauncher")
    TaskExecutorJobLauncher launcher;
    @Autowired
    Job tradeFeedJob;
    @Autowired
    MemoryTrades trades;
    @TempDir
    Path dir;
    String good = "2026-10-24,BUY,AAPL,1,10,USD,\n";
    String bad = "2026-10-24,SELL,AAPL,1,0,USD,\n";

    @BeforeEach
    void reset() {
        trades.stored.clear();
        trades.calls = 0;
        trades.failAt = -1;
    }

    JobParameters parameters(String data, long maxBad, double percent) throws Exception {
        Path file = dir.resolve("feed.csv");
        Files.writeString(file, FeedCsv.HEADER + "\n" + data);
        return new JobParametersBuilder()
            .addString("feedId", FeedFiles.hash(file))
            .addString("policyId", UUID.randomUUID().toString())
            .addString("path", file.toString(), false)
            .addString("report", dir.resolve("errors.csv").toString(), false)
            .addLong("chunkSize", 2L, false)
            .addLong("maxRecordBytes", 1024L, false)
            .addLong("maxBadRecords", maxBad, false)
            .addDouble("maxBadPercent", percent, false)
            .addString("correlationId", "test", false)
            .toJobParameters();
    }

    @Test
    void exactBadLimitSkipsAndAcceptsRemaining() throws Exception {
        var execution = launcher.run(tradeFeedJob, parameters(good + bad + good, 1, 100));
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        assertEquals(2, trades.stored.size());
        assertEquals(1, execution.getExecutionContext().getLong("badRows"));
    }

    @Test
    void overBadLimitRejectsWithNoPersistenceOrPublicationCandidate() throws Exception {
        var execution = launcher.run(tradeFeedJob, parameters(good + bad + bad, 1, 100));
        assertEquals(BatchStatus.FAILED, execution.getStatus());
        assertEquals("true", execution.getExecutionContext().getString("feedRejected"));
        assertEquals(0, trades.calls);
        assertTrue(trades.stored.isEmpty());
    }

    @Test
    void percentageUsesWholeFileAndRejectsBeforeIngestion() throws Exception {
        var execution = launcher.run(tradeFeedJob, parameters(good + bad, 10, 49));
        assertEquals(BatchStatus.FAILED, execution.getStatus());
        assertEquals(0, trades.calls);
    }

    @Test
    void exactPercentageAcceptedDespiteFirstRowInvalid() throws Exception {
        assertEquals(
            BatchStatus.COMPLETED,
            launcher.run(tradeFeedJob, parameters(bad + good, 10, 50)).getStatus());
        assertEquals(1, trades.stored.size());
    }

    @Test
    void emptyFeedRejected() throws Exception {
        var execution = launcher.run(tradeFeedJob, parameters("", 10, 100));
        assertEquals(BatchStatus.FAILED, execution.getStatus());
        assertEquals(0, trades.calls);
    }

    @Test
    void malformedHeaderRejected() throws Exception {
        var p = parameters(good, 10, 100);
        Files.writeString(Path.of(p.getString("path")), "badheader\n");
        var execution = launcher.run(tradeFeedJob, p);
        assertEquals(BatchStatus.FAILED, execution.getStatus());
        assertEquals("true", execution.getExecutionContext().getString("feedRejected"));
        assertEquals(0, trades.calls);
    }

    @Test
    void restartReplaysPartialChunkWithoutDuplicateTrades() throws Exception {
        var params = parameters(good.repeat(5), 0, 100);
        trades.failAt = 4;
        var first = launcher.run(tradeFeedJob, params);
        assertEquals(BatchStatus.FAILED, first.getStatus());
        assertEquals(3, trades.stored.size());
        trades.failAt = -1;
        var second = launcher.run(tradeFeedJob, params);
        assertEquals(BatchStatus.COMPLETED, second.getStatus());
        assertEquals(5, trades.stored.size());
        assertEquals(7, trades.calls);
    }

    @Test
    void changedSourceFailsIntegrityBeforeFurtherWrites() throws Exception {
        var params = parameters(good.repeat(5), 0, 100);
        trades.failAt = 4;
        launcher.run(tradeFeedJob, params);
        int calls = trades.calls;
        Files.writeString(Path.of(params.getString("path")), FeedCsv.HEADER + "\n" + good.repeat(6));
        assertEquals(BatchStatus.FAILED, launcher.run(tradeFeedJob, params).getStatus());
        assertEquals(calls, trades.calls);
    }

    @Configuration
    @EnableBatchProcessing
    @Import(FeedJobConfiguration.class)
    static class Config {
        @Bean
        DataSource dataSource() {
            return new EmbeddedDatabaseBuilder()
                .generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .build();
        }

        @Bean
        JdbcTransactionManager transactionManager(DataSource ds) {
            return new JdbcTransactionManager(ds);
        }

        @Bean
        FeedProperties feedProperties() {
            return new FeedProperties(Path.of("target/test-feeds"), 2, 1, 100, 1024, "v1");
        }

        @Bean
        MemoryTrades trades() {
            return new MemoryTrades();
        }

        @Bean
        TradeService tradeService(MemoryTrades trades) {
            return new TradeService(trades, Clock.systemUTC(), UUID::randomUUID);
        }
    }

    static class MemoryTrades implements TradeRepository {
        final Map<String, StoredTrade> stored = new HashMap<>();
        int calls, failAt = -1;

        public TradeAcceptance insertOrGet(StoredTrade trade) {
            calls++;
            if (calls == failAt) throw new IllegalStateException("injected storage failure");
            var old = stored.putIfAbsent(trade.idempotencyKey(), trade);
            return new TradeAcceptance(old == null ? trade : old, old == null);
        }

        public Optional<StoredTrade> findById(String id) {
            return stored.values().stream().filter(t -> t.tradeId().equals(id)).findFirst();
        }
    }
}
