package com.example.finance.feed;

import com.example.finance.trader.application.TradeService;

import java.nio.file.Path;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class FeedJobConfiguration {
    @Bean
    TaskExecutorJobLauncher feedJobLauncher(JobRepository repository) throws Exception {
        var launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(repository);
        launcher.setTaskExecutor(new SyncTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    @Bean
    @StepScope
    FeedCsv feedReader(
        @Value("#{jobParameters['path']}") String path,
        @Value("#{jobParameters['maxRecordBytes']}") Long limit) {
        return new FeedCsv(Path.of(path), limit.intValue());
    }

    @Bean
    @StepScope
    FeedValidation feedValidation(
        @Value("#{jobParameters['path']}") String path,
        @Value("#{jobParameters['report']}") String report,
        @Value("#{jobParameters['chunkSize']}") Long chunk,
        @Value("#{jobParameters['maxRecordBytes']}") Long limit,
        @Value("#{jobParameters['maxBadRecords']}") Long maxBad,
        @Value("#{jobParameters['maxBadPercent']}") Double percent) {
        return new FeedValidation(
            Path.of(path), Path.of(report), chunk.intValue(), limit.intValue(), maxBad, percent);
    }

    @Bean
    @StepScope
    ItemWriter<FeedCsv.Row> tradeWriter(
        TradeService service,
        @Value("#{jobParameters['feedId']}") String hash,
        @Value("#{jobParameters['correlationId']}") String correlation) {
        return chunk -> {
            for (var row : chunk)
                service.accept("feed:" + hash + ":row:" + row.line(), row.trade(), correlation);
        };
    }

    @Bean
    @StepScope
    SimpleCompletionPolicy feedCompletionPolicy(@Value("#{jobParameters['chunkSize']}") Long size) {
        return new SimpleCompletionPolicy(size.intValue());
    }

    @Bean
    Job tradeFeedJob(
        JobRepository repository,
        PlatformTransactionManager transactionManager,
        FeedProperties properties,
        FeedValidation validator,
        FeedCsv reader,
        ItemWriter<FeedCsv.Row> tradeWriter,
        SimpleCompletionPolicy completionPolicy) {
        Step validate =
            new StepBuilder("validateFeed", repository)
                .tasklet(validator, transactionManager)
                .listener(validator)
                .build();
        Step integrity =
            new StepBuilder("verifyImmutableFeed", repository)
                .tasklet(
                    (contribution, context) -> {
                        var p = contribution.getStepExecution().getJobParameters();
                        if (!FeedFiles.hash(Path.of(p.getString("path"))).equals(p.getString("feedId")))
                            throw new IllegalStateException(
                                "Processing file changed; restore original before restart");
                        return RepeatStatus.FINISHED;
                    },
                    transactionManager)
                .allowStartIfComplete(true)
                .build();
        Step ingest =
            new StepBuilder("ingestTrades", repository)
                .<FeedCsv.Row, FeedCsv.Row>chunk(completionPolicy, transactionManager)
                .reader(reader)
                .processor(row -> row.error() == null ? row : null)
                .writer(tradeWriter)
                .build();
        return new JobBuilder("tradeFeed", repository)
            .start(validate)
            .next(integrity)
            .next(ingest)
            .build();
    }
}
