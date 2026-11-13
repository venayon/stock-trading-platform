package com.example.finance.feed;

import java.io.*;
import java.nio.file.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Validation restarts from the beginning; never writes business trades.
 */
public class FeedValidation implements Tasklet, StepExecutionListener {
    private final Path path, report;
    private final int chunk, limit;
    private final long maxBad;
    private final double maxPercent;
    private FeedCsv reader;
    private BufferedWriter output;
    private long valid, bad;
    private boolean rejected;

    public FeedValidation(
        Path path, Path report, int chunk, int limit, long maxBad, double maxPercent) {
        this.path = path;
        this.report = report;
        this.chunk = chunk;
        this.limit = limit;
        this.maxBad = maxBad;
        this.maxPercent = maxPercent;
    }

    @Override
    public void beforeStep(StepExecution step) {
        try {
            valid = bad = 0;
            rejected = false;
            reader = new FeedCsv(path, limit);
            reader.open(new ExecutionContext());
            output = Files.newBufferedWriter(report);
            output.write("line,errorCode\n");
        } catch (FeedCsv.Rejected e) {
            rejected = true;
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext context)
        throws Exception {
        for (int i = 0; i < chunk; i++) {
            FeedCsv.Row row = reader.read();
            if (row == null) {
                if (valid == 0 || bad * 100.0 / (valid + bad) > maxPercent) {
                    rejected = true;
                    throw new FeedCsv.Rejected("FEED_QUALITY_LIMIT");
                }
                return RepeatStatus.FINISHED;
            }
            contribution.incrementReadCount();
            if (row.error() == null) valid++;
            else {
                bad++;
                output.write(row.line() + "," + row.error() + "\n");
                if (bad > maxBad) {
                    rejected = true;
                    throw new FeedCsv.Rejected("BAD_RECORD_LIMIT");
                }
            }
        }
        output.flush();
        return RepeatStatus.CONTINUABLE;
    }

    @Override
    public ExitStatus afterStep(StepExecution step) {
        try {
            if (reader != null) reader.close();
            if (output != null) output.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ExecutionContext ec = step.getJobExecution().getExecutionContext();
        ec.putLong("validRows", valid);
        ec.putLong("badRows", bad);
        if (rejected) {
            ec.putString("feedRejected", "true");
            return new ExitStatus("REJECTED");
        }
        return null;
    }
}
