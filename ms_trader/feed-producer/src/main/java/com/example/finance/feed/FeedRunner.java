package com.example.finance.feed;

import jakarta.annotation.PreDestroy;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class FeedRunner {
    private static final Logger log = LoggerFactory.getLogger(FeedRunner.class);
    private static final long LOCK = 70612201L;
    private final FeedProperties config;
    private final DataSource dataSource;
    private final Job job;
    private final TaskExecutorJobLauncher launcher;
    private final JobRepository repository;
    private final JobExplorer explorer;
    private final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
    private final Map<String, Map<String, Object>> requests =
        Collections.synchronizedMap(new LinkedHashMap<>());
    private final boolean scheduleEnabled;

    public FeedRunner(
        FeedProperties config,
        DataSource dataSource,
        Job tradeFeedJob,
        TaskExecutorJobLauncher feedJobLauncher,
        JobRepository repository,
        JobExplorer explorer,
        @Value("${feed.schedule-enabled:true}") boolean scheduleEnabled)
        throws Exception {
        this.config = config;
        this.dataSource = dataSource;
        job = tradeFeedJob;
        launcher = feedJobLauncher;
        this.repository = repository;
        this.explorer = explorer;
        this.scheduleEnabled = scheduleEnabled;
        Files.createDirectories(config.directory());
        if (Files.isSymbolicLink(config.directory()))
            throw new IllegalArgumentException("Shared directory cannot be a symbolic link");
        for (String dir : List.of("inbox", "processing", "archive", "rejected", "reports")) {
            Path path = config.directory().resolve(dir);
            Files.createDirectories(path);
            if (Files.isSymbolicLink(path))
                throw new IllegalArgumentException("Feed subdirectories cannot be symbolic links");
        }
    }

    @Scheduled(
        fixedDelayString = "${feed.interval:PT2H}",
        initialDelayString = "${feed.initial-delay:PT30S}")
    public void scheduled() {
        if (scheduleEnabled)
            try {
                submit(null, null);
            } catch (ResponseStatusException busy) {
                log.info("Feed scan already queued");
            }
    }

    public String submit(String filename, Long restart) {
        if (filename != null
            && (!filename.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,180}\\.csv") || filename.contains("..")))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Use a CSV basename inside inbox");
        String id = UUID.randomUUID().toString();
        synchronized (requests) {
            if (requests.size() >= 100) requests.remove(requests.keySet().iterator().next());
            requests.put(id, Map.of("requestId", id, "status", "QUEUED"));
        }
        try {
            executor.execute(() -> run(id, filename, restart));
        } catch (RejectedExecutionException e) {
            requests.remove(id);
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Feed runner is busy");
        }
        return id;
    }

    public Map<String, Object> request(String id) {
        var state = requests.get(id);
        if (state == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        return state;
    }

    private void run(String id, String filename, Long restart) {
        requests.put(id, Map.of("requestId", id, "status", "RUNNING"));
        try (Connection connection = dataSource.getConnection();
             Statement lock = connection.createStatement()) {
            try (ResultSet result = lock.executeQuery("select pg_try_advisory_lock(" + LOCK + ")")) {
                result.next();
                if (!result.getBoolean(1)) {
                    requests.put(id, Map.of("requestId", id, "status", "BUSY"));
                    return;
                }
            }
            try {
                List<Long> executions = new ArrayList<>();
                if (restart != null) {
                    JobExecution old = explorer.getJobExecution(restart);
                    if (old == null
                        || !old.getJobInstance().getJobName().equals("tradeFeed")
                        || (old.getStatus() != BatchStatus.FAILED && old.getStatus() != BatchStatus.STOPPED)
                        || "true".equals(old.getExecutionContext().getString("feedRejected", "false")))
                        throw new IllegalArgumentException(
                            "Only technical FAILED/STOPPED feed executions can restart");
                    JobExecution result = launcher.run(job, old.getJobParameters());
                    executions.add(result.getId());
                    dispose(result);
                } else {
                    // Recover claimed files first. A technical failure is retried at most once per scan.
                    try (var paths = Files.list(config.directory().resolve("processing"))) {
                        for (Path path : paths.filter(p -> p.toString().endsWith(".csv")).sorted().toList())
                            process(path, id, executions);
                    }
                    Path inbox = config.directory().resolve("inbox");
                    try (var paths = Files.list(inbox)) {
                        for (Path path :
                            paths
                                .filter(p -> filename == null || p.getFileName().toString().equals(filename))
                                .filter(p -> p.toString().endsWith(".csv"))
                                .sorted()
                                .toList()) {
                            Path ready = path.resolveSibling(path.getFileName() + ".ready");
                            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                                || !Files.isRegularFile(ready, LinkOption.NOFOLLOW_LINKS)) continue;
                            Path claimed =
                                config
                                    .directory()
                                    .resolve("processing")
                                    .resolve(UUID.randomUUID() + "-" + path.getFileName());
                            Files.move(path, claimed, StandardCopyOption.ATOMIC_MOVE);
                            Files.deleteIfExists(ready);
                            process(claimed, id, executions);
                        }
                    }
                }
                requests.put(id, Map.of("requestId", id, "status", "FINISHED", "executionIds", executions));
            } finally {
                lock.execute("select pg_advisory_unlock(" + LOCK + ")");
            }
        } catch (Exception e) {
            log.error("Feed scan failed: {}", e.getClass().getSimpleName());
            requests.put(
                id,
                Map.of(
                    "requestId",
                    id,
                    "status",
                    "FAILED",
                    "error",
                    "Inspect execution status and application logs"));
        }
    }

    private void process(Path path, String correlation, List<Long> executions) throws Exception {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) return;
        var identity = FeedFiles.identify(path, config.policyId());
        String hash = identity.hash();
        JobParameters parameters =
            new JobParametersBuilder()
                .addString("feedId", hash)
                .addString("policyId", identity.policy())
                .addString("path", path.toString(), false)
                .addString(
                    "report",
                    config
                        .directory()
                        .resolve("reports")
                        .resolve(hash + "-" + identity.policy() + ".csv")
                        .toString(),
                    false)
                .addLong("chunkSize", (long) config.chunkSize(), false)
                .addLong("maxRecordBytes", (long) config.maxRecordBytes(), false)
                .addLong("maxBadRecords", config.maxBadRecords(), false)
                .addDouble("maxBadPercent", config.maxBadPercent(), false)
                .addString("correlationId", correlation, false)
                .toJobParameters();
        JobExecution previous = repository.getLastJobExecution("tradeFeed", parameters);
        if (previous != null) {
            if (previous.isRunning()) return; // Never steal a job whose process ownership is unknown.
            if (previous.getStatus() == BatchStatus.COMPLETED || isRejected(previous)) {
                move(path, isRejected(previous) ? "rejected" : "archive");
                executions.add(previous.getId());
                return;
            }
            // Preserve policy and source path on restart. Duplicate file content waits for original
            // recovery.
            if (!path.toString().equals(previous.getJobParameters().getString("path"))) return;
            parameters = previous.getJobParameters();
        }
        JobExecution result = launcher.run(job, parameters);
        executions.add(result.getId());
        dispose(result);
    }

    private boolean isRejected(JobExecution execution) {
        return "true".equals(execution.getExecutionContext().getString("feedRejected", "false"));
    }

    private void dispose(JobExecution execution) throws Exception {
        if (execution.getStatus() == BatchStatus.COMPLETED || isRejected(execution))
            move(
                Path.of(execution.getJobParameters().getString("path")),
                isRejected(execution) ? "rejected" : "archive");
    }

    private void move(Path source, String destination) throws Exception {
        if (Files.exists(source)) {
            Path target = config.directory().resolve(destination).resolve(source.getFileName());
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            Path manifest = FeedFiles.identityPath(source);
            if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS))
                Files.move(manifest, FeedFiles.identityPath(target), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}
