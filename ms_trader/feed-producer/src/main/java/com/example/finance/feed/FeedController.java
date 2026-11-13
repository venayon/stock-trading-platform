package com.example.finance.feed;

import java.util.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/feeds")
public class FeedController {
    private final FeedRunner runner;
    private final JobExplorer explorer;
    private final MongoTemplate mongo;
    public FeedController(FeedRunner runner, JobExplorer explorer, MongoTemplate mongo) {
        this.runner = runner;
        this.explorer = explorer;
        this.mongo = mongo;
    }

    @PostMapping("/run")
    public ResponseEntity<?> run(@RequestBody(required = false) RunRequest request) {
        return queued(runner.submit(request == null ? null : request.fileName(), null));
    }

    @PostMapping("/executions/{id}/restart")
    public ResponseEntity<?> restart(@PathVariable long id) {
        return queued(runner.submit(null, id));
    }

    private ResponseEntity<?> queued(String id) {
        return ResponseEntity.accepted()
            .body(
                Map.of("requestId", id, "status", "QUEUED", "statusUrl", "/api/feeds/requests/" + id));
    }

    @GetMapping("/requests/{id}")
    public Map<String, Object> request(@PathVariable String id) {
        return runner.request(id);
    }

    @GetMapping("/executions/{id}")
    public Map<String, Object> execution(@PathVariable long id) {
        JobExecution e = explorer.getJobExecution(id);
        if (e == null || !e.getJobInstance().getJobName().equals("tradeFeed"))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        return Map.of(
            "executionId",
            id,
            "status",
            e.getStatus(),
            "feedId",
            e.getJobParameters().getString("feedId"),
            "publication",
            publication(e.getJobParameters().getString("feedId")),
            "rejected",
            e.getExecutionContext().getString("feedRejected", "false"),
            "validRows",
            e.getExecutionContext().getLong("validRows", 0),
            "badRows",
            e.getExecutionContext().getLong("badRows", 0),
            "steps",
            e.getStepExecutions().stream()
                .map(
                    s ->
                        Map.of(
                            "name",
                            s.getStepName(),
                            "status",
                            s.getStatus(),
                            "read",
                            s.getReadCount(),
                            "written",
                            s.getWriteCount(),
                            "filtered",
                            s.getFilterCount()))
                .toList());
    }

    private Map<String, Long> publication(String hash) {
        String prefix = "^feed:" + hash + ":row:";
        return Map.of(
            "pending",
            mongo.count(
                Query.query(
                    Criteria.where("idempotencyKey")
                        .regex(prefix)
                        .and("publicationStatus")
                        .is("PENDING")),
                "trades"),
            "published",
            mongo.count(
                Query.query(
                    Criteria.where("idempotencyKey")
                        .regex(prefix)
                        .and("publicationStatus")
                        .is("PUBLISHED")),
                "trades"));
    }

    @GetMapping("/executions")
    public List<Long> executions() {
        return explorer.getJobInstances("tradeFeed", 0, 50).stream()
            .flatMap(i -> explorer.getJobExecutions(i).stream())
            .map(JobExecution::getId)
            .toList();
    }

    public record RunRequest(String fileName) {
    }
}
