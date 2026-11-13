# Stock Trading Platform — High-Level Design

> Implementation update (2026-10-24): [ADR 0016](../adr/0016-mongodb-acceptance-idempotency.md) defines durable
> acceptance; [ADR 0017](../adr/0017-asynchronous-avro-publication.md) defines the implemented asynchronous Avro relay and
> Kafdrop stack. Producer observability is implemented via [ADR 0018](../adr/0018-producer-observability.md); the
> consumer/routing/DLT slice is implemented via [ADR 0019](../adr/0019-consumer-processing.md). Load generation remains
> planned.

Date: 2026-10-24 · Status: Proposed for review · Owner: Venu Ranaboyana

Scope: Iterations 1 and 2 of [AI_DEVELOPMENT_PROMPT.md](../adr/AI_DEVELOPMENT_PROMPT.md).
Implementation details: [LLD](LLD.md). Decisions: [ADR register](../adr/README.md).
Development sequence: [TDD plan](TDD_PLAN.md). These documents propose a design; no application or infrastructure
implementation is claimed.

## 1. Executive summary

Build a local demo for accepting and processing imaginary stock orders.
Use Java 21 and Spring Boot for both services.
Store accepted orders in MongoDB and publish Avro records through Schema Registry.
Run a single Kafka broker and controller in KRaft mode through Docker Compose.
Use six partitions and symbol keys to demonstrate concurrency and per-symbol Kafka ordering.
Iteration 1 delivers the API, persistence, publication, load generator, and baseline monitoring.
Iteration 2 consumes orders, stores processed results, and routes to a second topic.
Route BUY and SELL to separate sets of partitions using an explicit versioned algorithm.
Return durable PENDING acceptance, then publish asynchronously from an embedded outbox.
Persist publication state and require idempotency keys so failures are visible and retryable.
Accept at-least-once events and deduplicate by trade ID; do not claim cross-system exactly-once delivery.
Provision monitoring from files and use Kafdrop for manual inspection.
Risk 1: MongoDB and Kafka cannot be committed atomically with the chosen publication design.
Risk 2: skewed symbols and retry failures can cause partition backlog or reordered business effects.
Risk 3: constrained laptop resources and historical dependency constraints can distort load results.

## 2. Architecture diagram

```text
Iteration 1
Playwright / caller
       | POST/GET :8081 + Idempotency-Key + X-Correlation-ID
       v
 ms_trader -------> MongoDB / trader.trades
       | persist PENDING -> HTTP 202; relay: claim -> Avro -> ACK -> PUBLISHED
       v
 Kafka: stock-trades [0 1 2 3 4 5], key=symbol, RF=1
       |                          ^
       |                    Schema Registry :8085
       |                          |
       +---------------------- Kafdrop :9000 (Avro inspection)
       |
Iteration 2
 ms_trade_processor (group=stock-trade-processor-v1, concurrency=3)
       | upsert PENDING -> send routed record -> ACK -> mark COMPLETE
       | then commit source offset
       +-----> MongoDB / processor.processed_trades
       +-----> Kafka: stock-trades-processed [0 1 2 | 3 4 5]
       |            BUY buckets 0..2 | SELL buckets 3..5
       +-----> Kafka: stock-trades-dlt [0 1 2 3 4 5] on exhausted/terminal failure
       ^
 REST :8082 POST /api/process (enqueue retained source record), GET /api/processed...

 Prometheus :9090 --scrapes--> both apps /actuator/prometheus
 Grafana :3000 ------queries--> Prometheus
 Kafka admin polling in processor -> committed-offset lag gauge -> Prometheus
 All containers: trading-net; host clients use localhost:9092.
```

Ordering means log append order within one topic partition. Concurrent REST callers, retries, DLT recovery and manual
replay do not imply business-time ordering. This demo does not execute trades, maintain balances, or connect to an
exchange.

## 3. Docker Compose design

Use one `infra/docker-compose.yml`, project `stock-trading`, bridge network `trading-net`. Infrastructure and trader are
the default stack; processor uses the `iteration2` Compose profile. Use named volumes; bind configuration read-only.
Bind published ports to `127.0.0.1`. Do not publish MongoDB or Kafka controller ports unless needed for local tooling.

The following are proposed reproducible baseline tags, not claims of current support or validated combinations. Before
implementation, resolve available patches and platform digests, inspect the Maven dependency tree, and pass the
compatibility tests in the TDD plan. Never use `latest`.

| Service            | Proposed image/tag                    | Host → container     | Volumes/config                               | Readiness and dependencies                                                           |
|--------------------|---------------------------------------|----------------------|----------------------------------------------|--------------------------------------------------------------------------------------|
| kafka              | confluentinc/cp-kafka:7.9.4           | 9092→9092            | kafka-data                                   | `kafka-topics --bootstrap-server kafka:29092 --list`; fixed cluster ID, node 1       |
| schema-registry    | confluentinc/cp-schema-registry:7.9.4 | 8085→8081            | none                                         | HTTP `/subjects`; Kafka healthy                                                      |
| topic-init         | same cp-kafka tag                     | none                 | init script RO                               | one-shot; Kafka healthy; validate existing settings as well as create missing topics |
| schema-init        | curlimages/curl:8.12.1                | none                 | schema/config scripts RO                     | one-shot; Registry healthy; register schema and set compatibility explicitly         |
| kafdrop            | obsidiandynamics/kafdrop:4.1.0        | 9000→9000            | none                                         | HTTP health probe via verified image tooling; Kafka and Registry healthy             |
| mongodb            | mongo:7.0.16                          | optional 27017→27017 | mongo-data                                   | `mongosh --quiet --eval 'db.adminCommand({ping:1})'`                                 |
| ms-trader          | local build, Java 21 runtime          | 8081→8081            | application config RO                        | Actuator readiness; Mongo healthy, both init jobs completed                          |
| ms-trade-processor | local build, Java 21 runtime          | 8082→8082            | application config RO                        | same dependencies; profile iteration2                                                |
| prometheus         | prom/prometheus:v2.55.1               | 9090→9090            | prometheus-data; prometheus.yml RO           | HTTP `/-/ready`; scrape targets can join later                                       |
| grafana            | grafana/grafana:11.6.0                | 3000→3000            | grafana-data; provisioning and dashboards RO | HTTP `/api/health`; Prometheus healthy                                               |

Probe executables must be checked against each actual image; add a probe in the application image rather than assuming
curl is installed. Use 10s interval, 5s timeout, 12 retries, and 30s startup allowance as initial values. Startup
ordering does not replace application reconnection. Set bounded memory and a documented Docker memory budget after
measuring the baseline.

Kafka combined roles are `broker,controller`, quorum voter `1@kafka:29093`, controller listener `CONTROLLER:29093`,
internal `INTERNAL:29092`, external `EXTERNAL:9092`. Advertise `kafka:29092` internally and `localhost:9092` externally.
Inter-broker listener is INTERNAL. Internal offsets/transaction replication factors and transaction minimum ISR are 1.
Disable automatic topic creation. KRaft avoids a ZooKeeper container; this single-node configuration provides no high
availability.

Proposed application baseline: Spring Boot 3.5.7, Boot-managed Spring Kafka/Kafka client dependencies, Confluent
serializer 7.9.4, Avro runtime and Maven plugin 1.12.0, Java 21, Node 22 and Playwright 1.49.1. Boot 3.5 meets the
prompt's 3.3+ constraint. Confluent 7.9 bundles Kafka 3.9, which meets 3.7+. Confluent server images retain their
bundled JVM; do not force Java 21 into them. Avro overrides and serializer dependencies require a round-trip
compatibility test. Prometheus 2.55, Grafana 11 and Playwright 1.49 follow the requested historical families; review
patch/security maintenance before adopting this demo elsewhere.

## 4. Kafka topic design

| Topic                  | Partitions | RF / min ISR | Retention | Cleanup | Key / purpose                                 |
|------------------------|-----------:|--------------|-----------|---------|-----------------------------------------------|
| stock-trades           |          6 | 1 / 1        | 7 days    | delete  | normalized symbol; immutable accepted trades  |
| stock-trades-processed |          6 | 1 / 1        | 7 days    | delete  | normalized symbol; explicit side/symbol route |
| stock-trades-dlt       |          6 | 1 / 1        | 14 days   | delete  | preserve original key and source partition    |

Six partitions allow up to six active consumers and demonstrate a concurrency of three without overwhelming a laptop.
Equal keys land together within each topic; a popular symbol can still create a hot partition. Measure symbol skew, then
consider a different key only if per-symbol ordering can be relaxed. Do not partition solely by side on the source
topic: two values underuse six partitions. Do not increase partition count casually: keyed routing changes. Retention
bounds recovery; MongoDB is the durable query source. Provision topics explicitly and fail startup on mismatched
partition count or policy.

Iteration 2 republishes to the processed topic; consumers cannot move records between source partitions. BUY uses 0–2
and SELL uses 3–5. LLD defines the exact hash. Filtering assigned partitions would discard or strand work and is
rejected. Output ordering is per symbol and side, not across BUY and SELL.

Kafdrop success: all six source partitions exist, offsets increase, keys are canonical symbols, Avro values decode with
Registry configured, and a varied-symbol run exercises multiple partitions. Empty partitions in a small sample are
valid. Offsets are not business success counts; duplicates, retention and control records invalidate naive comparisons.

## 5. Avro schema governance

`schemas/StockTrade.avsc` remains authoritative; preserve `com.example.finance.avro.StockTrade`. Publish a reusable
`stock-trade-schema` Maven artifact generated during `generate-sources` using `avro-maven-plugin:1.12.0`, Java strings
and decimal logical-type support. Both services depend on that artifact; never copy the schema into both repositories.
Record the artifact version with each build.

Use StringSerializer for keys, KafkaAvroSerializer for values, TopicNameStrategy, subjects `stock-trades-value` and
`stock-trades-processed-value`. Set `BACKWARD_TRANSITIVE` on both subjects, explicitly register schemas during bootstrap
and disable producer automatic registration. Old data must remain readable by new consumers. Upgrade consumers before
producers; use FULL_TRANSITIVE instead if mixed-version readers must consume new writers indefinitely.

Keep price as Avro bytes decimal (18,6), Java BigDecimal, never double. Dates are epoch days mapped to LocalDate. Add
optional fields with defaults; never rename/remove fields, change the namespace, precision/scale or enum symbols without
compatibility and behavioral review. Registry acceptance alone does not prove application compatibility. DLT values
preserve serialized bytes; do not register a StockTrade subject for arbitrary poison bytes.

## 6. ms_trader design

Layers: `api` controllers/DTOs/advice; `application` TradeService; `domain` Trade/PublicationState; `persistence`
document/repository; `messaging` publisher/mapper; `config`; `observability`. Controllers do not call MongoDB or Kafka
directly.

POST validates and atomically inserts a PENDING trade with its idempotency key, then returns 202 after MongoDB
acknowledgment. A durable relay claims due records, publishes Avro, waits for broker acknowledgment and stores PUBLISHED
with event coordinates. GET exposes current status. Same-key canonical retries return 200; changed payloads conflict.
Broker outages do not prevent acceptance when MongoDB is available. MongoDB write uncertainty returns 503 and callers
must retry the same key.

The record itself acts as an embedded outbox. Claims have 60-second leases and unique fencing tokens; a relay processes
up to 20 records per one-second poll and retries failures with 2–60-second backoff. A crash after Kafka ACK before
completion can still duplicate an event. [ADR 0017](../adr/0017-asynchronous-avro-publication.md) supersedes the earlier
synchronous proposal; consumers must deduplicate trade IDs.

Use unique idempotency key and immutable trade ID, canonical payload digest, bounded paging, ProblemDetail responses and
correlation IDs. See LLD for exact response semantics, validation and state transitions. Mongo failure before acceptance
returns 503 and publishes nothing. Publish success followed by Mongo failure returns 503/UNKNOWN and is recovered by
retry. Structured JSON logs and low-cardinality metrics are required.

## 7. ms_trade_processor design

Group `stock-trade-processor-v1`; concurrency 3; record listener; `enable.auto.commit=false`; `AckMode.RECORD`;
`auto.offset.reset=earliest` for the first group start. Commit only after Mongo state and routed publication have
completed, or after acknowledged DLT recovery. Use specific Avro reader and ErrorHandlingDeserializer so malformed
records reach recovery.

Upsert by trade ID, persist immutable input digest and route, publish output, store completion metadata, then let the
listener return. COMPLETE redeliveries skip work; PENDING records resume. A send acknowledgment followed by crash before
completion can duplicate output; downstream must deduplicate trade IDs. No consumer filtering and no claim of Kafka
transactions making Mongo atomic.

Retry transient failures three times after initial delivery, at 1s intervals. Terminal
mapping/validation/deserialization failures go directly to DLT. DLT publication must be acknowledged before advancing
the source offset; DLT failure keeps the original record retryable. A DLT allows subsequent records through and
therefore breaks strict end-to-end order. REST manual replay queues the retained canonical source event; it never seeks
the live group's offsets.

## 8. Playwright load generator design

```text
load-generator/
  package.json / package-lock.json
  playwright.config.js
  fixtures/trades.js
  tests/smoke.spec.js
  tests/load.spec.js
  scripts/reconcile.js
  reports/                         # ignored generated output
```

Use Playwright APIRequestContext, no browser. Config: `BASE_URL`, `WORKERS` (default 4), `REQUESTS_PER_WORKER` (250),
`DURATION_SECONDS` (optional stop deadline), `BUY_RATIO` (0.5), `SYMBOLS` (at least 12), `RUN_ID`, request timeout
(15s). Each worker runs a bounded loop; one in-flight request per worker, not an unbounded Promise.all. Use stable
run/worker/sequence idempotency keys and the same key for retries. Record seeds and payloads sufficient for
reproducibility. Playwright workers are closed-loop concurrency, not a precise arrival-rate model; switch to k6 if
fixed-rate saturation analysis is needed.

Assert expected status, JSON schema, trade ID, correlation ID and Kafka metadata for every 202. Separate fresh accepts,
idempotent responses, recoverable failures and terminal errors. Reconcile unique submitted IDs against trader documents
and, in Iteration 2, completed processor documents within a bounded drain window. Count records using an isolated
verification consumer; report duplicates separately. Kafdrop confirms routing visually. Report throughput, p50/p95/p99,
failures, backlog, resource budget and warmup separately; performance targets remain to be measured.

## 9. Observability design

Store configuration at `infra/prometheus/prometheus.yml`, `infra/grafana/provisioning/{datasources,dashboards}/`, and
`infra/grafana/dashboards/trading-overview.json`. These are planned implementation files. Use datasource UID
`prometheus`, dashboard UID `stock-trading-overview`; avoid manual dashboard edits as the authoritative source.

```yaml
scrape_configs:
  - job_name: ms-trader
    metrics_path: /actuator/prometheus
    scrape_interval: 5s
    static_configs: [{targets: ['ms-trader:8081']}]
  - job_name: ms-trade-processor
    metrics_path: /actuator/prometheus
    scrape_interval: 5s
    static_configs: [{targets: ['ms-trade-processor:8082']}]
```

Host-run profile uses `host.docker.internal` on Docker Desktop instead of Compose service DNS; Linux needs an explicit
host-gateway mapping. Grafana provisioning uses URL `http://prometheus:9090`, type prometheus, default datasource;
dashboard provider loads `/var/lib/grafana/dashboards` with editing disabled.

| Panel                     | Prometheus expression / meaning                                                               |
|---------------------------|-----------------------------------------------------------------------------------------------|
| Fresh accepted rate       | `sum(rate(trades_accepted_total[1m]))`                                                        |
| Published rate            | `sum(rate(trades_published_total[1m]))`; includes acknowledged retry sends                    |
| Completed rate            | `sum(rate(trades_processed_total[1m]))`; first COMPLETE transitions                           |
| Committed group lag       | `sum(trades_consumer_lag{topic="stock-trades"})`                                              |
| Publish/processing errors | `sum(rate(trades_errors_total[1m])) by (stage)`                                               |
| HTTP 5xx ratio            | 5xx rate / all request rate from `http_server_requests_seconds_count`, guard zero denominator |
| Processing p95            | `histogram_quantile(0.95,sum(rate(trades_processing_seconds_bucket[5m])) by (le))`            |
| JVM / availability        | `jvm_memory_used_bytes`, `jvm_gc_pause_seconds_count`, `process_cpu_usage`, `up`              |
| Recovery                  | `trades_pending_publications`, `trades_dlt_total`, `trades_lag_poll_success`                  |

Lag is sampled every 5s using Kafka Admin end offsets minus committed group offsets, clamped to zero. An uninitialized
group starts at source log-start offsets. Use fixed topic/partition/group labels. Export sampling success and last
successful sample timestamp; a failed poll must not turn lag into zero. Native client metrics alone do not measure
committed backlog reliably.

Document alerts initially: lag >1000 for 2m; pending >0 for 1m; any DLT increment over 5m; 5xx >1% with at least 100
requests over 5m; expected target down for 1m; lag sampling stale >30s. Processor absence in Iteration 1 must not
trigger target-down alerts. Thresholds are initial demo values, not agreed SLOs.

## 10. Configuration & secrets

Use `application.yml` shared defaults and `application-local.yml` for host-run endpoints. Compose passes internal
endpoints explicitly: `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `MONGODB_URI`, `SERVER_PORT`. Other named
settings include source/output/DLT topics, group ID, concurrency, retry limits and reconciliation interval. Fail fast on
missing configuration; do not silently use production-looking endpoints.

Local unauthenticated Kafka/Registry/Mongo are private to the Compose network; restrict optional host ports. Supply
Grafana administrator credentials via an ignored local env/secret file, with an example listing names only. Never commit
passwords, tokens, real counterparty data or credential-bearing connection strings. Cache Maven/npm artifacts and
container images for offline operation; startup schema registration stays local and needs no internet.

## 11. Testing strategy

Use JUnit 5 for domain and orchestration tests, MVC slice tests for HTTP contracts, and Testcontainers for actual
MongoDB, Kafka and Schema Registry interoperability. Test failure windows and duplicate delivery, not just successful
mocks. Avro tests check exact decimal/date values, enum handling, previous-writer compatibility and malformed-byte DLT
recovery. Compose smoke tests cover DNS, healthchecks, init idempotency, Prometheus targets and a dashboard query.

Playwright verifies the REST-to-Kafka-to-Mongo path and reconciles unique IDs after drain. Failure injection covers
unavailable Mongo, unavailable broker/Registry, DLT send failure, process restart between send and state update, and
consumer rebalance. See the [TDD plan](TDD_PLAN.md) for the red–green–refactor order. Build only the smallest approved
behavior in each cycle.

## 12. Design decisions log

The [ADR register](../adr/README.md) contains the 14 original numbered proposals and accepted implementation records
0015–0017 with options, choice, rationale, consequences, runner-up and switch trigger. All are proposed, not accepted.
The two failure-sensitive decisions have expanded
records: [implemented publication](../adr/0017-asynchronous-avro-publication.md)
and [routing](../adr/0007-processor-routing.md).

## 13. Iteration 1 → Iteration 2 handoff

Freeze the StockTrade fullname, decimal/date encoding, subject names and compatibility policy. Verify six source
partitions, key normalization, source retention, idempotency semantics and correlation headers. Publish a schema
artifact consumed by both services. Freeze metric names in section 9 and REST contracts in the LLD. Prove a PENDING
trade survives restart and eventually republishes; document duplicate events as expected.

Iteration 1 already provisions processed/DLT topics and observability infrastructure; starting the processor profile
must not recreate source data. Provide deterministic seed data, pinned images, cached dependencies, a fresh-volume
bootstrap and a persistent-volume restart test. Sign off the review checklist before application implementation, and the
handoff evidence before Iteration 2.

## 14. Open questions / assumptions

All due dates mean “by the design review, before the first implementation cycle”; no calendar commitment has been
supplied.

- Architect / owner: accept the 202-after-Kafka-ACK contract and 503-with-durable-pending behavior? Due: design review.
- Backend: require Idempotency-Key for all POST requests and preserve keys for the lifetime of stored trades? Due:
  design review.
- Architect: accept per-symbol-and-side output ordering, duplicates and DLT gaps? Due: design review.
- DevOps: approve dependency patches/image digests and the measured laptop resource budget? Due: compatibility cycle.
- QA: agree a baseline dataset and initial performance target after a smoke measurement? Due: before load cycle.
- Backend: accept immutable accepted trades and replay from retained Mongo source documents only? Due: design review.
- Owner: confirm synthetic data only, no authorization/business execution requirements in these iterations? Due: design
  review.
- Team: confirm roles for checklist approval; named people other than the owner are not assigned yet. Due: design
  review.

## Green-light checklist

Unchecked means work or review remains; documentation does not imply implementation evidence.

| #  | Approval check                                                        | Owner     | Status |
|----|-----------------------------------------------------------------------|-----------|--------|
| 1  | KRaft design accepted; runtime has no ZooKeeper                       | DevOps    | ☐     |
| 2  | Explicit Registry compatibility and subject policy accepted           | Architect | ☐     |
| 3  | Six partitions, symbol key and skew trade-off accepted                | Architect | ☐     |
| 4  | Schema source and Maven generation verified                           | Backend   | ☐     |
| 5  | Sync publication, recovery and duplicate semantics accepted           | Backend   | ☐     |
| 6  | Correlation propagation contract accepted                             | Architect | ☐     |
| 7  | Explicit output routing algorithm accepted                            | Architect | ☐     |
| 8  | Retries, DLT acknowledgment and ordering gaps accepted                | Backend   | ☐     |
| 9  | Monitoring provisioning files implemented and verified in infra cycle | DevOps    | ☐     |
| 10 | Playwright plan and reconciliation assertions accepted                | QA        | ☐     |
| 11 | Environment override and secret policy accepted                       | DevOps    | ☐     |
| 12 | ADR options and switch triggers reviewed                              | Architect | ☐     |
| 13 | Open questions assigned and resolved or explicitly deferred           | Owner     | ☐     |
| 14 | Iteration handoff contract accepted                                   | Architect | ☐     |

Decision: ☐ Approved to start Iteration 1 application TDD ☐ Changes requested.
The infra/compatibility verification cycle supplies evidence for checks 1, 4 and 9 before application work; it is a
separate proposed preparation step, not authorization implied by this document.

## Technical references

- [Confluent 7.9 interoperability](https://docs.confluent.io/platform/7.9/installation/versions-interoperability.html):
  7.9 maps to Kafka 3.9.
- [Confluent JVM requirements](https://docs.confluent.io/platform/current/installation/system-requirements.html): server
  JVM support differs from the applications' Java 21 requirement.
- [Spring Kafka exception handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html):
  recovery and DLT failures require explicit offset/error policy. Verify APIs against the pinned Spring Kafka version
  during implementation.

## Implemented scheduled feed extension

The independent producer in `ms_trader/feed-producer` scans shared CSV storage every two hours and through REST on port
8083. PostgreSQL stores Batch metadata; validated trades share the existing MongoDB trader collection and leased Kafka
outbox. See [feed HLD/LLD](FEED_PRODUCER.md), [ADR 0021](../adr/0021-scheduled-feed-producer.md), and
the [beginner mindmap](../guides/SPRING_BATCH_FEED.md).
