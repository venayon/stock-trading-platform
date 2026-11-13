# ms_trade_processor

Java 21 / Spring Boot 3.5.16 Avro consumer with MongoDB deduplication, explicit partition routing, dead-letter recovery,
query/replay APIs and Prometheus metrics.

## Run

Start the shared infrastructure first. Bootstrap now includes stock-trades, stock-trades-processed and stock-trades-dlt
plus their valid Avro subjects:

```sh
# From infra/
python3 scripts/init-monitoring-env.py
docker compose up -d --wait

# From schemas/ (required once per schema change or fresh checkout)
mvn install

# From ms_trade_processor/
mvn verify
java -jar target/ms-trade-processor-0.0.1-SNAPSHOT.jar
```

The trader and processor run as separate host Java processes. Both must run for the full REST-to-processing pipeline.
Dependencies and pinned test images must be cached before offline builds. Defaults:

- [Processor Swagger](http://localhost:8082/swagger-ui.html), [OpenAPI JSON](http://localhost:8082/v3/api-docs)
- [Processed trades](http://localhost:8082/api/processed)
- MongoDB: `mongodb://localhost:27017/processor`, collection `processed_trades`
- Kafka localhost:9092; Registry http://localhost:8085
- [Routed output in Kafdrop](http://localhost:9000/topic/stock-trades-processed)
- [Dead-letter topic](http://localhost:9000/topic/stock-trades-dlt)
- [Combined dashboard](http://localhost:3000/d/stock-trading-overview)

`MONGODB_URI`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `SERVER_PORT` and `SERVER_ADDRESS` override local
endpoints. Update Prometheus if the server port changes. `processor.listener.enabled=false` disables consumption for
API-only inspection; it is true by default. Concurrency defaults to three; the group and topic contract are explicitly
named in configuration/code. Readiness checks MongoDB for API availability; consumer connectivity is additionally
visible through lag sampling and listener logs. Liveness is independent of dependencies.

## Processing guarantees

Group `stock-trade-processor-v1` consumes stock-trades with specific.avro.reader=true, earliest initial offset, auto
commit disabled, concurrency 3 and RECORD ack mode. The first start therefore processes retained historical events as
well as new ones. The service validates canonical fields, strict UUID identity and symbol/key equality before writing.
Exact decimal/date/quantity values use the shared schema; Mongo stores Decimal128 price, ISO date and Int64 quantity.

For each trade, atomically insert/read a PENDING document keyed by tradeId and compare its versioned immutable payload
digest. COMPLETE duplicates return without another output send. A different payload for an existing ID is terminal and
goes to DLT. Pending work acquires a unique 60-second lease; competing live owners retry, and stale tokens cannot
complete another owner's claim.

Publish output and wait for Kafka ACK, persist COMPLETE plus source/output coordinates, then return from the listener so
its source offset may commit. Failures release the owned lease when possible; if Mongo itself is down, lease expiry
permits retry. A crash after output ACK but before completion can produce duplicate physical output. This is
at-least-once processing with one logical Mongo document, not cross-system exactly once. Downstream consumers must
deduplicate tradeId.

Route version 1 requires exactly six target partitions:

```text
bucket = floorMod(canonicalSymbol.hashCode(), 3)
BUY partition = bucket          # 0..2
SELL partition = 3 + bucket     # 3..5
```

AAPL routes BUY=1 / SELL=4; MSFT BUY=2 / SELL=5. Keys remain canonical symbols; output values remain StockTrade. Headers
carry correlation ID, tradeId, original source coordinates and routeVersion. Ordering is local to symbol+side in Kafka
append order, not across sides or business dates. Partition-count/hash changes require a contract migration.

## Retry and DLT

Transient listener failures get three retries after the initial attempt, one second apart. Invalid fields/identity
conflicts and deserialization failures recover directly. DeadLetterPublishingRecoverer publishes to the source partition
of stock-trades-dlt and must observe send success before normal recovered-offset advancement. A failed DLT send throws
and retains the source record for recovery. No failed record is silently skipped.

DLT preserves raw value bytes for malformed Avro and valid Avro for decoded failures, as well as original source
coordinates and exception type headers. The delegated serializers support String/byte[] keys and StockTrade/byte[]
values. Stack trace/message exception headers are excluded. Registry pre-registers a DLT subject for valid Avro;
arbitrary raw poison bytes do not become valid Avro simply because that subject exists. Kafdrop can decode the valid
records; use DEFAULT inspection for malformed bytes.

DLT recovery allows later records to progress, creating an ordering gap. Mongo outages can eventually isolate an
otherwise valid trade in DLT if Kafka is healthy. After repairing the dependency, replay its canonical retained source.
A live lease may need to expire before a replay can claim it. No automated DLT redrive or manual editing of accepted
payloads is provided.

## API

- `GET /api/processed/{tradeId}` returns canonical fields, processingStatus, routeVersion, target partition, first
  source coordinates, output coordinates and timestamps. Internal digest/lease fields are not returned.
- `GET /api/processed?page=0&size=20&symbol=AAPL&side=BUY&processingStatus=COMPLETE` returns
  `{items,page,size,totalElements,totalPages}`. Size 1–100; page nonnegative; sorting createdAt DESC then ID DESC. No
  unbounded collection endpoint.
- `POST /api/process` with `{"tradeId":"UUID"}` reads the retained, immutable, PUBLISHED trader document through a
  read-only source adapter and republishes it to stock-trades. 202 means source enqueue ACK, not completion. Poll GET.
  Already COMPLETE returns 200 without enqueue; unknown source 404; unpublished source 409; bad request 400; unavailable
  dependency 503.

Replay accesses the trader database on the same configured Mongo server. It never updates producer documents or seeks
the active consumer group. Correcting arbitrary malformed bytes and overwriting conflicting trade identities are
deliberately outside this API. Price is a six-decimal string in query responses, preserving JavaScript precision.

## Observability

`/actuator/prometheus`, `/actuator/health/liveness`, `/actuator/health/readiness` are exposed. ECS JSON logs preserve
scoped tradeId/correlationId fields; invalid/missing Kafka correlation IDs receive UUIDs. Metrics have a fixed
application tag and never trade identity/symbol labels:

| Meter                                      | Meaning                                                                                        |
|--------------------------------------------|------------------------------------------------------------------------------------------------|
| trades_consumed_total                      | Listener deliveries, including retries/duplicates; decoding failures occur before the listener |
| trades_processed_total                     | Observed first successful COMPLETE transitions                                                 |
| trades_deduplicated_total                  | COMPLETE redeliveries skipped                                                                  |
| trades_processor_errors_total              | Failed listener attempts                                                                       |
| trades_dlt_total                           | Successfully acknowledged dead-letter sends                                                    |
| trades_processing_seconds_*{outcome}       | Processing histogram for success/failure/duplicate                                             |
| trades_consumer_lag{topic,partition,group} | Broker end minus committed group offset, clamped to zero; first group start uses log start     |
| trades_lag_poll_success                    | Last Admin sampling attempt succeeded: 1/0                                                     |
| trades_lag_last_success_timestamp_seconds  | Last successful sample time                                                                    |

Lag sampling runs every five seconds with bounded Admin calls. Failures retain the last known per-partition values and
expose failed/stale sampling; an absent initial sample is unknown, not zero. Lag counts uncommitted source records,
including retries; DLT recovery can drain lag without a COMPLETE business result. Counters reset with the process and
are not an audit ledger. Lag describes the shared group; if adding replicas, aggregate once per partition/group rather
than summing duplicate global samples from every process.

Grafana now has 20 panels (14 producer + six consumer). Prometheus adds consumer-down, DLT, high-lag and
stale-lag-sampling rules. Local rules are evaluated without external notification delivery. Credentials remain in
ignored infra/.env.

## Tests and verification

```sh
mvn test       # Eight unit/API cases
mvn verify     # Also eight Kafka/Registry/Mongo Testcontainers cases; Docker required
```

All 16 consumer cases pass. Tests cover producer-aligned canonical validation (including Unicode counterparty values),
null correlation headers, retryable source database errors, deterministic side/symbol routing, validation without side
effects, ACK-before-completion, duplicate suppression, exact Avro values/provenance, immutable identity conflicts,
raw-byte DLT, committed-offset progress, refusal to commit while DLT is unavailable, replay, pagination and
expired-owner fencing. The DLT-unavailable test temporarily deletes its isolated test topic, verifies no offset advance,
recreates it and verifies recovery.

Live producer submissions reached Mongo COMPLETE and routed output in Kafdrop (BUY/MSFT=2, SELL/MSFT=5). Query/replay,
health, Prometheus target and committed-lag sampling passed. The provisioned 20-panel dashboard and all query
expressions were checked through authenticated Grafana/Prometheus APIs; visual browser rendering was not verified.

See [ADR 0019](../docs/adr/0019-consumer-processing.md) and
the [exported API](../docs/api/trade-processor.openapi.json).
