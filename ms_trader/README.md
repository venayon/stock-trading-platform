# ms_trader

Java 21 / Spring Boot 3.5.16 service with trade validation, MongoDB acceptance, idempotency, lookup and Swagger UI. A
durable relay publishes accepted trades asynchronously as Avro events to Kafka. Acceptance returns PENDING; GET shows
PUBLISHED and Kafka coordinates after acknowledgment.

## Run locally

Start Docker Desktop, select JDK 21, then:

```sh
# From infra/
python3 scripts/init-monitoring-env.py
docker compose up -d --wait

# From schemas/
mvn install

# From ms_trader/
mvn verify
java -jar target/ms-trader-0.0.1-SNAPSHOT.jar
```

The application fails startup if MongoDB is unavailable or the unique idempotency index cannot be established. The
default database is `trader` on localhost:27017. `MONGODB_URI` overrides the URI; `SERVER_PORT` defaults to 8081.
Default connection/selection/socket timeouts are bounded. Writes explicitly require majority acknowledgment and
journaling; idempotency lookups read the primary.

Compose runs MongoDB 7.0.16 bound to loopback, with named volume `stock-trading_mongo-data`. Stopping/restarting
containers preserves trades; avoid deleting that volume if you want to retain data. Kafka, Schema Registry and Kafdrop
are included. Prometheus and provisioned Grafana are included.

- Grafana: http://localhost:3000/d/stock-trading-overview (admin; password in ignored infra/.env)
- Prometheus: http://localhost:9090/targets
- Metrics: http://localhost:8081/actuator/prometheus
- Kafdrop: http://localhost:9000/topic/stock-trades
- Schema Registry: http://localhost:8085
- Swagger UI: http://localhost:8081/swagger-ui.html
- OpenAPI JSON / YAML: http://localhost:8081/v3/api-docs / http://localhost:8081/v3/api-docs.yaml
- Exported contract: [stock-trader.openapi.json](../docs/api/stock-trader.openapi.json)
- Browser validation preview: http://localhost:8081/ (validates only; use Swagger or the API to accept a trade)

## Accept and retrieve a trade

```sh
curl -i http://localhost:8081/api/trades \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-trade' \
  -d '{"tradeDate":"2026-10-24","side":"BUY","symbol":" aapl ","quantity":10,"price":"123.450000"}'
```

A fresh accepted trade returns HTTP 202 with `tradeId`, `publicationStatus: PENDING`, canonical trade fields and
`createdAt`. `Location` points to `GET /api/trades/{tradeId}`. Poll that URL for `publicationStatus: PUBLISHED` and
`event: {topic, partition, offset}`. A fresh 202 guarantees durable intent, not completed publication.

| Request outcome                                               | Response                                                                     |
|---------------------------------------------------------------|------------------------------------------------------------------------------|
| Fresh valid key and payload                                   | 202; one durably acknowledged PENDING document                               |
| Same key, same canonical payload                              | 200; same ID/timestamp, current publication state; `Idempotent-Replay: true` |
| Same key, different canonical payload                         | 409 `IDEMPOTENCY_CONFLICT`; stored trade is unchanged                        |
| Missing/multiple/invalid key, invalid trade or malformed JSON | 400; no storage call                                                         |
| Storage unavailable or write result uncertain                 | 503 with `Retry-After: 2`; retry the same key and payload                    |
| GET valid known ID / unknown ID / malformed ID                | 200 / 404 / 400                                                              |

Supply exactly one `Idempotency-Key`: 1–128 printable ASCII characters, not blank. Keys are case-sensitive and are not
trimmed. Keep the same key for every retry after a timeout or 503; MongoDB might already have accepted the first
attempt. A different key intentionally creates another trade. Keys have no TTL and remain associated with stored trades
for their lifetime.

Idempotency uses a unique binary MongoDB index and an insert-first operation. A duplicate-key race loads the winning
document and compares a SHA-256 digest of versioned, ordered canonical JSON. All seven trade fields are hashed,
including explicit null counterparty; generated IDs, timestamps, correlation IDs and the key itself are excluded.
Canonically equivalent symbols/prices/blank counterparties therefore replay safely. A new application instance does not
reset idempotency. There is no JVM-only lock and no multi-document transaction requirement.

MongoDB stores price as Decimal128, quantity as Int64, date as an ISO string (including the full Avro epoch-day range),
and timestamps as BSON dates. It stores original correlation context but does not return internal keys/hashes in API
responses. No delete/update/list API exists in this slice.

## Kafka publication and recovery

The relay scans up to 20 due trades per poll, one at a time, every second. Each atomic Mongo claim assigns a unique
token and a 60-second lease. The relay maps the canonical trade to the shared generated StockTrade record, keys the
Kafka message by symbol, and waits for acknowledgment before conditionally persisting PUBLISHED and its coordinates.
Competing relays cannot claim the same live lease; stale tokens cannot overwrite a newer completion. Older PENDING
documents without relay fields are recovered automatically.

Failures remain PENDING with `lastPublicationError: PUBLICATION_FAILED`, an incremented attempt count and exponential
retry delays from 2 to 60 seconds. If the failure-state write itself fails, lease expiry permits recovery. There is no
retry limit or producer DLT in this slice; inspect old pending documents and logs. `PUBLICATION_ENABLED=false` disables
the relay deliberately.

This embedded outbox provides at-least-once delivery, not atomic MongoDB/Kafka exactly-once delivery. A crash after
Kafka ACK but before Mongo completion can duplicate a physical event. Consumers must deduplicate by trade ID. Normal
HTTP idempotent retries do not requeue PUBLISHED records. Changes in publication status/metadata are expected between
repeated HTTP responses.

Host configuration: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `SCHEMA_REGISTRY_URL=http://localhost:8085`. Producer
configuration explicitly sets `acks=all`, idempotence, max in-flight 5 and bounded metadata/delivery/Registry timeouts.
Source topic must have six partitions; wrong topology leaves publication pending. The standalone broker provides no
redundancy.

The shared schema artifact uses Avro 1.12.0, Confluent serializer 7.9.4 and Boot-managed Kafka clients. Schema Registry
uses TopicNameStrategy (`stock-trades-value`), BACKWARD_TRANSITIVE, and explicit bootstrap registration; producer
automatic registration is off. The Confluent dependency's legacy Swagger annotation artifact is excluded so springdoc's
Jakarta annotation implementation remains authoritative; the dependency tree and real serializer round-trip were
verified.

Kafka headers include tradeId, X-Correlation-ID and schemaVersion=1 (the event contract version, not a Registry schema
ID). Keys are canonical symbols. See [infra instructions](../infra/README.md) for Kafdrop inspection.

## Validation preview and rules

`POST /api/trades/validate` returns a normalized preview without persistence or publication. Both endpoints share the
validator and strict JSON parsing. ProblemDetail errors include stable codes, field errors where available and a
correlation ID. Valid UUID `X-Correlation-ID` is echoed; missing/invalid values are replaced.

| Field        | Validation and canonical form                                                                                              |
|--------------|----------------------------------------------------------------------------------------------------------------------------|
| tradeDate    | Required strict ISO LocalDate within signed 32-bit epoch-day range; no market-calendar rule                                |
| side         | Exactly BUY or SELL                                                                                                        |
| symbol       | Trim and uppercase using Locale.ROOT, then `[A-Z0-9][A-Z0-9.-]{0,15}`                                                      |
| quantity     | Required JSON integer, 1 to 9223372036854775807; no fractions, string coercion or overflow                                 |
| price        | Required, positive, at most 999999999999.999999; representable at scale six without rounding; extra trailing zeros allowed |
| currency     | USD, GBP or EUR; omitted defaults to USD; explicit null rejected                                                           |
| counterparty | Optional/null; trim, blank → null; maximum 128 UTF-16 code units after trimming                                            |

Price accepts a plain decimal string or JSON number; exponent strings are rejected. Responses use a six-decimal string.
Unknown properties, duplicate JSON keys, trailing JSON values, invalid dates and wrong value types are rejected. Client
checks improve feedback; the server enforces the rules independently. JavaScript clients should avoid Number conversion
of prices or large quantities; the preview form uses string/BigInt arithmetic.

## Tests and evidence

```sh
mvn test                                      # Unit/API tests; MongoDB mocked at the HTTP boundary
mvn verify                                    # Also runs real MongoDB Testcontainers integration tests
node --test src/test/js/validation.test.mjs     # Client validation tests
```

Integration tests require a reachable Docker daemon and pull pinned `mongo:7.0.16` plus Testcontainers support images on
first use. `mvn -o verify` works once Maven dependencies and images are cached. Missing Docker fails integration
verification rather than silently skipping it. Mockito runs as a startup agent to support Spring test listeners without
dynamic attachment.

Verified on Java 21: **104 unit/API cases + 12 real-container integration cases**, all passing. The existing 28 client
cases passed in earlier slices; frontend code is unchanged. Tests include exact Avro values and headers, acknowledged
coordinates, leases/recovery/duplicates, Mongo idempotency, instrumentation semantics, real backlog aggregation, health
dependency behavior and JSON correlation cleanup.

Live verification passed for acceptance/publication and broker recovery in the publication slice. Observability checks
additionally confirm target UP, live business counters, healthy probes, authenticated Grafana datasource/dashboard
provisioning, all 14 panel query expressions and correlated JSON business events. Prometheus configuration and
alert-rule fixture tests pass. No visual browser inspection was available.

See the [observability runbook](../docs/design/OBSERVABILITY.md) for metric meanings, alerts, dashboards, scrape
routing, credentials and troubleshooting. Only health/info/prometheus are exposed; env and other administrative
endpoints remain unexposed. Readiness requires MongoDB; liveness is independent of dependency health. Process counters
are observations, not durable business totals.

[ADR 0017](../docs/adr/0017-asynchronous-avro-publication.md) records the asynchronous embedded-outbox design. This
replaces the original proposed HTTP-after-Kafka-ACK flow and preserves the implemented durable acceptance contract from
ADR 0016.
