# Stock Trading Platform design

The architecture documents cover both planned iterations. Validation and durable MongoDB acceptance with idempotency are
implemented in `ms_trader`; asynchronous Avro publication, the deduplicating/routing consumer, Kafdrop, Prometheus and
Grafana are implemented.

- [Kafka concepts and presentation guide](guides/KAFKA_CONCEPTS.md)
- [Kafka interview preparation](guides/KAFKA_INTERVIEW.md)
- [Development requirements](adr/AI_DEVELOPMENT_PROMPT.md)
- [High-level design: both iterations and review checklist](design/HLD.md)
- [Low-level design: API, storage, state machines and failure contracts](design/LLD.md)
- [Architecture Decision Record register](adr/README.md)
- [Collaborative TDD sequence](design/TDD_PLAN.md)
- [API/performance test instructions](../load-generator/README.md)
- [Measured latency and sizing baseline](performance/BASELINE_2026-10-24.md)
- [Observability runbook](design/OBSERVABILITY.md)
- [Processor API OpenAPI export](api/trade-processor.openapi.json)
- [Trade API OpenAPI export](api/stock-trader.openapi.json)

ADRs 0001–0014 capture the original proposals; ADR 0005 is superseded by ADR
0017. [ADR 0015](adr/0015-validation-and-openapi.md) records
validation; [ADR 0016](adr/0016-mongodb-acceptance-idempotency.md) records durable
acceptance; [ADR 0017](adr/0017-asynchronous-avro-publication.md) records Kafka publication and Kafdrop. See
the [service README](../ms_trader/README.md) for local form/Swagger URLs and verification results.

[ADR 0018](adr/0018-producer-observability.md) records the implemented producer metrics, health, correlated logs and
monitoring stack.

[ADR 0019](adr/0019-consumer-processing.md) records the consumer, routing, DLT and replay implementation.

[ADR 0020](adr/0020-playwright-capacity-tests.md) records the Playwright API and resource/latency capacity methodology.

[Spring Batch feed producer guide and mindmap](guides/SPRING_BATCH_FEED.md) explains scheduled and REST-triggered huge
CSV ingestion with PostgreSQL job tables, MongoDB trades, Kafka publication and restart behavior. See
its [HLD/LLD](design/FEED_PRODUCER.md) and [ADR 0021](adr/0021-scheduled-feed-producer.md).
