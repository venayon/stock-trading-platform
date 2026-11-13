# Architecture Decision Records

Date: 2026-10-24 · Decisions 0001–0014 record the original proposals; publication proposal 0005 is superseded by ADR
0017. [ADR 0015](0015-validation-and-openapi.md) records the accepted validation slice requested by the owner.

Source: [development prompt](AI_DEVELOPMENT_PROMPT.md). Designs: [HLD](../design/HLD.md), [LLD](../design/LLD.md).
Delivery: [TDD plan](../design/TDD_PLAN.md).

Each numbered row is a compact ADR recording options, decision, rationale, consequences and a revisit condition.
Expanded records cover the two decisions with the largest failure/contract impact.

| ID / decision                                                   | Options considered                                      | Chosen                                                    | Rationale                                                | Consequences                                                        | Runner-up / switch trigger                                                            |
|-----------------------------------------------------------------|---------------------------------------------------------|-----------------------------------------------------------|----------------------------------------------------------|---------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| 0001 Kafka metadata                                             | KRaft; ZooKeeper                                        | Single combined KRaft node                                | Fewer local processes; matches prompt                    | No HA; RF=1 only                                                    | Separate KRaft controllers/brokers when testing quorum failures; no new ZK dependency |
| 0002 Avro distribution                                          | Registry wire format; plain Avro; JSON                  | Confluent Registry + Avro                                 | Shared IDs and compatibility validation                  | Registry availability and serializer dependencies                   | Plain Avro only for disconnected file interchange with explicit schema distribution   |
| 0003 Decimal encoding                                           | bytes logical decimal; double; minor-unit long          | bytes decimal(18,6), BigDecimal                           | Exact representation matches drafted schema              | Scale/range checks and generated logical-type conversion            | Minor-unit long if all domains adopt one immutable scale and a new schema contract    |
| 0004 Source partition key                                       | symbol; tradeId; side                                   | Canonical symbol, six partitions                          | Per-symbol partition locality with useful parallelism    | Hot symbols, no global ordering; repartitioning changes mapping     | tradeId if business accepts relaxed symbol order and skew dominates                   |
| [0005 Publication consistency](0005-publication-consistency.md) | Sync; fire-and-forget; transactional outbox             | Original sync proposal; superseded by ADR 0017            | Keeps response metadata and small initial scope          | Cross-system gap; duplicates; 503 may mean durable acceptance       | Atomic embedded outbox when API can return 202 before publication metadata            |
| 0006 Consumer failure recovery                                  | Block forever; skip; retry + DLT                        | 3 retries then ACK-verified DLT; terminal failures direct | Bounded failure handling and inspectable evidence        | Ordering gaps; DLT failure blocks offset progress                   | Indefinite pause if strict per-key sequencing becomes a requirement                   |
| [0007 Partition routing](0007-processor-routing.md)             | Reproduce; filter; assign-only                          | Explicit side/symbol partition on second topic            | Actually creates independently inspectable routed output | Extra topic/send; per-side order only                               | Symbol-only processed routing if cross-side order is required                         |
| 0008 Load generation                                            | Playwright; k6; JMeter                                  | Playwright JS APIRequestContext                           | Required stack and reusable REST assertions              | Closed-loop load, not accurate arrival-rate control                 | k6 for fixed-rate load models and richer load execution                               |
| 0009 Grafana provisioning                                       | Files; UI edits; API bootstrap                          | Datasource/provider/dashboard files                       | Repeatable fresh startup and reviewable diffs            | UI edits must be exported/reviewed                                  | API provisioning for large dynamic environments                                       |
| 0010 Mongo separation                                           | One shared collection; separate DBs; separate servers   | One server, trader/processor databases                    | Clear ownership with low laptop cost                     | Shared server failure/resource domain; replay reads source DB       | Separate servers if failure isolation becomes a learning requirement                  |
| 0011 Schema compatibility                                       | BACKWARD_TRANSITIVE; FULL_TRANSITIVE; none              | TopicNameStrategy + BACKWARD_TRANSITIVE                   | New consumers read all retained schema versions          | Consumer-first upgrades; old readers not universally protected      | FULL_TRANSITIVE for mandatory mixed-version rolling readers                           |
| 0012 Delivery guarantees                                        | At-most-once; at-least-once; cross-system exactly-once  | At-least-once + trade-ID dedup                            | Honest about Mongo/Kafka failure boundaries              | Duplicate physical output is accepted                               | Kafka-only transactions if state moves entirely to Kafka and requirements justify it  |
| 0013 Idempotency                                                | Optional keys; generated ID only; required client key   | Required Idempotency-Key + canonical payload digest       | Safely resolves client timeout and duplicate POST        | Key storage lifetime matches trades; caller responsibility          | Optional key only if duplicate trade creation becomes explicitly acceptable           |
| 0014 Local dependencies                                         | Latest tags; pinned historical baseline; cloud services | Exact local versions then verified digests                | Reproducible/offline laptop stack                        | Cache artifacts; review old family support and dependency overrides | New supported families when constraints permit upgrade and contract suite passes      |

## Decision lifecycle

Proposed → Accepted after review → Superseded by a new numbered record when behavior changes. Preserve accepted history
and link successor records. Mark rejected options explicitly rather than deleting review context. Version changes must
carry compatibility-test evidence; a successful image pull alone is insufficient.

## Implementation decision

[0015 — Validation and OpenAPI](0015-validation-and-openapi.md): server enforcement, client feedback, exact numeric
handling, and a preview endpoint that does not imply trade acceptance.

[0016 — MongoDB acceptance and idempotency](0016-mongodb-acceptance-idempotency.md): durable PENDING acceptance,
unique-key races, same-payload retries and explicit HTTP semantics before Kafka exists. This record takes precedence
over the original proposed 202-after-Kafka-ACK contract for the implemented slice.

[0017 — Asynchronous Avro publication](0017-asynchronous-avro-publication.md): embedded outbox, bounded retries,
token-fenced leases, Schema Registry, and Kafdrop. This is the implemented publication decision and supersedes the
original synchronous response proposal.

[0018 — Producer observability](0018-producer-observability.md): process-aware counters, durable-backlog sampling,
health probes, ECS correlation logs, provisioned Grafana and locally evaluated Prometheus alerts.

[0019 — Consumer processing](0019-consumer-processing.md): SpecificRecord consumption, Mongo deduplication/leases,
explicit routing, ACK-verified DLT and committed lag. Implements the second-topic routing proposal in ADR 0007.

[0020 — Playwright capacity testing](0020-playwright-capacity-tests.md): API contracts, fixed-arrival load, latency and
reconciliation gates, CPU/RSS measurement and bounded sizing claims.

[0021 — Scheduled feed producer](0021-scheduled-feed-producer.md): immutable CSV feeds, whole-feed validation, JDBC
Batch metadata, checkpointed ingestion and shared Mongo/Kafka acceptance.
