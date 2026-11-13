# Collaborative TDD plan

Date: 2026-10-24 · Status: Validation, MongoDB acceptance and asynchronous Avro publication implemented; producer
observability implemented; load/processor cycles remain proposed

Read [HLD](HLD.md), [LLD](LLD.md), and [ADRs](../adr/README.md) first. The prompt asks for design review before
implementation. This document prepares that review and our subsequent work together.

## Working agreement

Choose one behavior, agree its input/output and failure semantics, write one meaningful failing test, implement the
minimum to pass, then refactor while green. Review the behavior and test result together before taking the next slice.
Avoid generating the whole system or a large speculative test suite in advance. Preserve unit feedback speed; add real
dependency tests where mocks cannot establish correctness.

No performance threshold is invented as an acceptance requirement. First establish correctness and a measured baseline.
Do not mark a planned test as executed.

## Preparation cycle

Resolve the HLD review questions and mark ADRs accepted or revised. Agree the proposed infrastructure verification
exception to the pre-code checklist. Bootstrap pinned build dependencies and verify Java 21, schema generation and Avro
1.12/serializer interoperability against real Kafka/Registry/Mongo containers. Produce explicit topic/schema bootstrap
and monitoring files, then verify fresh-start and restart behavior. This supplies the runtime evidence required by the
prompt's checklist before application TDD.

## Iteration 1 slices

| Cycle | First red test / behavior                                                                           | Minimum green change                              | Verification                                                     |
|-------|-----------------------------------------------------------------------------------------------------|---------------------------------------------------|------------------------------------------------------------------|
| 1     | Canonical trade rejects invalid quantity/scale and accepts exact decimal/date                       | Domain request/value validation                   | Unit, boundary and normalization cases                           |
| 2     | POST valid request delegates and returns documented envelope; invalid request returns ProblemDetail | Controller DTOs and advice, fake service          | MVC slice; no Mongo/Kafka needed yet                             |
| 3     | Same-key concurrent requests produce one immutable trade; changed payload conflicts                 | Mongo document, indexes and atomic create-or-read | Real Mongo Testcontainer; canonical hash cases                   |
| 4     | StockTrade retains exact price/date through Registry serialization                                  | Schema module, mapper, serializers                | Kafka + Registry round-trip; previous writer fixture             |
| 5     | PUBLISHED occurs only after broker ACK and persisted metadata                                       | Service publication orchestration                 | Orchestration unit test plus real broker integration             |
| 6     | Kafka failure leaves visible pending trade; completed retry does not resend                         | Error mapping and same-key retry path             | Broker outage, duplicate/concurrent retries                      |
| 7     | Restart after save/ACK windows recovers pending work                                                | Leases and bounded reconciler                     | Persisted-state restart/fault-injection tests; permit duplicates |
| 8     | GET returns 404 or bounded sorted results                                                           | Query endpoints/indexes                           | MVC contract and repository integration                          |
| 9     | Correlation header reaches Kafka; metrics expose stable names                                       | Logging/context/Actuator meters                   | Header assertions, scrape endpoint, bounded labels               |
| 10    | Fresh Compose stack accepts and exposes one inspectable trade                                       | Compose wiring and Playwright smoke               | Full path, Kafdrop checklist, monitoring targets                 |
| 11    | Bounded worker run reconciles unique IDs and reports latency/errors                                 | Playwright fixtures/report/reconciliation         | Small deterministic run before larger load                       |

For cycle 1, the first candidate test is: `quantity=0` produces a validation error and performs no
persistence/publication. Add the next boundary only after this behavior is green. We will write the test together when
implementation begins.

## Iteration 1 exit evidence

- Reviewed REST/schema contract, accepted publication ADR and six-partition topic configuration.
- Successful 202 trace with matching Mongo state and Kafka coordinates.
- Failure/restart trace demonstrating pending recovery and documented duplicates.
- Generated schema artifact consumed by the producer; round-trip compatibility suite passes.
- Offline-after-caching startup, persistent restart, Prometheus scrape and provisioned dashboard evidence.
- Playwright report separating accepted unique trades, retries, failures and physical events.

## Iteration 2 slices

| Cycle | First red test / behavior                                                      | Minimum green change                                                 | Verification                                           |
|-------|--------------------------------------------------------------------------------|----------------------------------------------------------------------|--------------------------------------------------------|
| 12    | Router gives AAPL BUY=1/SELL=4 and remains deterministic for all fixtures      | Pure route-v1 function and partition validation                      | Unit including negative hashes; no broker              |
| 13    | One valid source record becomes one queryable COMPLETE document                | Listener, persistence, output send and completion                    | Real Kafka/Registry/Mongo; assert output partition     |
| 14    | COMPLETE duplicate skips send; conflicting payload is terminal                 | Digest check/dedup state handling                                    | Duplicate/restart integrations                         |
| 15    | Output failure blocks normal commit; output ACK/crash can duplicate            | Pending lease/recovery and record ack policy                         | Read committed group offsets, simulate failure windows |
| 16    | Poison bytes go to same-partition DLT; failed DLT send does not advance offset | ErrorHandlingDeserializer, typed recovery serializers, error handler | Real malformed values/keys and DLT failure             |
| 17    | Manual replay enqueues canonical data; COMPLETE replay is a no-op              | Replay endpoint and source read adapter                              | REST-to-Kafka integration; 404/409/503 cases           |
| 18    | Committed lag grows with stopped processing and drains after recovery          | Admin lag sampler and dashboard panels                               | Real group offsets, missing-group/stale-poll tests     |
| 19    | End-to-end workload reconciles complete unique trades and routing              | Extend Playwright verification                                       | Compose run, restart, replay and DLT inspection        |

## Test execution and reporting

Planned commands after build files exist: `mvn test` for unit/MVC tests, `mvn verify` for Failsafe `*IT` dependency
tests, `docker compose config` for config validation, `docker compose up --build --wait` for startup, and `npm ci` /
`npx playwright test` in load-generator. These commands are not runnable acceptance evidence in the current skeleton.
Pin Testcontainers versions and images; tests must not fetch floating images silently.

Keep failure injection deterministic using controllable publisher/repository seams for unit tests, plus selected
integration tests that stop a dependency or terminate a process at a known boundary. Testcontainers requires a reachable
Docker daemon. Record command, versions, outcome and relevant artifact paths for each completed cycle. Refactor only
after required checks pass; broaden tests when a new risk or failure justifies it.

## Progress — 2026-10-24

Owner requested completing validation as one batch and adding client feedback plus Swagger. Service validation now
covers all trade fields and normalization; a side-effect-free `/api/trades/validate` endpoint and browser form are
implemented. This extends cycle 1 with a preview API; trade acceptance from cycles 2 onward still needs its own
implementation. Java verification: 64 cases pass. Client verification: 28 cases pass.
See [ADR 0015](../adr/0015-validation-and-openapi.md) and the [service README](../../ms_trader/README.md).

## Acceptance progress — 2026-10-24

Implemented POST acceptance and GET by ID with MongoDB, a unique binary idempotency index, canonical payload digests and
explicit PENDING responses. Same-key normalized retries return the stored trade; changed payloads conflict. This
completes durable intent and key handling from cycles 2–3 and the by-ID portion of cycle 8; cycle 5's Kafka
acknowledgment behavior and list queries remain unimplemented. [ADR 0016](../adr/0016-mongodb-acceptance-idempotency.md)
records the current HTTP contract.

Verification: 89 unit/API cases, six real MongoDB integration cases and 28 client cases pass. Real tests cover 12-way
concurrency, competing payloads, fresh clients, exact BSON types and index setup refusal. Live packaged API checks
passed, including a retry after actual application restart. Next: agree the publication response contract, then add
schema generation, Avro round-trip tests and Kafka publication/recovery.

## Publication progress — 2026-10-24

Implemented the shared schema artifact, SpecificRecord mapper, Kafka publisher, token-fenced Mongo relay and
KRaft/Registry/Kafdrop Compose services. [ADR 0017](../adr/0017-asynchronous-avro-publication.md) preserves 202/PENDING
acceptance and makes publication asynchronous. Original cycle 5's synchronous HTTP requirement is superseded; GET
exposes acknowledged event coordinates later. Publication/recovery work in cycles 4–7 is covered by the new tests.
Metrics and broad load verification remain open.

Verification: 94 unit/API cases plus 11 real-container integration cases pass. The relay tests were red before
implementation. Live checks confirmed Kafdrop's HTML Avro view and recovery of a trade accepted during an actual broker
outage. A deliberate ACK-before-completion test demonstrates duplicate physical events; consumers must deduplicate by
trade ID. All local infrastructure containers are healthy and topic bootstrap reruns validate existing settings.

## Observability progress — 2026-10-24

Implemented Actuator/Prometheus meters, background Mongo backlog sampling, scoped ECS identity logs, dependency-aware
readiness and liveness, and file-provisioned Prometheus/Grafana with 14 panels and five alert
rules. [ADR 0018](../adr/0018-producer-observability.md) and the [runbook](OBSERVABILITY.md) define semantics and
operation. Consumer lag waits for a real processor/group.

104 unit/API and 12 real-container cases pass; Promtool config/rule tests pass. Live scraping, counter increments,
probes, Grafana provisioning and all dashboard queries were verified. A test-log inspection found duplicate correlation
fields; a regression test now protects the corrected scoped logging. Load generation and consumer work remain open.

## Consumer progress — 2026-10-24

Implemented SpecificRecord consumption, processing identity/leases, explicit side+symbol output routing, queries, source
replay, retries and ACK-verified DLT, plus consumer metrics/lag and dashboard
integration. [ADR 0019](../adr/0019-consumer-processing.md) records semantics. Eight unit/API cases and eight
real-container cases pass, including a DLT-unavailable offset test and stale-token fencing. Live trader-to-routed-output
checks and all dashboard queries pass. A general Playwright load generator and broader load/rebalance validation remain
future work.

## API and performance progress — 2026-10-24

Implemented Playwright API contracts and a configurable fixed-arrival performance harness with full accepted-ID
reconciliation, sampled completion latency, queue-growth gates, app CPU/RSS and Docker telemetry. 22 API and seven
analyzer cases pass. A 10 RPS regression profile passes; the 20 RPS exploration fails completion/backlog gates and
remains a recorded capacity finding. See [ADR 0020](../adr/0020-playwright-capacity-tests.md) and
the [measured baseline](../performance/BASELINE_2026-10-24.md). Constrained-resource validation, longer soaks and
broader workload matrices remain explicit follow-up experiments.
