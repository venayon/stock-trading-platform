# ADR 0021 — Scheduled CSV producer with JDBC Batch metadata

Date: 2026-10-24. Status: Accepted for the user-requested implementation.

## Context

Large BUY/SELL feeds must be scanned every two hours or on demand, with configurable chunks and acceptable invalid rows.
The user confirmed CSV columns `tradeDate,side,symbol,quantity,price,currency,counterparty` and whole-feed validation
before trades: tolerate and report invalid rows within policy; exceeding policy rejects the feed before persistence or
publication. Business trades must use the same trader MongoDB database and topic. Job tables must use lightweight
PostgreSQL.

## Decision

Create an independent Java 21 / Boot 3.5.16 / Batch 5.2.6 JVM under `ms_trader/feed-producer`, port 8083. Attach a plain
`core` classifier to the existing trader jar and depend on its canonical validation, Mongo idempotency and token-fenced
outbox publisher. Do not scan the trader's REST application package in the new JVM.

Use `postgres:16.6-alpine` on host 5433 for the JDBC JobRepository, with a named volume, 32 MiB shared buffers, 20
server connections and a 256 MiB demo cap. Secrets live in ignored `.env` or environment. A PostgreSQL session advisory
lock serializes scans across instances. Timer and REST share a bounded asynchronous worker and a synchronous Batch
launcher inside that worker.

Require immutable ready-marked files and same-filesystem atomic claiming. Stream SHA-256, validate the entire file
without business writes, verify the hash, then ingest checkpointed chunks. Reject over the inclusive absolute count or
final percentage limit; require at least one valid row. Use a bounded physical-line reader; multiline CSV fields are
unsupported. Reports contain row numbers and error codes.

A JobInstance is content hash + explicit policy version. A row idempotency key is content hash + physical row number.
PostgreSQL chunk commits and MongoDB acceptance are independent; deterministic keys make replay safe. Kafka uses the
existing durable outbox and at-least-once contract. Technical ingestion failure does not undo already accepted rows.
COMPLETED means ingestion completed, with Kafka publication state exposed separately.

## Alternatives and switch triggers

| Alternative                             | Why not selected                                               | Revisit when                                                                                    |
|-----------------------------------------|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| Plain scheduled loop without Batch      | No durable step history/checkpoints                            | Work is tiny and restart/history are unnecessary                                                |
| HTTP forwarding every row to trader     | Adds per-row network overhead and couples availability         | Independent ownership/security requires the trader API boundary                                 |
| Duplicate validation and publisher code | Risks incompatible business and durability rules               | Never without an explicit distinct domain contract                                              |
| Durable staging of every row            | More storage/cleanup and release protocol                      | Multiple sequential passes dominate I/O or technically interrupted feeds need atomic visibility |
| One-pass persist with skip limit        | A late rejection can follow earlier published trades           | Requirement explicitly allows partial feed acceptance                                           |
| H2 production metadata                  | User requested PostgreSQL; cross-process coordination needed   | Isolated ephemeral teaching mode only; H2 is used in job tests                                  |
| Partition one file across workers       | Adds ordering, checkpoint and whole-feed validation complexity | Representative benchmarks justify parallel ingestion                                            |

## Consequences and limits

Memory is bounded by record and chunk limits, while successful ingestion makes four sequential file passes including
ingestion. Restart seeks ingestion checkpoints, but hash verification still reads the whole file. Files, reports and job
history need operator retention policies. Same content under a different filename deduplicates; different content with
overlapping trades does not. Changing policy reevaluates content but preserves row keys.

Hard crashes can leave STARTED metadata. Do not steal those executions automatically; an operator must prove the worker
is gone and repair execution state before restarting. PostgreSQL/MongoDB/Kafka/filesystem state is not a single
transaction. The attached core dependency introduces build order (schema, trader install, feed app build); extraction
into a dedicated shared library is a future option if more producers appear.

See [HLD/LLD](../design/FEED_PRODUCER.md) and [beginner guide/mindmap](../guides/SPRING_BATCH_FEED.md) for contracts,
table roles, settings, commands and verification.
