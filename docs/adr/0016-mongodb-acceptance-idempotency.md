# ADR 0016 — Durable PENDING acceptance with MongoDB idempotency

Date: 2026-10-24 · Status: Accepted for the requested acceptance slice

> Later implementation: [ADR 0017](0017-asynchronous-avro-publication.md) adds asynchronous Avro publication while
> retaining this durable-acceptance contract. Statements below about publication not existing describe the acceptance-only
> slice.

## Context

The owner requested implementing acceptance with MongoDB and idempotency, following the explicitly proposed behavior: a
valid submission persists one PENDING trade with a generated ID, and the same key creates no duplicate. Validation and
Swagger already exist; Kafka publication has not been implemented.

The original HLD/LLD and proposed ADR 0005 describe returning 202 only after Kafka acknowledgment with broker metadata.
That behavior cannot truthfully describe this slice. This record takes precedence for the implemented acceptance API. It
does not mark the full original architecture approved or claim publication is implemented.

## Decision

`POST /api/trades` requires exactly one client `Idempotency-Key` and validates/canonicalizes input before storage.
Insert one immutable PENDING document with generated UUID, versioned SHA-256 canonical payload digest, timestamp and
original correlation context. Await majority/journaled MongoDB acknowledgment, then return 202 with trade ID/state and a
queryable Location. No event metadata is returned. `GET /api/trades/{id}` reads stored state.

Create and verify a unique, binary-collation key index before enabling acceptance. Use insert-first, catch a
duplicate-key race, read the key's winning document from the primary and compare its digest. Same key and digest returns
200 with the original ID/timestamp and `Idempotent-Replay: true`; changed digest returns 409 without an update. An
unrelated unique-key violation remains a storage failure. Keys retain exact printable ASCII spelling and have no
independent expiry.

A storage exception or ambiguous acknowledgment returns 503 and instructs retry with the same key. Do not infer from a
timeout that nothing was persisted. Persisted state survives application restarts. Store exact Decimal128 prices, Int64
quantities and ISO dates; the entire mutation is one Mongo document, so standalone MongoDB is sufficient. Use a named
Compose volume and bounded driver timeouts.

## Alternatives and consequences

A preliminary find-then-insert check cannot enforce uniqueness across competing processes; rejected. JVM locks are
process-local; rejected as the guarantee. Upserting the request would risk overwriting an existing immutable trade;
rejected. MongoDB's unique index provides the actual concurrency boundary. Test doubles do not establish this guarantee;
real integration tests do.

The runner-up is the original synchronous Kafka-ACK contract, revisited when the publisher is implemented. If durable
acceptance remains the API contract, add a persistent relay/outbox mechanism and expose eventual publication state; do
not silently upgrade the meaning of 202 or invent Kafka coordinates. Pending trades currently remain pending
indefinitely because no publisher/worker exists yet. This limitation is visible in Swagger and service documentation.

The browser form remains a side-effect-free validation preview; acceptance is available through Swagger or a direct
client. List/update/delete APIs, authentication, Kafka and recovery workers are separate slices.

## Verification

89 unit/API cases and six real MongoDB integration cases pass. Tests establish concurrent same-key uniqueness,
differing-payload conflicts, restart with a fresh client/repository, exact BSON boundaries, primary-key collision
handling and refusal of an incompatible index. A service fault test simulates lost acknowledgment after persistence.
Live HTTP checks confirm acceptance, retry, conflict and lookup, followed by the same-ID retry after an actual
application restart. Existing 28 client cases pass; Compose validates and MongoDB is healthy.
