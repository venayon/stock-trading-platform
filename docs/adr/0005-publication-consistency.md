# ADR 0005 — Persist intent, publish synchronously, reconcile uncertainty

Date: 2026-10-24 · Status: Superseded original proposal — see [ADR 0017](0017-asynchronous-avro-publication.md)

## Context

The requested POST sequence is validate → persist → publish → 202 with Kafka metadata. MongoDB and Kafka have no shared
atomic commit. An HTTP timeout does not prove that Kafka rejected the event. The repository currently contains no
implementation.

## Decision

Persist one immutable trade document with PENDING publication state and a unique client idempotency key. Acquire a
bounded lease, publish, await broker acknowledgment, then persist PUBLISHED plus coordinates before returning 202.
Same-key completed retries return 200. Failed or ambiguous operations return 503 with a stable trade ID where known. A
bounded reconciler resumes pending work using the same lease protocol.

Accept at-least-once publication. Producer idempotence does not deduplicate application retries across sessions.
Consumers deduplicate trade IDs. See [LLD sections 2–4](../design/LLD.md) for the executable contract and crash windows.

## Options and trade-offs

- **Chosen: synchronous acknowledgment with durable intent.** Preserves the requested response metadata and makes
  recovery inspectable, but adds Kafka latency to the API and cannot avoid duplicates.
- **Runner-up: atomic embedded outbox.** Insert trade and outbox intent together in one Mongo document, return durable
  acceptance, relay asynchronously. Stronger separation of HTTP availability from Kafka, but the first response cannot
  promise Kafka coordinates. A separate outbox collection would require a Mongo replica set and transaction for atomic
  insertion.
- **Rejected: fire-and-forget after save.** A successful HTTP response could conceal an unrecoverable publication
  failure.
- **Rejected: Kafka transactions around Mongo writes.** Kafka transaction atomicity does not extend to MongoDB.

## Consequences and switch trigger

Mongo remains the source of accepted trade intent. Pending records need monitoring and reconciliation. Duplicate
physical events and temporary 503 responses after persistence are intentional documented outcomes. Leases fence Mongo
writes but cannot prevent an old paused sender from publishing.

Switch to the embedded outbox when the owner accepts a response such as 202/PENDING without Kafka metadata, or if broker
outages must not reduce API acceptance availability. This changes the API contract and deserves a superseding ADR.

## Verification before acceptance

Prove same-key concurrency creates one trade; broker timeout retains PENDING; restart recovers it; crash after Kafka ACK
can duplicate an event; completed retries reuse metadata; no record is lost silently. Do not approve a test that asserts
impossible cross-system exactly-once behavior.
