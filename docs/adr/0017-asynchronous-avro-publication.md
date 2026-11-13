# ADR 0017 — Publish Avro with an embedded MongoDB outbox and inspect through Kafdrop

Date: 2026-10-24 · Status: Accepted for the Kafka publication/Kafdrop slice

## Context

The owner requested Kafka publication and Kafdrop after durable PENDING acceptance was implemented. Returning
202/PENDING before publication is now the established API behavior. The original synchronous proposal in ADR 0005 would
change that behavior and couple HTTP availability to the broker.

## Decision

Keep durable acceptance from ADR 0016. The trade document itself holds publication intent and recovery state; there is
no separate collection or cross-document transaction. A scheduled relay atomically claims due PENDING documents with a
unique token and 60-second lease. It sends a generated Avro StockTrade keyed by symbol, awaits Kafka acknowledgment,
then conditionally marks PUBLISHED with topic/partition/offset. Completion and failure updates require the current
token. Expired leases can be reclaimed; legacy PENDING documents without relay fields remain eligible.

Use bounded sends and Registry timeouts, producer idempotence/acks=all and a single-thread, bounded relay batch. Retry
failed attempts indefinitely with backoff capped at 60 seconds. If Mongo is unavailable after ACK, leave recoverable
pending state rather than fabricating completion. A later retry can duplicate an already acknowledged event; consumers
must deduplicate trade IDs. A lease fences Mongo updates, not sends from an old paused process. No per-business-time
ordering or cross-system exactly-once guarantee is claimed.

Generate Java SpecificRecord classes with Avro 1.12.0 in the shared `stock-trade-schema` artifact from
`schemas/StockTrade.avsc`. Preserve LocalDate and BigDecimal types. Use Confluent 7.9.4 serialization with
TopicNameStrategy and explicit pre-registration under `stock-trades-value`; automatic producer registration is disabled.
Set BACKWARD_TRANSITIVE explicitly. Exclude legacy Swagger annotations transitively brought by Confluent to retain
springdoc's Jakarta version; verify the dependency tree and serialization together.

Compose runs one KRaft broker, Schema Registry and Kafdrop alongside existing MongoDB. Bootstrap creates/validates six
source partitions, RF/min ISR 1, seven-day delete retention and schema registration. Persistent Kafka storage also
preserves Registry's schema log. Host/internal Kafka listeners are separate; host ports bind to loopback. Kafdrop
defaults to AVRO values and string keys.

## Alternatives and consequences

The synchronous publish runner-up is the superseded ADR 0005 proposal. Revisit it only if an explicit requirement makes
broker acknowledgment part of HTTP success. A separate outbox collection would require atomic multi-document
coordination and adds no value for this one-document intent; rejected for this local slice. An in-memory queue would
lose accepted work on a crash; rejected. Kafka transactions do not atomically commit MongoDB state.

Acceptance remains available during broker/Registry outages as long as MongoDB is available; publication becomes
eventual. Idempotent retries can observe status changing from PENDING to PUBLISHED. A separate relay process can be
introduced later using the same lease protocol. Pending-age metrics and alerts are still future observability work;
retries currently expose durable attempt/error fields and sanitized logs. A permanently invalid stored record remains
pending for operator repair. Source partition key is symbol, so hot symbols can still skew load.

## Verification

94 unit/API cases and 11 real-container cases passed. Real Kafka/Registry/Mongo tests prove exact Avro values, headers,
acknowledged coordinates, no extra send for completed retries, competing leases, legacy documents, expired-owner fencing
and duplicate physical events in the ACK-before-completion window.

Live Compose checks verified the 202/PENDING → PUBLISHED path, stable retry metadata, and the published trade ID/symbol
in Kafdrop's HTML Avro message view. The broker was stopped, a new trade was accepted and remained pending, and it
published automatically after restart. All long-running containers are healthy. Topic initialization reruns validate
existing configuration. Visual browser rendering was not inspected.

References: [Confluent Docker configuration](https://docs.confluent.io/platform/7.9/installation/docker/config-reference.html), [Kafdrop source/documentation](https://github.com/obsidiandynamics/kafdrop).
