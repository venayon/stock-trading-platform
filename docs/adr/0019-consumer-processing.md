# ADR 0019 — Consume, deduplicate and route trades with acknowledged DLT recovery

Date: 2026-10-24 · Status: Accepted for the requested consumer implementation

## Decision

Implement Java 21 / Boot 3.5.16 ms_trade_processor on port 8082, consuming SpecificRecord StockTrade in group
stock-trade-processor-v1. Use three record listeners, explicit auto-commit=false and RECORD acknowledgment. This
implements the second-topic routing approach from ADR 0007; filtering assigned source records is not routing and remains
rejected.

MongoDB's processor.processed_trades collection owns immutable input identity and processing state, separate from trader
data. Insert/read by unique trade ID, compare a versioned canonical Avro digest, acquire a token-fenced 60-second lease,
publish to an explicit processed partition, persist COMPLETE with output coordinates, then permit source commit.
COMPLETE duplicates skip sending; same ID/different payload is terminal. Expired-owner updates cannot overwrite a newer
owner.

Route version 1 uses floorMod (Java symbol.hashCode (),3), plus zero for BUY and three for SELL. Exactly six target
partitions are required. Preserve symbol key and StockTrade value, add source/correlation/route headers. Output ordering
is per symbol+side; no cross-side/global business-time ordering is promised. Changing routing/partition count or the
digest encoding requires an explicit migration.

Retry transient failures three times at one-second intervals after initial delivery; terminal validation/deserialization
failures recover directly. Publish to stock-trades-dlt on the source partition and verify the send result. Acknowledged
DLT recovery can advance the offset; failed recovery throws and retains it. Use delegated serializers to preserve raw
poison bytes alongside valid Avro records. Register valid Avro subjects for source, processed and DLT topics; raw DLT
bytes remain raw. Exclude exception message/stack headers while retaining provenance and exception type.

Expose bounded query APIs and POST /api/process for canonical source replay. A completed trade replay is a no-op. Other
replays read only a retained PUBLISHED source on the same Mongo server, republish to the source topic, return enqueue
acknowledgment and never seek active consumer offsets. No arbitrary malformed-byte repair or identity overwrite is
implemented.

## Guarantees and trade-offs

This is at-least-once processing with deduplicated logical Mongo results. Output ACK followed by crash/completion
uncertainty can duplicate output. Kafka transactions would not make MongoDB atomic, so they are not used to claim
exactly-once effects. Another consumer must deduplicate trade IDs downstream.

A live-lease collision is retryable, and lease expiry recovers a crashed owner. Bounded retries plus DLT prioritize
visible isolation; DLT creates ordering gaps and can contain valid events after infrastructure failure. Strict
sequencing would require pausing/recovering the partition instead, a different requirement. Offline retained-source
replay is bounded by producer document retention and availability.

Metrics distinguish delivery attempts, completed trades, duplicates, listener failures and acknowledged DLT sends. Admin
polling measures committed group lag rather than fetched-record lag; failures retain last values with explicit freshness
signals. Add the processor scrape target, six dashboard panels and four local alerts. No metric label uses a trade ID or
symbol. API readiness currently requires MongoDB; Kafka/group availability is assessed separately via lag sampling and
listener logs.

## Evidence

Eight unit/API cases and eight real Kafka/Schema Registry/Mongo cases pass. The router test was red before
implementation. Tests establish exact values/provenance, completion and source commit, no second output for completed
duplicates, payload-conflict isolation, raw-byte preservation, stale-lease fencing, replay and bounded queries.
Additional checks cover null correlation headers, producer-aligned Unicode validation and retryable raw-driver source
database outages. A real test removes the DLT topic and proves the source offset cannot advance until recovery
publication succeeds after restoration.

Live checks confirmed trader REST → source Avro → processor Mongo COMPLETE → routed output, BUY/MSFT partition 2 and
SELL/MSFT partition 5, completed replay no-op, query pagination, Kafdrop visibility and metrics. The authenticated
provisioned 20-panel dashboard and its queries execute. Prometheus config and existing alert fixtures validate with the
new consumer rules. Browser rendering was not visually inspected.

Reference: [Spring Kafka exception handling and DLT recovery](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).
