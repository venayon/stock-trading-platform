# ADR 0007 — Route by side and symbol into a second topic

Date: 2026-10-24 · Status: Implemented by [ADR 0019](0019-consumer-processing.md)

## Context

Iteration 2 must demonstrate routing to specific Kafka partitions. A consumer cannot relocate records already stored in
source partitions. Filtering delivered records risks committing skipped work, while manual assignment alone changes
ownership without creating routed output.

## Decision

Consume all assigned `stock-trades` partitions in group `stock-trade-processor-v1`. Publish each valid record to
`stock-trades-processed` using an explicit partition: BUY → `floorMod(symbol.hashCode(),3)`, SELL →
`3 + floorMod(symbol.hashCode(),3)`. Both topics have exactly six partitions. Keep the canonical symbol as key and label
the algorithm routeVersion=1.

Persist input and route before output publication, mark COMPLETE after its acknowledgment, and only then permit source
offset commit. Duplicates after a crash remain possible; deduplicate logical processing by trade ID. Output value stays
StockTrade; add provenance through headers. See [LLD section 5](../design/LLD.md).

## Options and trade-offs

- **Chosen: second-topic publication.** Observable explicit partition routing, at the cost of another topic and send
  failure boundary.
- **Runner-up: symbol-only routing.** Better cross-side symbol locality; switch if BUY and SELL for one symbol must
  share a partition.
- **Rejected: consumer filtering.** Does not route and may discard records.
- **Rejected: manual assignment only.** Demonstrates ownership rather than record routing, and bypasses normal group
  rebalancing.
- **Rejected: side-only mapping to two partitions.** Leaves four partitions unused and concentrates load.

## Consequences

Order is local to symbol+side and Kafka append sequence; not across sides or business timestamps. Six partitions and
Java hash behavior are part of the route contract. Partition-count changes require a versioned migration/new topic, not
a silent setting edit. DLT recovery/replay introduces ordering gaps.

## Verification before acceptance

Test every side against a symbol fixture including negative hash values, assert exact partition range and deterministic
mapping, consume all source partitions, verify output coordinates, and reject a target topic with a different partition
count. Prove output-send failure cannot commit the source offset unless acknowledged DLT recovery succeeds.
