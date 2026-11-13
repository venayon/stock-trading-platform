# Kafka concepts — explained through our stock trading platform

Audience: someone learning Kafka or presenting this project to a team.

Read sections 1–4 first. Use section 12 as a presentation script and section 13 for a live demonstration. This is a demo
of imaginary trades, not exchange execution.

## 1. The whole system in one picture

Kafka is a durable event log. Producers append records; consumers independently read them and track their progress.
Reading a record does not normally remove it.

```text
Caller
  |
  | POST /api/trades
  v
ms_trader --------------------------> MongoDB: trader.trades
  |                                   save PENDING intent
  | HTTP 202 = durable acceptance
  |
  | background relay publishes Avro
  v
Kafka broker: stock-trades [P0 P1 P2 P3 P4 P5]
  |                         key = stock symbol
  | consumer group: stock-trade-processor-v1
  v
ms_trade_processor -----------------> MongoDB: processor.processed_trades
  |
  | explicit side + symbol routing
  v
Kafka broker: stock-trades-processed [P0 P1 P2 | P3 P4 P5]
                                       BUY   |   SELL

Failed processing ---> stock-trades-dlt [P0 P1 P2 P3 P4 P5]

Schema Registry: shared Avro contracts
Kafdrop: inspect brokers, topics, partitions and messages
Prometheus + Grafana: inspect rates, failures, pending work and lag
```

A service can have both roles: the processor is a **consumer** of stock-trades and a **producer** of
stock-trades-processed and the DLT.

Kafka does not validate our trading business rules, save MongoDB documents, or automatically run this processing logic.
Those are application responsibilities.

## 2. The core vocabulary

| Concept                   | Plain explanation                                                    | Example here                                      |
|---------------------------|----------------------------------------------------------------------|---------------------------------------------------|
| Event / record / message  | A piece of information written to Kafka                              | A StockTrade describing an accepted BUY           |
| Producer                  | A client that writes records                                         | ms_trader's publication relay                     |
| Consumer                  | A client that reads records                                          | A processor listener                              |
| Topic                     | A named stream of related records                                    | stock-trades                                      |
| Partition                 | One ordered log within a topic                                       | stock-trades partition 2                          |
| Offset                    | A record's position within its partition                             | Offset 37 in partition 2                          |
| Key                       | Optional record data used for partition selection and locality       | AAPL                                              |
| Value                     | The record payload                                                   | Avro StockTrade with ID, side, quantity and price |
| Headers                   | Extra record metadata                                                | X-Correlation-ID, source-offset, routeVersion     |
| Broker                    | A Kafka server that stores and serves partition data                 | The kafka Docker service                          |
| Cluster                   | Brokers and their Kafka metadata/control system                      | Our local single-node cluster                     |
| Consumer group            | Consumers cooperating to divide the subscribed partitions            | stock-trade-processor-v1                          |
| Commit                    | Save a group's progress for a partition                              | Commit 38 after handling offset 37                |
| Lag                       | Distance between the partition end and group progress                | End 100 − committed 85 = 15                       |
| Replica                   | Another copy of a partition on another broker                        | Our replication factor is 1: one copy             |
| Leader                    | Replica responsible for ordinary client reads/writes for a partition | Broker 1 leads our demo partitions                |
| ISR                       | In-sync replicas eligible for acknowledgment/leadership policies     | Only broker 1 in this demo                        |
| Controller                | Manages Kafka cluster metadata and leadership changes                | Combined with our broker in KRaft mode            |
| Serializer / deserializer | Converts objects to bytes / bytes back to objects                    | KafkaAvroSerializer / KafkaAvroDeserializer       |
| Schema Registry           | Stores versioned schemas and checks configured compatibility         | StockTrade schema subjects                        |
| DLT / DLQ                 | Application-selected topic for isolated failures                     | stock-trades-dlt                                  |

Do not confuse a **partition**, a **broker**, and a **network port**. Our six partitions share one broker. They do not
need six servers or six ports.

## 3. Topics and partitions

### A topic is split into independent logs

```text
stock-trades
  P0: [offset 0] [offset 1] [offset 2] ...
  P1: [offset 0] [offset 1] ...
  P2: [offset 0] [offset 1] [offset 2] [offset 3] ...
  P3: ...
  P4: ...
  P5: ...
```

Partition 0 offset 1 and partition 2 offset 1 identify different records. A useful record address is the tuple **topic +
partition + offset**. It is separate from our business tradeId, which identifies the logical trade across retries and
topics.

Offsets start at zero in a new partition, but retained data need not start at zero forever. Retention, compaction and
transaction/control records can also make offset differences differ from visible business-message counts.

### Why partition a topic?

Partitions provide parallelism and let data be distributed across brokers when a cluster has multiple brokers. They also
define the scope of ordering.

With normal group subscription, one partition is owned by one active consumer in a group at a time. A consumer may own
multiple partitions. Rebalances, crashes and already-running application work still require correct retry/deduplication
handling.

Our source topic has six partitions and processor concurrency three. An illustrative assignment is:

```text
One processor process
  listener C1 -> P0, P1
  listener C2 -> P2, P3
  listener C3 -> P4, P5
```

This is an example, not a promised assignment. Kafka's assignor decides ownership.

| Consumers in this group | Active partition owners possible for this six-partition topic |
|------------------------:|---------------------------------------------------------------|
|                       1 | One consumer owns all six                                     |
|                       3 | Typically two partitions each                                 |
|                       6 | Up to one partition each                                      |
|                       8 | At most six own source partitions; the others have none       |

Three concurrent listeners inside one Java process are three Kafka consumer clients; they are not three application
servers. More consumers do not automatically fix a single hot partition.

### Keys, partition selection and ordering

The source producer uses the canonical symbol as its key. With the current keyed partitioning configuration and fixed
partition count, records with the same key go to the same source partition. Different symbols can share a partition.

For the source topic, Kafka's normal keyed partition selection is used. Do **not** assume it is Java String.hashCode
modulo six.

The processed topic uses our separate explicit algorithm:

```java
int bucket = Math.floorMod(symbol.hashCode(), 3);
int partition = side.equals("BUY") ? bucket : 3 + bucket;
```

| Symbol | Processed BUY partition | Processed SELL partition |
|--------|------------------------:|-------------------------:|
| AAPL   |                       1 |                        4 |
| MSFT   |                       2 |                        5 |

The consumer cannot move an existing source record to a different partition. It routes by **publishing a new record to a
second topic**.

Kafka preserves log order within a partition. It does not provide one global order across partitions, business-date
order, or automatic ordering of external database effects. Our routing separates BUY and SELL, and DLT/replay can
introduce further ordering gaps.

A popular symbol may create a hot partition. Adding partitions changes key mapping, so it is a contract/ordering change
to review, not a harmless performance switch.

## 4. Consumer groups, offsets and rebalances

### Same group shares work; different groups have independent progress

```text
stock-trades
  |
  +--> group: stock-trade-processor-v1 -> processing
  |
  +--> group: hypothetical-audit-v1   -> independent audit reader
```

The audit group is an example, not another service we have built. Within one group, consumers divide partitions.
Different groups can each read the topic independently, provided records are still retained.

Kafdrop's inspections do not mean our processing group completed anything. Each reader's progress is separate.

### Position versus committed offset

A consumer can fetch records and advance its in-memory position before it finishes processing them. A committed offset
is the saved checkpoint used for recovery. These are different measurements.

Example:

```text
Read source offset 37
    -> validate
    -> save/claim Mongo processing state
    -> publish output and wait for ACK
    -> persist COMPLETE with output coordinates
    -> commit source offset 38
```

The commit normally identifies the **next** offset to read, not the last completed record's offset. Kafka stores group
checkpoints in the internal __consumer_offsets topic. Broker storage of that checkpoint does not atomically commit our
MongoDB update.

We use enable.auto.commit=false and Spring Kafka RECORD acknowledgment mode, so ordinary listener completion controls
progress. Our DLT recovery is an explicit alternative success path: commit is allowed only after the failed record is
successfully isolated.

### What does earliest mean?

Our auto.offset.reset=earliest applies when the group has no usable committed offset, including an out-of-range
checkpoint situation. It does not mean “start from zero on every restart.” Normally a restarted group resumes from its
saved progress. Earliest means the earliest **retained** data, which may no longer be offset zero.

### What is a rebalance?

A rebalance changes partition ownership, for example when group members join, leave or fail to poll within their
configured interval. Consumers coordinate ownership and may resume previously incomplete work. Rebalances are one reason
duplicates must be safe.

Our max.poll.interval.ms is five minutes. Long processing, retries or blocked dependencies can exceed the allowed
interval if bounds are chosen badly. Adding concurrency should be tested together with processing time and failure
recovery.

## 5. Lag: behind in reading is not the same as failed business processing

For one partition, our sampler calculates:

```text
lag = max(0, broker end offset − committed group offset)
```

If the end offset is 100, the next append would use 100. If the group's committed offset is 85, its offset distance to
the end is 15. The group has committed through offset 84. This example assumes a simple uninterrupted log; offset
distance is not always an exact visible-message count.

| Observation                             | Possible interpretation                                                      |
|-----------------------------------------|------------------------------------------------------------------------------|
| Lag increases continuously              | Arrival rate exceeds processing/commit rate, or the consumer is blocked      |
| Lag briefly rises then drains           | A recoverable burst or short pause                                           |
| One partition dominates lag             | Skewed keys or a problem specific to that partition                          |
| Lag is zero but DLT count increased     | Progress advanced through failure isolation; some trades may not be COMPLETE |
| Lag stays unchanged while polling fails | Last known sample is stale; current lag is unknown                           |

Our dashboard uses committed lag, not just records fetched by a client. Always inspect lag sampling success and
freshness. Zero lag does not prove correct output, exact-once behavior, or zero DLT events.

## 6. Durability: broker, replicas, ACKs, ISR and KRaft

A partition has a leader and a configured replication factor. In a replicated cluster, followers copy the leader's log.
ISR means the replicas currently considered in sync; min.insync.replicas and producer acknowledgments influence when
writes can succeed.

We configure acks=all and min ISR 1, but replication factor is also 1. Therefore an acknowledged write has only one
Kafka replica in this demo. Six partitions are not six copies of every record. A persistent Docker volume helps a
restarted broker recover its stored log, but it does not provide availability during an outage or protection from losing
that disk.

KRaft manages Kafka metadata using a controller quorum. Broker and controller are separate roles even when they share
one process. Our local node has both roles and a fixed cluster ID. The controller manages metadata; it does not act as a
general relay forwarding every business record between clients and brokers. This setup has no ZooKeeper container. See
the [Kafka KRaft documentation](https://kafka.apache.org/39/operations/kraft/) for deployment differences beyond this
laptop demo.

## 7. Avro, serializers and Schema Registry

Kafka stores bytes. Our Java objects are converted into bytes before sending and reconstructed after reading. JSON
accepted over REST and Avro stored in Kafka are different representations of the same trade.

```text
REST JSON -> validated Java data -> generated StockTrade -> Avro bytes
                                                        |
Kafka bytes -> Avro deserializer -> generated StockTrade -> validation/processing
```

The authoritative schema is [schemas/StockTrade.avsc](../../schemas/StockTrade.avsc). Its generated Java classes are
shared by both services through the schema Maven artifact. Price uses decimal (18,6) and BigDecimal, avoiding
floating-point rounding. Trade date uses an Avro logical date, mapped to LocalDate.

Schema Registry is a companion service, not the Kafka broker. It stores schemas under subjects, such as
stock-trades-value. A subject groups schema versions under a compatibility policy; a schema ID identifies a registered
schema. Subject versions and globally assigned schema IDs are different identifiers.

This project's Confluent 7.9 serializer uses a format marker, a four-byte schema ID and the encoded payload. The
complete schema is not repeated in every value. Clients can cache schema lookups, so Registry traffic is not necessarily
one HTTP request per event.
See [Confluent 7.9 serialization](https://docs.confluent.io/platform/7.9/schema-registry/fundamentals/serdes-develop/index.html).

BACKWARD_TRANSITIVE means a proposed new reader schema must remain compatible with all previous registered writer
schemas for that subject. It does not prove that old applications can interpret every new event or that business
behavior remains compatible. We explicitly register schemas during bootstrap and disable producer automatic
registration.

Kafdrop uses Registry to decode valid Avro. Logical values may be displayed using underlying representations, such as
bytes or epoch days. Raw malformed DLT bytes will not become decodable Avro merely because a schema subject exists.

## 8. Delivery guarantees, idempotency and the embedded outbox

| Term                   | Meaning in this project                                                                               |
|------------------------|-------------------------------------------------------------------------------------------------------|
| At most once           | Commit/skip too early and a crash can lose processing; not our chosen flow                            |
| At least once          | Recover incomplete work and accept possible duplicate delivery                                        |
| Producer idempotence   | Kafka deduplicates supported producer protocol retries; not a universal business deduplication system |
| HTTP idempotency       | Same client key + same canonical request reuses one accepted trade                                    |
| Consumer deduplication | Same immutable tradeId reuses one logical processing document                                         |
| Exactly once           | Requires a precisely defined boundary; not a guarantee we make for MongoDB plus Kafka                 |

Our embedded outbox is the durable PENDING state inside the accepted Mongo trade document. A background relay sends it
and marks PUBLISHED after ACK. A unique lease token and expiry coordinate relay ownership. This is application
coordination, not a built-in Kafka outbox object.

```text
HTTP POST -> Mongo ACK -> 202/PENDING
                        |
                        +-> relay -> Kafka ACK -> Mongo PUBLISHED
```

A crash between Kafka ACK and the Mongo PUBLISHED update can cause a repeat send. Similarly, a processor can publish
output and crash before saving COMPLETE. Kafka producer idempotence alone does not remove these application-level crash
windows.

Kafka transactions can coordinate Kafka writes and offsets when configured appropriately, but they do not automatically
include MongoDB. Our solution therefore uses at-least-once delivery and tradeId-based deduplication.
The [Kafka delivery-semantics design](https://kafka.apache.org/40/design/design/) explains the boundaries to consider.

A partition offset is a location; an HTTP idempotency key identifies a client request; a tradeId identifies a logical
trade; and a correlation ID helps trace an operation. None substitutes for all the others.

## 9. Retries, dead-letter topics and replay

A DLT is an ordinary Kafka topic with a special application purpose. Kafka does not automatically create a complete
business recovery workflow for it.

Our processor tries transient failures three times after the initial attempt, one second apart. Terminal
validation/deserialization failures go directly to recovery. A successful DLT send permits source progress; a failed DLT
send must not permit that progress.

| Failure                                   | What happens here                                                      |
|-------------------------------------------|------------------------------------------------------------------------|
| Producer cannot reach Kafka               | Accepted Mongo intent remains PENDING and relay retries                |
| Consumer receives malformed Avro          | Original bytes are preserved in the same source-numbered DLT partition |
| Same tradeId arrives with changed payload | Conflict goes to DLT; original processing document is not overwritten  |
| Consumer output send fails                | Completion is not marked; retry or DLT recovery follows                |
| DLT send fails                            | Source offset remains uncommitted and recovery is retried              |

A retry repeats an incomplete attempt. A replay deliberately makes an older event available again. Our POST /api/process
re-enqueues a retained canonical source record without seeking the active group's offsets. It returns 200/no enqueue for
an already COMPLETE trade. It cannot fix arbitrary malformed bytes or overwrite an immutable identity conflict.

DLT recovery can break strict chronological processing, and a valid event may be isolated after an infrastructure
outage. Someone still needs to investigate and decide whether replay is appropriate.

## 10. Retention, compaction and internal topics

Consumption does not delete our source records. Retention controls how long Kafka keeps them independently of a group's
progress.

| Topic                  | Purpose               | Partitions | Retention | Cleanup |
|------------------------|-----------------------|-----------:|-----------|---------|
| stock-trades           | Accepted trade events |          6 | 7 days    | delete  |
| stock-trades-processed | Routed output         |          6 | 7 days    | delete  |
| stock-trades-dlt       | Failure evidence      |          6 | 14 days   | delete  |

Retention is an expiry policy, not a promise that each record disappears at the exact millisecond its age crosses the
setting. Kafka operates on log segments. A slow reader can lose access to old events once retention removes them.

Compaction is a different policy: it eventually preserves the latest value per key while older versions can be removed.
We do not compact our trade-event topics. Compaction by symbol could remove distinct historical trades for the same
stock, which would not match our immutable event-history purpose. Tombstones are null-valued records used in
compacted-log deletion semantics; our trade consumer does not accept them as valid trades.

Kafka also has internal data, including __consumer_offsets for group progress. Registry stores schemas in its internal _
schemas topic. KRaft's metadata log is control-plane state, not another business topic to read as StockTrade. An
internal transaction-state topic may be used when Kafka transactions are involved; its presence is not evidence that our
Mongo operations are transactional with Kafka.

## 11. Components and ports: what connects to what?

| Component          | Responsibility                                            | Host endpoint   |
|--------------------|-----------------------------------------------------------|-----------------|
| ms_trader          | REST validation, durable acceptance and publication       | localhost:8081  |
| Kafka broker       | Store/fetch records and handle Kafka client requests      | localhost:9092  |
| ms_trade_processor | Consume, deduplicate, route and expose processing queries | localhost:8082  |
| Schema Registry    | Schema lookup, registration and compatibility             | localhost:8085  |
| Kafdrop            | Kafka inspection UI                                       | localhost:9000  |
| MongoDB            | Durable accepted/processed application documents          | localhost:27017 |
| Prometheus         | Scrape/store metrics and evaluate local rules             | localhost:9090  |
| Grafana            | Query Prometheus and show dashboards                      | localhost:3000  |

Inside Docker, clients use kafka:29092 rather than localhost:9092. The controller listener is kafka:29093 and is not
published to the host. Schema Registry is schema-registry:8081 inside Docker, even though its host port is 8085.

Bootstrap servers are initial discovery addresses. Clients then learn advertised broker addresses from metadata. Those
advertised addresses must be reachable from the client's network. Inside a container, localhost means that container
itself, not the Kafka container or your host laptop.

Kafdrop is not the broker. Registry is not the broker. Grafana is not a consumer of our trade-event topic: it queries
metrics stored by Prometheus. Prometheus scrapes HTTP metrics; it does not consume trade events to infer business
completion.

## 12. A three-minute explanation you can present

> “Our producer accepts an imaginary stock trade and first saves it in MongoDB. The 202 response means it is durably
> accepted, not yet processed.
>
> “A background relay publishes an Avro StockTrade to the stock-trades Kafka topic. The stock symbol is the key. The
> topic has six partitions, which are six ordered logs on our single demo broker.
>
> “The processor has three consumers in one group, so they share those six partitions. Each reads events, validates
> them, deduplicates by trade ID and publishes to a second topic. BUY uses partitions zero to two; SELL uses three to
> five, with a symbol bucket within each side.
>
> “After Kafka acknowledges the output and MongoDB stores COMPLETE, the consumer commits its source progress. If it
> crashes in between those steps, an event can be delivered again. That is why we need idempotency and deduplication.
>
> “Failures are retried or isolated in a dead-letter topic. The source offset advances through DLT recovery only after
> that DLT send succeeds.
>
> “Schema Registry shares the Avro contracts, Kafdrop lets us inspect events, and Prometheus plus Grafana show rates,
> errors and committed consumer lag. We have persistent local storage, but one broker and replication factor one do not
> provide high availability.”

If asked “Why Kafka rather than calling the processor directly?”, explain that durable retained events let producers and
consumers run at different times, absorb temporary backlogs and support additional independent reader groups. The costs
include eventual completion, schema/routing governance, operational components and explicit duplicate/failure handling.

## 13. A short live demonstration

Use the existing running services. The CLI commands below inspect state; the POST intentionally creates one synthetic
trade. Do not reset offsets or delete volumes during a demonstration.

1. Open [producer Swagger](http://localhost:8081/swagger-ui.html)
   and [processor Swagger](http://localhost:8082/swagger-ui.html).
2. Submit this request with a new Idempotency-Key. Reuse the same key only when demonstrating retry of the same request.

```sh
curl -i http://localhost:8081/api/trades \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: kafka-demo-msft-buy-001' \
  -d '{"tradeDate":"2026-10-24","side":"BUY","symbol":"MSFT","quantity":3,"price":"123.456789"}'
```

3. Copy tradeId from the response. GET /api/trades/{tradeId} should eventually show PUBLISHED and source event
   coordinates. GET /api/processed/{tradeId} on port 8082 should eventually show COMPLETE and output partition 2 for
   BUY/MSFT.
4. Open [source topic](http://localhost:9000/topic/stock-trades)
   and [processed topic](http://localhost:9000/topic/stock-trades-processed). Inspect the matching trade ID with value
   format AVRO and key format DEFAULT. Source and output coordinates are different; output routing did not move the
   original record.
5. Repeat the same POST with the same key and payload. It should reuse the trade ID. Explain that this HTTP retry does
   not intentionally create another logical trade or requeue completed publication.
6. Open the [dashboard](http://localhost:3000/d/stock-trading-overview). Explain acceptance versus
   publication/completion rates, then lag and its sampling-health panel. Wait for multiple five-second scrapes before
   expecting useful rates or percentiles.

From infra/, inspect topic layout and group progress:

```sh
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 --describe --topic stock-trades

docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 --describe --group stock-trade-processor-v1

curl http://localhost:8085/subjects
```

CURRENT-OFFSET is the group's committed checkpoint; LOG-END-OFFSET is the partition end. Values change while traffic
runs. A new or inactive group may not yet have the checkpoint/member information you expect.

For failure demonstrations, prefer the isolated Testcontainers suite rather than disrupting the shared running stack. It
already tests a missing DLT topic and proves the source offset cannot advance until recovery publication succeeds. Run
mvn verify in ms_trade_processor with Docker available.

## 14. Common questions and mistakes

| Question / statement                                        | Explanation                                                                                                  |
|-------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| “Six partitions means six brokers?”                         | No. Our one broker stores all six partitions.                                                                |
| “Replication factor six means six-way processing?”          | No. Replication concerns copies; partitions/group assignments concern processing parallelism.                |
| “All consumers get every event?”                            | Different groups can read independently; consumers within one group share partition ownership.               |
| “More than six consumers will speed up this source topic?”  | Not beyond its six partition owners in this group; a hot partition can still limit throughput.               |
| “Kafka deletes a message after consumption?”                | Not in our retained-log design. Retention/cleanup governs removal.                                           |
| “Offset 50 uniquely identifies a trade?”                    | Not without topic and partition; it also is not the logical trade ID.                                        |
| “earliest replays everything after every restart?”          | Existing usable committed offsets take precedence.                                                           |
| “acks=all means every broker in the cluster?”               | It concerns the partition's in-sync replicas under the configured policy, not every cluster node.            |
| “Producer idempotence makes Mongo plus Kafka exactly once?” | No. Separate application/database failure windows remain.                                                    |
| “Zero lag means every trade succeeded?”                     | DLT recovery and stale sampling can invalidate that conclusion; inspect COMPLETE state and DLT too.          |
| “Changing the partition count is harmless?”                 | It can change key mapping and ordering relationships.                                                        |
| “Schema compatibility proves business compatibility?”       | It does not validate all application meanings, enum handling or deployment-order assumptions.                |
| “Retry and replay are identical?”                           | Retry recovers an attempt; replay deliberately reintroduces retained data.                                   |
| “A DLT fixes the record?”                                   | It preserves failure evidence and enables progress; investigation and repair/replay remain application work. |

## 15. Project files and further reading

For questions, sample answers and scenario practice, use the [interview guide](KAFKA_INTERVIEW.md).

- [Producer implementation/run instructions](../../ms_trader/README.md)
- [Consumer implementation/run instructions](../../ms_trade_processor/README.md)
- [Compose endpoints and bootstrap](../../infra/README.md)
- [Observability runbook](../design/OBSERVABILITY.md)
- [Publication decision](../adr/0017-asynchronous-avro-publication.md)
- [Consumer/routing/recovery decision](../adr/0019-consumer-processing.md)

For deeper technical details, start with
the [Apache Kafka design](https://kafka.apache.org/40/design/design/), [KRaft operations](https://kafka.apache.org/39/operations/kraft/)
and [Confluent 7.9 serialization](https://docs.confluent.io/platform/7.9/schema-registry/fundamentals/serdes-develop/index.html).
Our running configuration and service READMEs take precedence over examples written for a different release or
deployment.
