# Kafka interview preparation — questions, answers and project examples

Use this alongside the [Kafka concepts guide](KAFKA_CONCEPTS.md). Answers below describe our actual implementation and
distinguish it from alternatives we have not built.

For each answer: state the concept, give one project example, then explain the main trade-off. Be ready for the
follow-up rather than memorizing a paragraph.

## 1. Your project introduction

### A 60-second answer: “Tell me about your Kafka project.”

> “I worked on a local, event-driven stock trading demo using Java 21, Spring Boot, Kafka in KRaft mode, Avro, Schema
> Registry and MongoDB.
>
> “The producer API validates a trade and saves durable PENDING intent with an idempotency key. A background relay
> publishes the event, then records the broker acknowledgment. The consumer validates and deduplicates by trade ID, routes
> output to explicit BUY/SELL partition buckets, and saves processing completion before allowing the source offset to
> commit.
>
> “We use at-least-once delivery and acknowledge that crashes can duplicate physical events. We handle failures with
> retries and an acknowledgment-verified dead-letter topic. Real-container tests verify that source progress does not
> advance when DLT publication fails. Prometheus, Grafana and correlated JSON logs provide visibility into rates, failures
> and committed lag.”

Adapt “I worked on” to your actual contribution. Explain what you understand and can demonstrate; do not claim
production operation, financial execution or measured scale that this demo has not established.

### What is implemented versus proposed?

| Implemented                                                | Not a claim we should make                                             |
|------------------------------------------------------------|------------------------------------------------------------------------|
| One local KRaft broker; six partitions per business topic  | Highly available production cluster                                    |
| Shared Avro schema and explicit Registry bootstrap         | Every possible schema migration has been validated                     |
| Durable pending intent, leases and trade-ID deduplication  | Atomic exactly-once effects across MongoDB and Kafka                   |
| Routed output, retries, DLT and source replay              | Automated repair of arbitrary malformed events                         |
| Real Kafka/Registry/Mongo integration tests                | Proven production throughput, latency or uptime SLO                    |
| Metrics, dashboard, locally evaluated alerts and JSON logs | Distributed tracing, centralized log search or external alert delivery |

## 2. Foundation questions

### 1. What is Kafka?

Kafka is a distributed event-streaming system built around durable partitioned logs. Producers append records and
consumers read them while tracking their progress. Records can be retained independently of consumption, enabling replay
and independent consumer groups.

**Project example:** stock-trades stores accepted trade events for the processor to consume.

**Follow-up:** Kafka can serve messaging, event-streaming and integration use cases; calling it “just a queue” hides
retention, replay and partitioning behavior.

### 2. What is a topic? What is a partition?

A topic is a named stream. A partition is one ordered log within it and is a unit of storage placement and consumer
assignment.

**Project example:** stock-trades has six partitions on one local broker. Six partitions do not mean six brokers or six
network ports.

### 3. Why use multiple partitions?

They enable parallel consumers within a group and distribute data across brokers in a multi-broker cluster. The
trade-offs include per-partition rather than global ordering, overhead and possible key skew.

**Follow-up:** Increasing partitions can change where keys map. Review ordering and routing contracts before changing
the count.

### 4. What is a record key?

The key can influence partition selection and keep related records together under a stable partitioning policy. It is
separate from the record value and headers.

**Project example:** the source key is the canonical stock symbol. The logical trade ID lives in the payload and
supports deduplication.

### 5. Does Kafka guarantee ordering?

Kafka provides order within a partition's log. It does not provide a global order across partitions or automatically
order external database effects by business time.

**Project example:** processed BUY and SELL trades use different partition sets, so we do not promise cross-side symbol
ordering. Retries and DLT replay can also alter when business effects occur.

### 6. What is an offset?

An offset identifies a position within a partition. The full record address includes topic, partition and offset. It is
not a globally unique business ID.

**Follow-up:** Offsets can have gaps or refer to data no longer retained. Offset differences are not always exact counts
of visible business records.

### 7. What is a consumer group?

A group coordinates consumers that share partition ownership. Under normal group assignment, one active consumer owns a
partition within that group at a time. A separate group has independent progress over the same topic.

**Project example:** stock-trade-processor-v1 runs three concurrent listeners over six source partitions.

### 8. What happens with eight consumers and six partitions?

For that topic in one group, at most six consumers can own partitions. Others have no source partitions assigned. With
three consumers, each may own multiple partitions; the exact assignment depends on the assignor.

**Follow-up:** Concurrency three in one Spring application means three consumer clients, not three servers.

### 9. Does reading a message delete it?

No. Our topics use time-based delete retention. Consumer progress and log cleanup are separate mechanisms.

**Project example:** another group can read retained source events even after our processing group commits them.

### 10. What is a broker, and what does KRaft do?

A broker stores and serves partition data. KRaft provides Kafka metadata management through controller roles and a
metadata quorum. Our demo combines broker and controller roles on one node and uses no ZooKeeper.

**Trade-off:** combined single-node mode is convenient locally but provides no high availability.

## 3. Delivery and correctness questions

### 11. What is the difference between producer acknowledgment and consumer commit?

A producer acknowledgment confirms a send according to the broker/replica policy. A consumer commit stores a group's
recovery checkpoint. Neither alone proves that an external business operation completed.

**Project example:** source publication ACK makes a producer document PUBLISHED. Consumer source commit follows
acknowledged output publication and Mongo COMPLETE, or acknowledged DLT recovery.

### 12. What do acks=0, 1 and all mean?

- 0: the producer does not wait for a broker acknowledgment; delivery uncertainty is greater.
- 1: wait for the partition leader's acknowledgment, without requiring follower acknowledgment for that request.
- all: wait according to the in-sync replica policy; min.insync.replicas controls the minimum acceptable ISR size for
  these writes.

**Project example:** we use all, but replication factor and minimum ISR are both one. This is still one Kafka copy, not
high availability.

### 13. Explain at-most-once, at-least-once and exactly-once processing.

At-most-once permits loss while avoiding recovery duplicates at the chosen boundary. At-least-once recovers incomplete
work but permits duplicates. Exactly-once must specify which reads, writes and effects are included in its atomic
boundary.

**Project answer:** our Mongo/Kafka pipeline is at least once, with deduplicated logical Mongo results. It does not have
atomic cross-system exactly-once effects.

### 14. Does enable.idempotence=true solve all duplicates?

No. It handles supported Kafka producer protocol retry duplication. It does not identify repeated HTTP business requests
or atomically coordinate Kafka acknowledgments with MongoDB state changes.

**Follow-up:** distinguish producer idempotence, HTTP idempotency and consumer business deduplication.

### 15. How did you implement HTTP idempotency?

We require a client key, normalize and validate the request, and insert a document protected by a unique MongoDB key
index. A duplicate-key race loads the winning document and compares a versioned canonical payload digest.

Same key/same canonical payload returns the existing trade; changed payload returns 409. The database index enforces
concurrency safety across processes, rather than a JVM-only lock.

### 16. Why not check existence and then insert?

Two requests can both observe “absent” before either inserts. A preliminary read does not enforce uniqueness. Insert
under a unique index and handle the race explicitly.

**Follow-up:** a different idempotency key intentionally creates a new logical request, even if its payload looks
identical.

### 17. How do you avoid losing an event between a database write and a Kafka send?

Persist publication intent durably with the accepted trade. Our trade document acts as an embedded outbox: the relay
discovers PENDING intent and retries after failures or restart.

**Trade-off:** this avoids relying on an in-memory queue, but does not remove the ACK-before-completion duplicate
window.

### 18. What if Kafka accepts the event and the application crashes before saving PUBLISHED?

The durable document can still look PENDING. Recovery may publish it again. We accept physical duplicates and use the
immutable trade ID for downstream deduplication.

**Incorrect answer to avoid:** “acks=all means the database and Kafka committed together.”

### 19. What if output is published but the consumer crashes before committing its source offset?

It may receive the source event again. If Mongo COMPLETE was durably recorded, the duplicate skips output publication.
If the crash happened before COMPLETE, output may be repeated. That is a separate window from ordinary consumer
redelivery.

### 20. Why are you not using Kafka transactions?

Kafka transactions can atomically coordinate Kafka output and Kafka offsets when configured appropriately. Our pipeline
also writes MongoDB, which is outside that transaction. We chose explicit at-least-once semantics and deduplication
rather than claiming transactions solve the whole boundary.

**Follow-up:** reconsider Kafka transactions if state/effects move to Kafka-only processing or a cooperating storage
design changes the atomicity boundary.

### 21. What does committing offset 38 mean?

It normally identifies the next offset to read: progress through offset 37 has been committed for that partition. It
does not mean every record with offset 37 in every partition has completed.

### 22. Why disable automatic commits?

We need source progress tied to processing/recovery outcomes. We disable auto commit and use Spring Kafka RECORD mode,
allowing normal progress only after the listener completes its required work. Our DLT handler must also confirm
publication before recovery succeeds.

**Follow-up:** manual/record acknowledgment settings alone are insufficient; verify error-handler behavior and crash
windows with real tests.

### 23. What is a lease or fencing token in your implementation?

A lease gives a processing attempt temporary ownership of a Mongo record. A unique token conditions completion updates
so an old owner cannot overwrite a newer claim after expiry.

**Important limitation:** the token fences Mongo writes. It cannot stop an old paused process from making a Kafka send,
so duplicates remain possible.

## 4. Scaling and operations questions

### 24. What is consumer lag?

We measure broker end offset minus the group's committed offset, clamped to zero, per partition. It describes
uncommitted progress, not an exact number of failed business operations.

**Follow-up:** zero lag can coexist with DLT events. Failed sampling can leave a stale zero. Inspect freshness and
business state too.

### 25. What causes growing lag?

Examples include arrival rate above processing capacity, dependency latency/outages, repeated failures, partition skew
and rebalances. Diagnose per partition and correlate with processing latency, errors, CPU, dependency health and commit
progress before changing capacity.

### 26. How would you choose a partition count?

Start from expected throughput, measured processing cost, desired parallelism, key distribution, ordering constraints
and operational overhead. Load-test the choice. Our six-partition choice supports the local concurrency/routing
demonstration; it is not a production sizing result.

### 27. How would you fix a hot partition?

First identify whether one key dominates or a particular record/dependency blocks progress. More consumers cannot split
one assigned partition within a group. Possible changes include reducing processing cost, isolating a workload or
changing the key/routing contract if ordering requirements permit it.

Do not promise that adding partitions or salting keys preserves existing ordering automatically.

### 28. What triggers rebalances?

Group membership/subscription changes and failures such as exceeding the poll interval can change assignment. A
rebalance may lead to recovery of unfinished work; idempotency remains necessary.

**Project example:** max.poll.interval.ms is five minutes, and retries/sends are bounded to avoid ordinary attempts
consuming that whole interval.

### 29. What does auto.offset.reset=earliest do?

It selects the earliest retained position when no usable committed checkpoint exists. It does not override valid saved
offsets on every restart.

**Follow-up:** changing the group ID creates different group progress; deliberately replaying production data requires
reviewing side effects and retention.

### 30. What are replication factor and ISR?

Replication factor is the configured copy count for each partition. ISR is the set of replicas currently considered in
sync. Durability and availability depend on replication, acknowledgment and minimum-ISR policy together.

**Project answer:** RF=1 is a laptop simplification. We cannot tolerate broker outage with continued Kafka availability
or loss of the only storage copy.

### 31. Retention versus compaction?

Delete retention expires log segments under retention policy. Compaction eventually retains the latest state per key and
can remove older keyed records. They serve different purposes and can be combined in appropriate configurations.

**Project example:** we use delete retention because trades are immutable historical events. Compaction by symbol could
discard distinct trades for the same stock.

### 32. What is a DLT, and is it sufficient recovery?

A DLT is a normal topic used by the application to isolate failures with provenance. It is not automatic business
repair. We preserve failure evidence, monitor it and replay retained canonical source data when appropriate.

**Follow-up:** sending to DLT allows later work to progress and therefore can create ordering gaps.

### 33. What happens if the DLT itself is unavailable?

The recoverer must throw and retain source progress. Otherwise the application can lose its recovery evidence while
committing past the failed event.

**Project evidence:** an integration test deletes the isolated DLT topic, verifies the source offset does not advance,
restores the topic and verifies recovery.

### 34. Why use Avro and Schema Registry?

Avro gives us a shared typed binary contract with logical decimal/date types. Registry distributes schema IDs and
enforces the configured compatibility policy. Both services use generated SpecificRecord classes from one schema
artifact.

**Trade-off:** the extra tooling and Registry dependency must be configured, cached and tested; Registry compatibility
does not prove business compatibility.

### 35. What does BACKWARD_TRANSITIVE mean?

A new reader schema must be compatible with all previous registered writer schemas under that subject. It is stronger
than checking only the last version, but does not guarantee every old reader can consume every new writer or that
application semantics remain correct.

**Follow-up:** deployment ordering, defaults, enum handling and decimal changes need explicit review and contract tests.

### 36. Why can a Kafka client bootstrap successfully and still fail to communicate?

Bootstrap is discovery. The broker then advertises addresses clients must reach. Incorrect advertised listeners can
return a container-only address to a host client, or localhost to a different container.

**Project example:** host applications use localhost:9092; Docker clients use kafka:29092. These are network contexts,
not different topics.

## 5. Scenario questions: answer with diagnosis and evidence

### “The API returns 202, but there is no processed trade. What do you check?”

1. GET the accepted trade: 202 guarantees Mongo acceptance, not publication.
2. If PENDING, check relay attempts, sampling freshness, Kafka/Registry connectivity and producer logs.
3. If PUBLISHED, inspect its source topic/partition/offset in Kafdrop.
4. Check processor group assignment, committed lag, processing errors and DLT.
5. Query the processor document by tradeId; correlate logs using correlationId.

Do not create a different idempotency key as the first troubleshooting action.

### “Lag is zero, but the business result is missing.”

Check DLT and the processor document, then sampling freshness and the group/topic being observed. Zero lag can reflect
successful failure isolation rather than successful business processing. Also confirm you are querying the correct Mongo
database and trade ID.

### “A trade was processed twice after a restart.”

Identify whether that means duplicate Kafka deliveries, duplicate output events or duplicate logical database records.
Locate the ACK/completion/commit crash window. Check the unique ID constraint, payload digest, completion state and
lease token. Duplicate physical events are possible by design; duplicate logical Mongo documents should not be.

### “Throughput dropped after adding consumers.”

Inspect partition count, assignments, rebalances, key skew and shared dependency capacity. Consumers beyond partition
count can be idle, and more active work can overload Mongo or downstream processing. Compare measured throughput and
latency before/after rather than assuming more concurrency helps.

### “One invalid event blocks a partition.”

Check whether it is retryable, whether deserialization failures reach the recoverer, whether the DLT has the required
partitions, and whether the recoverer's send is acknowledged. A blocked partition when both processing and DLT fail is
intentional protection against skipping unrecovered data.

### “How would you evolve the price field?”

Our current decimal precision/scale and BigDecimal conversion are contract decisions. Do not change them casually in
place. Review Registry compatibility and reader/writer behavior, introduce a versioned migration when needed, and test
old/new values and deployment order. Using double would introduce an inappropriate precision change.

## 6. Project decisions an interviewer may challenge

| Challenge                                   | Defensible answer                                                                                   | When to revisit                                                         |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| Why MongoDB?                                | Durable immutable intent, atomic single-document state and unique-index deduplication fit this demo | Query/state requirements or operational constraints change              |
| Why asynchronous acceptance?                | Broker outages do not stop durable API acceptance while Mongo is available                          | Product requires broker ACK as part of HTTP success                     |
| Why not a separate outbox collection?       | Intent and trade state fit atomically in one document                                               | Multiple independent intents/lifecycles justify extra coordination      |
| Why symbol as source key?                   | Demonstrates symbol locality and partition-level order                                              | Measured skew dominates and ordering can be relaxed                     |
| Why side+symbol output routing?             | Explicitly demonstrates routing to BUY/SELL partition sets                                          | Cross-side ordering for a symbol becomes required                       |
| Why only one broker?                        | Reproducible local learning environment                                                             | Availability/failover is part of the environment being tested           |
| Why bounded retries and DLT?                | Isolates failures with inspectable evidence                                                         | Strict sequencing requires pausing a partition instead                  |
| Why no exactly-once claim?                  | Mongo and Kafka do not share our atomic commit boundary                                             | A different state/storage design establishes a narrower atomic boundary |
| Why not add consumers until lag disappears? | Partition ownership, skew and shared resources limit useful parallelism                             | Measurements show processing capacity scales with extra owners          |

## 7. How to discuss testing

Explain the invariant, the injected failure and the observed result:

| Invariant                                                         | Test approach                                                                |
|-------------------------------------------------------------------|------------------------------------------------------------------------------|
| Invalid input has no intended persistence/publication side effect | Domain/service and MVC checks                                                |
| One idempotency key creates one accepted trade                    | Concurrent requests against real Mongo uniqueness                            |
| Same immutable trade ID has one logical processed document        | Real duplicate delivery and conflict tests                                   |
| COMPLETE follows output ACK                                       | Service orchestration checks and real output coordinates                     |
| Failed DLT publication cannot commit source progress              | Remove DLT in isolated Kafka, inspect committed offsets, restore and recover |
| Old processing owner cannot overwrite a new one                   | Expire/reclaim lease, reject old token completion                            |
| Decimal/date/quantity survive the pipeline                        | Real Avro serialization and Mongo round-trip checks                          |
| Monitoring does not report a failed sample as zero                | Preserve previous snapshot and expose failure/freshness                      |

Use test counts as supporting detail, not the main proof. The invariant and the failure window are more informative. Do
not equate a passing local suite with demonstrated production behavior.

## 8. Practice exercises

### Exercise A: partition routing

Implement our route-v1 function. Explain why floorMod is used rather than assuming Java's % always returns a nonnegative
value. Test AAPL/MSFT, both sides and a symbol with a negative hash. Explain why this function is not the source
producer's default keyed partitioner.

### Exercise B: offset reasoning

A partition end is 500 and the group's committed offset is 470.

- Offset distance is 30.
- The committed checkpoint normally means resume at 470.
- It does not establish how many logical trades are COMPLETE.
- If recovery sends ten records to DLT and commits through them, lag can fall without ten successful business results.

### Exercise C: crash windows

Draw these steps and mark every place a crash can occur:

```text
Mongo accepted intent -> Kafka source ACK -> Mongo PUBLISHED
Kafka source read -> Mongo PENDING -> output ACK -> Mongo COMPLETE -> source commit
```

For each gap, state what recovery reads, whether a send can repeat and which identifier prevents duplicate logical
state.

### Exercise D: troubleshoot with tools

Using the [concepts guide demo](KAFKA_CONCEPTS.md), locate one trade in the API, Mongo-backed query response, source
topic and processed topic. Explain each offset, the route version, the group checkpoint and the correlation ID. Show why
these identifiers serve different purposes.

## 9. A mock interview you can run in 20 minutes

| Time          | Task                                                                               |
|---------------|------------------------------------------------------------------------------------|
| 0–2 minutes   | Give your project introduction without claiming unbuilt production capabilities    |
| 2–6 minutes   | Explain partitions, keys, ordering, consumer groups and offsets using a drawing    |
| 6–11 minutes  | Defend outbox/idempotency choices and explain two crash windows                    |
| 11–15 minutes | Diagnose growing lag and DLT publication failure                                   |
| 15–18 minutes | Explain schema evolution and one trade-off you would change under new requirements |
| 18–20 minutes | Describe your strongest real-container test and one current limitation             |

Self-check: can you answer “what can still go wrong?” after every guarantee you describe? Can you distinguish
observations from measurements and implemented behavior from proposals?

## 10. Further reading and project evidence

- [Kafka concepts and live demonstration](KAFKA_CONCEPTS.md)
- [Producer README](../../ms_trader/README.md)
- [Consumer README](../../ms_trade_processor/README.md)
- [Publication ADR](../adr/0017-asynchronous-avro-publication.md)
- [Consumer ADR](../adr/0019-consumer-processing.md)
- [Observability runbook](../design/OBSERVABILITY.md)

For deeper explanations, use the official references linked in the concepts guide. Adapt every interview answer to the
exact Kafka/client version and environment being discussed.
