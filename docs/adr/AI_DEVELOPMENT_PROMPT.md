---
title: AI Development Prompt — Stock Trading Platform
version: 1.0
covers: Iteration 1 → Iteration 2
last_updated: 2026-10-24
owner: Venu Ranaboyana
status: ready-for-review
---

## 🎯 Role

You are a **senior Java/Spring Boot architect and DevOps engineer** helping design a multi-iteration,
event-driven stock trading demo system.

## 🧭 Goal

Produce a **detailed implementation plan and design-decision document** (not full code yet) for the two
iterations described below. The output will be reviewed by a team before coding begins.

## 🖥️ Environment

- **Java 21**, **Spring Boot 3.3+**
- Local development only (laptop/CI), all infra via **Docker Compose**
- **Kafka (KRaft) + Kafdrop + Schema Registry (Avro)** running locally in Docker
- **Observability**: Prometheus + Grafana (Grafana dashboards read from Prometheus; Prometheus
  scrapes Spring Boot Actuator `/actuator/prometheus`)
- No cloud dependencies. Keep it runnable offline after image pulls.

---

## 📦 Context: System Under Construction

A simplified "stock trader" event pipeline:

- **`ms_trader`** (producer): REST API for imaginary BUY/SELL stock orders, persists to MongoDB,
  publishes Avro events to Kafka, exposes metrics.
- **`ms_trade_processor`** (consumer, Iteration 2): consumes Avro events, routes to specific Kafka
  partitions, exposes REST for querying processed trades, exposes metrics.
- **Avro schema** (already drafted, reuse and refine): `StockTrade` with fields `tradeId`,
  `tradeDate` (logical date), `side` (enum BUY/SELL), `symbol`, `quantity` (long),
  `price` (decimal 18,6), `currency`, `counterparty` (nullable).
- **Load generator**: **Playwright (JavaScript)** project that fires large volumes of BUY/SELL REST
  requests against `ms_trader`.
- **Kafka UI inspection**: **Kafdrop**.

---

## 🚀 Iteration 1 — Scope

1. **Local Docker Compose stack**: Kafka (KRaft mode preferred), Kafdrop, Schema Registry, MongoDB,
   Prometheus, Grafana. Provide a single `docker-compose.yml` design; list networks, volumes, ports,
   healthchecks, and startup ordering.
2. **Kafka topic + partitions**: Design a `stock-trades` topic with an explicit partition count and
   replication factor. Justify partition count and partitioning key.
3. **Producer app `ms_trader`**:
  - Java 21, Spring Boot 3.x, Spring Web, Spring Data MongoDB, Spring Kafka.
  - REST: `POST /api/trades` (accepts JSON buy/sell), `GET /api/trades/{id}`, `GET /api/trades` (list).
  - On POST: validate → persist to MongoDB → publish Avro `StockTrade` to Kafka → return 202 with
    tradeId and event metadata.
  - Structured JSON logs, correlation ID propagation, Actuator + Micrometer + Prometheus endpoint wired.
4. **Playwright (JavaScript) load generator**: posts concurrent BUY/SELL requests. Design its
   structure (config, fixtures, workers, reporting) — no full code yet.
5. **Verification via Kafdrop**: describe what "success" looks like in Kafdrop (topic, partitions,
   message counts, key distribution, Avro payload visibility).

## ⚙️ Iteration 2 — Scope (builds on Iteration 1)

1. **Consumer app `ms_trade_processor`**:
  - Java 21, Spring Boot 3.x, Spring Kafka, Spring Web, Spring Data MongoDB (separate DB or collection).
  - Consumes `StockTrade` Avro events with `specific.avro.reader=true`.
  - **Routes messages to specific Kafka partitions** (e.g., partition-by-symbol or partition-by-side)
    — propose the routing strategy, explain whether this means re-producing to a second topic (`stock-trades-processed`)
    with explicit partition assignment, or consumer-side partition
    filtering, and pick one with justification.
  - REST: `POST /api/process` (manual replay/test), `GET /api/processed/{id}`, `GET /api/processed`.
  - Exposes Prometheus metrics (consumer lag, records consumed, processing time).
2. **Observability integration**: Prometheus + Grafana added to Docker Compose, scrape configs for both
   apps, provisioned Grafana datasource + one starter dashboard JSON (producer rate, consumer lag,
   error rate, JVM basics).

---

## 📬 What I Want From You (Deliverables)

Produce a **design document** with the following sections. Be opinionated, state trade-offs, and give
concrete recommendations (versions, ports, names). Do **not** dump full source files — show only
critical snippets or config fragments where needed to make a decision clear.

1. **Executive summary** (10–15 lines): what will be built, why these choices, top 3 risks.
2. **Architecture diagram (ASCII)** covering Iteration 1 and 2, showing data flow, topics, partitions,
   and observability paths.
3. **Docker Compose design**: services table (image, tag, ports, volumes, healthchecks, depends_on),
   network, Kafka KRaft vs Zookeeper decision, Schema Registry compatibility mode.
4. **Kafka topic design**: topic name (s), partition count, replication factor, retention, cleanup policy, **partition
   key strategy** (justify key choice with respect to ordering, hot partitions, and
   Iteration 2 routing).
5. **Avro schema governance**: schema evolution rules, subject naming (`TopicNameStrategy` vs
   `RecordNameStrategy`), compatibility mode, where `.avsc` files live, Maven plugin config summary.
6. **`ms_trader` design**: package layout, key classes (Controller, Service, Repository,
   KafkaProducer, Mapper), error handling strategy (validation, Kafka publish failure, MongoDB
   failure — synchronous vs outbox), idempotency approach, correlation ID approach, logging schema (fields), metrics to
   expose.
7. **`ms_trade_processor` design**: consumer group ID, concurrency, ack mode, error handling + DLT
   strategy, **partition routing mechanism (decide and justify)**, idempotency/dedup approach, REST
   surface, metrics to expose.
8. **Playwright load generator design**: project layout, config (base URL, concurrency, duration,
   payload mix), how to assert success, how results feed back into verification (e.g., counts vs
   Kafdrop vs Mongo).
9. **Observability design**: Prometheus scrape configs, Grafana provisioning (datasource + dashboard
   provider), the specific metrics/panels for the starter dashboard, alerting (even if just documented).
10. **Configuration & secrets**: `application.yml` profile strategy (`local`), env var overrides,
    no secrets in repo.
11. **Testing strategy**: unit, integration (Testcontainers for Kafka + Mongo), end-to-end (Compose + Playwright),
    contract check on Avro schema.
12. **Design decisions log (ADR-style table)**: Decision | Options considered | Chosen | Rationale |
    Consequences. Include at least 10 rows covering: KRaft vs ZK, Schema Registry vs plain Avro,
    decimal encoding, partition key, outbox vs sync publish, DLT strategy, partition routing approach
    in Iteration 2, Playwright vs k6/JMeter, Grafana provisioning method, MongoDB single vs multiple
    databases.
13. **Iteration 1 → Iteration 2 handoff**: what must be stable at end of Iteration 1 for Iteration 2
    to start (topic contract, schema compatibility, metrics naming, compose extensibility).
14. **Open questions / assumptions** (max 8 bullets) that the team must confirm before coding.

---

## 🧱 Constraints & Style

- Target **Java 21**, **Spring Boot 3.3+**, **Confluent Platform 7.9+**, **Avro 1.12**, **Kafka 3.7+ (KRaft)**,
  **MongoDB 7**, **Prometheus 2.5x**, **Grafana 11.x**, **Playwright 1.4x**
  — flag any version conflicts.
- Prefer **explicit, named configuration** over magic defaults.
- Where a decision is contentious, give the **runner-up** and the trigger that would make you switch.
- Keep it readable: use tables, short paragraphs, and numbered decisions.
- Call out **anti-patterns to avoid** (e.g., committing to a partition key that causes hot partitions,
  relying on `auto.create.topics.enable`, storing secrets in compose).

---

## 📤 Output Format

Markdown, with the **14 numbered sections** above. Aim for depth over breadth; when in doubt, pick a
decision and defend it. End with a **one-page "green-light checklist"** the team can use to approve
the plan.

---

## ✅ Green-Light Checklist (to be completed by the team after AI responds)

Use this checklist during design review. Do not start Iteration 1 coding until every row is **✅**.

| #  | Check                                                                             | Owner     | Status |
|:---|:----------------------------------------------------------------------------------|:----------|:-------|
| 1  | Kafka runs in KRaft mode; no Zookeeper container                                  | DevOps    | ☐     |
| 2  | Schema Registry compatibility mode is explicit (not default)                      | Architect | ☐     |
| 3  | Partition count and partition key justified against hot-partition risk            | Architect | ☐     |
| 4  | Avro `.avsc` files live in `schemas/` and are consumed by Maven plugin            | Backend   | ☐     |
| 5  | Producer publish strategy decided (sync/async/outbox) with failure semantics      | Backend   | ☐     |
| 6  | Correlation ID propagation path documented end-to-end                             | Architect | ☐     |
| 7  | Iteration 2 partition-routing mechanism chosen and justified                      | Architect | ☐     |
| 8  | DLT strategy for consumer defined (topic name, retries, headers)                  | Backend   | ☐     |
| 9  | Prometheus scrape configs + Grafana provisioning committed as files (not manual)  | DevOps    | ☐     |
| 10 | Playwright project layout and success assertions defined                          | QA        | ☐     |
| 11 | No secrets in Compose or `application.yml`; env var overrides documented          | DevOps    | ☐     |
| 12 | ADR table contains ≥10 rows with runner-up options                                | Architect | ☐     |
| 13 | Open questions list has owners and due dates                                      | PM        | ☐     |
| 14 | Iteration 1 → 2 handoff contract (topic, schema subject, metric names) signed off | Architect | ☐     |

**Green-light decision**: ☐ Approved to start Iteration 1 ☐ Changes requested

---

## 📎 Appendix A — Existing `StockTrade.avsc` (reference)

```json
{
  "type": "record",
  "name": "StockTrade",
  "namespace": "com.example.finance.avro",
  "fields": [
    { "name": "tradeId",      "type": "string" },
    { "name": "tradeDate",    "type": { "type": "int", "logicalType": "date" } },
    { "name": "side",         "type": { "type": "enum", "name": "Side", "symbols": ["BUY","SELL"] } },
    { "name": "symbol",       "type": "string" },
    { "name": "quantity",     "type": "long" },
    { "name": "price",        "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6 } },
    { "name": "currency",     "type": "string", "default": "USD" },
    { "name": "counterparty", "type": ["null","string"], "default": null }
  ]
}
