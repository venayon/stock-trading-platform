# Producer observability runbook

Implemented 2026-10-24. Decision: [ADR 0018](../adr/0018-producer-observability.md).

## Start and open

```sh
# From infra/, before the first Compose startup:
python3 scripts/init-monitoring-env.py
docker compose up -d --wait
```

Run the trader as described in [ms_trader README](../../ms_trader/README.md). Monitoring does not prevent starting while
the app is down; target-down alerts become active after one minute.

- [Trading dashboard](http://localhost:3000/d/stock-trading-overview) — Grafana username `admin`; initial password is
  `GRAFANA_ADMIN_PASSWORD` in ignored `infra/.env`. The setup script preserves existing credentials. Environment
  variables initialize a new Grafana database; editing the file does not rotate a password already stored in its volume.
- [Prometheus targets](http://localhost:9090/targets) — `ms-trader`, `ms-trade-processor` and `prometheus` should be UP.
- [Prometheus alerts](http://localhost:9090/alerts) — locally evaluated rules; external notifications are not
  configured.
- [App metrics](http://localhost:8081/actuator/prometheus), [liveness](http://localhost:8081/actuator/health/liveness), [readiness](http://localhost:8081/actuator/health/readiness).

Prometheus scrapes host.docker.internal:8081 every five seconds, with a four-second scrape timeout. Docker Desktop
access to the loopback app was verified. On Linux, the Compose host-gateway entry resolves the hostname but a
loopback-only app may need `SERVER_ADDRESS` set to a host interface reachable from Docker. If `SERVER_PORT` changes,
update `infra/prometheus/prometheus.yml` too.

## Metric contract

Every app meter has application=ms-trader; Prometheus adds job/instance. Counters reset on application restart. No trade
identity or symbol is a metric label.

| Prometheus name                                                  | Meaning                                                                                              |
|------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| trades_accepted_total                                            | Fresh inserts whose acknowledgment was observed by this process                                      |
| trades_idempotent_replays_total                                  | Same-key/same-canonical-payload responses observed by the service                                    |
| trades_published_total                                           | Kafka send ACKs observed; can include duplicate application sends                                    |
| trades_publication_completed_total                               | Successfully acknowledged, token-owned Mongo PUBLISHED transitions                                   |
| trades_publication_retries_total                                 | Failed publication/completion attempts requesting recovery                                           |
| trades_publication_stale_total                                   | ACKs whose stale lease token could not complete the document                                         |
| trades_errors_total{stage,code}                                  | Domain/relay errors with finite enum labels; malformed HTTP JSON is covered by HTTP status metrics   |
| trades_publication_seconds_*{outcome}                            | Attempt duration histogram, including send and completion; success/failure/stale                     |
| trades_pending_publications                                      | Last successful global Mongo pending count; NaN before first sample                                  |
| trades_pending_oldest_age_seconds                                | Age of the oldest sampled pending trade; zero after a successful empty sample                        |
| trades_backlog_poll_success                                      | Last sampling attempt succeeded: 1/0                                                                 |
| trades_backlog_last_success_timestamp_seconds                    | UTC epoch seconds of the last successful sample; zero before first success                           |
| http_server_requests_seconds_*                                   | HTTP status/count/latency; histograms enabled; dashboard/alert API panels filter mapped /api/ routes |
| jvm_memory_used_bytes, jvm_gc_pause_seconds_*, process_cpu_usage | JVM memory, GC and process CPU                                                                       |

Prometheus counters are not exact business totals. A Mongo write may succeed without the process observing its
acknowledgment, and a restart loses local meter state. MongoDB remains the source of accepted and completed logical
trade state. A failed sample retains old data; always inspect poll success and sample age before using pending gauges.
With multiple app instances sharing Mongo, queue gauges duplicate the same global state and must not be summed blindly.

The dashboard uses a 15-minute default range and five-second refresh. Percentiles and rates need multiple scrapes and
relevant traffic; an empty percentile after no requests is expected. Six consumer panels show
attempts/completions/duplicates, committed lag, processing latency, failures/DLT, sampling freshness and target
availability. Kafka broker internals, centralized log storage and distributed tracing are not included.

## Alerts and response

| Alert                | Condition                                                        | First response                                                                            |
|----------------------|------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| TraderDown           | Scrape failure for 1 minute                                      | Check app process, port, /actuator/prometheus and host gateway reachability               |
| PendingTradeTooOld   | Oldest pending >60s for 1 minute, with successful fresh sampling | Check publication errors, broker/Registry and pending document attempt/error/lease fields |
| BacklogSamplingStale | Failed poll or >30s since success, for 1 minute                  | Check MongoDB and scheduler logs; do not trust old queue values                           |
| PublicationFailures  | Failed attempts increased in 5 minutes                           | Correlate retry logs with tradeId; investigate send versus completion stage               |
| TraderHttpErrors     | API 5xx ratio >1% for 1 minute, at least 100 API requests/5m     | Inspect storage errors and readiness; retry uncertain requests with their original key    |

Thresholds are starter demo values, not agreed SLOs. Kafka outages should leave acceptance ready while work becomes
pending; Mongo outages affect readiness. Liveness must not fail simply because either dependency is down. Pending error
fields retain sanitized codes, and JSON logs include correlation context. Do not create a new idempotency key to recover
an uncertain request.

## Verification and configuration changes

```sh
# From ms_trader/:
mvn verify

# From infra/:
docker compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec -T prometheus promtool test rules /etc/prometheus/tests/alerts.test.yml
docker compose restart prometheus
```

Prometheus config/rules are loaded on startup; restart after editing them. Grafana polls provisioned dashboards every 30
seconds; edit the committed JSON rather than relying on UI changes. Datasource changes require a Grafana restart.
Retention is seven days for Prometheus; named volumes persist monitoring state. Stopping the stack retains those
volumes.

Structured console logging uses ECS JSON. Filter by correlationId to join HTTP and relay activity; scoped context is
restored after handling. Do not add identities as metric tags or repeat the same field in MDC and structured log
arguments.

## Consumer monitoring

The processor exposes metrics on localhost:8082 and samples end offsets minus committed group offsets every five
seconds. Check trades_lag_poll_success and sample age before trusting lag; failed polls retain earlier values. DLT
recovery can reduce lag without producing a COMPLETE business result. ProcessorDown, ProcessorDlt, ProcessorLagHigh (>
1000 for 2m with a fresh successful sample), and ProcessorLagSamplingStale rules complement the producer alerts.
Consumer and producer counters reset independently. See the [processor contract](../../ms_trade_processor/README.md).
