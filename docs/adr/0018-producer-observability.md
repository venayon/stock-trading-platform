# ADR 0018 — Producer observability with Prometheus, provisioned Grafana and correlated logs

Date: 2026-10-24 · Status: Accepted for the observability slice

## Context

The owner requested observability after validation, MongoDB acceptance and asynchronous Kafka publication. The processor
does not exist yet, so consumer lag is not available. Monitoring must distinguish durable backlog from process counters
and must not compromise acceptance during a Kafka outage.

## Decision

Use Boot 3.5 Actuator and its managed Micrometer Prometheus registry. Expose only health, info and prometheus. Liveness
includes the application state; readiness also requires MongoDB, the acceptance dependency. Kafka failures affect
publication metrics rather than acceptance readiness. Scrape the host-run service every five seconds through
host.docker.internal; the existing loopback API remains usable on the verified Docker Desktop environment.

Instrument new acceptance, idempotent replays, acknowledged sends, completed publication transitions, retries, stale
completions and bounded stage/code errors. Time publication attempts and enable HTTP request histograms. Acknowledgment
and completion counters are deliberately separate: a completion write may fail after Kafka accepted the event. Counters
are process observations, not an audit ledger; restarts or uncertain acknowledgments can make them differ from Mongo
counts. Use rate/increase functions that tolerate resets.

Sample pending count and oldest createdAt from MongoDB every five seconds, outside the scrape request. Before a
successful sample, pending gauges are NaN. On failure, retain the prior snapshot and expose success=0 plus the timestamp
of the last success. Age continues to grow from the sampled oldest timestamp. Two scheduling threads keep the sampler
from waiting behind ordinary relay sends. Consumers must assess sample freshness before trusting a displayed queue
value.

Provision Prometheus 2.55.1, Grafana 11.6.0, a datasource and the 14-panel Stock Trading Overview dashboard from
repository files. Use stable datasource/dashboard UIDs. Store TSDB and Grafana state in named volumes. Grafana's initial
administrator password is generated into ignored infra/.env, never embedded in committed Compose/YAML/JSON. Dashboard
changes belong in the source JSON; UI edits cannot overwrite the provisioned design.

Evaluate alerts for scrape failure, old pending work, failed/stale backlog sampling, publication attempt failures and
API HTTP 5xx ratio. HTTP ratio excludes metrics/static traffic and requires at least 100 API requests in five minutes.
Alerts are visible in Prometheus; no Alertmanager or external notification channel is configured. No fake consumer lag
or processor scrape target is added.

Emit ECS JSON console logs with scoped tradeId and correlationId fields. Restore MDC after requests and worker scopes,
including exceptions, and avoid duplicating the same JSON key in both MDC and key-value arguments. Never use trade IDs,
keys, symbols, correlation IDs or exception messages as metric labels. Error labels are enums, and timer outcomes are a
fixed enum.

## Alternatives and consequences

Manual Grafana setup is the runner-up, suitable only for temporary experiments; provisioning makes the reviewed
dashboard repeatable. Querying Mongo inside every scrape risks blocking metrics during an outage; rejected. Reporting
zero when sampling fails creates false health; rejected. Client-native consumer metrics cannot supply producer-only
consumer lag; defer it until a real group exists. Loki/tracing and broker-level JMX exporters are outside this slice.

The monitoring stack is local and unauthenticated Prometheus access is bound to loopback. Grafana requires its local
administrator login. Linux host-run deployments may need an application bind-address override or a containerized
application to make the Docker host gateway reachable. Changing the app port requires updating the scrape target. Queue
gauges represent the shared database while counters represent this process; revise aggregation when adding multiple app
instances.

## Verification

116 Java cases pass (104 unit/API and 12 real-container cases), including acceptance/replay metric semantics, ACK versus
completion, stale backlog preservation, real Mongo aggregation, probe dependency behavior, MDC cleanup and the
structured-log duplicate-field regression. Promtool configuration/rule validation and rule fixtures pass.

Live checks verified target UP, a synthetic acceptance/replay/publication reflected in scraped counters, healthy probes,
authenticated Grafana datasource/dashboard provisioning and successful execution of all 14 panel queries. Emitted
business logs parse as JSON and contain identity fields without appender errors. Browser rendering was not visually
verified because no browser was connected.

References: [Boot 3.5 metrics](https://docs.spring.io/spring-boot/3.5/reference/actuator/metrics.html), [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).
