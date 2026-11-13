# Playwright API and capacity tests

SDET coverage for the running stock-trading demo: functional REST contracts, deterministic ingestion load, sampled
end-to-end latency, full accepted-trade reconciliation, and measured CPU/memory sizing evidence.

## Prerequisites

Run the existing [infrastructure](../infra/README.md), [trader](../ms_trader/README.md)
and [processor](../ms_trade_processor/README.md). Both /actuator/health/readiness endpoints must return 200. Install
Node 20+ and dependencies:

```sh
npm ci
npm run test:unit
npm test
```

These are API-only tests. No browser installation or UI automation is required. Playwright is pinned to 1.49.1 to match
the project baseline; package-lock.json pins transitive dependencies. The measured runs used Node 20.16.0. Initial npm
downloads need network access; tests use local services afterward.

Tests deliberately create synthetic trades with unique idempotency keys. They do not delete application data, reset
Kafka offsets, restart services or erase volumes. Run against a quiet test environment for capacity comparisons. No
credentials are required by these local API endpoints.

## What is covered

- **22 API cases**: acceptance/processing and routing, same-key concurrency, replay and conflict behavior, validation
  boundaries, exact decimal/64-bit values, bounded queries, health and Swagger.
- **7 analyzer cases**: quantiles, backlog slope, metric parsing, CPU units, restart detection, routing, configuration
  and missing-RSS behavior.
- **One parameterized performance scenario**: fixed-arrival rate stages with configurable duration, concurrency,
  thresholds, resource sampling and reconciliation. A CLI entry point runs the same harness.

```sh
# Passing measured local regression profile:
RATES=10 DURATION_SECONDS=30 npm run test:perf

# Capacity exploration; 20 RPS failed the current pipeline's configured gates:
RATES=5,10,20 DURATION_SECONDS=30 npm run perf

# Brief harness check, not sizing evidence:
RATES=2 DURATION_SECONDS=6 WARMUP_SECONDS=0 BASELINE_SECONDS=1 \
  SAMPLE_COMPLETION_EVERY=2 DOCKER_STATS=false npm run test:perf
```

Performance tests run sequentially. Do not parallelize stage tests against one environment: shared queues, caches and
resources would confound the measurements. Playwright's generic “slow test” suggestion is not a reason to split this
experiment into simultaneous loads.

## Configuration

| Variable                      | Default               | Meaning                                                                           |
|-------------------------------|-----------------------|-----------------------------------------------------------------------------------|
| TRADER_URL                    | http://localhost:8081 | Producer origin                                                                   |
| PROCESSOR_URL                 | http://localhost:8082 | Processor origin                                                                  |
| RATES                         | 5,10,20               | Offered fresh POST requests per second, one stage at a time                       |
| DURATION_SECONDS              | 30                    | Measured duration per stage                                                       |
| WARMUP_SECONDS                | 10                    | Separate excluded warm-up at the first rate                                       |
| BASELINE_SECONDS              | 5                     | Idle resource sample window before warm-up                                        |
| CONCURRENCY                   | 16                    | Maximum concurrent POST requests                                                  |
| OBSERVER_CONCURRENCY          | 32                    | Maximum concurrent sampled completion observers; also reconciliation worker count |
| SAMPLE_COMPLETION_EVERY       | 10                    | Poll one selected trade per ten offered slots during the active stage             |
| DRAIN_SECONDS                 | 60                    | Bounded completion/reconciliation wait                                            |
| HTTP_P95_MS                   | 250                   | Successful acceptance p95 ceiling                                                 |
| E2E_P95_MS                    | 5000                  | Sampled client-observed completion p95 ceiling                                    |
| MAX_ERROR_RATE                | 0.01                  | Unexpected response/request error fraction ceiling                                |
| MAX_DROPPED_RATE              | 0.01                  | Unissued arrival-slot fraction ceiling                                            |
| MAX_BACKLOG_GROWTH_PER_SECOND | 0.5                   | Maximum fitted slope for producer pending count and consumer committed lag        |
| HOT_KEY_RATIO                 | 0                     | Deterministic fraction directed to AAPL to examine key skew                       |
| DOCKER_STATS                  | true                  | Sample stock-trading Compose infrastructure with Docker CLI                       |
| TRADER_PID / PROCESSOR_PID    | auto                  | Optional actual host JVM PIDs for RSS sampling                                    |
| OUTPUT_DIR                    | reports/perf          | Root for unique per-run artifacts                                                 |

Thresholds are starter demo assumptions, not approved business SLOs. Adjust them before using the test as a deployment
gate. URL credentials/query strings are rejected to avoid leaking tokens into reports. Concurrency values must be
integers. Runs are bounded to 100,000 scheduled requests including warm-up.

## Load and latency method

The workload alternates BUY/SELL through a deterministic twelve-symbol cycle and bounded quantities/exact decimal
prices. This is a synthetic cycle, not a market trace; side and symbol selection are correlated. Keys include a unique
run ID and sequence. Automatic request retries are off so errors are not hidden.

A monotonic clock schedules arrivals independently of response completion. Slots missed because the generator runs late
or all POST contexts are busy are counted separately. The harness does not quietly convert server slowness into a lower
offered rate. Its JavaScript scheduler still has limits; inspect dropped slots, generator CPU and scheduling-delay
measurements.

POST latency measures the client call through the buffered response body. All-attempt and successful-response
distributions are stored separately. A 202 only measures durable acceptance, not completed Kafka processing.

Sampled completion observers start during load and poll every 500ms. Their p50/p95/p99 measure time from POST start
until COMPLETE is observed. These are **upper bounds**, including network/poll scheduling delay, not exact server
completion timestamps. Saturated observers fail the measurement-quality gate rather than silently omitting the problem.
The report includes sample counts; a small sample is not a statistically strong tail estimate.

After each stage, every accepted 202 trade ID is queried and checked for COMPLETE, expected price/quantity and correct
routed output. Drain/reconciliation time is separate from active-load resource windows. GET observer traffic is counted
separately; metrics sampling and external Prometheus traffic add further monitoring overhead. Reconciliation can
generate a burst after load.

The next stage starts after drain. Pending and lag samples are global state, so other callers or old retained work can
still affect results. Use the raw telemetry to check starting state and avoid concurrent experiments.

## Measurement and sizing

The sampler reads both services' Actuator Prometheus endpoints about once per second. It records cumulative process CPU
time, heap/nonheap used, direct-buffer use, process start time, pending backlog, committed lag and freshness.
Missing/failed samples remain missing; they are not converted into zero load.

**CPU cores = CPU-time delta in nanoseconds / elapsed-time delta in nanoseconds.** One CPU core means one CPU-second per
wall second, not one percent of a 16-core host. Average and sampled p95 CPU demand are reported. Process restart/counter
reset invalidates the affected sizing window.

On macOS/Linux, lsof can discover the localhost port owner and ps reports RSS in KiB. The sampler verifies the owner is
Java before treating its RSS as application memory. Explicit PIDs must also identify host JVMs. Do not use Docker's
port-proxy PID as Java RSS. Remote/VM/container targets need appropriate process access; without verified RSS, no
application memory-budget candidate is produced. Docker stats is separate container/cgroup evidence, not a substitute
silently labeled as JVM RSS.

Docker sampling runs roughly every five seconds for the stock-trading Compose project. It records per-container CPU
cores and CLI-reported memory usage. Infrastructure peaks span the full experiment, including baseline/warm-up/drain,
while app stage summaries use the active measurement window. Generator worker CPU/RSS is also recorded; Playwright
parent-process overhead is not included in that worker figure.

For an eligible passing stage, the harness proposes **unvalidated trial budgets**:

- CPU request: average observed cores / 0.60, rounded up to 0.25 core.
- CPU limit: maximum of that request and sampled p95 cores / 0.80, rounded up to 0.25 core.
- Heap trial: 1.5 × peak used heap, rounded up to 64 MiB, minimum 128 MiB.
- Memory trial: at least 1.5 × peak RSS and at least the heap trial plus 1.5 × max (128 MiB, RSS − used heap), rounded
  to 64 MiB.

RSS minus used heap is a conservative reserve proxy: it also includes resident free heap and other pages, not just
native allocations. Used heap is not Xmx, and Xmx is not total process/container memory. Candidates are selected only
from passing stages lasting at least 30 seconds with at least ten completion samples and valid CPU/RSS data. Short smoke
runs cannot nominate a final trial profile.

These formulas are explicit experiment assumptions, not universal JVM sizing rules. The apps were not CPU/RSS-limited in
our baseline. Validate candidates under actual OS/container quotas and the chosen Xmx, observe CPU throttling/GC/native
memory, and repeat cold starts and longer soaks. ActiveProcessorCount changes JVM ergonomics; it is not a CPU quota. Do
not extrapolate beyond measured load or assume more CPU fixes a scheduler/IO bottleneck.

## Gates and artifacts

A stage checks acceptance p95, sampled completion p95, errors, unambiguous request outcomes, dropped arrivals, full
accepted-ID reconciliation, observer health, both queue-growth slopes and fresh telemetry/no process reset. Network/5xx
outcomes fail the unambiguous-outcome gate because a write may have succeeded without returning an ID; preserve the
original key for investigation.

The full scenario fails if any measured stage fails. It does not turn an intentionally overloaded stage green or retry
it away. The highest passing offered stage is reported separately. Even a passing short run is not a
maximum-production-capacity claim.

- `reports/api-html/index.html`, `reports/api-junit.xml`: functional results.
- `reports/perf-html/index.html`, `reports/perf-junit.xml`: performance scenario results.
- `reports/perf/<runId>/summary.md` and `summary.json`: stage gates, distributions, measurements and trial budgets.
- `requests.ndjson`: per-request status, timing, sequence and synthetic key/ID.
- `resources.ndjson`: app and generator samples.
- `docker.ndjson`: infrastructure samples when enabled.
- Playwright's test result includes an attached summary JSON.

CLI exits 0 for a passing matrix and 1 for failed gates or errors. JUnit is ready for a future CI runner; a full
environment-starting CI workflow is not implemented. Reports/dependencies are ignored by Git; no entire environment or
authentication state is dumped.

See the [measured local baseline](../docs/performance/BASELINE_2026-10-24.md)
and [JSON evidence](../docs/performance/local-baseline-2026-10-24.json). The Playwright API suite passed 22 cases,
analyzer suite seven cases, and the 10 RPS regression profile passed. The 20 RPS exploration correctly failed
backlog/completion gates while all accepted trades eventually reconciled.
