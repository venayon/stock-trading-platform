# ADR 0020 — Playwright API correctness and measured capacity testing

Date: 2026-10-24 · Status: Accepted for the requested SDET/API/performance slice

Use Playwright APIRequestContext for functional tests and the performance harness, with no browser. Pin 1.49.1 and
commit package-lock.json for the original project baseline. Reuse one harness from a Playwright performance scenario and
a CLI. Keep API, analyzer and performance reports separate.

Schedule fresh synthetic POST arrivals at a configured rate independently of response completion. Bound concurrent POSTs
and completion observers. Count scheduler/concurrency drops instead of silently allowing response latency to reduce
offered load. Disable automatic retries, dispose API responses, and preserve raw request/resource evidence. Every
accepted 202 ID must reconcile to a COMPLETE result with the expected route and values. Ambiguous network/5xx outcomes
invalidate certification of the run even if the percentage error budget alone would permit them.

Separate acceptance latency from sampled end-to-end completion. Completion observers run during load; polling adds an
explicit upper-bound uncertainty. Report sample counts and fail observer saturation. Keep warm-up and final drain out of
the measured load window. Gate both producer-pending growth and consumer committed-lag growth using fresh successful
samples.

Measure application CPU from cumulative process CPU-time deltas in core equivalents, used JVM memory from Actuator, and
resident memory from verified host Java PIDs. Docker CLI samples describe infrastructure containers separately. Missing
RSS/telemetry is unknown, not zero, and cannot nominate a memory budget. Process restarts invalidate an affected sizing
window. Monitor generator overhead as well.

Trial budgets use explicit utilization/headroom formulas, not production guarantees. Select a candidate only from a
passing stage of at least 30 seconds with adequate samples and real CPU/RSS. Validate later under actual CPU/memory
limits and representative longer workloads. The runner-up for high-scale arrival generation is a dedicated tool such as
k6; switch when Playwright scheduling/observer overhead becomes the limiting factor. Retain these Playwright correctness
tests regardless of that choice.

Evidence: 22 API cases and seven analyzer cases passed; low-rate harness checks and the 10 RPS regression profile
passed. The measured 10/20 RPS matrix correctly failed at 20 due to completion latency and growing producer pending
work, while all accepted trades eventually reconciled. [Baseline report](../performance/BASELINE_2026-10-24.md) records
measured CPU/RSS/latency and unvalidated trial budgets. No application tuning, resource caps or production sizing
certificate is claimed by this change.
