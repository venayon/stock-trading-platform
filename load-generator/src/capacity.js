import {request} from "@playwright/test";
import {performance} from "node:perf_hooks";
import {appendFile, mkdir, writeFile} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {call, key, route, sleep, trade, waitComplete} from "./client.js";
import {resources} from "./resources.js";
import {latency, resourceSummary, slope} from "./stats.js";

export async function runCapacity(config) {
  const dir = path.resolve(config.out, config.runId);
  await mkdir(dir, {recursive: true});
  // Writes are serialized; raw artifacts contain synthetic IDs and whitelisted config, never environment dumps.
  let writing = Promise.resolve();
  const write = (kind, row) => {
    writing = writing.then(() =>
      appendFile(path.join(dir, kind + ".ndjson"), JSON.stringify(row) + "\n"),
    );
  };
  const resource = await resources(config, write),
    report = {
      runId: config.runId,
      createdAt: new Date().toISOString(),
      config,
      environment: {
        node: process.version,
        platform: os.platform(),
        architecture: os.arch(),
        logicalHostCpus: os.cpus().length,
        hostMemoryGiB: os.totalmem() / 1024 ** 3,
        pids: resource.pids,
      },
      stages: [],
      limitations: [
        "Local shared-host measurement; no production capacity claim.",
        "Sampled completion latency includes polling delay and is an observed upper bound.",
        "CPU/memory candidates must be rerun under actual CPU limits and JVM heap/memory caps.",
        "Fixed-arrival load and monitoring/reconciliation traffic share the generator and host.",
      ],
    };
  const control = await request.newContext({timeout: 10000});
  const contexts = await Promise.all(
    Array.from({length: config.concurrency}, () =>
      request.newContext({baseURL: config.traderUrl, timeout: 10000}),
    ),
  );
  const trackers = await Promise.all(
    Array.from({length: config.observers}, () =>
      request.newContext({timeout: 5000}),
    ),
  );
  let sequence = 0;
  try {
    for (const base of [config.traderUrl, config.processorUrl]) {
      const health = await call(
        control,
        "GET",
        base + "/actuator/health/readiness",
      );
      if (health.status !== 200) throw new Error(`Readiness failed: ${base}`);
    }
    await resource.sample();
    await sleep(config.baselineSeconds * 1000);
    report.baseline = Object.fromEntries(
      ["trader", "processor"].map((name) => [
        name,
        resourceSummary(resource.samples, name),
      ]),
    );

    async function stage(rate, seconds, label) {
      await resource.sample();
      const start = performance.now(),
        startEpoch = Date.now(),
        expected = Math.floor(rate * seconds),
        pending = new Set(),
        accepted = [],
        rows = [],
        completionJobs = [],
        completionMs = [],
        completionErrors = [];
      let dropped = 0,
        schedulerDropped = 0,
        concurrencyDropped = 0,
        polls = 0;
      const freeTrackers = [...trackers];
      const free = [...contexts];

      async function issue(context, index, due) {
        const seq = sequence++,
          payload = trade(seq, config.hotKeyRatio),
          idempotency = `${config.runId}-${label}-${seq}`,
          began = performance.now();
        const row = {
          label,
          sequence: seq,
          at: Date.now(),
          key: idempotency,
          scheduleDelayMs: began - due,
        };
        try {
          const result = await call(context, "POST", "/api/trades", {
            data: payload,
            headers: {
              "Idempotency-Key": idempotency,
              "X-Correlation-ID": key().slice(-36),
            },
          });
          Object.assign(row, {status: result.status, ms: result.ms});
          if (
            result.status === 202 &&
            typeof result.body.tradeId === "string"
          ) {
            accepted.push({id: result.body.tradeId, payload, start: began});
            row.tradeId = result.body.tradeId;
            if (index % config.sampleEvery === 0) {
              if (freeTrackers.length === 0)
                completionErrors.push("Completion observer saturated");
              else {
                const tracker = freeTrackers.pop();
                const job = (async () => {
                  try {
                    const done = await waitComplete(
                      tracker,
                      config.processorUrl,
                      result.body.tradeId,
                      {
                        timeoutMs: config.drainSeconds * 1000,
                        onPoll: () => polls++,
                      },
                    );
                    if (
                      done.targetPartition !==
                      route(payload.side, payload.symbol)
                    )
                      throw new Error("Wrong route");
                    completionMs.push(performance.now() - began);
                  } catch (error) {
                    completionErrors.push(error.message);
                  } finally {
                    freeTrackers.push(tracker);
                  }
                })();
                completionJobs.push(job);
              }
            }
          } else row.error = `Unexpected HTTP ${result.status}`;
        } catch (error) {
          row.error = error.message;
          row.ms = performance.now() - began;
          row.status = 0;
        } finally {
          rows.push(row);
          write("requests", row);
          free.push(context);
        }
      }

      for (let i = 0; i < expected; i++) {
        const due = start + (i * 1000) / rate;
        if (performance.now() > due + 1000 / rate) {
          dropped++;
          schedulerDropped++;
          continue;
        }
        await sleep(Math.max(0, due - performance.now()));
        if (!free.length) {
          dropped++;
          concurrencyDropped++;
          continue;
        }
        const context = free.pop();
        let job;
        job = issue(context, i, due).finally(() => pending.delete(job));
        pending.add(job);
      }
      await sleep(Math.max(0, start + seconds * 1000 - performance.now()));
      const offeredEnd = Date.now();
      await Promise.all(pending);
      const acceptanceEnd = Date.now();
      await resource.sample();
      // Every accepted ID is reconciled, not only the completion-latency sample.
      const deadline = performance.now() + config.drainSeconds * 1000;
      let cursor = 0,
        reconciled = 0;
      const mismatches = [];
      await Promise.all(
        trackers.map(async (tracker) => {
          while (cursor < accepted.length) {
            const item = accepted[cursor++];
            try {
              const remaining = deadline - performance.now();
              if (remaining <= 0) throw new Error("Drain deadline exceeded");
              const done = await waitComplete(
                tracker,
                config.processorUrl,
                item.id,
                {timeoutMs: remaining, onPoll: () => polls++},
              );
              if (
                done.tradeId !== item.id ||
                done.targetPartition !==
                route(item.payload.side, item.payload.symbol) ||
                done.trade.price !== item.payload.price ||
                done.trade.quantity !== item.payload.quantity ||
                done.output.topic !== "stock-trades-processed"
              )
                throw new Error("Processed contract mismatch");
              reconciled++;
            } catch (error) {
              mismatches.push({tradeId: item.id, error: error.message});
            }
          }
        }),
      );
      await Promise.all(completionJobs);
      await resource.sample();
      const window = resource.samples.filter(
          (s) => s.at >= startEpoch && s.at <= acceptanceEnd,
        ),
        validPending = window.filter(
          (s) =>
            s.services.trader?.pendingPollSuccess === 1 &&
            s.at / 1000 - s.services.trader.pendingSampleAt < 15 &&
            s.services.trader.pending != null,
        );
      const growth = slope(
        validPending.map((s) => [s.at, s.services.trader.pending]),
      );
      const validLag = window.filter(
        (s) =>
          s.services.processor?.lagPollSuccess === 1 &&
          s.at / 1000 - s.services.processor.lagSampleAt < 15 &&
          s.services.processor.lag != null,
      );
      const lagGrowth = slope(
        validLag.map((s) => [s.at, s.services.processor.lag]),
      );
      const successes = rows.filter((r) => r.status === 202 && !r.error),
        errors = rows.length - successes.length;
      const successLatency = latency(successes.map((r) => r.ms)),
        e2e = latency(completionMs);
      const measured = Object.fromEntries(
        ["trader", "processor"].map((name) => [
          name,
          resourceSummary(window, name),
        ]),
      );
      const errorRate = rows.length ? errors / rows.length : 1,
        droppedRate = expected ? dropped / expected : 1;
      const fresh =
        window.length >= 5 &&
        window.every(
          (s) =>
            s.services.trader?.cpuNs != null &&
            s.services.processor?.cpuNs != null,
        ) &&
        validPending.length >= 5 &&
        validLag.length >= 5;
      const gates = {
        httpLatency:
          successLatency.p95Ms != null &&
          successLatency.p95Ms <= config.httpP95Ms,
        completionLatency: e2e.p95Ms != null && e2e.p95Ms <= config.e2eP95Ms,
        errors: errorRate <= config.maxErrorRate,
        unambiguousOutcomes: rows.every((r) => r.status > 0 && r.status < 500),
        arrivalSchedule: droppedRate <= config.maxDroppedRate,
        reconciliation:
          reconciled === accepted.length &&
          new Set(accepted.map((a) => a.id)).size === accepted.length &&
          accepted.length > 0,
        completionObserver: completionErrors.length === 0,
        backlog: growth != null && growth <= config.maxBacklogGrowth,
        consumerBacklog:
          lagGrowth != null && lagGrowth <= config.maxBacklogGrowth,
        telemetry:
          fresh && !Object.values(measured).some((s) => s.counterReset),
      };
      const result = {
        label,
        offeredRps: rate,
        durationSeconds: seconds,
        startEpochMs: startEpoch,
        offeredEndEpochMs: offeredEnd,
        acceptanceEndEpochMs: acceptanceEnd,
        endEpochMs: Date.now(),
        expectedArrivals: expected,
        issued: rows.length,
        dropped,
        schedulerDropped,
        concurrencyDropped,
        errorRate,
        droppedRate,
        accepted: accepted.length,
        acceptedRps: accepted.length / ((acceptanceEnd - startEpoch) / 1000),
        httpAll: latency(rows.map((r) => r.ms)),
        httpSuccessful: successLatency,
        scheduleDelay: latency(rows.map((r) => r.scheduleDelayMs)),
        sampledCompletion: e2e,
        completionErrors,
        observerGetRequests: polls,
        reconciled,
        mismatches,
        backlogGrowthPerSecond: growth,
        consumerLagGrowthPerSecond: lagGrowth,
        resources: measured,
        gates,
        passed: Object.values(gates).every(Boolean),
      };
      console.log(
        `${label}: offered ${rate}/s, accepted ${accepted.length}, p95 ${successLatency.p95Ms?.toFixed(1)}ms, complete ${reconciled}/${accepted.length}, ${result.passed ? "PASS" : "FAIL"}`,
      );
      return result;
    }

    if (config.warmupSeconds > 0)
      report.warmup = await stage(
        config.rates[0],
        config.warmupSeconds,
        "warmup",
      );
    for (const [index, rate] of config.rates.entries())
      report.stages.push(
        await stage(rate, config.durationSeconds, `stage-${index + 1}`),
      );
    report.highestPassingOfferedRps = Math.max(
      0,
      ...report.stages.filter((s) => s.passed).map((s) => s.offeredRps),
    );
    const passing = report.stages.filter(
      (s) =>
        s.passed &&
        s.durationSeconds >= 30 &&
        s.sampledCompletion.count >= 10 &&
        Object.values(s.resources).every((r) => r.candidate),
    );
    const best = passing.sort((a, b) => b.offeredRps - a.offeredRps)[0];
    report.recommendedTrial = best
      ? {
        offeredRps: best.offeredRps,
        resources: Object.fromEntries(
          Object.entries(best.resources).map(([name, r]) => [
            name,
            r.candidate,
          ]),
        ),
        status: "UNVALIDATED_TRIAL_BUDGET",
        requires:
          "Repeat with actual CPU/memory limits, longer soak and target workload",
      }
      : null;
    report.result = report.stages.every((s) => s.passed) ? "PASS" : "FAIL";
  } catch (error) {
    report.result = "ERROR";
    report.error = error.stack;
  } finally {
    await Promise.all(
      [...contexts, ...trackers, control].map((c) => c.dispose()),
    );
    await resource.stop();
    await writing;
  }
  report.warnings = [...resource.warnings];
  report.dockerSamples = resource.docker.length;
  const dockerNames = [
    ...new Set(resource.docker.flatMap((s) => s.containers.map((c) => c.name))),
  ];
  report.infrastructure = dockerNames.map((name) => {
    const values = resource.docker.flatMap((s) =>
      s.containers.filter((c) => c.name === name),
    );
    const cpus = values.map((v) => v.cpuCores).filter(Number.isFinite);
    const memory = values.map((v) => v.memoryBytes).filter(Number.isFinite);
    return {
      name,
      peakCpuCores: cpus.length ? Math.max(...cpus) : null,
      peakMemoryMiB: memory.length ? Math.max(...memory) / 1024 ** 2 : null,
    };
  });
  report.generator = resourceSummary(resource.samples.map((s) => ({
    at: s.at, services: {generator: {...s.generator, startTime: 0}},
  })), "generator");
  report.artifactDirectory = dir;
  await writeFile(
    path.join(dir, "summary.json"),
    JSON.stringify(report, null, 2) + "\n",
  );
  const f = (n) => (n == null ? "n/a" : n.toFixed(2));
  const lines = [
    `# Local capacity run ${report.runId}`,
    `\nResult: **${report.result}**. Highest passing offered stage: **${report.highestPassingOfferedRps ?? 0} requests/s**. This is a tested local stage, not maximum production capacity.\n`,
    "| Offered RPS | Accepted RPS | POST p95 ms | Sampled COMPLETE p95 ms | Reconciled | Pending / lag slope /s | Result |",
    "|---:|---:|---:|---:|---:|---:|---|",
    ...report.stages.map(
      (s) =>
        `| ${s.offeredRps} | ${f(s.acceptedRps)} | ${f(s.httpSuccessful.p95Ms)} | ${f(s.sampledCompletion.p95Ms)} | ${s.reconciled}/${s.accepted} | ${f(s.backlogGrowthPerSecond)} / ${f(s.consumerLagGrowthPerSecond)} | ${s.passed ? "PASS" : "FAIL"} |`,
    ),
    "\n## Measured application resources\n",
    "| Stage | Service | Average CPU cores | CPU p95 cores | Peak RSS MiB | Peak heap used MiB | Trial CPU request/limit | Trial heap / memory MiB |",
    "|---|---|---:|---:|---:|---:|---|---|",
    ...report.stages.flatMap((s) =>
      Object.entries(s.resources).map(
        ([name, r]) =>
          `| ${s.label} | ${name} | ${f(r.averageCpuCores)} | ${f(r.p95CpuCores)} | ${f(r.peakRssMiB)} | ${f(r.peakHeapUsedMiB)} | ${r.candidate ? `${r.candidate.cpuRequestCores}/${r.candidate.cpuLimitCores}` : "insufficient data"} | ${r.candidate ? `${r.candidate.heapCapMiB}/${r.candidate.memoryLimitMiB}` : "insufficient data"} |`,
      ),
    ),
    "\nCandidate budgets are **unvalidated** and must be retested with actual JVM/container limits. Short runs cannot establish leak-free behavior or production SLOs.\n",
    ...report.limitations.map((l) => `- ${l}`),
    ...report.warnings.map((l) => `- Warning: ${l}`),
  ];
  await writeFile(path.join(dir, "summary.md"), lines.join("\n") + "\n");
  return report;
}
