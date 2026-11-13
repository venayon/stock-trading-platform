export function quantile(values, q) {
  if (!values.length) return null;
  const a = [...values].sort((x, y) => x - y);
  return a[Math.min(a.length - 1, Math.max(0, Math.ceil(q * a.length) - 1))];
}

export function latency(values) {
  return {
    count: values.length,
    p50Ms: quantile(values, .5),
    p95Ms: quantile(values, .95),
    p99Ms: quantile(values, .99),
    maxMs: values.length ? Math.max(...values) : null
  };
}

export function slope(points) {
  if (points.length < 2) return null;
  const x0 = points[0][0], p = points.map(([x, y]) => [(x - x0) / 1000, y]), n = p.length,
    mx = p.reduce((s, v) => s + v[0], 0) / n, my = p.reduce((s, v) => s + v[1], 0) / n;
  const denominator = p.reduce((s, v) => s + (v[0] - mx) ** 2, 0);
  return denominator ? p.reduce((s, v) => s + (v[0] - mx) * (v[1] - my), 0) / denominator : null;
}

export function prometheus(text) {
  const rows = [];
  for (const line of text.split('\n')) {
    if (line.startsWith('#')) continue;
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)/);
    if (match) {
      const value = Number(match[3]);
      if (Number.isFinite(value)) rows.push({name: match[1], labels: match[2] ?? '', value});
    }
  }
  return rows;
}

export function fromMetrics(text) {
  const rows = prometheus(text), one = name => rows.find(r => r.name === name)?.value ?? null, sum = (name, filter) => {
    const values = rows.filter(r => r.name === name && (!filter || filter(r.labels)));
    return values.length ? values.reduce((s, r) => s + r.value, 0) : null;
  };
  return {
    cpuNs: one('process_cpu_time_ns_total'),
    startTime: one('process_start_time_seconds'),
    cpuCount: one('system_cpu_count'),
    heapUsedBytes: sum('jvm_memory_used_bytes', l => l.includes('area="heap"')),
    nonHeapUsedBytes: sum('jvm_memory_used_bytes', l => l.includes('area="nonheap"')),
    directBufferBytes: sum('jvm_buffer_memory_used_bytes', l => l.includes('id="direct"')),
    pending: one('trades_pending_publications'),
    pendingPollSuccess: one('trades_backlog_poll_success'),
    pendingSampleAt: one('trades_backlog_last_success_timestamp_seconds'),
    lag: sum('trades_consumer_lag'),
    lagPollSuccess: one('trades_lag_poll_success'),
    lagSampleAt: one('trades_lag_last_success_timestamp_seconds')
  };
}

const peak = (samples, field) => {
  const a = samples.map(s => s[field]).filter(Number.isFinite);
  return a.length ? Math.max(...a) : null;
};
const round = (n, step) => Math.ceil(n / step) * step;

export function resourceSummary(samples, name) {
  const points = samples.filter(s => s.services?.[name]?.cpuNs != null).map(s => ({at: s.at, ...s.services[name]}));
  const pairs = [];
  let reset = false;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1], b = points[i];
    if (a.startTime !== b.startTime || b.cpuNs < a.cpuNs) {
      reset = true;
      continue;
    }
    const dt = (b.at - a.at) * 1e6;
    if (dt > 0) pairs.push((b.cpuNs - a.cpuNs) / dt);
  }
  const first = points[0], last = points.at(-1),
    mean = !reset && points.length > 1 ? (last.cpuNs - first.cpuNs) / ((last.at - first.at) * 1e6) : null;
  const rss = peak(points, 'rssBytes'), heap = peak(points, 'heapUsedBytes');
  const MiB = 1024 ** 2;
  const cpuP95 = quantile(pairs, .95);
  let candidate = null;
  if (rss != null && heap != null && mean != null && cpuP95 != null && !reset) {
    const heapBytes = round(Math.max(128 * MiB, heap * 1.5), 64 * MiB);
    const native = Math.max(128 * MiB, rss - heap);
    candidate = {
      cpuRequestCores: round(mean / .6, .25),
      cpuLimitCores: round(Math.max(mean / .6, cpuP95 / .8), .25),
      heapCapMiB: heapBytes / MiB,
      memoryLimitMiB: round(Math.max(rss * 1.5, heapBytes + native * 1.5), 64 * MiB) / MiB,
      status: 'UNVALIDATED_TRIAL_BUDGET'
    };
  }
  return {
    samples: points.length,
    counterReset: reset,
    averageCpuCores: mean,
    p95CpuCores: cpuP95,
    peakRssMiB: rss == null ? null : rss / MiB,
    peakHeapUsedMiB: heap == null ? null : heap / MiB,
    peakNonHeapUsedMiB: peak(points, 'nonHeapUsedBytes') == null ? null : peak(points, 'nonHeapUsedBytes') / MiB,
    candidate
  };
}
