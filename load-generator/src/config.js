import {randomUUID} from 'node:crypto';

export function config(env = process.env) {
  const number = (key, fallback, min, max) => {
    const value = Number(env[key] ?? fallback);
    if (!Number.isFinite(value) || value < min || value > max) throw new Error(`${key} must be ${min}..${max}`);
    return value;
  };
  const rates = (env.RATES ?? '5,10,20').split(',').map(Number);
  if (!rates.length || rates.some(n => !Number.isFinite(n) || n < 1 || n > 1000)) throw new Error('RATES must contain numbers 1..1000');
  const url = (key, fallback) => {
    const value = env[key] ?? fallback;
    const u = new URL(value);
    if (!['http:', 'https:'].includes(u.protocol) || u.username || u.password || u.search || u.hash) throw new Error(`Invalid ${key}`);
    return value.replace(/\/$/, '');
  };
  const durationSeconds = number('DURATION_SECONDS', 30, 5, 3600), warmupSeconds = number('WARMUP_SECONDS', 10, 0, 300);
  if (rates.reduce((sum, r) => sum + Math.floor(r * durationSeconds), Math.floor(rates[0] * warmupSeconds)) > 100000) throw new Error('Run exceeds 100000 scheduled requests; split into runs');
  for (const name of ['CONCURRENCY', 'SAMPLE_COMPLETION_EVERY', 'OBSERVER_CONCURRENCY']) if (env[name] != null && !Number.isInteger(Number(env[name]))) throw new Error(`${name} must be an integer`);
  return {
    runId: randomUUID(),
    traderUrl: url('TRADER_URL', 'http://localhost:8081'),
    processorUrl: url('PROCESSOR_URL', 'http://localhost:8082'),
    rates,
    durationSeconds,
    warmupSeconds,
    baselineSeconds: number('BASELINE_SECONDS', 5, 0, 60),
    concurrency: number('CONCURRENCY', 16, 1, 256),
    observers: number('OBSERVER_CONCURRENCY', 32, 1, 256),
    drainSeconds: number('DRAIN_SECONDS', 60, 5, 600),
    sampleEvery: number('SAMPLE_COMPLETION_EVERY', 10, 1, 10000),
    httpP95Ms: number('HTTP_P95_MS', 250, 1, 60000),
    e2eP95Ms: number('E2E_P95_MS', 5000, 1, 600000),
    maxErrorRate: number('MAX_ERROR_RATE', .01, 0, 1),
    maxDroppedRate: number('MAX_DROPPED_RATE', .01, 0, 1),
    maxBacklogGrowth: number('MAX_BACKLOG_GROWTH_PER_SECOND', .5, 0, 10000),
    hotKeyRatio: number('HOT_KEY_RATIO', 0, 0, 1),
    dockerStats: env.DOCKER_STATS !== 'false',
    out: env.OUTPUT_DIR ?? 'reports/perf',
    traderPid: env.TRADER_PID,
    processorPid: env.PROCESSOR_PID
  };
}
