import test from 'node:test';
import assert from 'node:assert/strict';
import {fromMetrics, quantile, resourceSummary, slope} from '../../src/stats.js';
import {route} from '../../src/client.js';
import {config} from '../../src/config.js';

test('nearest rank quantiles and absent samples', () => {
  assert.equal(quantile([4, 1, 3, 2], .5), 2);
  assert.equal(quantile([], .95), null);
});
test('backlog slope is records per second', () => assert.equal(slope([[1000, 1], [2000, 3], [3000, 5]]), 2));
test('metrics distinguish missing from zero and ignore NaN', () => {
  const s = fromMetrics('process_cpu_time_ns_total 100\njvm_memory_used_bytes{area="heap",id="a"} 5\njvm_memory_used_bytes{area="heap",id="b"} 6\ntrades_pending_publications NaN');
  assert.equal(s.heapUsedBytes, 11);
  assert.equal(s.pending, null);
});
test('CPU is measured in cores; RSS candidates require actual RSS and stable counter', () => {
  const samples = [0, 1000].map((at, i) => ({
    at,
    services: {trader: {cpuNs: i * 5e8, startTime: 1, rssBytes: 256 * 1024 ** 2, heapUsedBytes: 64 * 1024 ** 2}}
  }));
  assert.equal(resourceSummary(samples, 'trader').averageCpuCores, .5);
  assert.ok(resourceSummary(samples, 'trader').candidate);
  samples[1].services.trader.startTime = 2;
  assert.equal(resourceSummary(samples, 'trader').candidate, null);
});
test('route matches Java hash buckets', () => {
  assert.equal(route('BUY', 'MSFT'), 2);
  assert.equal(route('SELL', 'AAPL'), 4);
});
test('configuration rejects invalid rates and bounds', () => {
  assert.throws(() => config({RATES: '0'}));
  assert.throws(() => config({DURATION_SECONDS: 'NaN'}));
  assert.throws(() => config({TRADER_URL: 'http://user:secret@host'}));
});

test('missing RSS is unknown and never yields a memory sizing candidate', () => {
  const samples = [0, 1000].map((at, i) => ({
    at,
    services: {trader: {cpuNs: i * 1e8, startTime: 1, heapUsedBytes: 123}}
  }));
  const result = resourceSummary(samples, 'trader');
  assert.equal(result.peakRssMiB, null);
  assert.equal(result.peakNonHeapUsedMiB, null);
  assert.equal(result.candidate, null);
});
