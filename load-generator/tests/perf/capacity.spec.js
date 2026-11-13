import {expect, test} from '@playwright/test';
import {config} from '../../src/config.js';
import {runCapacity} from '../../src/capacity.js';

test('capacity stages meet latency, arrival, reconciliation and backlog gates', async ({}, testInfo) => {
  const settings = config();
  test.setTimeout((settings.rates.length * (settings.durationSeconds + settings.drainSeconds * 2) + settings.warmupSeconds + settings.drainSeconds * 2 + 120) * 1000);
  const result = await runCapacity(settings);
  await testInfo.attach('capacity summary', {
    path: result.artifactDirectory + '/summary.json',
    contentType: 'application/json'
  });
  expect(result.result, result.error ?? JSON.stringify(result.stages.map(s => ({
    label: s.label,
    gates: s.gates
  })))).toBe('PASS');
});
