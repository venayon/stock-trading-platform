import {expect, test} from '@playwright/test';
import {call, key, route, trade, waitComplete} from '../../src/client.js';

const processor = process.env.PROCESSOR_URL ?? 'http://localhost:8082';

test('acceptance, publication, processing and completed replay preserve the contract', async ({request}) => {
  const payload = trade(1), idempotency = key(), correlation = key().slice(-36);
  const accepted = await call(request, 'POST', '/api/trades', {
    data: payload,
    headers: {'Idempotency-Key': idempotency, 'X-Correlation-ID': correlation}
  });
  expect(accepted.status).toBe(202);
  expect(accepted.body.publicationStatus).toBe('PENDING');
  expect(accepted.headers['x-correlation-id']).toBe(correlation);
  expect(accepted.headers.location).toBe(`/api/trades/${accepted.body.tradeId}`);
  const processed = await waitComplete(request, processor, accepted.body.tradeId);
  expect(processed.trade.price).toBe(payload.price);
  expect(processed.trade.quantity).toBe(payload.quantity);
  expect(processed.targetPartition).toBe(route(payload.side, payload.symbol));
  expect(processed.output.topic).toBe('stock-trades-processed');
  const source = await call(request, 'GET', accepted.headers.location);
  expect(source.body.publicationStatus).toBe('PUBLISHED');
  expect(source.body.event).toMatchObject(processed.source);
  const retry = await call(request, 'POST', '/api/trades', {data: payload, headers: {'Idempotency-Key': idempotency}});
  expect(retry.status).toBe(200);
  expect(retry.headers['idempotent-replay']).toBe('true');
  expect(retry.body.tradeId).toBe(accepted.body.tradeId);
  const replay = await call(request, 'POST', `${processor}/api/process`, {data: {tradeId: accepted.body.tradeId}});
  expect(replay.status).toBe(200);
  expect(replay.body.output).toEqual(processed.output);
});

test('concurrent same-key submissions create one logical trade; changed payload conflicts', async ({request}) => {
  const payload = trade(2), idempotency = key();
  const responses = await Promise.all(Array.from({length: 8}, () => call(request, 'POST', '/api/trades', {
    data: payload,
    headers: {'Idempotency-Key': idempotency}
  })));
  expect(responses.filter(r => r.status === 202)).toHaveLength(1);
  expect(responses.every(r => [200, 202].includes(r.status))).toBeTruthy();
  expect(new Set(responses.map(r => r.body.tradeId)).size).toBe(1);
  const conflict = await call(request, 'POST', '/api/trades', {
    data: {...payload, quantity: 999},
    headers: {'Idempotency-Key': idempotency}
  });
  expect(conflict.status).toBe(409);
  expect(conflict.body.errorCode).toBe('IDEMPOTENCY_CONFLICT');
  const processed = await waitComplete(request, processor, responses[0].body.tradeId);
  expect(processed.trade.quantity).toBe(payload.quantity);
});

const invalids = [['zero quantity', {quantity: 0}], ['negative quantity', {quantity: -1}], ['fractional quantity', {quantity: 1.5}], ['missing quantity', {quantity: undefined}], ['zero price', {price: '0'}], ['negative price', {price: '-1'}], ['null price', {price: null}], ['excess scale', {price: '0.0000001'}], ['price overflow', {price: '1000000000000'}], ['bad side', {side: 'HOLD'}], ['bad symbol', {symbol: '!'}], ['unsupported currency', {currency: 'JPY'}], ['explicit null currency', {currency: null}], ['invalid date', {tradeDate: '2026-02-30'}], ['long counterparty', {counterparty: 'x'.repeat(129)}], ['unknown field', {unexpected: true}]];
for (const [name, patch] of invalids) test(`rejects ${name}`, async ({request}) => {
  const result = await call(request, 'POST', '/api/trades', {
    data: {...trade(), ...patch},
    headers: {'Idempotency-Key': key()}
  });
  expect(result.status).toBe(400);
  expect(result.headers['content-type']).toContain('application/problem+json');
  expect(result.body.tradeId).toBeUndefined();
  expect(result.body.correlationId).toBeTruthy();
});

test('missing key and integer overflow are rejected', async ({request}) => {
  expect((await call(request, 'POST', '/api/trades', {data: trade()})).status).toBe(400);
  const json = JSON.stringify(trade()).replace('"quantity":1', '"quantity":9223372036854775808');
  expect((await call(request, 'POST', '/api/trades', {
    data: json,
    headers: {'Content-Type': 'application/json', 'Idempotency-Key': key()}
  })).status).toBe(400);
});

test('normalization and exact long/decimal boundaries survive JSON and Avro', async ({request}) => {
  const idempotency = key(),
    payload = {...trade(), symbol: ' brk.b ', price: '999999999999.999999', counterparty: ' demo '};
  const raw = JSON.stringify(payload).replace('"quantity":1', '"quantity":9223372036854775807');
  const accepted = await call(request, 'POST', '/api/trades', {
    data: raw,
    headers: {'Content-Type': 'application/json', 'Idempotency-Key': idempotency}
  });
  expect(accepted.status).toBe(202);
  const processed = await waitComplete(request, processor, accepted.body.tradeId);
  expect(processed.trade.symbol).toBe('BRK.B');
  expect(processed.trade.counterparty).toBe('demo');
  expect(processed.trade.price).toBe(payload.price);
  // Inspect the raw response to avoid Number rounding of the 64-bit quantity.
  const response = await request.get(`${processor}/api/processed/${accepted.body.tradeId}`);
  try {
    expect(await response.text()).toContain('9223372036854775807');
  } finally {
    await response.dispose();
  }
});

test('queries are bounded and unknown IDs return 404', async ({request}) => {
  const list = await call(request, 'GET', `${processor}/api/processed?size=1`);
  expect(list.status).toBe(200);
  expect(list.body.items.length).toBeLessThanOrEqual(1);
  expect((await call(request, 'GET', `${processor}/api/processed?size=101`)).status).toBe(400);
  expect((await call(request, 'GET', `${processor}/api/processed/not-uuid`)).status).toBe(400);
  expect((await call(request, 'GET', `${processor}/api/processed/${key().slice(-36)}`)).status).toBe(404);
});

test('health, metrics and Swagger endpoints are available', async ({request}) => {
  for (const base of [process.env.TRADER_URL ?? 'http://localhost:8081', processor]) {
    for (const path of ['/actuator/health/readiness', '/actuator/health/liveness', '/v3/api-docs']) expect((await call(request, 'GET', base + path)).status).toBe(200);
    const metrics = await call(request, 'GET', base + '/actuator/prometheus');
    expect(metrics.status).toBe(200);
    expect(metrics.body).toContain('jvm_memory_used_bytes');
  }
});
