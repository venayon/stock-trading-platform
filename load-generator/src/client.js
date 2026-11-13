import {performance} from "node:perf_hooks";
import {randomUUID} from "node:crypto";

export const symbols = [
  "AAPL",
  "MSFT",
  "NVDA",
  "AMZN",
  "GOOG",
  "META",
  "TSLA",
  "AMD",
  "ORCL",
  "IBM",
  "INTC",
  "JPM",
];

export function trade(sequence = 0, hotKeyRatio = 0) {
  return {
    tradeDate: "2026-10-24",
    side: sequence % 2 ? "SELL" : "BUY",
    symbol:
      sequence % 100 < hotKeyRatio * 100
        ? "AAPL"
        : symbols[sequence % symbols.length],
    quantity: 1 + (sequence % 100),
    price: (100000000 + (sequence % 1000000))
      .toString()
      .replace(/(.{6})$/, ".$1"),
    currency: "USD",
    counterparty: null,
  };
}

export function key(prefix = "api") {
  return `${prefix}-${randomUUID()}`;
}

export function route(side, symbol) {

  let hash = 0;

  for (const c of symbol) {
    hash = (Math.imul(31, hash) + c.charCodeAt(0)) | 0;
  }
  return (side === "BUY" ? 0 : 3) + (((hash % 3) + 3) % 3);
}

export async function call(context, method, url, options = {}) {

  const start = performance.now();
  let response;
  try {
    response = await context.fetch(url, {method, failOnStatusCode: false, maxRetries: 0, ...options,});
    const text = await response.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
    return {
      status: response.status(),
      headers: response.headers(),
      body,
      ms: performance.now() - start,
    };
  } finally {
    await response?.dispose();
  }
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitComplete(context, processorUrl, id, {
  timeoutMs = 30000, pollMs = 500, onPoll = () => {
  }
} = {},) {
  const deadline = performance.now() + timeoutMs;
  let last;
  while (performance.now() < deadline) {
    onPoll();
    last = await call(context, "GET", `${processorUrl}/api/processed/${id}`, {
      timeout: Math.min(5000, Math.max(1, deadline - performance.now())),
    });
    if (last.status === 200 && last.body.processingStatus === "COMPLETE")
      return last.body;
    if (last.status !== 404 && last.status !== 200)
      throw new Error(`Processing query returned ${last.status}`);
    await sleep(Math.min(pollMs, Math.max(0, deadline - performance.now())));
  }
  throw new Error(
    `Trade ${id} not COMPLETE before deadline; last status ${last?.status}`,
  );
}
