import test from "node:test";
import assert from "node:assert/strict";
import {requestJson, validateTrade,} from "../../main/resources/static/validation.mjs";

const valid = {
  tradeDate: "2026-10-24",
  side: "BUY",
  symbol: "AAPL",
  quantity: "10",
  price: "123.450000",
  currency: "USD",
  counterparty: "",
};

const invalid = [
  ["quantity", ""],
  ["quantity", "0"],
  ["quantity", "-1"],
  ["quantity", "1.5"],
  ["quantity", "9223372036854775808"],
  ["price", ""],
  ["price", "0"],
  ["price", "-1"],
  ["price", "0.0000001"],
  ["price", "1000000000000"],
  ["price", "1e2"],
  ["price", "NaN"],
  ["tradeDate", ""],
  ["tradeDate", "2026-02-30"],
  ["tradeDate", "2025-02-29"],
  ["tradeDate", "2026-1-1"],
  ["tradeDate", "+999999999-01-01"],
  ["side", "buy"],
  ["symbol", ""],
  ["symbol", "A!"],
  ["symbol", ".A"],
  ["symbol", "A".repeat(17)],
  ["currency", "JPY"],
  ["currency", "usd"],
  ["counterparty", "x".repeat(129)],
];
for (const [field, value] of invalid) {
  test(`rejects ${field}=${value}`, () =>
    assert.ok(validateTrade({...valid, [field]: value})[field]));
}
test("accepts exact boundaries and normalization candidates", () => {
  assert.deepEqual(
    validateTrade({
      ...valid,
      tradeDate: "2024-02-29",
      symbol: " brk.b ",
      quantity: "9223372036854775807",
      price: "999999999999.999999",
      counterparty: "  demo  ",
    }),
    {},
  );
  assert.deepEqual(validateTrade({...valid, price: "0.000001"}), {});
  assert.deepEqual(validateTrade({...valid, price: "1.2300000"}), {});
});
test("serializes long quantity without JavaScript Number rounding", () => {
  const body = requestJson({...valid, quantity: "9223372036854775807"});
  assert.match(body, /"quantity":9223372036854775807/);
  assert.equal(JSON.parse(body).price, "123.450000");
});
test("refuses to serialize invalid requests", () => {
  assert.throws(() => requestJson({...valid, quantity: "0"}));
});
