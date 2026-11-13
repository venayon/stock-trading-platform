const trim = (value) => value.replace(/^[\x00-\x20]+|[\x00-\x20]+$/g, "");

function validDate(text) {
  if (!/^(?:\d{4}|[+-]\d{5,9})-\d{2}-\d{2}$/.test(text)) return false;
  const [, ys, ms, ds] = text.match(/^([+-]?\d+)-(\d{2})-(\d{2})$/);
  let year = Number(ys);
  const month = Number(ms),
    day = Number(ds);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month < 1 || month > 12 || day < 1 || day > days[month - 1]) return false;
  // Gregorian civil date -> epoch day, without Date's restricted year range.
  year -= month <= 2 ? 1 : 0;
  const era = Math.floor(year / 400),
    yoe = year - era * 400;
  const doy =
    Math.floor((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5) + day - 1;
  const epoch =
    era * 146097 +
    yoe * 365 +
    Math.floor(yoe / 4) -
    Math.floor(yoe / 100) +
    doy -
    719468;
  return epoch >= -2147483648 && epoch <= 2147483647;
}

export function validateTrade(input) {
  const errors = {};
  const quantity = input.quantity ?? "";
  if (
    !/^[0-9]+$/.test(quantity) ||
    BigInt(quantity) < 1n ||
    BigInt(quantity) > 9223372036854775807n
  )
    errors.quantity = "Enter a whole quantity from 1 to 9223372036854775807.";
  const price = input.price ?? "";
  if (!/^[+-]?[0-9]+(?:\.[0-9]+)?$/.test(price)) {
    errors.price = "Enter a plain positive decimal, without exponent notation.";
  } else {
    const [whole, fraction = ""] = price.replace(/^[+-]/, "").split(".");
    const significant = fraction.replace(/0+$/, "");
    if (significant.length > 6)
      errors.price =
        "Use at most six decimal places; extra trailing zeros are allowed.";
    else {
      const micros =
        BigInt(whole) * 1000000n + BigInt(significant.padEnd(6, "0"));
      if (
        price.startsWith("-") ||
        micros === 0n ||
        micros > 999999999999999999n
      )
        errors.price =
          "Price must be positive and at most 999999999999.999999.";
    }
  }
  if (!validDate(input.tradeDate ?? ""))
    errors.tradeDate =
      "Enter a valid ISO date within the Avro epoch-day range.";
  if (!["BUY", "SELL"].includes(input.side))
    errors.side = "Choose BUY or SELL.";
  if (
    !/^[A-Z0-9][A-Z0-9.-]{0,15}$/.test(trim(input.symbol ?? "").toUpperCase())
  )
    errors.symbol =
      "Use 1–16 letters, digits, dots or hyphens; start with a letter or digit.";
  if (
    !["USD", "GBP", "EUR"].includes(
      input.currency === undefined ? "USD" : input.currency,
    )
  )
    errors.currency = "Choose USD, GBP or EUR.";
  if (trim(input.counterparty ?? "").length > 128)
    errors.counterparty = "Use at most 128 characters after trimming.";
  return errors;
}

export function requestJson(input) {
  if (Object.keys(validateTrade(input)).length)
    throw new Error("Trade validation failed");
  const {quantity, ...fields} = input;
  // Preserve all 64-bit integer digits; JSON.stringify(Number(quantity)) can round them.
  return JSON.stringify(fields).replace(
    /}$/,
    `,"quantity":${BigInt(quantity).toString()}}`,
  );
}
