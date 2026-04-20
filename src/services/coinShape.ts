/**
 * Helpers for per-coin chart endpoints during the multi-coin migration.
 *
 * Context
 * -------
 * Historically every per-coin chart endpoint returned rows of shape:
 *   { timestamp, BTC, ETH, SOL, total, Cumulative }
 *
 * That shape bakes the coin list into the wire format, which breaks the moment
 * BULK lists a new market. We're migrating to a dictionary shape:
 *   { timestamp, coins: { BTC, ETH, SOL, BNB, DOGE, ... }, total, Cumulative }
 *
 * To avoid breaking every frontend chart on deploy day, we use an **additive**
 * migration: each endpoint returns BOTH shapes simultaneously. The legacy
 * BTC/ETH/SOL fields stay populated for existing clients, and a new `coins`
 * field is added with the full per-coin breakdown. Frontend charts migrate to
 * `coins` one at a time; once all are migrated we can drop the legacy fields.
 */

/**
 * Extract the "coin" part of a BULK symbol (e.g. "BTC-USD" → "BTC").
 * Handles odd cases by stripping the trailing "-USD" or defaulting to the
 * whole symbol if the suffix isn't present.
 */
export function coinFromSymbol(symbol: string): string {
  return symbol.endsWith('-USD') ? symbol.slice(0, -4) : symbol;
}

/**
 * Wrap a single chart row in the additive shape. Takes the per-coin data as a
 * dictionary and emits a row that has BOTH:
 *   - the legacy top-level BTC/ETH/SOL/XRP/GOLD/... fields (whichever exist)
 *   - the new `coins: { ... }` dictionary carrying every coin
 *
 * This is a pure transform — given the same dict and the same extras, always
 * produces the same row. Safe to call in hot loops.
 *
 * @param timestamp  ISO string or whatever the endpoint uses today
 * @param perCoin    { BTC: 123, ETH: 456, BNB: 78, ... } — any coin set
 * @param extras     top-level numeric fields that aren't per-coin (total,
 *                   Cumulative, value, etc.) — preserved unchanged
 */
export function buildAdditiveRow(
  timestamp: string,
  perCoin: Record<string, number>,
  extras: Record<string, number | string | null | undefined> = {}
): Record<string, unknown> {
  const row: Record<string, unknown> = { timestamp };

  // Legacy top-level fields — keep every coin that has a value. This
  // preserves backwards compatibility with charts reading `row.BTC`, `row.ETH`
  // etc. New coins also appear as top-level fields, so older code that
  // happens to look for a specific new coin by name also works.
  for (const [coin, value] of Object.entries(perCoin)) {
    if (typeof value === 'number' && isFinite(value)) {
      row[coin] = value;
    }
  }

  // New canonical field — full per-coin dictionary. This is what new charts
  // read going forward, and what `bucketWithOther()` on the frontend consumes.
  row.coins = { ...perCoin };

  // Carry extras (total, Cumulative, etc.) as-is.
  for (const [k, v] of Object.entries(extras)) {
    if (v !== undefined && v !== null) {
      row[k] = v;
    }
  }

  return row;
}

/**
 * Initialize a per-coin accumulator for every symbol currently listed on BULK.
 * Every symbol starts at zero — use this to seed a map before filling it from
 * SQL results, so coins that had no activity in the window still appear (with
 * zero values) on the chart.
 */
export function zeroCoinDict(symbols: readonly string[]): Record<string, number> {
  const dict: Record<string, number> = {};
  for (const sym of symbols) {
    dict[coinFromSymbol(sym)] = 0;
  }
  return dict;
}
