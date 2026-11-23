// wallet_daily: per wallet, per market, per day — the long-lived rollup behind
// windowed stats (7d/30d leaderboards, per-market history). Written live by the
// trade writer (jobs/wsListener upsertWalletDaily) and the liquidation recorder.

import { query, queryOne } from '../db';

/**
 * One-time seed, run BEFORE the live feed starts (so nothing is counted twice):
 * the ~2 days of raw trades we still hold, and the whole liquidation history.
 * Skipped once the table has rows.
 */
export async function seedWalletDaily(): Promise<void> {
  try {
    const has = await queryOne<{ n: string }>(`SELECT COUNT(*)::text AS n FROM (SELECT 1 FROM wallet_daily LIMIT 1) x`);
    if (Number(has?.n ?? 0) > 0) return;
    const t0 = Date.now();
    // Each trade row: wallet_address = taker (buys/sells on `side`), counterparty = maker (the other side).
    await query(`
      INSERT INTO wallet_daily (day, wallet_address, symbol, volume, trades, buy_volume, sell_volume, maker_volume)
      SELECT (timestamp AT TIME ZONE 'UTC')::date, p.w, symbol,
             SUM(value), COUNT(*),
             SUM(CASE WHEN p.buy THEN value ELSE 0 END),
             SUM(CASE WHEN p.buy THEN 0 ELSE value END),
             SUM(CASE WHEN p.maker THEN value ELSE 0 END)
      FROM trades
      CROSS JOIN LATERAL (VALUES (wallet_address, side = 'buy', false), (counterparty, side <> 'buy', true)) AS p(w, buy, maker)
      WHERE p.w IS NOT NULL AND NOT (p.maker AND counterparty = wallet_address)
      GROUP BY 1, 2, 3
      ON CONFLICT (day, wallet_address, symbol) DO NOTHING`);
    await query(`
      INSERT INTO wallet_daily (day, wallet_address, symbol, liq_count, liq_value)
      SELECT (timestamp AT TIME ZONE 'UTC')::date, wallet_address, symbol, COUNT(*), SUM(value)
      FROM liquidations WHERE wallet_address IS NOT NULL
      GROUP BY 1, 2, 3
      ON CONFLICT (day, wallet_address, symbol) DO UPDATE SET liq_count = EXCLUDED.liq_count, liq_value = EXCLUDED.liq_value`);
    const n = await queryOne<{ n: string }>(`SELECT COUNT(*)::text AS n FROM wallet_daily`);
    console.log(`📅 wallet_daily seeded: ${n?.n} rows in ${Date.now() - t0}ms`);
  } catch (e) {
    console.error('📅 wallet_daily seed failed:', e);
  }
}
