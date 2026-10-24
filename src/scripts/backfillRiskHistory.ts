// One-off backfill of liquidations / ADL events from BULK's per-account
// riskHistory, for a window in which the WS listener missed them (it read the
// string `reason` field after BULK switched the trade print to a numeric
// `reason_code` on ~2026-10-21 10:15 UTC).
//
// riskHistory is the exchange's own ledger of forced events per account, so
// it is authoritative. We walk every wallet our DB has seen active since the
// window start, pull its riskHistory once, and insert any liquidation / ADL in
// the window that isn't already stored (same wallet + symbol + timestamp ±2s).
// Trader counters are bumped exactly like the live path does.
//
//   docker compose exec -T app node dist/scripts/backfillRiskHistory.js --since 2026-10-21T10:00:00Z [--wallet <addr>]
import dotenv from 'dotenv';
dotenv.config();
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';
import { isSystemWallet } from '../services/systemWallets';

const args = process.argv.slice(2);
const arg = (k: string) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : undefined; };
const since = new Date(arg('--since') ?? '2026-10-21T10:00:00Z');
const only = arg('--wallet');
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const wallets: string[] = only
    ? [only]
    : (await query<{ wallet_address: string }>(
        `SELECT wallet_address FROM traders WHERE last_seen >= $1 ORDER BY total_volume DESC NULLS LAST`,
        [since],
      )).map((r) => r.wallet_address);
  console.log(`backfill riskHistory since ${since.toISOString()} for ${wallets.length} wallet(s)`);

  let liqIns = 0, adlIns = 0, skipped = 0, scanned = 0;
  for (const w of wallets) {
    if (isSystemWallet(w)) continue;
    let events;
    try { events = await bulkApi.getRiskHistory(w); } catch (e) { console.error(`  ${w.slice(0, 8)}: riskHistory failed`, (e as Error).message); continue; }
    scanned++;
    for (const e of events) {
      const ms = Math.floor(e.timestamp / 1e6);
      if (ms < since.getTime()) continue;
      const size = Math.abs(e.amount);
      const value = size * e.price;
      // Forced fill direction → position side: a forced BUY closes a SHORT.
      const side = e.isBuy ? 'short' : 'long';
      if (e.eventType === 'adl') {
        const dup = await query<{ n: string }>(
          `SELECT count(*)::text AS n FROM adl_events WHERE wallet_address = $1 AND symbol = $2 AND abs(extract(epoch FROM timestamp) * 1000 - $3) < 2000`,
          [w, e.symbol, ms]);
        if (parseInt(dup[0].n, 10) > 0) { skipped++; continue; }
        await query(
          `INSERT INTO adl_events (wallet_address, counterparty, symbol, side, size, price, value, timestamp)
           VALUES ($1, NULL, $2, $3, $4, $5, $6, to_timestamp($7/1000.0))`,
          [w, e.symbol, side, size, e.price, value, ms]);
        await query(
          `INSERT INTO traders (wallet_address, total_adl, adl_value, last_seen) VALUES ($1, 1, $2, NOW())
           ON CONFLICT (wallet_address) DO UPDATE SET total_adl = COALESCE(traders.total_adl, 0) + 1, adl_value = COALESCE(traders.adl_value, 0) + $2`,
          [w, value]);
        adlIns++;
      } else {
        const dup = await query<{ n: string }>(
          `SELECT count(*)::text AS n FROM liquidations WHERE wallet_address = $1 AND symbol = $2 AND abs(extract(epoch FROM timestamp) * 1000 - $3) < 2000`,
          [w, e.symbol, ms]);
        if (parseInt(dup[0].n, 10) > 0) { skipped++; continue; }
        await query(
          `INSERT INTO liquidations (wallet_address, symbol, side, size, price, value, timestamp)
           VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7/1000.0)) ON CONFLICT DO NOTHING`,
          [w, e.symbol, side, size, e.price, value, ms]);
        await query(
          `INSERT INTO traders (wallet_address, total_liquidations, liquidation_value, last_seen) VALUES ($1, 1, $2, NOW())
           ON CONFLICT (wallet_address) DO UPDATE SET total_liquidations = traders.total_liquidations + 1, liquidation_value = traders.liquidation_value + $2`,
          [w, value]);
        liqIns++;
        console.log(`  + ${w.slice(0, 8)} ${e.symbol} ${side} $${value.toFixed(0)} @ ${new Date(ms).toISOString()}`);
      }
    }
    await sleep(250); // stay polite to BULK's /account limiter
  }
  console.log(`done: scanned ${scanned} wallets, inserted ${liqIns} liquidations + ${adlIns} ADL, ${skipped} already present`);
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
