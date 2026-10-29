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
  // Discovery: every address we have any trace of since the window start —
  // the traders table (last_seen), BOTH sides of every trade in the tape, and
  // anyone already in the liquidations table. traders.last_seen alone missed
  // the very wallet that took a $520K cascade on Oct 21.
  const wallets: string[] = only
    ? [only]
    : (await query<{ w: string }>(
        `SELECT DISTINCT w FROM (
           SELECT wallet_address AS w FROM traders WHERE last_seen >= $1
           UNION SELECT wallet_address FROM trades WHERE timestamp >= $1
           UNION SELECT counterparty FROM trades WHERE timestamp >= $1 AND counterparty IS NOT NULL
           UNION SELECT wallet_address FROM liquidations WHERE timestamp >= $1 - INTERVAL '7 days'
         ) u WHERE w IS NOT NULL AND length(w) BETWEEN 32 AND 44`,
        [since],
      )).map((r) => r.w);
  console.log(`backfill riskHistory since ${since.toISOString()} for ${wallets.length} wallet(s)`);

  let liqIns = 0, adlIns = 0, vaultIns = 0, skipped = 0, scanned = 0;
  // Wallets BULK rate-limited; retried at the end with growing backoff rather
  // than dropped. Dropping them is why a single pass left 946 wallets (18%)
  // unscanned and why re-running whole passes converged so slowly.
  const retry: string[] = [];
  const spacing = Number(arg('--spacing') ?? 250);

  const processWallet = async (w: string): Promise<'ok' | 'throttled'> => {
    // System wallets (liquidation engine, risk vault) must never be recorded
    // as liquidated traders — but the vault IS the source of risk_vault
    // shortfall events, so we still scan it and keep only those. Skipping it
    // outright left vault_events permanently empty.
    const systemOnly = isSystemWallet(w);
    let events;
    try {
      events = await bulkApi.getRiskHistory(w);
    } catch (e) {
      // Defensive: getRiskHistory signals failure by returning null, but a
      // throw would also mean "no answer", so queue it either way.
      console.error(`  ${w.slice(0, 8)}: riskHistory threw`, (e as Error).message);
      return 'throttled';
    }
    // null = upstream refused us (429/timeout). Must NOT be read as "this
    // wallet has no forced events" — queue it for retry instead.
    if (events === null) return 'throttled';
    scanned++;
    for (const e of events) {
      const ms = Math.floor(e.timestamp / 1e6);
      if (ms < since.getTime()) continue;
      const size = Math.abs(e.amount);
      const value = size * e.price;
      // Forced fill direction → position side: a forced BUY closes a SHORT.
      const side = e.isBuy ? 'short' : 'long';
      // BULK has a third event type, 'risk_vault' (the vault absorbing a
      // shortfall), and may add more. Anything that is not a liquidation or an
      // ADL must NOT be filed as one — the original `else` branch here treated
      // every non-ADL event as a liquidation, which turned the vault's 199
      // shortfall absorptions into phantom liquidations.
      if (e.eventType === 'risk_vault') {
        // Shortfall the vault absorbed. BULK encodes the amount in the reason
        // string: "absorbed_shortfall:absorbed 26092.10 / 26092.10, 100.0%".
        const m = /absorbed\s+([\d.]+)\s*\/\s*([\d.]+)/.exec(e.reason ?? '');
        const absorbed = m ? Number(m[1]) : null;
        const dup = await query<{ n: string }>(
          `SELECT count(*)::text AS n FROM vault_events WHERE vault_address = $1 AND symbol = $2 AND abs(extract(epoch FROM timestamp) * 1000 - $3) < 2000`,
          [w, e.symbol, ms]);
        if (parseInt(dup[0].n, 10) > 0) { skipped++; continue; }
        await query(
          `INSERT INTO vault_events (vault_address, symbol, side, size, price, notional, absorbed, reason, timestamp)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8, to_timestamp($9/1000.0))`,
          [w, e.symbol, side, size, e.price, value, Number.isFinite(absorbed as number) ? absorbed : null, e.reason ?? null, ms]);
        vaultIns++;
        continue;
      }
      // For a system wallet, vault events above are the ONLY thing we keep.
      if (systemOnly) continue;
      if (e.eventType !== 'liquidation' && e.eventType !== 'adl') continue;
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
    return 'ok';
  };

  for (const w of wallets) {
    if ((await processWallet(w)) === 'throttled') retry.push(w);
    await sleep(spacing); // stay polite to BULK's /account limiter
  }

  // Retry rounds: each round waits longer between wallets, so a wallet is only
  // abandoned after BULK has refused it repeatedly at a very slow cadence.
  let round = 0;
  while (retry.length > 0 && round < 5) {
    round++;
    const queue = retry.splice(0, retry.length);
    const wait = spacing * Math.pow(3, round); // 750ms, 2.2s, 6.7s, 20s, 60s
    console.log(`retry round ${round}: ${queue.length} throttled wallet(s), ${wait}ms apart`);
    await sleep(5000 * round); // let the limiter cool before the round
    for (const w of queue) {
      if ((await processWallet(w)) === 'throttled') retry.push(w);
      await sleep(wait);
    }
    // No-progress guard: if a whole round resolved nothing, BULK is refusing
    // us outright and slower rounds will only burn hours. Stop and let the
    // operator re-run later rather than grind at 60s/wallet overnight.
    if (retry.length >= queue.length) {
      console.error(`retry round ${round} resolved none of ${queue.length} — stopping early`);
      break;
    }
  }
  if (retry.length) console.error(`STILL THROTTLED after ${round} rounds: ${retry.length} wallet(s) — re-run to pick them up`);

  console.log(`done: scanned ${scanned} wallets, inserted ${liqIns} liquidations + ${adlIns} ADL + ${vaultIns} vault events, ${skipped} already present, ${retry.length} unresolved`);
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
