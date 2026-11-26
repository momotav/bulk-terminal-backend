// Every 5 minutes, per market:
//   • liquidity_snapshots — spread, ±2% depth and the round-trip cost of trading
//     $10K / $100K / $1M (walk the book; past its 20 levels, BULK's own impact
//     curve from the WS). One /l2book call per market per run, paced.
//   • positioning_snapshots — where the top traders stand (long/short notional,
//     counts, average entries, top-5 concentration), from the live account stream.
//     No BULK calls.
// Kept for good: a few rows per market per 5 min.

import { query } from '../db';
import { getCache } from '../services/cache';
import { getActiveSymbols } from '../services/markets';
import { allLiveAccounts } from '../services/accountStream';
import { loadOrderBook } from '../routes/analytics';
import { snapshotCohorts } from '../services/cohorts';

const EVERY_MS = 5 * 60_000;
const PACE_MS = 350;
const SIZES = [10_000, 100_000, 1_000_000] as const;

type Level = { px: number; sz: number };
type ImpactSide = { logMin: number; logRange: number; bps: number[] } | null;

function impactBps(side: ImpactSide, size: number): number | null {
  if (!side || !side.bps.length || !(size > 0) || !(side.logRange > 0)) return null;
  const i = Math.max(0, Math.min(side.bps.length - 1, ((Math.log(size) - side.logMin) / side.logRange) * (side.bps.length - 1)));
  const lo = Math.floor(i), hi = Math.min(side.bps.length - 1, lo + 1);
  return side.bps[lo] + (i - lo) * (side.bps[hi] - side.bps[lo]);
}

/** Average fill vs mid (bps) for `usd` walking one side; null if the shown levels run out. */
function walk(levels: Level[], mid: number, usd: number): number | null {
  let left = usd, qty = 0, cost = 0;
  for (const l of levels) {
    const take = Math.min(left, l.px * l.sz);
    qty += take / l.px; cost += take; left -= take;
    if (left <= 1e-9) break;
  }
  if (left > 1e-6 || qty <= 0) return null;
  return Math.abs(cost / qty - mid) / mid * 1e4;
}

async function liquidity(symbols: string[], ts: Date): Promise<void> {
  for (const symbol of symbols) {
    try {
      const book = await loadOrderBook(symbol, 20);
      const mid = Number(book?.stats?.mid);
      if (!(mid > 0)) continue;
      const curve = await getCache<{ book: { buy: ImpactSide; sell: ImpactSide } | null }>(`analytics:impact:${symbol}`)
        ?? await getCache<{ book: { buy: ImpactSide; sell: ImpactSide } | null }>(`analytics:impact_stale:${symbol}`);
      const cost = SIZES.map((usd) => {
        const buy = walk(book.asks ?? [], mid, usd) ?? impactBps(curve?.book?.buy ?? null, usd / mid);
        const sell = walk(book.bids ?? [], mid, usd) ?? impactBps(curve?.book?.sell ?? null, usd / mid);
        return buy !== null && sell !== null ? buy + sell : null;
      });
      await query(
        `INSERT INTO liquidity_snapshots (ts, symbol, mid, spread_bps, bid_depth_2pct, ask_depth_2pct, cost_10k, cost_100k, cost_1m)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING`,
        [ts, symbol, mid, book.stats.spreadBps ?? null, book.stats.bidDepth2pctUsd ?? null, book.stats.askDepth2pctUsd ?? null, cost[0], cost[1], cost[2]],
      );
    } catch { /* one market missing a snapshot is fine */ }
    await new Promise((r) => setTimeout(r, PACE_MS));
  }
}

async function positioning(ts: Date): Promise<void> {
  const accounts = allLiveAccounts();
  if (accounts.length < 50) return; // stream not warm yet
  type Agg = { long: number; short: number; ln: number; sn: number; lEntry: number; sEntry: number; perWallet: number[] };
  const by = new Map<string, Agg>();
  for (const { positions } of accounts) {
    for (const p of positions) {
      const size = Number(p.size) || 0, n = Math.abs(Number(p.notional) || 0), entry = Number(p.price) || 0;
      if (!size || n < 10) continue;
      const a = by.get(p.symbol) ?? { long: 0, short: 0, ln: 0, sn: 0, lEntry: 0, sEntry: 0, perWallet: [] };
      if (size > 0) { a.long += n; a.ln++; a.lEntry += entry * n; } else { a.short += n; a.sn++; a.sEntry += entry * n; }
      a.perWallet.push(n);
      by.set(p.symbol, a);
    }
  }
  for (const [symbol, a] of by) {
    const total = a.long + a.short;
    const top5 = a.perWallet.sort((x, y) => y - x).slice(0, 5).reduce((s, v) => s + v, 0);
    await query(
      `INSERT INTO positioning_snapshots (ts, symbol, long_usd, short_usd, long_n, short_n, long_entry, short_entry, top5_share, wallets)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING`,
      [ts, symbol, a.long, a.short, a.ln, a.sn, a.long ? a.lEntry / a.long : null, a.short ? a.sEntry / a.short : null, total ? top5 / total : null, accounts.length],
    ).catch(() => {});
  }
}

let running = false;
async function tick(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const ts = new Date(Math.floor(Date.now() / EVERY_MS) * EVERY_MS); // aligned buckets
    const symbols = await getActiveSymbols().catch(() => [] as string[]);
    await positioning(ts);
    await snapshotCohorts(ts).catch((e) => console.error('cohort snapshot failed:', e));
    await liquidity(symbols, ts);
  } catch (e) {
    console.error('market snapshots failed:', e);
  } finally {
    running = false;
  }
}

export function startMarketSnapshots(): void {
  setTimeout(() => void tick(), 90_000); // after the account stream has its snapshots
  setInterval(() => void tick(), EVERY_MS);
}
