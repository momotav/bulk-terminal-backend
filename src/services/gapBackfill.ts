// Gap backfill: when our live trade feed misses a stretch (the socket went silent,
// the server restarted, BULK hiccuped), rebuild that window from BULK's per-account
// history, which keeps ~8 days.
//
// BULK has no exchange-wide trade or liquidation history endpoint, so we walk
// accounts: every wallet seen recently, plus every counterparty we meet on the way
// (so a wallet that only traded during the gap still gets found). Each fill carries
// its slot:sequence trade id — the same id on both sides — so it is recorded once,
// through the live feed's own path (ingestTradePrint): trades, liquidations, ADLs,
// per-wallet volume. Paced on BULK's background budget.

import { query, queryOne } from '../db';
import { bulkApi } from './bulkApi';
import { isSystemWallet } from './systemWallets';
import { ingestTradePrint, onFeedGap, tradeQueueDepth } from '../jobs/wsListener';

const SPACING_MS = 450;          // between account calls (~2 req/s, BULK allows 5)
const MAX_SPACING_MS = 5000;     // adaptive: doubles after a failed call, eases back on success
let spacing = SPACING_MS;
const MAX_WALLETS = 6000;
const MAX_PAGES_PER_WALLET = 20; // 5,000 fills a page
const SEED_DAYS = 3;

export interface BackfillStatus {
  running: boolean; from: number | null; to: number | null; startedAt: number | null; finishedAt: number | null;
  walletsDone: number; walletsQueued: number; fills: number; trades: number; liquidations: number; adls: number; errors: number; note?: string;
}
const status: BackfillStatus = { running: false, from: null, to: null, startedAt: null, finishedAt: null, walletsDone: 0, walletsQueued: 0, fills: 0, trades: 0, liquidations: 0, adls: 0, errors: 0 };
export function getBackfillStatus(): BackfillStatus { return { ...status }; }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const toMs = (t: unknown): number => { const n = Number(t) || 0; return n > 1e14 ? Math.floor(n / 1e6) : n > 1e11 ? n : n * 1000; };

/** Map a time window to a slot window, from a busy account's recent fills (slot ↔ time). */
async function slotWindow(fromMs: number, toMs_: number): Promise<{ start: number; end: number } | null> {
  const refs = await query<{ wallet_address: string }>(`SELECT wallet_address FROM traders ORDER BY total_volume DESC LIMIT 5`);
  for (const { wallet_address } of refs) {
    if (isSystemWallet(wallet_address)) continue;
    const page = await bulkApi.accountRows(wallet_address, 'fills', { limit: 5000 });
    const pts = (page?.rows ?? []).map((r) => ({ s: Number(r.slot), t: toMs(r.timestamp) })).filter((p) => p.s > 0 && p.t > 0).sort((a, b) => a.t - b.t);
    if (pts.length < 2) continue;
    const a = pts[0], b = pts[pts.length - 1];
    if (b.t - a.t < 60_000) continue;
    const rate = (b.s - a.s) / (b.t - a.t); // slots per ms
    const margin = rate * 10 * 60_000;     // ±10 min of slots; rows are filtered by time anyway
    // No clamp to this account's minAvailableSlot: a busy account keeps far less
    // history (hours, not days), and the window applies to every wallet scanned.
    const start = Math.max(0, Math.floor(b.s + rate * (fromMs - b.t) - margin));
    const end = Math.ceil(b.s + rate * (toMs_ - b.t) + margin);
    if (end > start) return { start, end };
  }
  return null;
}

let chain: Promise<void> = Promise.resolve();

/** Queue a backfill of [fromMs, toMs]. Runs one window at a time. */
export function backfillGap(fromMs: number, toMs_: number, note?: string): Promise<void> {
  chain = chain.then(() => run(fromMs, toMs_, note)).catch((e) => { console.error('🩹 backfill failed:', e); });
  return chain;
}

async function run(fromMs: number, toMs_: number, note?: string): Promise<void> {
  Object.assign(status, { running: true, from: fromMs, to: toMs_, startedAt: Date.now(), finishedAt: null, walletsDone: 0, walletsQueued: 0, fills: 0, trades: 0, liquidations: 0, adls: 0, errors: 0, note });
  console.log(`🩹 Backfill ${new Date(fromMs).toISOString()} → ${new Date(toMs_).toISOString()}${note ? ` (${note})` : ''}`);
  const slots = await slotWindow(fromMs, toMs_);
  if (!slots) { status.running = false; status.finishedAt = Date.now(); status.note = 'could not map the window to slots'; console.error('🩹 backfill: no slot window'); return; }

  const seeds = await query<{ wallet_address: string }>(
    `SELECT wallet_address FROM traders WHERE last_seen > to_timestamp($1/1000.0) - make_interval(days => $2) ORDER BY total_volume DESC LIMIT $3`,
    [fromMs, SEED_DAYS, MAX_WALLETS],
  );
  const queue: string[] = []; const seen = new Set<string>();
  const add = (w: unknown) => { const a = typeof w === 'string' ? w : ''; if (a.length >= 32 && !seen.has(a) && !isSystemWallet(a) && seen.size < MAX_WALLETS) { seen.add(a); queue.push(a); } };
  seeds.forEach((r) => add(r.wallet_address));
  const done = new Set<string>(); // trade ids already recorded

  while (queue.length) {
    const wallet = queue.shift() as string;
    status.walletsQueued = queue.length;
    let cursor: string | null = null;
    for (let page = 0; page < MAX_PAGES_PER_WALLET; page++) {
      const res = await bulkApi.accountRows(wallet, 'fills', cursor ? { cursor, limit: 5000 } : { startSlot: slots.start, endSlot: slots.end, limit: 5000 });
      spacing = res ? Math.max(SPACING_MS, spacing - 50) : Math.min(MAX_SPACING_MS, spacing * 2);
      await sleep(spacing);
      if (!res) { status.errors++; break; }
      for (const r of res.rows) {
        const t = toMs(r.timestamp);
        if (t < fromMs || t > toMs_) continue;
        const id = `${r.slot}:${r.sequence}`;
        if (done.has(id)) continue;
        done.add(id);
        status.fills++;
        add(r.maker); add(r.taker);
        const rc = Number(r.reasonCode ?? 0);
        if (rc === 1 || rc === 3) status.liquidations++; else if (rc === 2) status.adls++; else status.trades++;
        // Same shape the live feed sends; isBuy is the taker's side.
        ingestTradePrint({ symbol: r.symbol, price: Number(r.price), size: Number(r.amount ?? r.size), side: r.isBuy ? 'buy' : 'sell',
          maker: r.maker ?? null, taker: r.taker ?? null, time: t, reason_code: rc }, null, { backfill: true });
        // Let the writer keep up (its queue drops the oldest past 5,000).
        while (tradeQueueDepth() > 1500) await sleep(500);
      }
      cursor = res.nextCursor;
      if (!cursor) break;
    }
    status.walletsDone++;
    if (status.walletsDone % 200 === 0) console.log(`🩹 backfill: ${status.walletsDone} wallets, ${status.fills} fills (${status.liquidations} liq), ${queue.length} queued`);
  }
  status.running = false; status.finishedAt = Date.now(); status.walletsQueued = 0;
  console.log(`🩹 Backfill done: ${status.walletsDone} wallets, ${status.fills} fills — ${status.trades} trades, ${status.liquidations} liquidations, ${status.adls} ADLs`);
}

/**
 * Wire the backfill: reconnects after a silence report their gap, and at startup
 * any stretch since the last recorded trade (a restart, or the feed dying while
 * nobody noticed) is recovered too.
 */
export function startGapBackfill(): void {
  const bootAt = Date.now();
  onFeedGap((from, to) => { void backfillGap(from, to, 'feed reconnected'); });
  setTimeout(async () => {
    try {
      // Holes in what we recorded: any stretch of the last 48h (our trades table
      // keeps ~2 days) with no trade for 15+ minutes. The exchange prints every few
      // seconds, so silence that long means we missed it. Filled holes have trades
      // again, so a later restart doesn't redo them.
      const holes = await query<{ a: string; b: string }>(
        `SELECT ts AS a, nxt AS b FROM (
           SELECT timestamp AS ts, LEAD(timestamp) OVER (ORDER BY timestamp) AS nxt
           FROM trades WHERE timestamp > NOW() - INTERVAL '48 hours' AND timestamp < to_timestamp($1/1000.0)
         ) x WHERE nxt - ts > INTERVAL '15 minutes' ORDER BY ts`,
        [bootAt],
      );
      for (const h of holes) {
        const a = new Date(h.a).getTime(), b = new Date(h.b).getTime();
        void backfillGap(a + 1, b - 1, 'startup: hole in recorded trades');
      }
      // And the stretch right before this boot, if the feed was already dead.
      const row = await queryOne<{ last: string | null }>(`SELECT MAX(timestamp) AS last FROM trades WHERE timestamp < to_timestamp($1/1000.0)`, [bootAt]);
      const last = row?.last ? new Date(row.last).getTime() : null;
      if (last && bootAt - last > 15 * 60_000) void backfillGap(Math.max(last + 1, bootAt - 7.5 * 86_400_000), bootAt, 'startup: trades missing before boot');
      if (!holes.length && !(last && bootAt - last > 15 * 60_000)) console.log('🩹 startup: no gaps in recorded trades');
    } catch (e) { console.error('🩹 startup gap check failed:', e); }
  }, 60_000);
}
