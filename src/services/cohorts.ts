// Trader cohorts and position changes, from the live account stream (the 300
// most active wallets by volume). No BULK calls.
//
//  • Cohorts: each wallet goes into one PnL group (all-time realized + open PnL,
//    same as the wallet page) and one account-size group (margin balance + open
//    PnL). Per group: wallets, long/short exposure, and per market.
//  • Every 5 minutes: each wallet's positions (wallet_positions, kept 8 days) so
//    "who added or cut in the last 15m … 7d" can be answered, and each group's
//    exposure (cohort_snapshots, kept for good) so you can see when a group flips.

import { query } from '../db';
import { allLiveAccounts, getLiveAccount } from './accountStream';

export type CohortKind = 'pnl' | 'size';
export interface CohortDef { key: string; label: string; min: number; max: number }

// Lower bound inclusive, upper exclusive.
export const PNL_COHORTS: CohortDef[] = [
  { key: 'pnl_xl_win', label: 'Big winners', min: 100_000, max: Infinity },
  { key: 'pnl_win', label: 'Winners', min: 10_000, max: 100_000 },
  { key: 'pnl_small_win', label: 'Small winners', min: 0, max: 10_000 },
  { key: 'pnl_small_loss', label: 'Small losers', min: -10_000, max: 0 },
  { key: 'pnl_loss', label: 'Losers', min: -100_000, max: -10_000 },
  { key: 'pnl_xl_loss', label: 'Big losers', min: -Infinity, max: -100_000 },
];
export const SIZE_COHORTS: CohortDef[] = [
  { key: 'size_whale', label: 'Whales', min: 1_000_000, max: Infinity },
  { key: 'size_large', label: 'Large', min: 100_000, max: 1_000_000 },
  { key: 'size_mid', label: 'Mid', min: 10_000, max: 100_000 },
  { key: 'size_small', label: 'Small', min: 1_000, max: 10_000 },
  { key: 'size_micro', label: 'Micro', min: -Infinity, max: 1_000 },
];
const DEFS: Record<CohortKind, CohortDef[]> = { pnl: PNL_COHORTS, size: SIZE_COHORTS };
const pick = (defs: CohortDef[], v: number) => defs.find((d) => v >= d.min && v < d.max)!;

const num = (v: unknown) => { const x = Number(v); return Number.isFinite(x) ? x : 0; };

export interface WalletState {
  wallet: string; pnl: number; accountValue: number;
  positions: { symbol: string; size: number; notional: number; entry: number }[];
}

/** Every tracked wallet with fresh state: PnL, account value, open positions. */
export function liveWallets(): WalletState[] {
  const out: WalletState[] = [];
  for (const { wallet } of allLiveAccounts()) {
    const a = getLiveAccount(wallet);
    if (!a) continue;
    const m = a.margin as Record<string, unknown>;
    // Account-plus-isolated totals when the wallet has isolated accounts.
    const realized = m.aggRealizedPnl !== undefined ? num(m.aggRealizedPnl) : num(m.realizedPnl);
    const balance = m.aggTotalMargin !== undefined ? num(m.aggTotalMargin) : num(m.totalMargin);
    const unrealized = num(m.unrealizedPnl);
    out.push({
      wallet, pnl: realized + unrealized, accountValue: balance + unrealized,
      positions: a.positions
        .map((p) => ({ symbol: String(p.symbol), size: num(p.size), notional: Math.abs(num(p.notional)), entry: num(p.price) }))
        .filter((p) => p.size !== 0 && p.notional >= 10),
    });
  }
  return out;
}

export interface CohortRow {
  key: string; label: string; min: number | null; max: number | null;
  wallets: number; withPositions: number; long: number; short: number;
  markets: { symbol: string; long: number; short: number }[];
}

export function buildCohorts(kind: CohortKind, wallets = liveWallets()): CohortRow[] {
  const defs = DEFS[kind];
  const rows = new Map<string, CohortRow & { _m: Map<string, { long: number; short: number }> }>();
  for (const d of defs) rows.set(d.key, {
    key: d.key, label: d.label, min: Number.isFinite(d.min) ? d.min : null, max: Number.isFinite(d.max) ? d.max : null,
    wallets: 0, withPositions: 0, long: 0, short: 0, markets: [], _m: new Map(),
  });
  for (const w of wallets) {
    const r = rows.get(pick(defs, kind === 'pnl' ? w.pnl : w.accountValue).key)!;
    r.wallets++;
    if (w.positions.length) r.withPositions++;
    for (const p of w.positions) {
      const m = r._m.get(p.symbol) ?? { long: 0, short: 0 };
      if (p.size > 0) { m.long += p.notional; r.long += p.notional; } else { m.short += p.notional; r.short += p.notional; }
      r._m.set(p.symbol, m);
    }
  }
  return Array.from(rows.values()).map(({ _m, ...r }) => ({
    ...r,
    markets: Array.from(_m.entries()).map(([symbol, v]) => ({ symbol, ...v })).sort((a, b) => b.long + b.short - (a.long + a.short)),
  }));
}

/** 5-minute snapshot: every wallet's positions, and every cohort's exposure. */
export async function snapshotCohorts(ts: Date): Promise<void> {
  const wallets = liveWallets();
  if (wallets.length < 50) return; // stream not warm yet
  const pos: unknown[] = [];
  for (const w of wallets) for (const p of w.positions) pos.push(ts, w.wallet, p.symbol, p.size, p.notional, p.entry);
  // Wallets with no positions still get a marker row, so "closed everything" is
  // told apart from "not tracked at that time".
  for (const w of wallets) if (!w.positions.length) pos.push(ts, w.wallet, '', 0, 0, 0);
  for (let i = 0; i < pos.length; i += 6 * 500) {
    const chunk = pos.slice(i, i + 6 * 500);
    const vals = Array.from({ length: chunk.length / 6 }, (_, k) => `($${k * 6 + 1},$${k * 6 + 2},$${k * 6 + 3},$${k * 6 + 4},$${k * 6 + 5},$${k * 6 + 6})`).join(',');
    await query(`INSERT INTO wallet_positions (ts, wallet, symbol, size, notional, entry) VALUES ${vals} ON CONFLICT DO NOTHING`, chunk).catch((e) => console.error('wallet_positions insert failed:', e.message));
  }
  for (const kind of ['pnl', 'size'] as CohortKind[]) {
    for (const c of buildCohorts(kind, wallets)) {
      const rows: unknown[][] = [[ts, c.key, 'ALL', c.long, c.short, c.wallets]];
      for (const m of c.markets) rows.push([ts, c.key, m.symbol, m.long, m.short, null]);
      const flat = rows.flat();
      const vals = rows.map((_, k) => `($${k * 6 + 1},$${k * 6 + 2},$${k * 6 + 3},$${k * 6 + 4},$${k * 6 + 5},$${k * 6 + 6})`).join(',');
      await query(`INSERT INTO cohort_snapshots (ts, cohort, symbol, long_usd, short_usd, wallets) VALUES ${vals} ON CONFLICT DO NOTHING`, flat).catch(() => {});
    }
  }
  await query(`DELETE FROM wallet_positions WHERE ts < NOW() - INTERVAL '8 days'`).catch(() => {});
}

export type ChangeKind = 'opened' | 'closed' | 'added' | 'reduced' | 'flipped';
export interface PositionChange {
  wallet: string; symbol: string; kind: ChangeKind; side: 'long' | 'short';
  sizeBefore: number; sizeNow: number; notionalBefore: number; notionalNow: number;
  /** Signed USD change in exposure: + more long / less short, − the reverse. */
  deltaUsd: number; entryNow: number | null;
}

/**
 * What each tracked wallet changed between the snapshot closest to `windowMin`
 * ago and the latest one. Only wallets present in both are compared.
 */
export async function positionChanges(windowMin: number): Promise<{ from: number | null; to: number | null; changes: PositionChange[] }> {
  const [{ latest } = { latest: null }] = await query<{ latest: Date | null }>(`SELECT MAX(ts) AS latest FROM wallet_positions`);
  if (!latest) return { from: null, to: null, changes: [] };
  const [{ before } = { before: null }] = await query<{ before: Date | null }>(
    `SELECT MAX(ts) AS before FROM wallet_positions WHERE ts <= $1::timestamp - make_interval(mins => $2)`, [latest, windowMin]);
  if (!before) return { from: null, to: new Date(latest).getTime(), changes: [] };
  const rows = await query<{ ts: Date; wallet: string; symbol: string; size: string; notional: string; entry: string }>(
    `SELECT ts, wallet, symbol, size, notional, entry FROM wallet_positions WHERE ts = $1 OR ts = $2`, [before, latest]);
  const a = new Map<string, Map<string, { size: number; notional: number; entry: number }>>();
  const b = new Map<string, Map<string, { size: number; notional: number; entry: number }>>();
  for (const r of rows) {
    const tgt = new Date(r.ts).getTime() === new Date(latest).getTime() ? b : a;
    const m = tgt.get(r.wallet) ?? new Map(); tgt.set(r.wallet, m);
    if (r.symbol) m.set(r.symbol, { size: Number(r.size), notional: Number(r.notional), entry: Number(r.entry) });
  }
  const changes: PositionChange[] = [];
  for (const [wallet, now] of b) {
    const was = a.get(wallet);
    if (!was) continue; // not tracked back then
    for (const symbol of new Set([...was.keys(), ...now.keys()])) {
      const p0 = was.get(symbol), p1 = now.get(symbol);
      const s0 = p0?.size ?? 0, s1 = p1?.size ?? 0;
      const n0 = p0?.notional ?? 0, n1 = p1?.notional ?? 0;
      // Ignore price drift: compare size, and need a real change (≥2% of size, ≥$500).
      if (s0 === s1) continue;
      const px = (n1 || n0) / Math.max(Math.abs(s1) || Math.abs(s0), 1e-12);
      const deltaUsd = (s1 - s0) * px;
      if (Math.abs(s1 - s0) * px < 500) continue;
      if (s0 && s1 && Math.sign(s0) === Math.sign(s1) && Math.abs(s1 - s0) / Math.abs(s0) < 0.02) continue;
      const kind: ChangeKind = !s0 ? 'opened' : !s1 ? 'closed' : Math.sign(s0) !== Math.sign(s1) ? 'flipped' : Math.abs(s1) > Math.abs(s0) ? 'added' : 'reduced';
      const side = (s1 || s0) > 0 ? 'long' : 'short';
      changes.push({ wallet, symbol, kind, side, sizeBefore: s0, sizeNow: s1, notionalBefore: n0, notionalNow: n1, deltaUsd, entryNow: p1 ? p1.entry : null });
    }
  }
  changes.sort((x, y) => Math.abs(y.deltaUsd) - Math.abs(x.deltaUsd));
  return { from: new Date(before).getTime(), to: new Date(latest).getTime(), changes };
}
