// Live account state for the most active traders, from BULK's account WebSocket
// stream instead of polling POST /account.
//
// Why: the leaderboard sweep polled ~250 full accounts every 5 minutes, which
// BULK started answering with hundreds of 429s per window; visitor requests then
// queued behind it. BULK's docs recommend streams over polling, and one `account`
// subscription takes many wallets (100 per connection; 15 connections per IP), so
// 3 connections hold the top 300 with no HTTP calls at all.
//
// State per wallet: margin (snapshot + marginUpdate) and positions (snapshot +
// positionUpdate). Order updates are noisy (market makers send hundreds a second)
// and unused here, so they're dropped before parsing. Parents with isolated
// accounts get no incremental margin deltas, so each shard reconnects every 30
// minutes for a fresh snapshot.

import WebSocket from 'ws';
import { query } from '../db';
import { isSystemWallet } from './systemWallets';

const WS_URL = process.env.BULK_WS_URL || 'wss://mainnet-ws1.bulk.trade';
const PER_CONN = 100;
const TRACK = 300;
const REFRESH_MS = 30 * 60_000;   // fresh snapshots (and re-pick the top wallets)
const PONG_TIMEOUT_MS = 60_000;

export interface LivePosition { symbol: string; size: number; price: number; notional: number; leverage: number; liquidationPrice: number; unrealizedPnl: number; fairPrice?: number; iso?: boolean; [k: string]: unknown }
export interface LiveAccount { margin: Record<string, number>; positions: LivePosition[]; snapshotAt: number; updatedAt: number }

const state = new Map<string, { margin: Record<string, number>; positions: Map<string, LivePosition>; snapshotAt: number; updatedAt: number }>();
const shards: { ws: WebSocket | null; users: string[]; pongAt: number; timer: NodeJS.Timeout | null }[] = [];
let tracked: string[] = [];

const posKey = (p: { symbol?: unknown; iso?: unknown }) => `${String(p.symbol)}:${p.iso ? 'i' : 'c'}`;

function onMessage(raw: WebSocket.Data): void {
  const text = raw.toString();
  // Cheap pre-filter: skip the high-volume messages we don't use without parsing them.
  if (!text.includes('"accountSnapshot"') && !text.includes('"marginUpdate"') && !text.includes('"positionUpdate"')) return;
  let m: any;
  try { m = JSON.parse(text); } catch { return; }
  const user = typeof m.topic === 'string' && m.topic.startsWith('account.') ? m.topic.slice(8) : null;
  const d = m.data;
  if (!user || !d) return;
  const now = Date.now();
  if (d.type === 'accountSnapshot') {
    const positions = new Map<string, LivePosition>();
    for (const p of Array.isArray(d.positions) ? d.positions : []) if (Number(p.size)) positions.set(posKey(p), p);
    state.set(user, { margin: { ...(d.margin ?? {}) }, positions, snapshotAt: now, updatedAt: now });
    return;
  }
  const s = state.get(user);
  if (!s) return; // deltas before a snapshot can't be applied
  if (d.type === 'marginUpdate') {
    const { type: _t, ...fields } = d;
    Object.assign(s.margin, fields);
  } else if (d.type === 'positionUpdate') {
    if (Number(d.size)) s.positions.set(posKey(d), d); else s.positions.delete(posKey(d));
  }
  s.updatedAt = now;
}

function openShard(i: number): void {
  const sh = shards[i];
  if (!sh || !sh.users.length) return;
  try { sh.ws?.terminate(); } catch { /* replaced below */ }
  const ws = new WebSocket(WS_URL);
  sh.ws = ws; sh.pongAt = Date.now();
  ws.on('open', () => ws.send(JSON.stringify({ method: 'subscribe', subscription: [{ type: 'account', user: sh.users }] })));
  ws.on('message', onMessage);
  ws.on('pong', () => { sh.pongAt = Date.now(); });
  ws.on('ping', () => { sh.pongAt = Date.now(); });
  ws.on('error', (e) => console.error(`👤 account stream ${i} error:`, e.message));
  ws.on('close', () => {
    if (sh.ws !== ws) return; // replaced on purpose
    sh.ws = null;
    setTimeout(() => { if (!sh.ws) openShard(i); }, 5_000 + Math.random() * 5_000);
  });
}

async function pickWallets(): Promise<string[]> {
  const rows = await query<{ wallet_address: string }>(
    `SELECT wallet_address FROM traders WHERE total_volume > 0 ORDER BY total_volume DESC LIMIT $1`, [TRACK + 20]);
  return rows.map((r) => r.wallet_address).filter((w) => !isSystemWallet(w)).slice(0, TRACK);
}

async function refresh(): Promise<void> {
  try {
    tracked = await pickWallets();
    const groups: string[][] = [];
    for (let i = 0; i < tracked.length; i += PER_CONN) groups.push(tracked.slice(i, i + PER_CONN));
    groups.forEach((users, i) => {
      if (!shards[i]) shards[i] = { ws: null, users, pongAt: Date.now(), timer: null };
      shards[i].users = users;
      // Staggered so the snapshots don't all land at once.
      setTimeout(() => openShard(i), i * 3_000);
    });
    // Drop state for wallets no longer tracked.
    const keep = new Set(tracked);
    for (const w of state.keys()) if (!keep.has(w)) state.delete(w);
    console.log(`👤 Account stream: tracking ${tracked.length} wallets on ${groups.length} connections`);
  } catch (e) {
    console.error('👤 account stream refresh failed:', e);
  }
}

export function startAccountStream(): void {
  void refresh();
  setInterval(() => void refresh(), REFRESH_MS);
  // Dead-socket check: ping every 30s; no pong for a minute → reconnect.
  setInterval(() => {
    shards.forEach((sh, i) => {
      if (!sh.ws || sh.ws.readyState !== WebSocket.OPEN) return;
      try { sh.ws.ping(); } catch { /* checked below */ }
      if (Date.now() - sh.pongAt > PONG_TIMEOUT_MS) { console.warn(`👤 account stream ${i}: no pong, reconnecting`); openShard(i); }
    });
  }, 30_000);
}

/**
 * A tracked wallet's live state, or null when we don't stream it or its snapshot
 * is older than `maxAgeMs` (then the caller falls back to POST /account).
 */
export function getLiveAccount(wallet: string, maxAgeMs = 40 * 60_000): LiveAccount | null {
  const s = state.get(wallet);
  if (!s || Date.now() - s.snapshotAt > maxAgeMs) return null;
  const positions = Array.from(s.positions.values());
  // Unrealized from the positions, which keep updating even where the parent's
  // margin deltas are suppressed (isolated accounts).
  const unrealized = positions.reduce((a, p) => a + (Number(p.unrealizedPnl) || 0), 0);
  return { margin: { ...s.margin, unrealizedPnl: positions.length ? unrealized : Number(s.margin.unrealizedPnl) || 0 }, positions, snapshotAt: s.snapshotAt, updatedAt: s.updatedAt };
}

/** Every tracked wallet's fresh live state (positioning snapshots). */
export function allLiveAccounts(maxAgeMs = 40 * 60_000): { wallet: string; positions: LivePosition[] }[] {
  const out: { wallet: string; positions: LivePosition[] }[] = [];
  for (const [wallet, s] of state) if (Date.now() - s.snapshotAt <= maxAgeMs) out.push({ wallet, positions: Array.from(s.positions.values()) });
  return out;
}

export function accountStreamStatus() {
  const open = shards.filter((s) => s.ws?.readyState === WebSocket.OPEN).length;
  let fresh = 0; for (const s of state.values()) if (Date.now() - s.snapshotAt < 40 * 60_000) fresh++;
  return { tracked: tracked.length, connections: shards.length, open, walletsWithState: fresh };
}
