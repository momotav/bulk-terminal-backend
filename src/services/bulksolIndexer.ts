// ----------------------------------------------------------------------------
// BulkSOL history indexer — all-time stake/unstake flow from the SPL token
//
// Walks the BulkSOL mint's transaction history via getSignaturesForAddress and
// records mintTo (stakes) and burn (unstakes) per day. This is the same
// walk-backward-with-a-cursor pattern as solanaIndexer.ts (pre-deposit):
//   - getSignaturesForAddress(mint) pages backward through every tx that
//     referenced the mint — which includes the stake pool's mintTo/burn.
//   - getTransaction(jsonParsed) gives the parsed SPL-token instructions;
//     we sum mintTo/burn amounts for the BulkSOL mint.
//   - Daily rollups (mint, burn, net, new stakers) + a resumable backfill
//     cursor let the charts show history all the way back to launch.
//
// Mints/burns reference the mint account, so gSFA(mint) reliably captures the
// economically meaningful flow. Holder-to-holder secondary transfers (plain
// spl-token `transfer`, no mint ref) are out of scope — distribution/whale
// charts read current holders directly instead.
// ----------------------------------------------------------------------------

import { pool, query } from '../db';
import { BULKSOL_MINT } from './stakingIndexer';

const RPC_URL = process.env.SOLANA_RPC_URL || '';
const LAMPORTS_PER_SOL = 1_000_000_000; // BulkSOL has 9 decimals
const MINT_TYPES = new Set(['mintTo', 'mintToChecked']);
const BURN_TYPES = new Set(['burn', 'burnChecked']);
const MAX_BACKFILL_PAGES = 4; // pages per run so a cold backfill doesn't block

let rpcId = 0;
let running = false;

export function isBulkSolHistoryConfigured(): boolean {
  return RPC_URL.length > 0;
}

async function rpc<T>(method: string, params: unknown[]): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(RPC_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method, params }),
        signal: AbortSignal.timeout(45_000),
      });
      if (res.status === 429 || res.status >= 500) throw new Error(`RPC ${method} HTTP ${res.status}`);
      if (!res.ok) throw new Error(`RPC ${method} HTTP ${res.status}`);
      const json = (await res.json()) as { result?: T; error?: { message: string } };
      if (json.error) throw new Error(`RPC ${method}: ${json.error.message}`);
      return json.result as T;
    } catch (e) {
      lastErr = e;
      if (attempt < 3) await new Promise((r) => setTimeout(r, 500 * 2 ** attempt));
    }
  }
  throw lastErr;
}

async function ensureSchema(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS bulksol_daily (
      day          date PRIMARY KEY,
      mint_amount  numeric NOT NULL DEFAULT 0,   -- BulkSOL minted (staked)
      burn_amount  numeric NOT NULL DEFAULT 0,   -- BulkSOL burned (unstaked)
      mint_count   integer NOT NULL DEFAULT 0,
      burn_count   integer NOT NULL DEFAULT 0,
      new_wallets  integer NOT NULL DEFAULT 0
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS bulksol_first_seen (
      owner     text PRIMARY KEY,
      first_day date NOT NULL
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS bulksol_index_state (
      id                integer PRIMARY KEY DEFAULT 1,
      newest_signature  text,
      oldest_signature  text,
      backfill_complete boolean NOT NULL DEFAULT false,
      total_indexed     bigint NOT NULL DEFAULT 0
    )
  `);
  await query(`INSERT INTO bulksol_index_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING`);
}

interface SigInfo { signature: string; blockTime: number | null; err: unknown }
interface ParsedIx { program?: string; programId?: string; parsed?: { type?: string; info?: Record<string, unknown> } }
interface ParsedTx {
  blockTime: number | null;
  meta: {
    err: unknown;
    postTokenBalances?: { accountIndex: number; owner?: string; mint: string }[];
    innerInstructions?: { instructions: ParsedIx[] }[];
  } | null;
  transaction: { message: { accountKeys: { pubkey: string }[] | string[]; instructions: ParsedIx[] } };
}

function sigs(before?: string): Promise<SigInfo[]> {
  return rpc<SigInfo[]>('getSignaturesForAddress', [BULKSOL_MINT, { limit: 1000, ...(before ? { before } : {}) }]);
}

/** Extract per-day mint/burn totals + new-staker owners from one tx. */
function parseTx(tx: ParsedTx | null): { day: string; mint: number; burn: number; mintCount: number; burnCount: number; newOwners: string[] } | null {
  if (!tx || !tx.meta || tx.meta.err || !tx.blockTime) return null;
  const day = new Date(tx.blockTime * 1000).toISOString().slice(0, 10);

  // account pubkey → owner, from postTokenBalances (for new-staker attribution)
  const keys = (tx.transaction.message.accountKeys || []).map((k) => (typeof k === 'string' ? k : k.pubkey));
  const ownerOf = new Map<string, string>();
  for (const b of tx.meta.postTokenBalances || []) {
    if (b.mint === BULKSOL_MINT && b.owner && keys[b.accountIndex]) ownerOf.set(keys[b.accountIndex], b.owner);
  }

  // Stake-pool mint/burn happen via CPI, so the token instructions live in
  // meta.innerInstructions — must walk both, not just top-level.
  const allIx: ParsedIx[] = [
    ...tx.transaction.message.instructions,
    ...(tx.meta.innerInstructions || []).flatMap((g) => g.instructions),
  ];
  let mint = 0, burn = 0, mintCount = 0, burnCount = 0;
  const newOwners: string[] = [];
  for (const ix of allIx) {
    if (ix.program !== 'spl-token') continue;
    const t = ix.parsed?.type;
    const info = ix.parsed?.info || {};
    if (info.mint !== BULKSOL_MINT) continue;
    const amt = Number(info.amount ?? (info.tokenAmount as { amount?: string } | undefined)?.amount ?? 0) / LAMPORTS_PER_SOL;
    if (t && MINT_TYPES.has(t)) {
      mint += amt; mintCount++;
      const owner = ownerOf.get(String(info.account));
      if (owner) newOwners.push(owner);
    } else if (t && BURN_TYPES.has(t)) {
      burn += amt; burnCount++;
    }
  }
  if (mintCount === 0 && burnCount === 0) return null;
  return { day, mint, burn, mintCount, burnCount, newOwners };
}

async function fetchTx(sig: string): Promise<ParsedTx | null> {
  return rpc<ParsedTx | null>('getTransaction', [sig, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 0 }]);
}

// Diagnostic: fetch the N most recent mint txns and return BOTH what the
// parser extracted AND the raw spl-token-ish instructions it saw, so we can
// see exactly how Sanctum represents mint/burn instead of guessing.
export async function debugSample(n = 5): Promise<unknown> {
  const recent = await rpc<{ signature: string; err: unknown }[]>('getSignaturesForAddress', [
    BULKSOL_MINT, { limit: n },
  ]);
  const out: unknown[] = [];
  for (const s of recent) {
    if (s.err) continue;
    const tx = await fetchTx(s.signature).catch(() => null);
    if (!tx) { out.push({ sig: s.signature, error: 'fetch failed' }); continue; }
    const parsed = parseTx(tx);
    const topIx = tx.transaction?.message?.instructions || [];
    const innerIx = (tx.meta?.innerInstructions || []).flatMap((g) => g.instructions);
    const summarize = (ix: ParsedIx) => ({
      program: ix.program ?? null,
      programId: ix.programId ?? null,
      type: ix.parsed?.type ?? null,
      // only include info keys + mint/amount fields to keep output small
      infoKeys: ix.parsed?.info ? Object.keys(ix.parsed.info) : null,
      mint: (ix.parsed?.info as Record<string, unknown> | undefined)?.mint ?? null,
      amount: (ix.parsed?.info as Record<string, unknown> | undefined)?.amount ?? null,
      tokenAmount: (ix.parsed?.info as Record<string, unknown> | undefined)?.tokenAmount ?? null,
    });
    out.push({
      sig: s.signature,
      blockTime: tx.blockTime,
      parsedResult: parsed,
      topLevel: topIx.map(summarize),
      inner: innerIx.map(summarize),
    });
  }
  return out;
}

/** Aggregate a batch of signatures into daily rows + first-seen owners. */
async function ingest(signatures: string[]): Promise<number> {
  let processed = 0;
  for (const sig of signatures) {
    const tx = await fetchTx(sig).catch(() => null);
    const parsed = parseTx(tx);
    if (!parsed) continue;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO bulksol_daily (day, mint_amount, burn_amount, mint_count, burn_count)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (day) DO UPDATE SET
           mint_amount = bulksol_daily.mint_amount + EXCLUDED.mint_amount,
           burn_amount = bulksol_daily.burn_amount + EXCLUDED.burn_amount,
           mint_count  = bulksol_daily.mint_count + EXCLUDED.mint_count,
           burn_count  = bulksol_daily.burn_count + EXCLUDED.burn_count`,
        [parsed.day, parsed.mint, parsed.burn, parsed.mintCount, parsed.burnCount],
      );
      for (const owner of parsed.newOwners) {
        const r = await client.query(
          `INSERT INTO bulksol_first_seen (owner, first_day) VALUES ($1, $2)
           ON CONFLICT (owner) DO NOTHING`,
          [owner, parsed.day],
        );
        if (r.rowCount) {
          await client.query(
            `INSERT INTO bulksol_daily (day, new_wallets) VALUES ($1, 1)
             ON CONFLICT (day) DO UPDATE SET new_wallets = bulksol_daily.new_wallets + 1`,
            [parsed.day],
          );
        }
      }
      await client.query('COMMIT');
      processed++;
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('bulksol ingest error:', (e as Error).message);
    } finally {
      client.release();
    }
  }
  return processed;
}

export async function runBulkSolHistory(): Promise<void> {
  if (!isBulkSolHistoryConfigured() || running) return;
  running = true;
  try {
    await ensureSchema();
    const st = (await query<{ newest_signature: string | null; oldest_signature: string | null; backfill_complete: boolean }>(
      `SELECT newest_signature, oldest_signature, backfill_complete FROM bulksol_index_state WHERE id = 1`,
    ))[0];

    // 1) Incremental: newest signatures since last run (page 0 only — recent).
    const recent = await sigs();
    if (recent.length) {
      const newestSeen = recent[0].signature;
      // Only ingest sigs newer than our stored newest (stop when we hit it).
      const fresh: string[] = [];
      for (const s of recent) {
        if (s.signature === st?.newest_signature) break;
        fresh.push(s.signature);
      }
      if (fresh.length) await ingest(fresh);
      await query(`UPDATE bulksol_index_state SET newest_signature = $1 WHERE id = 1`, [newestSeen]);
      // Seed oldest on first ever run.
      if (!st?.oldest_signature) {
        await query(`UPDATE bulksol_index_state SET oldest_signature = $1 WHERE id = 1`, [recent[recent.length - 1].signature]);
      }
    }

    // 2) Backfill: page backward from oldest a few pages per run.
    if (!st?.backfill_complete) {
      let before = st?.oldest_signature || (recent.length ? recent[recent.length - 1].signature : undefined);
      let pages = 0;
      while (before && pages < MAX_BACKFILL_PAGES) {
        const page = await sigs(before);
        if (page.length === 0) {
          await query(`UPDATE bulksol_index_state SET backfill_complete = true WHERE id = 1`);
          break;
        }
        await ingest(page.map((s) => s.signature));
        before = page[page.length - 1].signature;
        await query(`UPDATE bulksol_index_state SET oldest_signature = $1, total_indexed = total_indexed + $2 WHERE id = 1`, [before, page.length]);
        pages++;
        // Only an EMPTY page means we've truly reached the mint's first tx. A
        // short-but-nonempty page can happen mid-history, so we keep paging.
      }
    }
    console.log('💧 BulkSOL history pass complete');
  } catch (e) {
    console.error('❌ BulkSOL history indexer error:', (e as Error).message);
  } finally {
    running = false;
  }
}

// Wipe indexed history and re-walk from scratch. Needed after a parser change
// because daily counts are additive — re-running without a reset double-counts.
export async function resetBulkSolHistory(): Promise<void> {
  await ensureSchema();
  await query(`TRUNCATE bulksol_daily`);
  await query(`TRUNCATE bulksol_first_seen`);
  await query(`UPDATE bulksol_index_state SET newest_signature = NULL, oldest_signature = NULL, backfill_complete = false, total_indexed = 0 WHERE id = 1`);
  console.log('🔄 BulkSOL history reset — will re-walk from scratch');
  void runBulkSolHistory();
}

const POLL_MS = 15 * 60_000;
let timer: NodeJS.Timeout | null = null;

export function startBulkSolHistory(): void {
  if (!isBulkSolHistoryConfigured()) {
    console.log('ℹ️  BulkSOL history indexer disabled (SOLANA_RPC_URL not set)');
    return;
  }
  setTimeout(() => void runBulkSolHistory(), 25_000);
  timer = setInterval(() => void runBulkSolHistory(), POLL_MS);
  timer.unref?.();
  console.log('💧 BulkSOL history indexer scheduled (every 15m, backfills to launch)');
}
