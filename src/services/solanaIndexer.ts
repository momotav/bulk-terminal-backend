// ----------------------------------------------------------------------------
// Solana pre-deposit vault indexer
//
// Indexes USDC transfers in/out of BULK's pre-deposit vault directly from
// Solana mainnet, so BulkStats has real-time deposit analytics without a
// Dune dependency. Mirrors the Dune queries the team shared, but sources
// the same on-chain data ourselves.
//
// Strategy:
//   - getSignaturesForAddress pages backward through every tx that touched
//     the vault (newest → oldest). We backfill to the campaign start once,
//     then on each run only fetch signatures newer than what we've stored.
//   - For each new signature, getTransaction(jsonParsed) gives pre/post
//     token balances; we diff the vault's USDC balance to get the transfer
//     amount + direction, and read the counterparty from the instruction.
//   - Idempotent: signature is the PK, ON CONFLICT DO NOTHING.
//
// Uses raw JSON-RPC over fetch (no @solana/web3.js) to keep deps light.
// Requires SOLANA_RPC_URL (Helius/QuickNode/etc — the public endpoint is
// too rate-limited to page thousands of signatures).
//
// Credit budget (Helius free = 1M credits/month): getSignaturesForAddress
// and getTransaction are 1 credit each on the free tier. The efficient
// getTransactionsForAddress (100 credits/call, one-shot) is Developer-plan
// only, so we use the gSFA + getTransaction loop. One-time backfill of
// ~21K vault txns ≈ 21K credits; steady state ≈ 1 credit per 2-min poll
// plus 1 per new transfer — well under 100K/month. No paid plan needed.
// ----------------------------------------------------------------------------

import { query, pool } from '../db';

// BULK pre-deposit vault (mainnet) and USDC mint. These are the same
// constants the team's Dune queries filter on.
const VAULT = '7Wpp33Dn5KKUFjaij4zKYy1XZ9kdBtHjUatAT6NcjjGt';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
// Campaign start — don't index history older than this (matches Dune).
const CAMPAIGN_START = Date.parse('2026-06-01T00:00:00Z') / 1000;

const RPC_URL = process.env.SOLANA_RPC_URL || '';

// The addresses we page signatures for. The vault OWNER (7Wpp…) only
// directly references a subset of transactions; the USDC actually lives in
// token accounts (ATAs) OWNED by the vault, and getSignaturesForAddress on
// the owner does NOT include token-account txns. So at startup we resolve
// the vault's USDC token account(s) via getTokenAccountsByOwner and index
// signatures for those — that's where deposit/withdrawal txns reference.
// Falls back to the owner address if resolution fails.
let indexTargets: string[] = [VAULT];

async function resolveTokenAccounts(): Promise<void> {
  try {
    const result = await rpc<{ value: { pubkey: string }[] }>('getTokenAccountsByOwner', [
      VAULT,
      { mint: USDC_MINT },
      { encoding: 'jsonParsed' },
    ]);
    const atas = (result?.value || []).map((v) => v.pubkey);
    if (atas.length > 0) {
      // Index the token accounts (where deposits reference). Keep the owner
      // too in case some txns reference it directly.
      indexTargets = [...atas, VAULT];
      console.log(`💰 Vault USDC token accounts resolved: ${atas.join(', ')}`);
    }
  } catch (e) {
    console.error('Could not resolve vault token accounts, indexing owner only:', e);
  }
}

export function isIndexerConfigured(): boolean {
  return RPC_URL.length > 0;
}

interface RpcResponse<T> {
  result?: T;
  error?: { code: number; message: string };
}

let rpcId = 0;
async function rpc<T>(method: string, params: unknown[]): Promise<T> {
  const res = await fetch(RPC_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method, params }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`RPC ${method} HTTP ${res.status}`);
  const json = (await res.json()) as RpcResponse<T>;
  if (json.error) throw new Error(`RPC ${method}: ${json.error.message}`);
  return json.result as T;
}

interface SignatureInfo {
  signature: string;
  slot: number;
  blockTime: number | null;
  err: unknown;
}

// Page signatures for the vault. `until` stops once we reach a signature
// we've already indexed (incremental runs); `before` pages backward
// (backfill). Returns newest→oldest.
async function getSignatures(target: string, opts: { before?: string; until?: string }): Promise<SignatureInfo[]> {
  const params: [string, Record<string, unknown>] = [
    target,
    { limit: 1000, ...(opts.before ? { before: opts.before } : {}), ...(opts.until ? { until: opts.until } : {}) },
  ];
  return rpc<SignatureInfo[]>('getSignaturesForAddress', params);
}

interface ParsedTx {
  blockTime: number | null;
  slot: number;
  meta: {
    err: unknown;
    preTokenBalances: TokenBalance[];
    postTokenBalances: TokenBalance[];
  } | null;
  transaction: {
    message: {
      accountKeys: { pubkey: string }[];
    };
  };
}
interface TokenBalance {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount: { uiAmount: number | null; uiAmountString?: string; amount: string; decimals: number };
}

// Robustly extract a token balance's UI amount. `uiAmount` (a JS number)
// can come back null from the RPC for large balances, and using `|| 0`
// then silently zeroes big deposits — which is exactly what was making the
// vault look like it held thousands instead of millions. Prefer the string
// forms, which are always present and exact:
//   1) uiAmountString (decimal string, correct precision)
//   2) raw `amount` ÷ 10^decimals (integer string → human units)
//   3) uiAmount number as a last resort
function uiAmountOf(b: TokenBalance): number {
  const t = b.uiTokenAmount;
  if (t.uiAmountString != null && t.uiAmountString !== '') {
    const n = parseFloat(t.uiAmountString);
    if (!Number.isNaN(n)) return n;
  }
  if (t.amount != null && t.amount !== '') {
    const raw = parseFloat(t.amount);
    if (!Number.isNaN(raw)) return raw / Math.pow(10, t.decimals ?? 6);
  }
  return t.uiAmount ?? 0;
}

// Parse a single tx into a vault USDC transfer, or null if it isn't one.
// We diff the vault owner's USDC balance: a positive delta = deposit
// (someone sent USDC in), negative = withdrawal. The counterparty is the
// other USDC account owner whose balance moved the opposite way.
function parseTransfer(sig: SignatureInfo, tx: ParsedTx): {
  signature: string;
  slot: number;
  blockTime: number;
  direction: 'deposit' | 'withdrawal';
  counterparty: string;
  amount: number;
} | null {
  if (!tx.meta || tx.meta.err || !sig.blockTime) return null;
  if (sig.blockTime < CAMPAIGN_START) return null;

  const usdcBalances = (bals: TokenBalance[]) =>
    bals.filter((b) => b.mint === USDC_MINT);

  const pre = usdcBalances(tx.meta.preTokenBalances || []);
  const post = usdcBalances(tx.meta.postTokenBalances || []);

  // Build owner → {pre, post} USDC amount map.
  const byOwner = new Map<string, { pre: number; post: number }>();
  for (const b of pre) {
    if (!b.owner) continue;
    const cur = byOwner.get(b.owner) || { pre: 0, post: 0 };
    cur.pre = uiAmountOf(b);
    byOwner.set(b.owner, cur);
  }
  for (const b of post) {
    if (!b.owner) continue;
    const cur = byOwner.get(b.owner) || { pre: 0, post: 0 };
    cur.post = uiAmountOf(b);
    byOwner.set(b.owner, cur);
  }

  const vault = byOwner.get(VAULT);
  if (!vault) return null;
  const vaultDelta = vault.post - vault.pre;
  if (Math.abs(vaultDelta) < 1e-6) return null; // no net USDC movement

  const direction: 'deposit' | 'withdrawal' = vaultDelta > 0 ? 'deposit' : 'withdrawal';

  // Counterparty = the owner (not the vault) whose balance moved opposite
  // to the vault by the closest matching magnitude.
  let counterparty = '';
  let bestMatch = Infinity;
  for (const [owner, amt] of byOwner.entries()) {
    if (owner === VAULT) continue;
    const delta = amt.post - amt.pre;
    // Opposite sign to vault, similar magnitude.
    if (Math.sign(delta) === -Math.sign(vaultDelta)) {
      const diff = Math.abs(Math.abs(delta) - Math.abs(vaultDelta));
      if (diff < bestMatch) {
        bestMatch = diff;
        counterparty = owner;
      }
    }
  }
  // Fallback: first non-vault account key as counterparty if balance-based
  // matching failed (e.g. CPI routed through an intermediate).
  if (!counterparty) {
    const keys = tx.transaction.message.accountKeys.map((k) => k.pubkey);
    counterparty = keys.find((k) => k !== VAULT) || 'unknown';
  }

  return {
    signature: sig.signature,
    slot: sig.slot,
    blockTime: sig.blockTime,
    direction,
    counterparty,
    amount: Math.abs(vaultDelta),
  };
}

async function getState() {
  const rows = await query<{
    newest_signature: string | null;
    oldest_signature: string | null;
    backfill_complete: boolean;
    total_indexed: string;
  }>(`SELECT newest_signature, oldest_signature, backfill_complete, total_indexed
      FROM predeposit_index_state WHERE id = 1`);
  return rows[0];
}

async function persistTransfers(
  transfers: NonNullable<ReturnType<typeof parseTransfer>>[],
): Promise<number> {
  if (transfers.length === 0) return 0;
  const client = await pool.connect();
  let inserted = 0;
  try {
    await client.query('BEGIN');
    for (const t of transfers) {
      const r = await client.query(
        `INSERT INTO predeposit_transfers
           (signature, block_slot, block_time, direction, counterparty, amount_usdc)
         VALUES ($1, $2, to_timestamp($3), $4, $5, $6)
         ON CONFLICT (signature) DO NOTHING`,
        [t.signature, t.slot, t.blockTime, t.direction, t.counterparty, t.amount],
      );
      inserted += r.rowCount || 0;
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
  return inserted;
}

// Fetch + parse a batch of signatures into transfers. getTransaction is
// rate-limit-sensitive, so we throttle with a small concurrency cap.
async function resolveTransfers(sigs: SignatureInfo[]) {
  const out: NonNullable<ReturnType<typeof parseTransfer>>[] = [];
  const CONCURRENCY = 20;
  for (let i = 0; i < sigs.length; i += CONCURRENCY) {
    const batch = sigs.slice(i, i + CONCURRENCY);
    const txs = await Promise.all(
      batch.map((s) =>
        rpc<ParsedTx>('getTransaction', [
          s.signature,
          { encoding: 'jsonParsed', maxSupportedTransactionVersion: 0 },
        ]).catch(() => null),
      ),
    );
    txs.forEach((tx, j) => {
      if (tx) {
        const parsed = parseTransfer(batch[j], tx);
        if (parsed) out.push(parsed);
      }
    });
  }
  return out;
}

let running = false;

// One indexer pass. Incremental by default: fetches signatures newer than
// `newest_signature`. If backfill isn't complete, also pages backward from
// `oldest_signature` toward the campaign start (a few pages per run so we
// don't block on a huge cold backfill).
export async function runPredepositIndexer(): Promise<void> {
  if (!isIndexerConfigured()) return; // no RPC key yet — no-op
  if (running) return; // don't overlap runs
  running = true;
  try {
    // Resolve the vault's USDC token accounts once (deposits reference the
    // ATA, not the owner — getSignaturesForAddress on the owner misses them).
    if (indexTargets.length === 1) {
      await resolveTokenAccounts();
    }

    const state = await getState();
    let backfillComplete = state?.backfill_complete || false;
    let newestSig = state?.newest_signature || null;
    let totalNew = 0;

    // For each target address (vault token accounts + owner), walk its
    // signature history. On the first run (backfill not complete) we drain
    // the FULL history to the campaign start. On later runs we only fetch
    // signatures newer than the last newest we saw (incremental).
    console.log(`💰 Indexer run: targets = [${indexTargets.map((t) => t.slice(0, 8)).join(', ')}], backfillComplete=${backfillComplete}`);
    let reachedEndForAll = true;
    for (const target of indexTargets) {
      if (!backfillComplete) {
        // Full backfill: page backward until campaign start or empty. This
        // walks every signature for the target in one run — for ~21K txns
        // that's ~21 pages of getSignaturesForAddress + getTransaction per
        // sig. May take a few minutes; runs once.
        let before: string | undefined = undefined;
        let pages = 0;
        let reachedEnd = false;
        const MAX_PAGES = 200; // safety cap
        while (pages < MAX_PAGES) {
          const page = await getSignatures(target, { before });
          if (page.length === 0) { reachedEnd = true; break; }
          if (newestSig === null && pages === 0) newestSig = page[0].signature;
          const transfers = await resolveTransfers(page);
          totalNew += await persistTransfers(transfers);
          before = page[page.length - 1].signature;
          const oldestTime = page[page.length - 1].blockTime;
          if (oldestTime !== null && oldestTime < CAMPAIGN_START) { reachedEnd = true; break; }
          pages += 1;
        }
        // If we hit MAX_PAGES without reaching the end, backfill is NOT
        // complete for this target — leave the flag false so the next run
        // resumes rather than falsely declaring done.
        if (!reachedEnd) reachedEndForAll = false;
        console.log(`💰 Backfill ${target.slice(0, 8)}…: ${pages} pages, reachedEnd=${reachedEnd}`);
      } else {
        // Incremental: only signatures newer than our last newest.
        const fresh = await getSignatures(target, { until: state?.newest_signature || undefined });
        if (fresh.length > 0) {
          if (newestSig === null || newestSig === state?.newest_signature) newestSig = fresh[0].signature;
          const transfers = await resolveTransfers(fresh);
          totalNew += await persistTransfers(transfers);
        }
      }
    }

    // Only declare backfill complete if EVERY target genuinely reached its
    // end this run. A partial/capped/errored run leaves the flag false so
    // the next run resumes the drain instead of silently switching to
    // incremental-only (which was leaving it stuck at a partial count).
    if (!backfillComplete && reachedEndForAll) backfillComplete = true;
    if (totalNew > 0) console.log(`💰 Pre-deposit indexer: +${totalNew} transfers this run`);

    // Persist cursor + counters.
    const totalRow = await query<{ c: string }>(
      `SELECT COUNT(*)::text AS c FROM predeposit_transfers`,
    );
    await query(
      `UPDATE predeposit_index_state
         SET newest_signature = $1,
             backfill_complete = $2,
             last_run = NOW(),
             total_indexed = $3
       WHERE id = 1`,
      [newestSig, backfillComplete, parseInt(totalRow[0]?.c || '0', 10)],
    );
  } catch (e) {
    console.error('Pre-deposit indexer error:', e);
  } finally {
    running = false;
  }
}

const POLL_MS = 120_000; // 2 min — vault flow isn't high-frequency, and
                         // this halves the steady-state getSignaturesForAddress
                         // call count vs 60s. On Helius free (1M credits/mo)
                         // the dominant cost is one getTransaction per NEW
                         // transfer; empty polls are 1 credit each, so 2-min
                         // cadence keeps idle cost trivial (~720 credits/day).
let timer: NodeJS.Timeout | null = null;

export function startPredepositIndexer(): void {
  if (!isIndexerConfigured()) {
    console.log('ℹ️  Pre-deposit indexer disabled (SOLANA_RPC_URL not set)');
    return;
  }
  // First run shortly after boot, then on an interval.
  setTimeout(() => void runPredepositIndexer(), 8_000);
  timer = setInterval(() => void runPredepositIndexer(), POLL_MS);
  timer.unref?.();
  console.log('💰 Pre-deposit indexer scheduled (every 60s)');
}
