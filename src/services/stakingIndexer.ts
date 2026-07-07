// ----------------------------------------------------------------------------
// Native staking indexer — BULK validator
//
// Reads BULK's native staking directly from Solana mainnet (no BULK API), the
// same way solanaIndexer.ts reads the pre-deposit vault:
//   • getEpochInfo        → current epoch
//   • getVoteAccounts     → active stake + commission for the BULK vote account
//   • getProgramAccounts  → every stake account delegated to BULK, giving the
//                           delegator count and activating/deactivating split
//
// One row per epoch is upserted into `staking_native_snapshots`, so the live
// numbers stay fresh and per-epoch history accumulates for the charts.
//
// Confirmed addresses (mainnet):
//   Vote account (delegation target) : BULKEEKf9Hjy4nwCthjzheEk4joH23LLXttAHjqEZmB2
//   Validator identity               : BULKzVM41WAyQZfL34vxqdsYwEYH9mJAJyzRS4xraf8b
// ----------------------------------------------------------------------------

import { pool, query } from '../db';
import bs58 from 'bs58';

export const BULK_VOTE_ACCOUNT = 'BULKEEKf9Hjy4nwCthjzheEk4joH23LLXttAHjqEZmB2';
export const BULK_IDENTITY = 'BULKzVM41WAyQZfL34vxqdsYwEYH9mJAJyzRS4xraf8b';
// BulkSOL liquid staking (SPL multi-validator stake pool).
export const BULKSOL_MINT = 'BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn';
export const BULKSOL_POOL = '3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174';
const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
// SPL StakePool account field offsets (borsh): 1-byte account_type + 8 pubkeys
// (some with a bump byte) before the u64 balances. See spl-stake-pool state.
const OFF_VALIDATOR_LIST = 98;   // Pubkey (32)
const OFF_TOTAL_LAMPORTS = 258;  // u64
const OFF_POOL_SUPPLY = 266;     // u64
const STAKE_PROGRAM = 'Stake11111111111111111111111111111111111111';
// In a delegated stake account, the delegation's voter pubkey sits at byte 124
// (4-byte discriminant + 120-byte Meta). memcmp there isolates BULK's stakers.
const VOTER_OFFSET = 124;
const LAMPORTS_PER_SOL = 1_000_000_000;
const U64_MAX = '18446744073709551615'; // deactivationEpoch when not deactivating

const RPC_URL = process.env.SOLANA_RPC_URL || '';
let rpcId = 0;

export function isStakingConfigured(): boolean {
  return RPC_URL.length > 0;
}

interface RpcResponse<T> {
  result?: T;
  error?: { code: number; message: string };
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
      const json = (await res.json()) as RpcResponse<T>;
      if (json.error) throw new Error(`RPC ${method}: ${json.error.message}`);
      return json.result as T;
    } catch (e) {
      lastErr = e;
      if (attempt < 3) await new Promise((r) => setTimeout(r, 400 * 2 ** attempt));
    }
  }
  throw lastErr;
}

async function ensureSchema(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS staking_native_snapshots (
      epoch            integer PRIMARY KEY,
      active_stake     numeric NOT NULL,      -- SOL
      delegator_count  integer NOT NULL,
      commission       integer NOT NULL,      -- percent
      activating       numeric NOT NULL DEFAULT 0,
      deactivating     numeric NOT NULL DEFAULT 0,
      apy              numeric,               -- approximate, network-derived
      captured_at      timestamptz NOT NULL DEFAULT now()
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS staking_bulksol_snapshots (
      epoch          integer PRIMARY KEY,
      tvl_sol        numeric NOT NULL,       -- SOL backing the pool
      supply         numeric NOT NULL,       -- BulkSOL in circulation
      exchange_rate  numeric NOT NULL,       -- SOL per BulkSOL
      holders        integer,
      validators     integer,
      captured_at    timestamptz NOT NULL DEFAULT now()
    )
  `);
}

// ---- RPC result shapes (only the fields we read) --------------------------
interface EpochInfo { epoch: number; slotIndex: number; slotsInEpoch: number; }
interface VoteAccount { votePubkey: string; nodePubkey: string; activatedStake: number; commission: number; }
interface VoteAccountsResult { current: VoteAccount[]; delinquent: VoteAccount[]; }
interface InflationRate { total: number; validator: number; foundation: number; epoch: number; }
interface StakeAccount {
  account: { data: { parsed?: { info?: { stake?: { delegation?: { voter: string; stake: string; activationEpoch: string; deactivationEpoch: string } } } } } };
}

// ---- BulkSOL (SPL stake pool) ---------------------------------------------
interface AccountInfoB64 { data: [string, string]; }

function readU64LE(buf: Buffer, offset: number): number {
  // Values here (lamports, token base units) stay within 2^53 for this pool.
  return Number(buf.readBigUInt64LE(offset));
}

async function runBulkSol(epoch: number): Promise<void> {
  // Stake pool account → total SOL, pool token supply, validator-list address.
  const poolAcct = await rpc<{ value: AccountInfoB64 | null }>('getAccountInfo', [
    BULKSOL_POOL, { encoding: 'base64' },
  ]);
  if (!poolAcct.value) { console.warn('⚠️  BulkSOL pool account not found'); return; }
  const buf = Buffer.from(poolAcct.value.data[0], 'base64');

  const totalLamports = readU64LE(buf, OFF_TOTAL_LAMPORTS);
  const poolSupplyRaw = readU64LE(buf, OFF_POOL_SUPPLY);
  const tvlSol = totalLamports / LAMPORTS_PER_SOL;
  const supply = poolSupplyRaw / LAMPORTS_PER_SOL; // BulkSOL has 9 decimals
  const exchangeRate = supply > 0 ? tvlSol / supply : 0;

  // Validator count = the Vec length (u32 LE) at offset 5 of the validator list.
  let validators: number | null = null;
  try {
    const vlPubkey = bs58.encode(buf.subarray(OFF_VALIDATOR_LIST, OFF_VALIDATOR_LIST + 32));
    const vl = await rpc<{ value: AccountInfoB64 | null }>('getAccountInfo', [vlPubkey, { encoding: 'base64' }]);
    if (vl.value) {
      const vb = Buffer.from(vl.value.data[0], 'base64');
      validators = vb.readUInt32LE(5);
    }
  } catch { /* validators optional */ }

  // Holder count = token accounts of the mint with a non-zero balance. dataSlice
  // to just the 8-byte amount keeps the response small.
  let holders: number | null = null;
  try {
    const accts = await rpc<{ account: { data: [string, string] } }[]>('getProgramAccounts', [
      TOKEN_PROGRAM,
      {
        encoding: 'base64',
        dataSlice: { offset: 64, length: 8 },
        filters: [{ dataSize: 165 }, { memcmp: { offset: 0, bytes: BULKSOL_MINT } }],
      },
    ]);
    holders = accts.filter((a) => readU64LE(Buffer.from(a.account.data[0], 'base64'), 0) > 0).length;
  } catch { /* holders optional */ }

  await pool.query(
    `INSERT INTO staking_bulksol_snapshots
       (epoch, tvl_sol, supply, exchange_rate, holders, validators, captured_at)
     VALUES ($1, $2, $3, $4, $5, $6, now())
     ON CONFLICT (epoch) DO UPDATE SET
       tvl_sol = EXCLUDED.tvl_sol, supply = EXCLUDED.supply,
       exchange_rate = EXCLUDED.exchange_rate, holders = EXCLUDED.holders,
       validators = EXCLUDED.validators, captured_at = now()`,
    [epoch, tvlSol, supply, exchangeRate, holders, validators],
  );
  console.log(`💧 BulkSOL snapshot: ${supply.toFixed(0)} BulkSOL · ${tvlSol.toFixed(0)} SOL · rate ${exchangeRate.toFixed(4)}`);
}

export async function runStakingIndexer(): Promise<void> {
  if (!isStakingConfigured()) return;
  try {
    await ensureSchema();

    const [epochInfo, votes, inflation] = await Promise.all([
      rpc<EpochInfo>('getEpochInfo', []),
      rpc<VoteAccountsResult>('getVoteAccounts', [{ votePubkey: BULK_VOTE_ACCOUNT, keepUnstakedDelinquents: true }]),
      rpc<InflationRate>('getInflationRate', []).catch(() => null),
    ]);

    const epoch = epochInfo.epoch;
    const vote = votes.current[0] ?? votes.delinquent[0];
    if (!vote) {
      console.warn('⚠️  Staking indexer: BULK vote account not found in getVoteAccounts');
      return;
    }
    const activeStake = vote.activatedStake / LAMPORTS_PER_SOL;
    const commission = vote.commission;

    // Every stake account delegated to BULK (filtered on the voter pubkey).
    const accounts = await rpc<StakeAccount[]>('getProgramAccounts', [
      STAKE_PROGRAM,
      { encoding: 'jsonParsed', filters: [{ memcmp: { offset: VOTER_OFFSET, bytes: BULK_VOTE_ACCOUNT } }] },
    ]);

    let delegatorCount = 0;
    let activating = 0;
    let deactivating = 0;
    for (const a of accounts) {
      const d = a.account?.data?.parsed?.info?.stake?.delegation;
      if (!d || d.voter !== BULK_VOTE_ACCOUNT) continue;
      delegatorCount++;
      const sol = Number(d.stake) / LAMPORTS_PER_SOL;
      if (Number(d.activationEpoch) === epoch) activating += sol;
      if (d.deactivationEpoch !== U64_MAX && Number(d.deactivationEpoch) >= epoch) deactivating += sol;
    }

    // Approximate staking APY from network inflation, net of commission. This
    // is a v1 estimate (refine later with per-epoch getInflationReward).
    const apy = inflation ? +(inflation.total * (1 - commission / 100) * 100).toFixed(2) : null;

    await pool.query(
      `INSERT INTO staking_native_snapshots
         (epoch, active_stake, delegator_count, commission, activating, deactivating, apy, captured_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, now())
       ON CONFLICT (epoch) DO UPDATE SET
         active_stake = EXCLUDED.active_stake,
         delegator_count = EXCLUDED.delegator_count,
         commission = EXCLUDED.commission,
         activating = EXCLUDED.activating,
         deactivating = EXCLUDED.deactivating,
         apy = EXCLUDED.apy,
         captured_at = now()`,
      [epoch, activeStake, delegatorCount, commission, activating, deactivating, apy],
    );

    console.log(`🥩 Staking snapshot: epoch ${epoch} · ${activeStake.toFixed(0)} SOL · ${delegatorCount} delegators`);

    await runBulkSol(epoch).catch((e) => console.error("❌ BulkSOL snapshot error:", (e as Error).message));
  } catch (e) {
    console.error('❌ Staking indexer error:', (e as Error).message);
  }
}

// Stake moves slowly (per-epoch), and getProgramAccounts over ~4.5k stake
// accounts is a heavy call, so a 20-minute cadence keeps numbers fresh at
// trivial Helius cost.
const POLL_MS = 20 * 60_000;
let timer: NodeJS.Timeout | null = null;

export function startStakingIndexer(): void {
  if (!isStakingConfigured()) {
    console.log('ℹ️  Staking indexer disabled (SOLANA_RPC_URL not set)');
    return;
  }
  setTimeout(() => void runStakingIndexer(), 12_000);
  timer = setInterval(() => void runStakingIndexer(), POLL_MS);
  timer.unref?.();
  console.log('🥩 Native staking indexer scheduled (every 20m)');
}
