// ----------------------------------------------------------------------------
// Staking analytics routes
//
// Serves BULK's native validator staking metrics from the per-epoch snapshots
// written by services/stakingIndexer.ts (Solana mainnet). Mainnet-only, like
// pre-deposit — not network-scoped. (BulkSOL liquid-staking section: TODO.)
// ----------------------------------------------------------------------------

import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { getCache, setCache } from '../services/cache';
import { BULK_VOTE_ACCOUNT, BULK_IDENTITY, BULKSOL_MINT, BULKSOL_POOL } from '../services/stakingIndexer';

const router = Router();

// ---- Native: latest live snapshot -----------------------------------------
router.get('/native/summary', async (_req: Request, res: Response) => {
  const cacheKey = 'staking:native:summary';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const row = await queryOne<{
      epoch: number;
      active_stake: string;
      delegator_count: number;
      commission: number;
      activating: string;
      deactivating: string;
      apy: string | null;
      captured_at: string;
    }>(
      `SELECT epoch, active_stake, delegator_count, commission,
              activating, deactivating, apy, captured_at
       FROM staking_native_snapshots
       ORDER BY epoch DESC
       LIMIT 1`,
    );

    const result = {
      voteAccount: BULK_VOTE_ACCOUNT,
      identity: BULK_IDENTITY,
      epoch: row?.epoch ?? null,
      activeStake: row ? Number(row.active_stake) : 0,
      delegatorCount: row?.delegator_count ?? 0,
      commission: row?.commission ?? 0,
      activating: row ? Number(row.activating) : 0,
      deactivating: row ? Number(row.deactivating) : 0,
      apy: row?.apy != null ? Number(row.apy) : null,
      updatedAt: row?.captured_at ?? null,
    };

    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (e) {
    console.error('staking/native/summary error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load staking summary' });
  }
});

// ---- Native: time-series history for charts (?range=7d|30d|all) -----------
function rangeWhere(range: string | undefined): string {
  if (range === '7d') return "WHERE captured_at > now() - interval '7 days'";
  if (range === '30d') return "WHERE captured_at > now() - interval '30 days'";
  return '';
}

router.get('/native/history', async (req: Request, res: Response) => {
  const range = String(req.query.range || 'all');
  const cacheKey = `staking:native:history:${range}`;
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{ captured_at: string; active_stake: string; delegator_count: number; apy: string | null }>(
      `SELECT captured_at, active_stake, delegator_count, apy
       FROM staking_native_ts ${rangeWhere(range)}
       ORDER BY captured_at ASC`,
    );
    const result = rows.map((r) => ({
      t: new Date(r.captured_at).getTime(),
      activeStake: Number(r.active_stake),
      delegatorCount: r.delegator_count,
      apy: r.apy != null ? Number(r.apy) : null,
    }));
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (e) {
    console.error('staking/native/history error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load staking history' });
  }
});

// ---- BulkSOL: latest live snapshot (+ APY from exchange-rate growth) ------
const EPOCHS_PER_YEAR = 182; // Solana epoch ≈ 2 days

router.get('/bulksol/summary', async (_req: Request, res: Response) => {
  const cacheKey = 'staking:bulksol:summary';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      epoch: number; tvl_sol: string; supply: string; exchange_rate: string;
      holders: number | null; validators: number | null; captured_at: string;
    }>(
      `SELECT epoch, tvl_sol, supply, exchange_rate, holders, validators, captured_at
       FROM staking_bulksol_snapshots ORDER BY epoch DESC LIMIT 2`,
    );
    const latest = rows[0];
    const prev = rows[1];

    // APY from per-epoch pool-token appreciation, compounded to a year.
    let apy: number | null = null;
    if (latest && prev && Number(prev.exchange_rate) > 0) {
      const g = Number(latest.exchange_rate) / Number(prev.exchange_rate) - 1;
      const span = latest.epoch - prev.epoch || 1;
      apy = +((Math.pow(1 + g / span, EPOCHS_PER_YEAR) - 1) * 100).toFixed(2);
    }

    const result = {
      mint: BULKSOL_MINT,
      pool: BULKSOL_POOL,
      epoch: latest?.epoch ?? null,
      tvlSol: latest ? Number(latest.tvl_sol) : 0,
      supply: latest ? Number(latest.supply) : 0,
      exchangeRate: latest ? Number(latest.exchange_rate) : 0,
      holders: latest?.holders ?? null,
      validators: latest?.validators ?? null,
      apy,
      updatedAt: latest?.captured_at ?? null,
    };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (e) {
    console.error('staking/bulksol/summary error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load BulkSOL summary' });
  }
});

// ---- BulkSOL: time-series history (?range=7d|30d|all) ---------------------
router.get('/bulksol/history', async (req: Request, res: Response) => {
  const range = String(req.query.range || 'all');
  const cacheKey = `staking:bulksol:history:${range}`;
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{ captured_at: string; tvl_sol: string; supply: string; exchange_rate: string }>(
      `SELECT captured_at, tvl_sol, supply, exchange_rate
       FROM staking_bulksol_ts ${rangeWhere(range)}
       ORDER BY captured_at ASC`,
    );
    const result = rows.map((r) => ({
      t: new Date(r.captured_at).getTime(),
      tvlSol: Number(r.tvl_sol),
      supply: Number(r.supply),
      exchangeRate: Number(r.exchange_rate),
    }));
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (e) {
    console.error('staking/bulksol/history error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load BulkSOL history' });
  }
});

export default router;
