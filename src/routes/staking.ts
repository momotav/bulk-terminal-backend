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
import { BULK_VOTE_ACCOUNT, BULK_IDENTITY } from '../services/stakingIndexer';

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

// ---- Native: per-epoch history for charts ---------------------------------
router.get('/native/history', async (_req: Request, res: Response) => {
  const cacheKey = 'staking:native:history';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      epoch: number;
      active_stake: string;
      delegator_count: number;
      apy: string | null;
    }>(
      `SELECT epoch, active_stake, delegator_count, apy
       FROM staking_native_snapshots
       ORDER BY epoch ASC`,
    );

    const result = rows.map((r) => ({
      epoch: r.epoch,
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

export default router;
