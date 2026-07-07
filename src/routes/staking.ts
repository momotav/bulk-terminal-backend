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
import { BULK_VOTE_ACCOUNT, BULK_IDENTITY, BULKSOL_MINT, BULKSOL_POOL, getValidatorDistribution, getHolderBalances } from '../services/stakingIndexer';
import { isBulkSolHistoryConfigured } from '../services/bulksolIndexer';

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

// ---- BulkSOL: per-validator stake distribution (live from the pool) -------
router.get('/bulksol/validators', async (_req: Request, res: Response) => {
  const cacheKey = 'staking:bulksol:validators';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const dist = await getValidatorDistribution();
    const total = dist.reduce((a, v) => a + v.activeStake, 0);
    const result = {
      total,
      count: dist.length,
      validators: dist.map((v) => ({ ...v, share: total > 0 ? v.activeStake / total : 0 })),
    };
    await setCache(cacheKey, result, 120);
    res.json(result);
  } catch (e) {
    console.error('staking/bulksol/validators error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load validator distribution' });
  }
});

// ---- BulkSOL: daily flows (mint/burn/net) — all-time from the indexer -----
router.get('/bulksol/flows', async (req: Request, res: Response) => {
  const range = String(req.query.range || 'all');
  const cacheKey = `staking:bulksol:flows:${range}`;
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const where = range === '7d' ? "WHERE day > now() - interval '7 days'"
      : range === '30d' ? "WHERE day > now() - interval '30 days'" : '';
    const rows = await query<{ day: string; mint_amount: string; burn_amount: string; new_wallets: number }>(
      `SELECT day, mint_amount, burn_amount, new_wallets FROM bulksol_daily ${where} ORDER BY day ASC`,
    );
    let cumSupply = 0, cumWallets = 0;
    const result = rows.map((r) => {
      const mint = Number(r.mint_amount), burn = Number(r.burn_amount);
      cumSupply += mint - burn; cumWallets += r.new_wallets;
      return {
        t: new Date(r.day).getTime(),
        mint, burn, net: mint - burn,
        supply: cumSupply, newWallets: r.new_wallets, cumWallets,
      };
    });
    await setCache(cacheKey, result, 120);
    res.json(result);
  } catch (e) {
    console.error('staking/bulksol/flows error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load BulkSOL flows' });
  }
});

// ---- BulkSOL: current holder distribution + whale concentration -----------
const BUCKETS: { label: string; min: number; max: number }[] = [
  { label: '< 1', min: 0, max: 1 },
  { label: '1–10', min: 1, max: 10 },
  { label: '10–100', min: 10, max: 100 },
  { label: '100–1K', min: 100, max: 1_000 },
  { label: '1K–10K', min: 1_000, max: 10_000 },
  { label: '10K–100K', min: 10_000, max: 100_000 },
  { label: '> 100K', min: 100_000, max: Infinity },
];

router.get('/bulksol/holders', async (_req: Request, res: Response) => {
  const cacheKey = 'staking:bulksol:holders';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const balances = await getHolderBalances(); // sorted desc
    const total = balances.reduce((a, b) => a + b.amount, 0);

    const distribution = BUCKETS.map((bk) => {
      const inBucket = balances.filter((b) => b.amount >= bk.min && b.amount < bk.max);
      return { label: bk.label, holders: inBucket.length, total: inBucket.reduce((a, b) => a + b.amount, 0) };
    });
    const shareOf = (n: number) => {
      const top = balances.slice(0, n).reduce((a, b) => a + b.amount, 0);
      return { count: n, amount: top, share: total > 0 ? top / total : 0 };
    };
    const result = {
      holders: balances.length,
      total,
      distribution,
      concentration: [shareOf(1), shareOf(10), shareOf(100)],
    };
    await setCache(cacheKey, result, 120);
    res.json(result);
  } catch (e) {
    console.error('staking/bulksol/holders error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load holder distribution' });
  }
});

// ---- BulkSOL: backfill progress -------------------------------------------
const BULKSOL_LAUNCH = new Date('2025-10-15').getTime(); // approx mint launch

router.get('/bulksol/status', async (_req: Request, res: Response) => {
  try {
    let st: { backfill_complete: boolean; total_indexed: string } | null = null;
    let range: { earliest: string | null; latest: string | null; days: number } | null = null;
    try {
      st = (await query<{ backfill_complete: boolean; total_indexed: string }>(
        `SELECT backfill_complete, total_indexed FROM bulksol_index_state WHERE id = 1`,
      ))[0] ?? null;
      range = (await query<{ earliest: string | null; latest: string | null; days: number }>(
        `SELECT MIN(day) AS earliest, MAX(day) AS latest, COUNT(*)::int AS days FROM bulksol_daily`,
      ))[0] ?? null;
    } catch { /* tables not created yet — indexer hasn't run */ }

    const backfillComplete = st?.backfill_complete ?? false;
    const earliest = range?.earliest ?? null;
    let progress = 0;
    if (backfillComplete) progress = 1;
    else if (earliest) {
      const now = Date.now();
      const span = now - BULKSOL_LAUNCH;
      progress = span > 0 ? Math.min(1, Math.max(0, (now - new Date(earliest).getTime()) / span)) : 0;
    }

    res.json({
      configured: isBulkSolHistoryConfigured(),
      backfillComplete,
      totalIndexed: st ? Number(st.total_indexed) : 0,
      earliestDay: earliest,
      latestDay: range?.latest ?? null,
      days: range?.days ?? 0,
      progress,
    });
  } catch (e) {
    console.error('staking/bulksol/status error:', (e as Error).message);
    res.status(500).json({ error: 'Failed to load backfill status' });
  }
});

export default router;
