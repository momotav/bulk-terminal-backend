// ----------------------------------------------------------------------------
// Pre-deposit analytics routes
//
// Computes the deposit-campaign metrics from the indexed `predeposit_transfers`
// table (populated by services/solanaIndexer.ts from Solana mainnet). These
// are the same figures the team's Dune queries produce, rewritten as Postgres
// SQL over our own indexed data — KPIs, TVL history, distribution buckets, and
// a depositor leaderboard joined to BulkStats wallet profiles.
//
// All endpoints are cached briefly (the underlying data only changes when the
// indexer ingests new vault transfers, ~once a minute).
// ----------------------------------------------------------------------------

import { Router, Request, Response } from 'express';
import { query } from '../db';
import { getCache, setCache } from '../services/cache';

const router = Router();

const CAMPAIGN_START = '2026-06-01';

// ---- KPIs: live TVL, totals, depositor count, avg/median/largest ----------
router.get('/kpis', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:kpis';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      total_deposited: string;
      total_withdrawn: string;
      live_tvl: string;
      program_txns: string;
      unique_depositors: string;
      avg_deposit: string;
      median_deposit: string;
      largest_deposit: string;
    }>(`
      WITH d AS (
        SELECT
          COALESCE(SUM(amount_usdc) FILTER (WHERE direction = 'deposit'), 0)    AS total_deposited,
          COALESCE(SUM(amount_usdc) FILTER (WHERE direction = 'withdrawal'), 0) AS total_withdrawn,
          COUNT(*)                                                              AS program_txns,
          COUNT(DISTINCT counterparty) FILTER (WHERE direction = 'deposit')     AS unique_depositors,
          AVG(amount_usdc) FILTER (WHERE direction = 'deposit')                 AS avg_deposit,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY amount_usdc)
            FILTER (WHERE direction = 'deposit')                                AS median_deposit,
          MAX(amount_usdc) FILTER (WHERE direction = 'deposit')                 AS largest_deposit
        FROM predeposit_transfers
        WHERE block_time >= '${CAMPAIGN_START}'
      )
      SELECT
        total_deposited,
        total_withdrawn,
        total_deposited - total_withdrawn AS live_tvl,
        program_txns,
        unique_depositors,
        avg_deposit,
        median_deposit,
        largest_deposit
      FROM d
    `);

    const r = rows[0];
    const result = {
      liveTvl: parseFloat(r?.live_tvl || '0'),
      totalDeposited: parseFloat(r?.total_deposited || '0'),
      totalWithdrawn: parseFloat(r?.total_withdrawn || '0'),
      programTxns: parseInt(r?.program_txns || '0', 10),
      uniqueDepositors: parseInt(r?.unique_depositors || '0', 10),
      avgDeposit: parseFloat(r?.avg_deposit || '0'),
      medianDeposit: parseFloat(r?.median_deposit || '0'),
      largestDeposit: parseFloat(r?.largest_deposit || '0'),
      timestamp: Date.now(),
    };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit kpis error:', error);
    res.status(500).json({ error: 'Failed to compute KPIs' });
  }
});

// ---- TVL history: daily deposits/withdrawals + cumulative live balance -----
router.get('/tvl-history', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:tvl_history';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      day: string;
      deposits: string;
      withdrawals: string;
      net_flow: string;
      cumulative_deposits: string;
      live_balance: string;
    }>(`
      WITH days AS (
        SELECT generate_series(
          DATE '${CAMPAIGN_START}',
          CURRENT_DATE,
          INTERVAL '1 day'
        )::date AS day
      ),
      flows AS (
        SELECT
          block_time::date AS day,
          SUM(amount_usdc) FILTER (WHERE direction = 'deposit')    AS deposits,
          SUM(amount_usdc) FILTER (WHERE direction = 'withdrawal') AS withdrawals
        FROM predeposit_transfers
        WHERE block_time >= '${CAMPAIGN_START}'
        GROUP BY 1
      )
      SELECT
        d.day,
        COALESCE(f.deposits, 0)    AS deposits,
        COALESCE(f.withdrawals, 0) AS withdrawals,
        COALESCE(f.deposits, 0) - COALESCE(f.withdrawals, 0) AS net_flow,
        SUM(COALESCE(f.deposits, 0)) OVER (ORDER BY d.day) AS cumulative_deposits,
        SUM(COALESCE(f.deposits, 0) - COALESCE(f.withdrawals, 0)) OVER (ORDER BY d.day) AS live_balance
      FROM days d
      LEFT JOIN flows f ON f.day = d.day
      ORDER BY d.day
    `);

    const data = rows.map((r) => ({
      day: r.day,
      deposits: parseFloat(r.deposits),
      withdrawals: parseFloat(r.withdrawals),
      netFlow: parseFloat(r.net_flow),
      cumulativeDeposits: parseFloat(r.cumulative_deposits),
      liveBalance: parseFloat(r.live_balance),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit tvl-history error:', error);
    res.status(500).json({ error: 'Failed to compute TVL history' });
  }
});

// ---- Distribution: depositors bucketed by total deposited -----------------
router.get('/distribution', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:distribution';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      bucket: string;
      num_depositors: string;
      total_deposited: string;
      pct_depositors: string;
      pct_deposits: string;
    }>(`
      WITH d AS (
        SELECT counterparty, SUM(amount_usdc) AS total_usdc
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      bucketed AS (
        SELECT total_usdc,
          CASE
            WHEN total_usdc < 10      THEN '$0–10'
            WHEN total_usdc < 100     THEN '$10–100'
            WHEN total_usdc < 1000    THEN '$100–1K'
            WHEN total_usdc < 10000   THEN '$1K–10K'
            WHEN total_usdc < 100000  THEN '$10K–100K'
            WHEN total_usdc < 500000  THEN '$100K–500K'
            ELSE                           '$500K+'
          END AS bucket,
          CASE
            WHEN total_usdc < 10      THEN 1
            WHEN total_usdc < 100     THEN 2
            WHEN total_usdc < 1000    THEN 3
            WHEN total_usdc < 10000   THEN 4
            WHEN total_usdc < 100000  THEN 5
            WHEN total_usdc < 500000  THEN 6
            ELSE                           7
          END AS sort_order
        FROM d
      )
      SELECT
        bucket,
        COUNT(*)::text AS num_depositors,
        SUM(total_usdc)::text AS total_deposited,
        (100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0))::text AS pct_depositors,
        (100.0 * SUM(total_usdc) / NULLIF(SUM(SUM(total_usdc)) OVER (), 0))::text AS pct_deposits
      FROM bucketed
      GROUP BY bucket, sort_order
      ORDER BY sort_order
    `);

    const data = rows.map((r) => ({
      bucket: r.bucket,
      numDepositors: parseInt(r.num_depositors, 10),
      totalDeposited: parseFloat(r.total_deposited),
      pctDepositors: parseFloat(r.pct_depositors || '0'),
      pctDeposits: parseFloat(r.pct_deposits || '0'),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit distribution error:', error);
    res.status(500).json({ error: 'Failed to compute distribution' });
  }
});

// ---- Leaderboard: top depositors, joined to BulkStats wallet profiles -----
// The join to traders/users is the BulkStats differentiator — a standalone
// TVL dashboard can't link a depositor to their trading identity. We LEFT
// JOIN so depositors without a profile still appear (just without handle/pfp).
router.get('/leaderboard', async (req: Request, res: Response) => {
  const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
  const cacheKey = `predeposit:leaderboard:${limit}`;
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      rank: string;
      address: string;
      deposited: string;
      withdrawn: string;
      net: string;
      pct_of_total: string;
      txns: string;
      first_seen: string;
      last_seen: string;
      twitter_handle: string | null;
      twitter_name: string | null;
      twitter_avatar: string | null;
    }>(`
      WITH agg AS (
        SELECT
          counterparty AS address,
          SUM(amount_usdc) FILTER (WHERE direction = 'deposit')    AS deposited,
          SUM(amount_usdc) FILTER (WHERE direction = 'withdrawal') AS withdrawn,
          COUNT(*)        AS txns,
          MIN(block_time) AS first_seen,
          MAX(block_time) AS last_seen
        FROM predeposit_transfers
        WHERE block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      ranked AS (
        SELECT
          RANK() OVER (ORDER BY deposited DESC NULLS LAST) AS rank,
          address,
          COALESCE(deposited, 0)  AS deposited,
          COALESCE(withdrawn, 0)  AS withdrawn,
          COALESCE(deposited, 0) - COALESCE(withdrawn, 0) AS net,
          100.0 * COALESCE(deposited, 0) / NULLIF(SUM(COALESCE(deposited, 0)) OVER (), 0) AS pct_of_total,
          txns, first_seen, last_seen
        FROM agg
        WHERE COALESCE(deposited, 0) > 0
      )
      SELECT
        r.rank::text,
        r.address,
        r.deposited::text,
        r.withdrawn::text,
        r.net::text,
        r.pct_of_total::text,
        r.txns::text,
        r.first_seen::text,
        r.last_seen::text,
        u.twitter_handle,
        u.twitter_name,
        u.twitter_avatar
      FROM ranked r
      LEFT JOIN users u ON u.wallet_address = r.address
      ORDER BY r.deposited DESC
      LIMIT ${limit}
    `);

    const data = rows.map((r) => ({
      rank: parseInt(r.rank, 10),
      address: r.address,
      deposited: parseFloat(r.deposited),
      withdrawn: parseFloat(r.withdrawn),
      net: parseFloat(r.net),
      pctOfTotal: parseFloat(r.pct_of_total || '0'),
      txns: parseInt(r.txns, 10),
      firstSeen: r.first_seen,
      lastSeen: r.last_seen,
      twitterHandle: r.twitter_handle,
      twitterName: r.twitter_name,
      twitterAvatar: r.twitter_avatar,
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit leaderboard error:', error);
    res.status(500).json({ error: 'Failed to compute leaderboard' });
  }
});

// ---- Indexer status (for an "indexing…" banner while backfill runs) -------
router.get('/status', async (_req: Request, res: Response) => {
  try {
    const rows = await query<{
      backfill_complete: boolean;
      last_run: string | null;
      total_indexed: string;
    }>(`SELECT s.backfill_complete, s.last_run::text,
               (SELECT COUNT(*) FROM predeposit_transfers)::text AS total_indexed
        FROM predeposit_index_state s WHERE s.id = 1`);
    const r = rows[0];
    res.json({
      backfillComplete: r?.backfill_complete ?? false,
      lastRun: r?.last_run ?? null,
      totalIndexed: parseInt(r?.total_indexed || '0', 10),
      configured: (process.env.SOLANA_RPC_URL || '').length > 0,
    });
  } catch (error) {
    console.error('predeposit status error:', error);
    res.status(500).json({ error: 'Failed to read status' });
  }
});

// ---- Debug: indexed coverage (date range, counts) — confirms the table
// holds the full campaign window, not just recent txns. Handy while the
// backfill runs to verify it's reaching all-time data.
router.get('/debug', async (_req: Request, res: Response) => {
  try {
    const rows = await query<{
      total: string;
      deposits: string;
      withdrawals: string;
      earliest: string | null;
      latest: string | null;
      sum_dep: string;
      sum_wd: string;
    }>(`
      SELECT
        COUNT(*)::text AS total,
        COUNT(*) FILTER (WHERE direction = 'deposit')::text AS deposits,
        COUNT(*) FILTER (WHERE direction = 'withdrawal')::text AS withdrawals,
        MIN(block_time)::text AS earliest,
        MAX(block_time)::text AS latest,
        COALESCE(SUM(amount_usdc) FILTER (WHERE direction = 'deposit'), 0)::text AS sum_dep,
        COALESCE(SUM(amount_usdc) FILTER (WHERE direction = 'withdrawal'), 0)::text AS sum_wd
      FROM predeposit_transfers
    `);
    const stateRows = await query<{
      backfill_complete: boolean; oldest_signature: string | null;
      newest_signature: string | null; last_run: string | null;
    }>(`SELECT backfill_complete, oldest_signature, newest_signature, last_run::text
        FROM predeposit_index_state WHERE id = 1`);
    res.json({ table: rows[0], state: stateRows[0] });
  } catch (error) {
    console.error('predeposit debug error:', error);
    res.status(500).json({ error: 'debug failed' });
  }
});

// ---- Admin: reset the indexer so it re-runs a full backfill. Use after
// fixing indexing logic (e.g. switching to token-account indexing) so the
// drain restarts instead of staying stuck on a partial count. Clears the
// table and the cursor. Guarded by a token to avoid accidental wipes.
router.post('/reset', async (req: Request, res: Response) => {
  const token = req.query.token as string;
  if (token !== (process.env.PREDEPOSIT_RESET_TOKEN || 'bulkstats-reset')) {
    return res.status(403).json({ error: 'forbidden' });
  }
  try {
    await query(`TRUNCATE predeposit_transfers`);
    await query(
      `UPDATE predeposit_index_state
         SET newest_signature = NULL, oldest_signature = NULL,
             backfill_complete = FALSE, total_indexed = 0, last_run = NULL
       WHERE id = 1`,
    );
    res.json({ ok: true, message: 'Indexer reset — full backfill will run on next cycle' });
  } catch (error) {
    console.error('predeposit reset error:', error);
    res.status(500).json({ error: 'reset failed' });
  }
});

export default router;
