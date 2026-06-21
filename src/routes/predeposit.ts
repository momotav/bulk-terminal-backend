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

// ---- Depositor growth: new depositors per day + cumulative -----------------
router.get('/depositor-growth', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:depositor_growth';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      day: string;
      new_depositors: string;
      cumulative_depositors: string;
    }>(`
      WITH days AS (
        SELECT generate_series(DATE '${CAMPAIGN_START}', CURRENT_DATE, INTERVAL '1 day')::date AS day
      ),
      firsts AS (
        -- first day each wallet ever deposited
        SELECT counterparty, MIN(block_time::date) AS first_day
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      per_day AS (
        SELECT first_day AS day, COUNT(*) AS new_depositors
        FROM firsts GROUP BY first_day
      )
      SELECT
        d.day,
        COALESCE(p.new_depositors, 0)::text AS new_depositors,
        SUM(COALESCE(p.new_depositors, 0)) OVER (ORDER BY d.day)::text AS cumulative_depositors
      FROM days d
      LEFT JOIN per_day p ON p.day = d.day
      ORDER BY d.day
    `);
    const data = rows.map((r) => ({
      day: r.day,
      newDepositors: parseInt(r.new_depositors, 10),
      cumulativeDepositors: parseInt(r.cumulative_depositors, 10),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit depositor-growth error:', error);
    res.status(500).json({ error: 'Failed to compute depositor growth' });
  }
});

// ---- Daily activity: active depositors + deposit txns per day --------------
router.get('/daily-activity', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:daily_activity';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{
      day: string;
      active_depositors: string;
      deposit_txns: string;
    }>(`
      WITH days AS (
        SELECT generate_series(DATE '${CAMPAIGN_START}', CURRENT_DATE, INTERVAL '1 day')::date AS day
      ),
      daily AS (
        SELECT block_time::date AS day,
               COUNT(DISTINCT counterparty) AS active_depositors,
               COUNT(*)                     AS deposit_txns
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY 1
      )
      SELECT
        d.day,
        COALESCE(dl.active_depositors, 0)::text AS active_depositors,
        COALESCE(dl.deposit_txns, 0)::text      AS deposit_txns
      FROM days d
      LEFT JOIN daily dl ON dl.day = d.day
      ORDER BY d.day
    `);
    const data = rows.map((r) => ({
      day: r.day,
      activeDepositors: parseInt(r.active_depositors, 10),
      depositTxns: parseInt(r.deposit_txns, 10),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit daily-activity error:', error);
    res.status(500).json({ error: 'Failed to compute daily activity' });
  }
});

// ---- Avg deposit size per day — signals whale arrival vs retail tail ------
router.get('/avg-deposit-trend', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:avg_deposit_trend';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);

  try {
    const rows = await query<{ day: string; avg_deposit: string; median_deposit: string }>(`
      WITH days AS (
        SELECT generate_series(DATE '${CAMPAIGN_START}', CURRENT_DATE, INTERVAL '1 day')::date AS day
      ),
      daily AS (
        SELECT block_time::date AS day,
               AVG(amount_usdc)                                              AS avg_deposit,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY amount_usdc)      AS median_deposit
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY 1
      )
      SELECT d.day,
             COALESCE(dl.avg_deposit, 0)::text    AS avg_deposit,
             COALESCE(dl.median_deposit, 0)::text AS median_deposit
      FROM days d LEFT JOIN daily dl ON dl.day = d.day
      ORDER BY d.day
    `);
    const data = rows.map((r) => ({
      day: r.day,
      avgDeposit: parseFloat(r.avg_deposit),
      medianDeposit: parseFloat(r.median_deposit),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit avg-deposit-trend error:', error);
    res.status(500).json({ error: 'Failed to compute avg deposit trend' });
  }
});

// ---- Wallet concentration: top 1/10/100 wallets' share of total ------------
router.get('/concentration', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:concentration';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ top1: string; top10: string; top100: string; total: string; depositors: string }>(`
      WITH d AS (
        SELECT counterparty, SUM(amount_usdc) AS dep
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      ranked AS (SELECT dep, ROW_NUMBER() OVER (ORDER BY dep DESC) AS rn FROM d)
      SELECT
        COALESCE(SUM(dep) FILTER (WHERE rn <= 1), 0)::text   AS top1,
        COALESCE(SUM(dep) FILTER (WHERE rn <= 10), 0)::text  AS top10,
        COALESCE(SUM(dep) FILTER (WHERE rn <= 100), 0)::text AS top100,
        COALESCE(SUM(dep), 0)::text                          AS total,
        COUNT(*)::text                                       AS depositors
      FROM ranked
    `);
    const r = rows[0];
    const total = parseFloat(r?.total || '0') || 1;
    const result = {
      total: parseFloat(r?.total || '0'),
      depositors: parseInt(r?.depositors || '0', 10),
      top1: { usd: parseFloat(r?.top1 || '0'), pct: (parseFloat(r?.top1 || '0') / total) * 100 },
      top10: { usd: parseFloat(r?.top10 || '0'), pct: (parseFloat(r?.top10 || '0') / total) * 100 },
      top100: { usd: parseFloat(r?.top100 || '0'), pct: (parseFloat(r?.top100 || '0') / total) * 100 },
      timestamp: Date.now(),
    };
    await setCache(cacheKey, result, 30);
    res.json(result);
  } catch (error) {
    console.error('predeposit concentration error:', error);
    res.status(500).json({ error: 'Failed to compute concentration' });
  }
});

// ---- Gini coefficient over time — daily inequality of deposit holdings -----
// For each day, compute the Gini of cumulative deposits-per-wallet up to
// that day. 0 = perfectly equal, 1 = one wallet holds everything.
router.get('/gini', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:gini';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    // Pull per-wallet cumulative deposit at each day boundary is expensive;
    // instead compute Gini per day from that day's running totals using a
    // window. We approximate with daily snapshots of cumulative per-wallet.
    const rows = await query<{ day: string; gini: string }>(`
      WITH days AS (
        SELECT generate_series(DATE '${CAMPAIGN_START}', CURRENT_DATE, INTERVAL '1 day')::date AS day
      ),
      wallet_day AS (
        -- cumulative deposit per wallet as of each day they were active
        SELECT counterparty,
               block_time::date AS day,
               SUM(amount_usdc) AS amt
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty, block_time::date
      ),
      cum AS (
        SELECT d.day, w.counterparty,
               SUM(w.amt) AS cum_amt
        FROM days d
        JOIN wallet_day w ON w.day <= d.day
        GROUP BY d.day, w.counterparty
      ),
      gini_calc AS (
        SELECT day,
               COUNT(*) AS n,
               SUM(cum_amt) AS total,
               SUM(cum_amt * rn) AS weighted
        FROM (
          SELECT day, cum_amt,
                 ROW_NUMBER() OVER (PARTITION BY day ORDER BY cum_amt) AS rn
          FROM cum
        ) t
        GROUP BY day
      )
      SELECT day,
        CASE WHEN n > 1 AND total > 0
          THEN ((2.0 * weighted) / (n * total) - (n + 1.0) / n)::text
          ELSE '0' END AS gini
      FROM gini_calc
      ORDER BY day
    `);
    const data = rows.map((r) => ({ day: r.day, gini: Math.max(0, parseFloat(r.gini)) }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('predeposit gini error:', error);
    res.status(500).json({ error: 'Failed to compute gini' });
  }
});

// ---- Cohort analysis: deposits grouped by the week a wallet first joined --
router.get('/cohorts', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:cohorts';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ cohort_week: string; depositors: string; total_deposited: string }>(`
      WITH firsts AS (
        SELECT counterparty, MIN(block_time) AS first_time
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      cohorted AS (
        SELECT f.counterparty,
               date_trunc('week', f.first_time)::date AS cohort_week,
               (SELECT SUM(amount_usdc) FROM predeposit_transfers p
                 WHERE p.counterparty = f.counterparty AND p.direction = 'deposit') AS total
        FROM firsts f
      )
      SELECT cohort_week::text,
             COUNT(*)::text AS depositors,
             COALESCE(SUM(total), 0)::text AS total_deposited
      FROM cohorted
      GROUP BY cohort_week
      ORDER BY cohort_week
    `);
    const data = rows.map((r, i) => ({
      cohortWeek: r.cohort_week,
      label: `Week ${i + 1}`,
      depositors: parseInt(r.depositors, 10),
      totalDeposited: parseFloat(r.total_deposited),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('predeposit cohorts error:', error);
    res.status(500).json({ error: 'Failed to compute cohorts' });
  }
});

// ---- New vs returning depositors per day ----------------------------------
router.get('/new-vs-returning', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:new_vs_returning';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ day: string; new_count: string; returning_count: string }>(`
      WITH firsts AS (
        SELECT counterparty, MIN(block_time::date) AS first_day
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      daily AS (
        SELECT p.block_time::date AS day, p.counterparty, f.first_day
        FROM predeposit_transfers p
        JOIN firsts f ON f.counterparty = p.counterparty
        WHERE p.direction = 'deposit' AND p.block_time >= '${CAMPAIGN_START}'
        GROUP BY p.block_time::date, p.counterparty, f.first_day
      ),
      days AS (
        SELECT generate_series(DATE '${CAMPAIGN_START}', CURRENT_DATE, INTERVAL '1 day')::date AS day
      )
      SELECT d.day::text,
             COUNT(*) FILTER (WHERE dl.first_day = d.day)::text AS new_count,
             COUNT(*) FILTER (WHERE dl.first_day < d.day)::text AS returning_count
      FROM days d
      LEFT JOIN daily dl ON dl.day = d.day
      GROUP BY d.day
      ORDER BY d.day
    `);
    const data = rows.map((r) => ({
      day: r.day,
      newDepositors: parseInt(r.new_count, 10),
      returningDepositors: parseInt(r.returning_count, 10),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 15);
    res.json(result);
  } catch (error) {
    console.error('predeposit new-vs-returning error:', error);
    res.status(500).json({ error: 'Failed to compute new vs returning' });
  }
});

// ---- Deposit heatmap: day-of-week × hour-of-day -----------------------------
router.get('/heatmap', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:heatmap';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ dow: string; hour: string; count: string; volume: string }>(`
      SELECT
        EXTRACT(DOW FROM block_time)::int::text  AS dow,
        EXTRACT(HOUR FROM block_time)::int::text AS hour,
        COUNT(*)::text                           AS count,
        COALESCE(SUM(amount_usdc), 0)::text      AS volume
      FROM predeposit_transfers
      WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
      GROUP BY 1, 2
    `);
    const data = rows.map((r) => ({
      dow: parseInt(r.dow, 10),     // 0=Sunday … 6=Saturday
      hour: parseInt(r.hour, 10),   // 0..23 UTC
      count: parseInt(r.count, 10),
      volume: parseFloat(r.volume),
    }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('predeposit heatmap error:', error);
    res.status(500).json({ error: 'Failed to compute heatmap' });
  }
});

// ---- TVL milestones: when cumulative deposits crossed each threshold -------
router.get('/milestones', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:milestones';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ block_time: string; running: string }>(`
      SELECT block_time::text,
             SUM(amount_usdc) FILTER (WHERE direction = 'deposit')
               OVER (ORDER BY block_time)::text AS running
      FROM predeposit_transfers
      WHERE block_time >= '${CAMPAIGN_START}'
      ORDER BY block_time
    `);
    const thresholds = [1e6, 5e6, 1e7, 2.5e7, 5e7];
    const campaignStart = Date.parse(`${CAMPAIGN_START}T00:00:00Z`);
    const milestones: { threshold: number; reachedAt: string | null; daysFromStart: number | null }[] = [];
    let ti = 0;
    for (const row of rows) {
      const running = parseFloat(row.running || '0');
      while (ti < thresholds.length && running >= thresholds[ti]) {
        const reachedAt = row.block_time;
        const days = (Date.parse(reachedAt) - campaignStart) / 86_400_000;
        milestones.push({ threshold: thresholds[ti], reachedAt, daysFromStart: Math.round(days * 10) / 10 });
        ti++;
      }
      if (ti >= thresholds.length) break;
    }
    // Unreached thresholds
    for (; ti < thresholds.length; ti++) {
      milestones.push({ threshold: thresholds[ti], reachedAt: null, daysFromStart: null });
    }
    const result = { milestones, timestamp: Date.now() };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('predeposit milestones error:', error);
    res.status(500).json({ error: 'Failed to compute milestones' });
  }
});

// ---- Time-to-deposit: how soon after launch wallets first deposited -------
router.get('/time-to-deposit', async (_req: Request, res: Response) => {
  const cacheKey = 'predeposit:time_to_deposit';
  const cached = await getCache<unknown>(cacheKey);
  if (cached) return res.json(cached);
  try {
    const rows = await query<{ bucket: string; count: string; sort_order: string }>(`
      WITH firsts AS (
        SELECT counterparty, MIN(block_time) AS first_time
        FROM predeposit_transfers
        WHERE direction = 'deposit' AND block_time >= '${CAMPAIGN_START}'
        GROUP BY counterparty
      ),
      elapsed AS (
        SELECT EXTRACT(EPOCH FROM (first_time - TIMESTAMP '${CAMPAIGN_START} 00:00:00')) / 86400 AS days
        FROM firsts
      )
      SELECT bucket, COUNT(*)::text AS count, sort_order::text AS sort_order
      FROM (
        SELECT
          CASE
            WHEN days < 1  THEN 'Day 1'
            WHEN days < 3  THEN 'Days 2-3'
            WHEN days < 7  THEN 'Days 4-7'
            WHEN days < 14 THEN 'Week 2'
            WHEN days < 21 THEN 'Week 3'
            ELSE                'Week 4+'
          END AS bucket,
          CASE
            WHEN days < 1  THEN 1 WHEN days < 3  THEN 2 WHEN days < 7  THEN 3
            WHEN days < 14 THEN 4 WHEN days < 21 THEN 5 ELSE 6
          END AS sort_order
        FROM elapsed
      ) t
      GROUP BY bucket, sort_order
      ORDER BY sort_order
    `);
    const data = rows.map((r) => ({ bucket: r.bucket, count: parseInt(r.count, 10) }));
    const result = { data, timestamp: Date.now() };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('predeposit time-to-deposit error:', error);
    res.status(500).json({ error: 'Failed to compute time-to-deposit' });
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
