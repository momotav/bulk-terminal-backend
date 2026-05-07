import { Router, Request, Response } from 'express';
import { leaderboardService, TimeFrame } from '../services/leaderboard';
import { getCache, setCache } from '../services/cache';

const router = Router();

// Validate timeframe parameter
function validateTimeframe(tf: string | undefined): TimeFrame {
  const valid: TimeFrame[] = ['24h', '7d', '30d', 'all'];
  return valid.includes(tf as TimeFrame) ? (tf as TimeFrame) : 'all';
}

// Helper to wrap service calls with timeout
async function withTimeout<T>(promise: Promise<T>, ms: number = 5000): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error('Timeout')), ms))
  ]);
}

// GET /leaderboard/pnl - Top traders by PnL
router.get('/pnl', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getTopTradersByPnL(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard PnL error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No PnL data available yet' });
  }
});

// GET /leaderboard/liquidated - Most liquidated traders
router.get('/liquidated', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getMostLiquidated(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard liquidated error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No liquidation data available yet' });
  }
});

// GET /leaderboard/whales - Biggest current positions
router.get('/whales', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getBiggestPositions(limit));
    res.json({ data });
  } catch (error: any) {
    console.error('Leaderboard whales error:', error.message);
    res.json({ data: [], error: 'No whale data available yet' });
  }
});

// GET /leaderboard/active - Most active traders
router.get('/active', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getMostActive(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard active error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No activity data available yet' });
  }
});

// GET /leaderboard/volume - Top volume traders
router.get('/volume', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getTopVolume(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard volume error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No volume data available yet' });
  }
});

// GET /leaderboard/liquidations/recent - Recent liquidation events
router.get('/liquidations/recent', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getRecentLiquidations(limit);
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch liquidations' });
  }
});

// GET /leaderboard/trades/recent - Recent big trades
router.get('/trades/recent', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    const minValue = parseInt(req.query.minValue as string) || 10000;
    
    const data = await leaderboardService.getRecentTrades(limit, minValue);
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch trades' });
  }
});

// GET /leaderboard/rank/:wallet - Get wallet's rank across all leaderboards
router.get('/rank/:wallet', async (req: Request, res: Response) => {
  try {
    const wallet = req.params.wallet;
    
    if (!wallet || wallet.length < 32) {
      return res.status(400).json({ error: 'Invalid wallet address' });
    }
    
    const data = await leaderboardService.getWalletRank(wallet);
    res.json(data);
  } catch (error: any) {
    console.error('Wallet rank error:', error.message);
    res.status(500).json({ error: error.message || 'Failed to fetch wallet rank' });
  }
});

// ============================================================================
// GET /leaderboard/bulk - Proxy to BULK's official indexer leaderboard
//
// The BULK indexer at https://indexer.bulk.trade/v1/leaderboard is the
// authoritative source for tournament rankings. Our DB-backed leaderboards
// drift from BULK's because of differences in how we sum trades / fees /
// closes — for tournament viewing we want exact agreement with bulk.trade.
//
// This endpoint is a thin proxy with three additions on top of raw BULK
// data:
//   1. Caching — 60s in-memory so we don't hammer their indexer when 50
//      streamers all hit the leaderboard at once
//   2. Resilience — on BULK error we serve from a stale-cache (5min) rather
//      than returning a blank leaderboard mid-stream
//   3. Validation — restrict window/metric to known-good values so we don't
//      forward arbitrary user input to their indexer
//
// Per the BULK dev: window=24h actually tracks the last 12 hours of trades
// (indexer limitation). We don't fight this — just expose the same window
// values their UI uses, so what users see on bulkstats matches what they
// see on bulk.trade exactly.
// ============================================================================

const BULK_INDEXER_URL = 'https://indexer.bulk.trade/v1/leaderboard';

// Allowlists. Values outside these will return 400. These mirror what
// bulk.trade's own UI lets users pick — if they ever expand, we add to
// these arrays. Keep the order matching their UI for clarity.
const BULK_WINDOWS = ['24h', '7d', '30d', 'all'] as const;
const BULK_METRICS = [
  'cashflow_adjusted_roi',  // their default; closest to "real" trader skill
  'realized_pnl',            // simplest
  'net_realized_pnl',        // realized minus fees/funding
  'volume',
  'roi',
  'net_realized_roi',
  'win_rate',
] as const;

type BulkWindow = typeof BULK_WINDOWS[number];
type BulkMetric = typeof BULK_METRICS[number];

interface BulkLeaderboardRow {
  rank: number;
  wallet: string;
  realized_pnl: number;
  net_realized_pnl: number;
  volume: number;
  closed_count: number;
  roi: number | null;
  net_realized_roi: number | null;
  cashflow_adjusted_roi: number | null;
  win_rate: number;
  updated_at: string;
}

interface BulkLeaderboardResponse {
  window: string;
  metric: string;
  page: number;
  page_size: number;
  limit: number;
  offset: number;
  total: number;
  total_pages: number;
  has_next: boolean;
  has_prev: boolean;
  rows: BulkLeaderboardRow[];
}

router.get('/bulk', async (req: Request, res: Response) => {
  try {
    // Validate inputs against the allowlists. Anything off the allowlist
    // gets rejected before hitting BULK, so we never leak unexpected query
    // strings to their indexer.
    const window = (req.query.window as string) || '24h';
    const metric = (req.query.metric as string) || 'cashflow_adjusted_roi';
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(req.query.page_size as string) || 50));

    if (!BULK_WINDOWS.includes(window as BulkWindow)) {
      return res.status(400).json({
        error: `Invalid window. Must be one of: ${BULK_WINDOWS.join(', ')}`,
      });
    }
    if (!BULK_METRICS.includes(metric as BulkMetric)) {
      return res.status(400).json({
        error: `Invalid metric. Must be one of: ${BULK_METRICS.join(', ')}`,
      });
    }

    // Cache keys parametrize on every input so distinct queries don't
    // collide. 60s fresh, 5min stale (only consulted on BULK API failure).
    const queryKey = `${window}:${metric}:${page}:${pageSize}`;
    const freshKey = `bulk-leaderboard:fresh:${queryKey}`;
    const staleKey = `bulk-leaderboard:stale:${queryKey}`;

    const cached = await getCache<BulkLeaderboardResponse>(freshKey);
    if (cached) {
      res.setHeader('X-Bulkstats-Source', 'bulk-indexer');
      res.setHeader('X-Bulkstats-Cache', 'fresh');
      return res.json(cached);
    }

    // Fetch from BULK indexer. node-fetch is built into Node 18+ at this
    // point so we can use the global fetch without an import.
    const url = new URL(BULK_INDEXER_URL);
    url.searchParams.set('window', window);
    url.searchParams.set('metric', metric);
    url.searchParams.set('page', String(page));
    url.searchParams.set('page_size', String(pageSize));

    let bulkRes: globalThis.Response | null = null;
    try {
      // 5s timeout; BULK indexer is fast (<300ms typical) so anything past
      // 5s is a problem and we should fall back to stale.
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      bulkRes = await fetch(url.toString(), {
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });
      clearTimeout(timer);
    } catch (err) {
      console.error('BULK indexer fetch failed:', err);
      bulkRes = null;
    }

    if (!bulkRes || !bulkRes.ok) {
      // BULK API hiccup. Try stale cache so the page doesn't go blank.
      const stale = await getCache<BulkLeaderboardResponse>(staleKey);
      if (stale) {
        res.setHeader('X-Bulkstats-Source', 'bulk-indexer');
        res.setHeader('X-Bulkstats-Cache', 'stale');
        return res.json(stale);
      }
      // No stale data either — return a graceful empty so the frontend
      // shows "no data" instead of crashing on undefined.
      return res.status(503).json({
        error: 'BULK indexer unavailable; no cached data',
        window,
        metric,
        rows: [],
      });
    }

    const data = (await bulkRes.json()) as BulkLeaderboardResponse;

    // Cache both fresh and stale. Done in parallel since they're
    // independent writes.
    await Promise.all([
      setCache(freshKey, data, 60),
      setCache(staleKey, data, 300),
    ]);

    res.setHeader('X-Bulkstats-Source', 'bulk-indexer');
    res.setHeader('X-Bulkstats-Cache', 'miss');
    res.json(data);
  } catch (error: any) {
    console.error('GET /leaderboard/bulk error:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch BULK leaderboard' });
  }
});


// GET /leaderboard/bulk/rank/:address - Find a wallet's rank on the BULK
// indexer leaderboard for a specific window+metric.
//
// Strategy: paginate through the BULK indexer (page_size=100) until we
// either find the wallet or hit a sane cap. The cap exists because beyond
// ~2000 traders the rank doesn't carry much signal — anyone past rank 2000
// is not "ranked" in a meaningful sense, and we'd rather return found=false
// than spend 30 paged calls confirming someone's rank #4731.
//
// Response shape:
//   { found: true,  rank, total, metric, window, wallet, row }   // hit
//   { found: false, total, metric, window, wallet, scannedPages } // miss
//
// Cache: 60s fresh, 5min stale, same as the main leaderboard route. Keyed
// per-(wallet, window, metric) so different lookups don't collide.
router.get('/bulk/rank/:address', async (req: Request, res: Response) => {
  try {
    const address = String(req.params.address || '').trim();
    const window = (req.query.window as string) || '24h';
    const metric = (req.query.metric as string) || 'cashflow_adjusted_roi';

    if (!address) {
      return res.status(400).json({ error: 'address required' });
    }
    if (!BULK_WINDOWS.includes(window as BulkWindow)) {
      return res.status(400).json({
        error: `Invalid window. Must be one of: ${BULK_WINDOWS.join(', ')}`,
      });
    }
    if (!BULK_METRICS.includes(metric as BulkMetric)) {
      return res.status(400).json({
        error: `Invalid metric. Must be one of: ${BULK_METRICS.join(', ')}`,
      });
    }

    const cacheKey = `bulk-leaderboard-rank:${address}:${window}:${metric}`;
    const staleKey = `bulk-leaderboard-rank:stale:${address}:${window}:${metric}`;
    const cached = await getCache<unknown>(cacheKey);
    if (cached) {
      res.setHeader('X-Bulkstats-Cache', 'fresh');
      return res.json(cached);
    }

    // Walk pages. BULK indexer accepts page_size up to 100. We scan up
    // to 20 pages = top 2000 traders. Past that, rank is meaningless
    // for our use case.
    const PAGE_SIZE = 100;
    const MAX_PAGES = 20;
    let totalRows = 0;
    let foundRow: BulkLeaderboardRow | null = null;
    let foundRank = 0;
    let scannedPages = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
      const url = new URL(BULK_INDEXER_URL);
      url.searchParams.set('window', window);
      url.searchParams.set('metric', metric);
      url.searchParams.set('page', String(page));
      url.searchParams.set('page_size', String(PAGE_SIZE));

      let bulkRes: globalThis.Response | null = null;
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 5000);
        bulkRes = await fetch(url.toString(), {
          signal: controller.signal,
          headers: { Accept: 'application/json' },
        });
        clearTimeout(timer);
      } catch (err) {
        console.error(`BULK indexer fetch failed on page ${page}:`, err);
        break;
      }

      if (!bulkRes || !bulkRes.ok) break;

      const data = (await bulkRes.json()) as BulkLeaderboardResponse;
      scannedPages = page;
      totalRows = data.total ?? totalRows;

      const hit = data.rows.find((r) => r.wallet === address);
      if (hit) {
        foundRow = hit;
        foundRank = hit.rank;
        break;
      }
      if (!data.has_next) break;
    }

    const result = foundRow
      ? {
          found: true as const,
          rank: foundRank,
          total: totalRows,
          metric,
          window,
          wallet: address,
          row: foundRow,
        }
      : {
          found: false as const,
          total: totalRows,
          metric,
          window,
          wallet: address,
          scannedPages,
        };

    // Cache both fresh and stale. Misses cache too — re-scanning 20 pages
    // every render for a wallet that's not on the leaderboard would be
    // wasteful. Hits and misses both expire in 60s/5min.
    await Promise.all([
      setCache(cacheKey, result, 60),
      setCache(staleKey, result, 300),
    ]);

    res.setHeader('X-Bulkstats-Cache', 'miss');
    return res.json(result);
  } catch (error: any) {
    console.error('GET /leaderboard/bulk/rank error:', error);
    return res.status(500).json({ error: error.message || 'Failed to fetch rank' });
  }
});


export default router;
