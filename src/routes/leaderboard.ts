import { Router, Request, Response } from 'express';
import { leaderboardService, TimeFrame } from '../services/leaderboard';
import { getCache, setCache } from '../services/cache';
import { isSystemWallet, filterOutSystemWallets } from '../services/systemWallets';
import { bulkFetch } from '../services/bulkAuth';

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
    
    // Fetch a buffer so post-filtering BULK system wallets doesn't
    // drop the visible row count below the requested limit.
    const raw = await withTimeout(leaderboardService.getTopTradersByPnL(timeframe, limit + 5));
    const data = filterOutSystemWallets(raw, r => r.wallet_address).slice(0, limit);
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
    
    const raw = await withTimeout(leaderboardService.getMostLiquidated(timeframe, limit + 5));
    const data = filterOutSystemWallets(raw, r => r.wallet_address).slice(0, limit);
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
    
    const raw = await withTimeout(leaderboardService.getBiggestPositions(limit + 5));
    const data = filterOutSystemWallets(raw, r => r.wallet_address).slice(0, limit);
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
    
    const raw = await withTimeout(leaderboardService.getMostActive(timeframe, limit + 5));
    const data = filterOutSystemWallets(raw, r => r.wallet_address).slice(0, limit);
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
    
    const raw = await withTimeout(leaderboardService.getTopVolume(timeframe, limit + 5));
    const data = filterOutSystemWallets(raw, r => r.wallet_address).slice(0, limit);
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
    
    // Fetch a buffer extra so post-filtering system wallets doesn't
    // drop the result count below what the caller asked for.
    const rawData = await leaderboardService.getRecentLiquidations(limit + 10);
    const data = filterOutSystemWallets(rawData, l => l.wallet_address).slice(0, limit);
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
    
    const rawData = await leaderboardService.getRecentTrades(limit + 10, minValue);
    const data = filterOutSystemWallets(rawData, t => t.wallet_address).slice(0, limit);
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
// Per-wallet endpoint on BULK's indexer. Returns the full ranked row
// (rank, volume, closed_count, realized_pnl, etc.) for any wallet —
// including wallets ranked past page 20 of the leaderboard, which the
// paginated endpoint can't reach in reasonable time. Used by the
// /bulk/rank/:address route below.
const BULK_INDEXER_WALLET_URL = 'https://indexer.bulk.trade/v1/wallet';

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
      // Apply filter to cached responses too. Caches populated before
      // this filter shipped may still contain system wallets, and old
      // caches live for up to 60s (fresh) / 5min (stale). After those
      // TTLs the filter-then-cache path above takes over and this
      // becomes a no-op — but the safety belt is cheap.
      if (cached?.rows) {
        cached.rows = cached.rows.filter(row => !isSystemWallet(row.wallet));
      }
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
      bulkRes = await bulkFetch(url.toString(), {
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
        // Apply system-wallet filter to stale cache too — entries
        // cached before this filter shipped may include BULK system
        // accounts that we now hide.
        if (stale?.rows) {
          stale.rows = stale.rows.filter(row => !isSystemWallet(row.wallet));
        }
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

    // Strip out BULK's system/operational accounts (liquidation engine,
    // insurance fund, market-maker bots etc.) — these show up in BULK's
    // raw leaderboard with massive volume that would mislead users
    // browsing "top traders." They remain inspectable by direct URL.
    //
    // Filter BEFORE caching so every consumer of the cached response
    // sees the same filtered view. `total` reflects BULK's count (which
    // includes system wallets); we don't try to recompute it because
    // the ranking is BULK-side and adjusting `total` here would
    // misrepresent it. The visible row count may be 1 less than
    // `page_size` requested, which is acceptable.
    if (data?.rows) {
      data.rows = data.rows.filter(row => !isSystemWallet(row.wallet));
    }

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
// GET /leaderboard/bulk/rank/:address - Look up a wallet's stats + rank
// on the BULK indexer leaderboard for a specific window.
//
// Strategy: direct hit BULK's /v1/wallet/<addr> endpoint, which returns
// the wallet's full ranked row instantly regardless of where they sit
// in the rankings. Works for all 33K+ wallets on the exchange.
//
// Previously this route paginated through the leaderboard 100 rows at a
// time, capped at 2000 rows — meaning wallets ranked past 2000 always
// returned `found: false` even though they had real stats. The direct
// endpoint fixes that and is also dramatically faster (1 request vs up
// to 20).
//
// Response shape (unchanged for frontend compatibility):
//   { found: true,  rank, total, metric, window, wallet, row }   // hit
//   { found: false, total, metric, window, wallet, scannedPages } // miss
//
// Cache: 60s fresh, 5min stale.
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

    // Direct per-wallet lookup via BULK indexer's /v1/wallet/<addr>
    // endpoint. Returns the full ranked row (rank, volume, closed_count,
    // realized_pnl, etc.) for any wallet on the exchange — works for all
    // 33K+ wallets, not just the top 2000 the paginated endpoint could
    // reach.
    //
    // The metric filter is applied client-side: BULK's /v1/wallet
    // endpoint returns one row with all metrics, and `rank` in that
    // response is the wallet's rank by the default metric. We always
    // request `window=all` from BULK and use the row's metric values
    // directly — the `metric` param controls only the cache key (so
    // different metric requests don't collide) and the response shape
    // for downstream consumers.
    const url = new URL(`${BULK_INDEXER_WALLET_URL}/${encodeURIComponent(address)}`);
    url.searchParams.set('window', window);

    let bulkRes: globalThis.Response | null = null;
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      bulkRes = await bulkFetch(url.toString(), {
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });
      clearTimeout(timer);
    } catch (err) {
      console.error('BULK indexer wallet fetch failed:', err);
    }

    let result: unknown;
    if (bulkRes?.ok) {
      const row = (await bulkRes.json()) as BulkLeaderboardRow & { rank?: number };
      // Sanity check: the row should at least have a wallet field that
      // matches what we requested. If BULK ever changes shape, we bail
      // gracefully rather than serving garbage.
      if (row && row.wallet === address) {
        result = {
          found: true as const,
          rank: row.rank ?? 0,
          // BULK's per-wallet endpoint doesn't return `total` rows in the
          // ranking. We pass through 0 here; callers that need it can
          // derive it from the rank context (e.g. from the main /bulk
          // leaderboard route's response).
          total: 0,
          metric,
          window,
          wallet: address,
          row,
        };
      }
    }

    // Fallback for the not-found / network-error case. Keeps the
    // discriminated-union shape the frontend already understands.
    if (!result) {
      result = {
        found: false as const,
        total: 0,
        metric,
        window,
        wallet: address,
        scannedPages: 0,
      };
    }

    // Cache both fresh (60s) and stale (5min). Even misses are cached
    // briefly so a freshly-deposited wallet that BULK hasn't indexed yet
    // doesn't hammer the indexer on every render.
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
