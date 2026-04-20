import { Router, Request, Response } from 'express';
import { query } from '../db';
import { getCache, setCache } from '../services/cache';
import { getActiveSymbols } from '../services/markets';
import { buildAdditiveRow, coinFromSymbol, zeroCoinDict } from '../services/coinShape';

const router = Router();

// BULK API base URL
const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

// NOTE: the old `const MARKETS = ['BTC-USD', ...]` constant was removed.
// Every caller now resolves the live market list via `getActiveSymbols()`
// from `../services/markets`, so new coins listed on BULK appear here
// automatically with no code changes.

// Type for BULK ticker response
interface BulkTicker {
  symbol: string;
  priceChange: number;
  priceChangePercent: number;
  lastPrice: number;
  highPrice: number;
  lowPrice: number;
  volume: number;
  quoteVolume: number;
  markPrice: number;
  oraclePrice: number;
  openInterest: number;
  fundingRate: number;
  timestamp: number;
}

interface BulkStatsResponse {
  timestamp?: number;
  volume?: { totalUsd?: number };
  openInterest?: { totalUsd?: number };
  funding?: {
    rates?: {
      [symbol: string]: {
        current?: number;
        annualized?: number;
      };
    };
  };
  markets?: Array<{
    symbol: string;
    quoteVolume?: number;
    volume?: number;
    openInterest?: number;
    markPrice?: number;
    fundingRate?: number;
    lastPrice?: number;
  }>;
}

// BULK API Kline response
interface BulkKline {
  t: number;    // Open time (timestamp ms)
  T: number;    // Close time (timestamp ms)
  o: number;    // Open price
  h: number;    // High price
  l: number;    // Low price
  c: number;    // Close price
  v: number;    // Volume (in base asset, e.g., BTC)
  n: number;    // Number of trades
}

// Fetch klines from BULK API for a symbol
async function fetchKlines(symbol: string, interval: string = '1h', limit: number = 100): Promise<BulkKline[]> {
  try {
    const url = `${BULK_API_BASE}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
    const response = await fetch(url);
    if (!response.ok) return [];
    return await response.json() as BulkKline[];
  } catch (error) {
    console.error(`Failed to fetch klines for ${symbol}:`, error);
    return [];
  }
}

// Fetch klines for all markets and combine
// Helper: fetch klines for every market BULK currently has listed. The symbol
// list comes from the shared `getActiveSymbols()` helper (which proxies
// /exchangeInfo with 5-min cache + fallback), so new coins appear here with
// no code changes.
async function fetchAllKlines(interval: string = '1h', limit: number = 100): Promise<{ symbol: string; klines: BulkKline[] }[]> {
  const symbols = await getActiveSymbols();
  const results = await Promise.all(
    symbols.map(async (symbol) => ({
      symbol,
      klines: await fetchKlines(symbol, interval, limit)
    }))
  );
  return results;
}

// Helper: Fetch all tickers and sum volume/OI across every live market.
async function fetchTickersForStats(): Promise<{ volume24h: number; openInterest: number; timestamp: number }> {
  let totalVolume = 0;
  let totalOI = 0;
  let timestamp = Date.now();

  const symbols = await getActiveSymbols();
  const tickerPromises = symbols.map(symbol =>
    fetch(`${BULK_API_BASE}/ticker/${symbol}`)
      .then(r => r.ok ? r.json() as Promise<BulkTicker> : null)
      .catch(() => null)
  );

  const tickers = await Promise.all(tickerPromises);

  for (const ticker of tickers) {
    if (ticker) {
      totalVolume += ticker.quoteVolume || 0;
      totalOI += (ticker.openInterest || 0) * (ticker.markPrice || 0);
      if (ticker.timestamp) {
        timestamp = Math.max(timestamp, ticker.timestamp / 1000000); // Convert from nanoseconds
      }
    }
  }

  console.log(`📊 Fetched ${tickers.filter(t => t).length} tickers: Volume=$${totalVolume.toFixed(2)}, OI=$${totalOI.toFixed(2)}`);
  return { volume24h: totalVolume, openInterest: totalOI, timestamp };
}

// ============ BULK API TICKER PROXY (for live OI) ============

// Get ticker data (includes fundingRate and openInterest)
router.get('/ticker/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  
  try {
    const response = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching ticker:', error);
    res.status(500).json({ error: 'Failed to fetch ticker data' });
  }
});

// ============ EXCHANGE STATS (Dashboard header) ============

// Get exchange stats - transforms BULK API data for dashboard
router.get('/exchange-stats', async (req: Request, res: Response) => {
  const cacheKey = 'analytics:exchange_stats';
  
  // Check cache first (cache for 30 seconds)
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    let totalVolume24h = 0;
    let totalOpenInterest = 0;
    let timestamp = Date.now();
    
    // First try /stats endpoint (with timeout)
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout
      
      const response = await fetch(`${BULK_API_BASE}/stats?period=1d`, {
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      
      if (response.ok) {
        const bulkStats = await response.json() as BulkStatsResponse;
        timestamp = bulkStats.timestamp || Date.now();
        
        // Calculate from markets if available
        if (bulkStats?.markets && bulkStats.markets.length > 0) {
          for (const market of bulkStats.markets) {
            totalVolume24h += market.quoteVolume || 0;
            totalOpenInterest += (market.openInterest || 0) * (market.markPrice || 0);
          }
        }
        
        // Use totals if available
        if (bulkStats?.volume?.totalUsd && bulkStats.volume.totalUsd > 0) {
          totalVolume24h = bulkStats.volume.totalUsd;
        }
        if (bulkStats?.openInterest?.totalUsd && bulkStats.openInterest.totalUsd > 0) {
          totalOpenInterest = bulkStats.openInterest.totalUsd;
        }
      }
    } catch (e) {
      console.error('Failed to fetch BULK stats:', e);
    }
    
    // FALLBACK: If /stats returned 0s, fetch from individual tickers
    if (totalVolume24h === 0 && totalOpenInterest === 0) {
      console.log('⚠️ /stats returned 0s, falling back to individual tickers...');
      try {
        const tickerStats = await fetchTickersForStats();
        totalVolume24h = tickerStats.volume24h;
        totalOpenInterest = tickerStats.openInterest;
        timestamp = tickerStats.timestamp;
      } catch (e) {
        console.error('Failed to fetch tickers fallback:', e);
      }
    }
    
    // Get active traders from traders table (much faster than COUNT DISTINCT on trades)
    let activeTraders = 0;
    try {
      const tradersResult = await Promise.race([
        query(`
          SELECT COUNT(*) as count 
          FROM traders 
          WHERE last_seen > NOW() - INTERVAL '24 hours'
        `),
        new Promise((_, reject) => setTimeout(() => reject(new Error('DB timeout')), 2000))
      ]) as any[];
      activeTraders = parseInt(tradersResult[0]?.count || '0');
      
      // Fallback: if no recent activity, show total unique traders
      if (activeTraders === 0) {
        const totalResult = await query(`SELECT COUNT(*) as count FROM traders`);
        activeTraders = parseInt(totalResult[0]?.count || '0');
      }
    } catch (e) {
      console.error('Failed to get active traders:', e);
      // Fallback to total traders count
      try {
        const totalResult = await query(`SELECT COUNT(*) as count FROM traders`);
        activeTraders = parseInt(totalResult[0]?.count || '0');
      } catch (e2) {
        activeTraders = 0;
      }
    }
    
    // Get liquidations from DB (last 24h) - with timeout
    let liquidations24h = 0;
    try {
      const liqResult = await Promise.race([
        query(`
          SELECT COALESCE(SUM(value), 0) as total
          FROM liquidations 
          WHERE timestamp > NOW() - INTERVAL '24 hours'
        `),
        new Promise((_, reject) => setTimeout(() => reject(new Error('DB timeout')), 3000))
      ]) as any[];
      liquidations24h = parseFloat(liqResult[0]?.total || '0');
    } catch (e) {
      console.error('Failed to get liquidations (timeout or error):', e);
      liquidations24h = 0;
    }
    
    const result = {
      timestamp,
      volume24h: totalVolume24h,
      openInterest: totalOpenInterest,
      activeTraders,
      liquidations24h,
    };
    
    // Cache for 30 seconds
    await setCache(cacheKey, result, 30);
    
    res.json(result);
  } catch (error) {
    console.error('Error fetching exchange stats:', error);
    res.status(500).json({ error: 'Failed to fetch exchange stats' });
  }
});

// Exchange health endpoint - combines BULK API + DB data
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    let totalVolume24h = 0;
    let totalOI = 0;
    
    // First try /stats endpoint
    try {
      const statsRes = await fetch(`${BULK_API_BASE}/stats?period=1d`);
      if (statsRes.ok) {
        const bulkStats = await statsRes.json() as BulkStatsResponse;
        
        // Calculate volume from markets if available
        if (bulkStats?.markets && bulkStats.markets.length > 0) {
          for (const market of bulkStats.markets) {
            totalVolume24h += market.quoteVolume || 0;
            totalOI += (market.openInterest || 0) * (market.markPrice || 0);
          }
        }
        
        // Use provided totals if available
        if (bulkStats?.volume?.totalUsd && bulkStats.volume.totalUsd > 0) {
          totalVolume24h = bulkStats.volume.totalUsd;
        }
        if (bulkStats?.openInterest?.totalUsd && bulkStats.openInterest.totalUsd > 0) {
          totalOI = bulkStats.openInterest.totalUsd;
        }
      }
    } catch (e) {
      console.error('Failed to fetch BULK stats:', e);
    }
    
    // FALLBACK: If /stats returned 0s, fetch from individual tickers
    if (totalVolume24h === 0 && totalOI === 0) {
      console.log('⚠️ /stats returned 0s for exchange-health, falling back to tickers...');
      const tickerStats = await fetchTickersForStats();
      totalVolume24h = tickerStats.volume24h;
      totalOI = tickerStats.openInterest;
    }
    
    // Fetch from our DB for traders and liquidations
    const [tradersResult, liqResult] = await Promise.all([
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM trades WHERE timestamp >= NOW() - INTERVAL '24 hours'`).catch(() => [{ count: 0 }]),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations WHERE timestamp >= NOW() - INTERVAL '24 hours'`).catch(() => [{ count: 0, volume: 0 }])
    ]);
    
    res.json({
      total_volume_24h: totalVolume24h,
      total_open_interest: totalOI,
      total_traders: parseInt(tradersResult[0]?.count || '0'),
      total_liquidations_24h: parseInt(liqResult[0]?.count || '0'),
      liquidation_value_24h: parseFloat(liqResult[0]?.volume || '0')
    });
  } catch (error) {
    console.error('Error fetching exchange health:', error);
    res.status(500).json({ error: 'Failed to fetch exchange health' });
  }
});

// ============ RECENT ACTIVITY (Live Activity on Dashboard) ============

router.get('/recent-activity', async (req: Request, res: Response) => {
  const limit = parseInt(req.query.limit as string) || 20;
  
  try {
    // Get recent trades - fetch more to ensure we have enough after combining
    const trades = await query(`
      SELECT 
        'trade' as type,
        wallet_address,
        symbol,
        side,
        size,
        price,
        value,
        timestamp
      FROM trades
      ORDER BY timestamp DESC
      LIMIT $1
    `, [limit]).catch(() => []);
    
    // Get recent liquidations - always fetch at least 10 to ensure some show up
    const liquidations = await query(`
      SELECT 
        'liquidation' as type,
        wallet_address,
        symbol,
        side,
        size,
        price,
        value,
        timestamp
      FROM liquidations
      ORDER BY timestamp DESC
      LIMIT $1
    `, [Math.max(limit, 20)]).catch(() => []);
    
    // Strategy: Take all liquidations (up to half the limit) + fill rest with trades
    // This ensures liquidations are always visible even if trades are more frequent
    const maxLiquidations = Math.min(liquidations.length, Math.ceil(limit / 2));
    const recentLiquidations = liquidations.slice(0, maxLiquidations);
    
    // Fill remaining slots with trades
    const remainingSlots = limit - recentLiquidations.length;
    const recentTrades = trades.slice(0, remainingSlots);
    
    // Combine and sort by timestamp
    const combined = [...recentTrades, ...recentLiquidations]
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
    
    res.json({ 
      data: combined,
      meta: {
        trades: recentTrades.length,
        liquidations: recentLiquidations.length
      }
    });
  } catch (error) {
    console.error('Error fetching recent activity:', error);
    res.status(500).json({ error: 'Failed to fetch recent activity' });
  }
});

// ============ BULK API KLINES FOR CHARTS ============

// Volume chart from BULK API klines (aggregated by symbol)
router.get('/volume-chart-api', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const isAllTime = hours >= 8760; // 1 year or more = ALL time

  try {
    const now = Date.now();
    const startTime = isAllTime ? 0 : now - (hours * 60 * 60 * 1000);

    // Fetch the live list of markets from BULK (via shared helper — cached 5
    // min, falls back to known-good list). This replaces the old hardcoded
    // ['BTC-USD', 'ETH-USD', 'SOL-USD'] so new coins appear automatically.
    const symbols = await getActiveSymbols();

    // Fetch hourly klines for every market in parallel. Any that fail return
    // an empty array so one bad coin doesn't take down the chart.
    const klinesResults = await Promise.all(
      symbols.map(symbol =>
        fetch(`${BULK_API_BASE}/klines?symbol=${symbol}&interval=1h&limit=1000`)
          .then(r => r.ok ? r.json() : [])
          .catch(() => [])
      )
    );

    // Build a timestamp → { coin → volume_usd } map. Each symbol contributes
    // its hourly notional volume (base_volume * close_price).
    const hourlyMap = new Map<number, Record<string, number>>();

    symbols.forEach((symbol, i) => {
      const coin = coinFromSymbol(symbol);
      const klines = klinesResults[i] as any[];
      for (const k of klines) {
        const ts = k.t;
        if (!hourlyMap.has(ts)) hourlyMap.set(ts, zeroCoinDict(symbols));
        // Hourly notional USD volume = base volume * close price
        hourlyMap.get(ts)![coin] = (k.v || 0) * (k.c || 0);
      }
    });

    // Roll up to hourly (1D view) or daily (W/M/ALL view).
    let outputMap: Map<number, Record<string, number>>;
    let historicalCumulative = 0;

    const sumOfCoins = (dict: Record<string, number>): number =>
      Object.values(dict).reduce((s, v) => s + (typeof v === 'number' ? v : 0), 0);

    if (hours <= 24) {
      // Hourly bars for 1D view
      outputMap = new Map();
      const sortedHourly = Array.from(hourlyMap.entries()).sort((a, b) => a[0] - b[0]);
      for (const [ts, vol] of sortedHourly) {
        if (ts >= startTime) outputMap.set(ts, vol);
        else historicalCumulative += sumOfCoins(vol);
      }
    } else {
      // Aggregate hourly into daily bars for W/M/ALL
      const dailyMap = new Map<number, Record<string, number>>();
      for (const [ts, vol] of hourlyMap.entries()) {
        const date = new Date(ts);
        const dayStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
        if (!dailyMap.has(dayStart)) dailyMap.set(dayStart, zeroCoinDict(symbols));
        const day = dailyMap.get(dayStart)!;
        for (const [coin, v] of Object.entries(vol)) {
          day[coin] = (day[coin] || 0) + v;
        }
      }
      outputMap = new Map();
      const sortedDaily = Array.from(dailyMap.entries()).sort((a, b) => a[0] - b[0]);
      for (const [ts, vol] of sortedDaily) {
        if (isAllTime || ts >= startTime) outputMap.set(ts, vol);
        else historicalCumulative += sumOfCoins(vol);
      }
    }

    // Emit additive rows — legacy BTC/ETH/SOL fields + new `coins` dict.
    let cumulative = historicalCumulative;
    const data = Array.from(outputMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, vol]) => {
        const total = sumOfCoins(vol);
        cumulative += total;
        return buildAdditiveRow(
          new Date(ts).toISOString(),
          vol,
          { total, Cumulative: cumulative }
        );
      });

    console.log(`📊 Volume chart (${isAllTime ? 'ALL' : hours + 'h'}): ${data.length} bars, ${symbols.length} coins, cumulative: $${(cumulative/1e9).toFixed(2)}B`);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching volume chart from API:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart' });
  }
});

// Trades count chart from BULK API klines
router.get('/trades-chart-api', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const isAllTime = hours >= 8760; // 1 year or more = ALL time

  try {
    const now = Date.now();
    const startTime = isAllTime ? 0 : now - (hours * 60 * 60 * 1000);

    // Dynamic symbol list — same pattern as /volume-chart-api.
    const symbols = await getActiveSymbols();

    const klinesResults = await Promise.all(
      symbols.map(symbol =>
        fetch(`${BULK_API_BASE}/klines?symbol=${symbol}&interval=1h&limit=1000`)
          .then(r => r.ok ? r.json() : [])
          .catch(() => [])
      )
    );

    // Per-hour trade-count dictionary. BULK's klines `n` field is the number
    // of trades that occurred in that candle.
    const hourlyMap = new Map<number, Record<string, number>>();

    symbols.forEach((symbol, i) => {
      const coin = coinFromSymbol(symbol);
      const klines = klinesResults[i] as any[];
      for (const k of klines) {
        const ts = k.t;
        if (!hourlyMap.has(ts)) hourlyMap.set(ts, zeroCoinDict(symbols));
        hourlyMap.get(ts)![coin] = k.n || 0;
      }
    });

    let outputMap: Map<number, Record<string, number>>;
    let historicalCumulative = 0;

    const sumOfCoins = (dict: Record<string, number>): number =>
      Object.values(dict).reduce((s, v) => s + (typeof v === 'number' ? v : 0), 0);

    if (hours <= 24) {
      outputMap = new Map();
      const sortedHourly = Array.from(hourlyMap.entries()).sort((a, b) => a[0] - b[0]);
      for (const [ts, trades] of sortedHourly) {
        if (ts >= startTime) outputMap.set(ts, trades);
        else historicalCumulative += sumOfCoins(trades);
      }
    } else {
      const dailyMap = new Map<number, Record<string, number>>();
      for (const [ts, trades] of hourlyMap.entries()) {
        const date = new Date(ts);
        const dayStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
        if (!dailyMap.has(dayStart)) dailyMap.set(dayStart, zeroCoinDict(symbols));
        const day = dailyMap.get(dayStart)!;
        for (const [coin, v] of Object.entries(trades)) {
          day[coin] = (day[coin] || 0) + v;
        }
      }
      outputMap = new Map();
      const sortedDaily = Array.from(dailyMap.entries()).sort((a, b) => a[0] - b[0]);
      for (const [ts, trades] of sortedDaily) {
        if (isAllTime || ts >= startTime) outputMap.set(ts, trades);
        else historicalCumulative += sumOfCoins(trades);
      }
    }

    let cumulative = historicalCumulative;
    const data = Array.from(outputMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, trades]) => {
        const total = sumOfCoins(trades);
        cumulative += total;
        return buildAdditiveRow(
          new Date(ts).toISOString(),
          trades,
          { total, Cumulative: cumulative }
        );
      });

    console.log(`📊 Trades chart (${isAllTime ? 'ALL' : hours + 'h'}): ${data.length} bars, ${symbols.length} coins, cumulative: ${cumulative.toLocaleString()}`);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching trades chart from API:', error);
    res.status(500).json({ error: 'Failed to fetch trades chart' });
  }
});

// ============ REAL HISTORICAL OI & FUNDING FROM TICKER SNAPSHOTS ============

// Real Open Interest history from ticker_snapshots table
router.get('/open-interest-history/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const result = await query(`
      SELECT timestamp, open_interest_usd as value
      FROM ticker_snapshots
      WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);
    
    const data = result.map((row: any) => ({
      timestamp: row.timestamp,
      value: parseFloat(row.value || 0)
    }));
    
    res.json({ symbol, hours, dataPoints: data.length, data });
  } catch (error) {
    console.error('Error fetching OI history:', error);
    res.status(500).json({ error: 'Failed to fetch OI history' });
  }
});

// Real Funding Rate history from ticker_snapshots table
router.get('/funding-rate-history/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const result = await query(`
      SELECT timestamp, funding_rate as value
      FROM ticker_snapshots
      WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);
    
    const data = result.map((row: any) => ({
      timestamp: row.timestamp,
      value: parseFloat(row.value || 0)
    }));
    
    res.json({ symbol, hours, dataPoints: data.length, data });
  } catch (error) {
    console.error('Error fetching funding rate history:', error);
    res.status(500).json({ error: 'Failed to fetch funding rate history' });
  }
});

// Combined OI chart data for all symbols
router.get('/oi-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const cacheKey = `analytics:oi_chart:${hours}`;

  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    const result = await Promise.race([
      query(`
        SELECT 
          date_trunc('minute', timestamp) as timestamp,
          symbol,
          AVG(open_interest_usd) as value
        FROM ticker_snapshots
        WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
        GROUP BY date_trunc('minute', timestamp), symbol
        ORDER BY timestamp ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 5000))
    ]) as any[];

    const symbols = await getActiveSymbols();

    // Group by timestamp → per-coin OI dictionary. The SQL already groups by
    // symbol, so we just pivot rows into a { coin: value } shape keyed by ts.
    const dataMap = new Map<string, Record<string, number>>();
    for (const row of result) {
      const ts = new Date(row.timestamp).toISOString();
      if (!dataMap.has(ts)) dataMap.set(ts, zeroCoinDict(symbols));
      const coin = coinFromSymbol(row.symbol || '');
      if (coin) dataMap.get(ts)![coin] = parseFloat(row.value || 0);
    }

    // Build an intermediate array that has per-coin values + `total`. We do
    // anomaly detection on this, then wrap into the additive output shape.
    let data: Array<{ timestamp: string; total: number; coinValues: Record<string, number> }> =
      Array.from(dataMap.entries()).map(([timestamp, values]) => ({
        timestamp,
        coinValues: values,
        total: Object.values(values).reduce((s, v) => s + v, 0),
      }));

    // Per-coin anomaly detection — handles the case where the WS temporarily
    // drops and OI briefly appears as 0 or ~0. We do this per coin rather
    // than hardcoding BTC/ETH/SOL so new markets get the same treatment.
    const medianOf = (arr: number[]): number => {
      const pos = arr.filter(v => v > 0).sort((a, b) => a - b);
      return pos.length > 0 ? pos[Math.floor(pos.length / 2)] : 0;
    };
    const medians: Record<string, number> = {};
    const coverage: Record<string, number> = {};
    for (const coin of Object.keys(data[0]?.coinValues || {})) {
      const all = data.map(d => d.coinValues[coin] || 0);
      const pos = all.filter(v => v > 0);
      medians[coin] = medianOf(all);
      coverage[coin] = data.length > 0 ? pos.length / data.length : 0;
    }

    data = data.filter((point, index, arr) => {
      // Strategy 1: coin normally has data (>80% coverage) and its median is
      // significant, but the current point is exactly zero → probably a WS
      // restart artifact, drop this tick.
      for (const [coin, v] of Object.entries(point.coinValues)) {
        if (coverage[coin] > 0.8 && medians[coin] > 10_000_000 && v === 0) {
          return false;
        }
      }

      // Strategy 2: sudden > 90% drop for any coin that recovers on the next
      // tick — also most likely a WS restart, drop it.
      if (index > 0) {
        const prev = arr[index - 1];
        const next = arr[index + 1];
        for (const [coin, v] of Object.entries(point.coinValues)) {
          const p = prev.coinValues[coin] || 0;
          const n = next?.coinValues[coin] || 0;
          const m = medians[coin] || 0;
          if (v > 0 && p > m * 0.5 && v < p * 0.1) {
            if (next && n > p * 0.5) return false;
          }
        }
      }

      // Strategy 3: total collapses to near-zero but surrounding points are
      // healthy — same restart signature.
      if (point.total < 1000) {
        const prev = arr[index - 1];
        const next = arr[index + 1];
        if ((prev && prev.total > 100_000) || (next && next.total > 100_000)) {
          return false;
        }
      }

      return true;
    });

    // Emit additive rows.
    const out = data.map(d =>
      buildAdditiveRow(d.timestamp, d.coinValues, { total: d.total })
    );

    const response = { hours, dataPoints: out.length, data: out };

    // Cache for 60 seconds
    await setCache(cacheKey, response, 60);

    res.json(response);
  } catch (error) {
    console.error('Error fetching OI chart:', error);
    res.json({ hours, dataPoints: 0, data: [], error: 'No OI data available yet' });
  }
});

// Combined Funding Rate chart data for all symbols
router.get('/funding-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const cacheKey = `analytics:funding_chart:${hours}`;
  
  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    const result = await Promise.race([
      query(`
        SELECT 
          date_trunc('minute', timestamp) as timestamp,
          symbol,
          AVG(funding_rate) as value
        FROM ticker_snapshots
        WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
        GROUP BY date_trunc('minute', timestamp), symbol
        ORDER BY timestamp ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 5000))
    ]) as any[];
    
    const symbols = await getActiveSymbols();
    const dataMap = new Map<string, Record<string, number>>();

    for (const row of result) {
      const ts = new Date(row.timestamp).toISOString();
      if (!dataMap.has(ts)) dataMap.set(ts, zeroCoinDict(symbols));
      const coin = coinFromSymbol(row.symbol || '');
      if (coin) dataMap.get(ts)![coin] = parseFloat(row.value || 0);
    }

    let data: Array<{ timestamp: string; coinValues: Record<string, number> }> =
      Array.from(dataMap.entries()).map(([timestamp, values]) => ({
        timestamp,
        coinValues: values,
      }));

    // Per-coin coverage: how often a coin has a non-zero value in the window.
    // If a coin normally has data (>70% coverage) but this point shows 0, it's
    // most likely a WS restart artifact.
    const coverage: Record<string, number> = {};
    if (data.length > 0) {
      for (const coin of Object.keys(data[0].coinValues)) {
        const nonZero = data.filter(d => d.coinValues[coin] !== 0).length;
        coverage[coin] = nonZero / data.length;
      }
    }

    data = data.filter((point, index, arr) => {
      const prev = arr[index - 1];
      const next = arr[index + 1];

      // If 2+ coins that normally have data are simultaneously zero AND
      // neighboring points have data, skip this tick — WS restart.
      const missingWithHistory = Object.entries(point.coinValues).filter(
        ([coin, v]) => v === 0 && coverage[coin] > 0.7
      );
      if (missingWithHistory.length >= 2) {
        const hasNeighborData =
          (prev && Object.values(prev.coinValues).some(v => v !== 0)) ||
          (next && Object.values(next.coinValues).some(v => v !== 0));
        if (hasNeighborData) return false;
      }

      // All zeros, surrounded by data → definite restart, drop it.
      const allZero = Object.values(point.coinValues).every(v => v === 0);
      if (allZero) {
        const prevHasData = prev && Object.values(prev.coinValues).some(v => v !== 0);
        const nextHasData = next && Object.values(next.coinValues).some(v => v !== 0);
        if (prevHasData || nextHasData) return false;
      }

      return true;
    });

    const out = data.map(d => buildAdditiveRow(d.timestamp, d.coinValues));

    const response = { hours, dataPoints: out.length, data: out };

    // Cache for 60 seconds
    await setCache(cacheKey, response, 60);

    res.json(response);
  } catch (error) {
    console.error('Error fetching funding chart:', error);
    res.json({ hours, dataPoints: 0, data: [], error: 'No funding data available yet' });
  }
});

// ============ DATABASE CHARTS (Volume, Trades, Liquidations, ADL) ============

// Helper function to transform raw DB rows to chart format with cumulative.
// Output rows use the additive shape: legacy top-level BTC/ETH/SOL/... fields
// PLUS a canonical `coins: { ... }` dictionary covering every market. That
// means the same helper powers both old frontend code (reading row.BTC) and
// new code (reading row.coins.BNB etc).
function transformToChartData(
  rows: any[],
  historicalCumulative: number = 0
): Record<string, unknown>[] {
  const dataMap = new Map<string, { timestamp: string; total: number; coinValues: Record<string, number> }>();

  for (const row of rows) {
    const dateKey = new Date(row.day).toISOString();
    if (!dataMap.has(dateKey)) {
      dataMap.set(dateKey, { timestamp: dateKey, total: 0, coinValues: {} });
    }
    const entry = dataMap.get(dateKey)!;
    const value = parseFloat(
      row.volume || row.total_value || row.trade_count || row.liquidation_count || row.adl_count || 0
    );

    entry.total += value;

    // Use the coin part of the symbol as the dictionary key — works for any
    // market BULK has, including ones that didn't exist when this was written.
    const coin = coinFromSymbol(row.symbol || '');
    if (coin) {
      entry.coinValues[coin] = (entry.coinValues[coin] || 0) + value;
    }
  }

  // Sort and add cumulative
  const sorted = Array.from(dataMap.values()).sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  let cumulative = historicalCumulative;
  return sorted.map(entry => {
    cumulative += entry.total;
    return buildAdditiveRow(
      entry.timestamp,
      entry.coinValues,
      { total: entry.total, Cumulative: cumulative }
    );
  });
}

// Get volume chart data from database
router.get('/volume-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const isAllTime = hours >= 8760;
  
  try {
    // First get total all-time volume
    const totalResult = await query<{ total: string }>(`
      SELECT COALESCE(SUM(value), 0) as total FROM trades
    `);
    const totalAllTime = parseFloat(totalResult[0]?.total || '0');
    
    // Get visible period data
    const visibleRows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        SUM(value) as volume
      FROM trades
      WHERE timestamp >= NOW() - INTERVAL '${isAllTime ? 8760 : hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `);
    
    // Calculate visible sum
    let visibleSum = 0;
    for (const row of visibleRows) {
      visibleSum += parseFloat(row.volume || 0);
    }
    
    // Historical = total - visible
    const historicalCumulative = totalAllTime - visibleSum;
    
    const data = transformToChartData(visibleRows, historicalCumulative);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching volume chart:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart data' });
  }
});

// Get trades chart data from database
// NOTE: Only shows data from BULK API launch (April 13, 2026 19:00 UTC) for chart alignment
const BULK_API_START = '2026-04-13T19:00:00.000Z';

router.get('/trades-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const isAllTime = hours >= 8760;
  const cacheKey = `analytics:trades_chart:${hours}`;
  
  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Get total trades count ONLY from BULK API start date onwards (for chart alignment)
    const totalResult = await query<{ total: string }>(`
      SELECT COUNT(*) as total FROM trades WHERE timestamp >= '${BULK_API_START}'
    `);
    const totalAllTime = parseFloat(totalResult[0]?.total || '0');
    
    // Calculate the effective start time (max of user's requested period and BULK API start)
    const requestedStart = isAllTime ? BULK_API_START : `NOW() - INTERVAL '${hours} hours'`;
    
    // Get visible period data (always filtered to BULK API start at minimum)
    const visibleRows = await Promise.race([
      query(`
        SELECT 
          date_trunc('day', timestamp) as day,
          symbol,
          COUNT(*) as trade_count,
          SUM(value) as volume
        FROM trades
        WHERE timestamp >= GREATEST('${BULK_API_START}'::timestamp, ${isAllTime ? `'${BULK_API_START}'::timestamp` : `NOW() - INTERVAL '${hours} hours'`})
        GROUP BY date_trunc('day', timestamp), symbol
        ORDER BY day ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 8000))
    ]) as any[];
    
    // Calculate visible sum (count)
    let visibleSum = 0;
    for (const row of visibleRows) {
      visibleSum += parseFloat(row.trade_count || 0);
    }
    
    // Historical = total (from BULK start) - visible period
    const historicalCumulative = totalAllTime - visibleSum;
    
    const data = transformToChartData(visibleRows, historicalCumulative);
    const result = { data };
    
    // Cache for 60 seconds
    await setCache(cacheKey, result, 60);
    
    res.json(result);
  } catch (error) {
    console.error('Error fetching trades chart:', error);
    // Return empty data instead of 500 error
    res.json({ data: [], error: 'No trade data available yet' });
  }
});

// Get liquidations chart data from database
// NOTE: Only shows data from BULK API launch (April 13, 2026 19:00 UTC) for chart alignment
router.get('/liquidations-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const isAllTime = hours >= 8760;
  const cacheKey = `analytics:liquidations_chart:${hours}`;
  
  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Get total liquidation value ONLY from BULK API start date onwards
    const totalResult = await query<{ total: string }>(`
      SELECT COALESCE(SUM(value), 0) as total FROM liquidations WHERE timestamp >= '${BULK_API_START}'
    `);
    const totalAllTime = parseFloat(totalResult[0]?.total || '0');
    
    // Get visible period data (always filtered to BULK API start at minimum)
    const visibleRows = await Promise.race([
      query(`
        SELECT 
          date_trunc('day', timestamp) as day,
          symbol,
          COUNT(*) as liquidation_count,
          SUM(value) as total_value
        FROM liquidations
        WHERE timestamp >= GREATEST('${BULK_API_START}'::timestamp, ${isAllTime ? `'${BULK_API_START}'::timestamp` : `NOW() - INTERVAL '${hours} hours'`})
        GROUP BY date_trunc('day', timestamp), symbol
        ORDER BY day ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 5000))
    ]) as any[];
    
    // Calculate visible sum
    let visibleSum = 0;
    for (const row of visibleRows) {
      visibleSum += parseFloat(row.total_value || 0);
    }
    
    // Historical = total (from BULK start) - visible period
    const historicalCumulative = totalAllTime - visibleSum;
    
    const data = transformToChartData(visibleRows, historicalCumulative);
    const result = { data };
    
    // Cache for 60 seconds
    await setCache(cacheKey, result, 60);
    
    res.json(result);
  } catch (error) {
    console.error('Error fetching liquidations chart:', error);
    res.json({ data: [], error: 'No liquidation data available yet' });
  }
});

// Get ADL chart data from database
// NOTE: Only shows data from BULK API launch (April 13, 2026 19:00 UTC) for chart alignment
router.get('/adl-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const isAllTime = hours >= 8760;
  
  try {
    // Get total ADL value ONLY from BULK API start date onwards
    const totalResult = await query<{ total: string }>(`
      SELECT COALESCE(SUM(value), 0) as total FROM adl_events WHERE timestamp >= '${BULK_API_START}'
    `).catch(() => [{ total: '0' }]);
    const totalAllTime = parseFloat(totalResult[0]?.total || '0');
    
    // Get visible period data (always filtered to BULK API start at minimum)
    const visibleRows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as adl_count,
        SUM(value) as total_value
      FROM adl_events
      WHERE timestamp >= GREATEST('${BULK_API_START}'::timestamp, ${isAllTime ? `'${BULK_API_START}'::timestamp` : `NOW() - INTERVAL '${hours} hours'`})
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `).catch(() => []);
    
    // Calculate visible sum
    let visibleSum = 0;
    for (const row of visibleRows) {
      visibleSum += parseFloat(row.total_value || 0);
    }
    
    // Historical = total (from BULK start) - visible period
    const historicalCumulative = totalAllTime - visibleSum;
    
    const data = transformToChartData(visibleRows, historicalCumulative);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching ADL chart:', error);
    res.json({ data: [] }); // Return empty on error
  }
});

// ============ STATS ============

// Get overall stats
router.get('/stats', async (req: Request, res: Response) => {
  const cacheKey = 'analytics:overall_stats';
  
  // Check cache first (60 second TTL)
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Get BULK API stats for volume
    let totalVolume = 0;
    try {
      const statsRes = await fetch(`${BULK_API_BASE}/stats?period=all`);
      if (statsRes.ok) {
        const bulkStats = await statsRes.json() as BulkStatsResponse;
        if (bulkStats?.markets && bulkStats.markets.length > 0) {
          for (const market of bulkStats.markets) {
            totalVolume += market.quoteVolume || 0;
          }
        }
        if (bulkStats?.volume?.totalUsd && bulkStats.volume.totalUsd > 0) {
          totalVolume = bulkStats.volume.totalUsd;
        }
      }
    } catch (e) {
      console.error('Failed to fetch BULK stats for volume:', e);
    }
    
    // FALLBACK: If /stats returned 0, fetch from tickers
    if (totalVolume === 0) {
      console.log('⚠️ /stats returned 0 volume, falling back to tickers...');
      const tickerStats = await fetchTickersForStats();
      totalVolume = tickerStats.volume24h;
    }

    const [tradesResult, liqResult, tradersResult] = await Promise.all([
      query(`SELECT COUNT(*) as count FROM trades`).catch(() => [{ count: 0 }]),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations`).catch(() => [{ count: 0, volume: 0 }]),
      query(`SELECT COUNT(*) as count FROM traders`).catch(() => [{ count: 0 }])
    ]);
    
    const result = {
      trades: {
        count: parseInt(tradesResult[0]?.count || '0'),
        volume: totalVolume
      },
      liquidations: {
        count: parseInt(liqResult[0]?.count || '0'),
        volume: parseFloat(liqResult[0]?.volume || '0')
      },
      adl: {
        count: 0,
        volume: 0
      },
      uniqueTraders: parseInt(tradersResult[0]?.count || '0')
    };
    
    // Cache for 60 seconds
    await setCache(cacheKey, result, 60);
    
    res.json(result);
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// DEBUG: Check trades table data (fast version)
router.get('/debug/trades', async (req: Request, res: Response) => {
  try {
    const [countResult, sampleResult, tradersResult] = await Promise.all([
      query(`SELECT COUNT(*) as count FROM trades`),
      query(`SELECT wallet_address, symbol, side, size, price, value, timestamp FROM trades ORDER BY timestamp DESC LIMIT 5`),
      query(`SELECT 
        COUNT(*) as unique_wallets,
        SUM(total_volume) as total_volume,
        SUM(total_trades) as total_trades
      FROM traders`)
    ]);
    
    res.json({
      totalCount: countResult[0]?.count,
      sampleTrades: sampleResult,
      volumeStats: {
        total_trades: tradersResult[0]?.total_trades || countResult[0]?.count,
        total_volume: tradersResult[0]?.total_volume || 0,
        unique_wallets: tradersResult[0]?.unique_wallets || 0
      }
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// ============ KLINES PROXY ============

router.get('/klines/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const interval = (req.query.interval as string) || '1h';
  const startTime = req.query.startTime as string;
  const endTime = req.query.endTime as string;
  
  try {
    let url = `${BULK_API_BASE}/klines?symbol=${symbol}&interval=${interval}`;
    if (startTime) url += `&startTime=${startTime}`;
    if (endTime) url += `&endTime=${endTime}`;
    
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching klines:', error);
    res.status(500).json({ error: 'Failed to fetch klines' });
  }
});

// ============ NEW: BULK API DIRECT ENDPOINTS ============

// Volume chart from BULK API /klines (no PostgreSQL needed!)
router.get('/volume-chart-bulk', async (req: Request, res: Response) => {
  const interval = (req.query.interval as string) || '1h';

  try {
    // Fetch klines for all markets (fetchAllKlines itself iterates MARKETS —
    // we migrate that separately below).
    const allKlines = await fetchAllKlines(interval, 500);

    // Per-timestamp per-coin dictionary. No hardcoded coin list — we build
    // the dict organically as symbols come in, so any new BULK market
    // automatically appears here.
    type Entry = { timestamp: string; total: number; coinValues: Record<string, number> };
    const dataMap = new Map<number, Entry>();

    for (const { symbol, klines } of allKlines) {
      const coin = coinFromSymbol(symbol);
      for (const kline of klines) {
        const timestamp = kline.t;
        if (!dataMap.has(timestamp)) {
          dataMap.set(timestamp, {
            timestamp: new Date(timestamp).toISOString(),
            total: 0,
            coinValues: {},
          });
        }
        const entry = dataMap.get(timestamp)!;
        // Volume in quote (USD) = volume in base * close price
        const volumeUsd = kline.v * kline.c;
        entry.total += volumeUsd;
        entry.coinValues[coin] = (entry.coinValues[coin] || 0) + volumeUsd;
      }
    }

    const data = Array.from(dataMap.values())
      .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
      .map(e => buildAdditiveRow(e.timestamp, e.coinValues, { total: e.total }));

    res.json({
      data,
      source: 'bulk-api',
      interval,
      note: 'Data directly from BULK Exchange API /klines endpoint'
    });
  } catch (error) {
    console.error('Error fetching volume chart from BULK API:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart' });
  }
});

// OI & Funding from BULK API /stats (current snapshot only)
router.get('/market-stats-bulk', async (req: Request, res: Response) => {
  try {
    const response = await fetch(`${BULK_API_BASE}/stats?period=1d`);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    
    const stats = await response.json() as BulkStatsResponse;
    
    // Format for frontend
    const markets = (stats.markets || []).map(m => ({
      symbol: m.symbol,
      volume24h: m.quoteVolume || 0,
      openInterest: (m.openInterest || 0) * (m.markPrice || m.lastPrice || 0),
      openInterestCoins: m.openInterest || 0,
      fundingRate: m.fundingRate || 0,
      price: m.markPrice || m.lastPrice || 0
    }));
    
    res.json({
      timestamp: stats.timestamp,
      totalVolume24h: stats.volume?.totalUsd || 0,
      totalOpenInterest: stats.openInterest?.totalUsd || 0,
      markets,
      source: 'bulk-api'
    });
  } catch (error) {
    console.error('Error fetching market stats from BULK API:', error);
    res.status(500).json({ error: 'Failed to fetch market stats' });
  }
});

// All tickers from BULK API
router.get('/tickers-bulk', async (req: Request, res: Response) => {
  try {
    // Use live symbol list so new markets appear automatically.
    const symbols = await getActiveSymbols();
    const tickers = await Promise.all(
      symbols.map(async (symbol) => {
        try {
          const response = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
          if (!response.ok) return null;
          return await response.json() as BulkTicker;
        } catch {
          return null;
        }
      })
    );

    res.json({
      tickers: tickers.filter(t => t !== null),
      source: 'bulk-api'
    });
  } catch (error) {
    console.error('Error fetching tickers from BULK API:', error);
    res.status(500).json({ error: 'Failed to fetch tickers' });
  }
});

// ============ USER STATISTICS CHARTS ============

// Unique Traders By Coin (daily breakdown) - USES PRE-AGGREGATED TABLE
router.get('/unique-traders-by-coin', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720; // Default 30 days

  try {
    const cacheKey = `unique-traders-coin:${hours}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Fast query from pre-aggregated table
    const data = await query<{
      day: string;
      symbol: string;
      unique_traders: string;
    }>(`
      SELECT day, symbol, unique_traders
      FROM daily_stats
      WHERE day > NOW() - INTERVAL '${hours} hours'
      ORDER BY day ASC, symbol ASC
    `);

    // Get total unique per day
    const totals = await query<{
      day: string;
      total_unique: string;
    }>(`
      SELECT day, total_unique
      FROM daily_unique_traders
      WHERE day > NOW() - INTERVAL '${hours} hours'
      ORDER BY day ASC
    `);

    const symbols = await getActiveSymbols();

    // If pre-aggregated tables are empty, fall back to direct query (slower)
    if (data.length === 0) {
      console.log('⚠️ daily_stats empty, falling back to direct query');
      const fallback = await query<{
        day: string;
        symbol: string;
        traders: string;
        total: string;
      }>(`
        WITH daily_traders AS (
          SELECT DATE(timestamp) as day, symbol, wallet_address
          FROM trades
          WHERE timestamp > NOW() - INTERVAL '${hours} hours'
            AND wallet_address IS NOT NULL
        ),
        per_symbol AS (
          SELECT day, symbol, COUNT(DISTINCT wallet_address) as traders
          FROM daily_traders
          GROUP BY day, symbol
        ),
        per_day AS (
          SELECT day, COUNT(DISTINCT wallet_address) as total
          FROM daily_traders
          GROUP BY day
        )
        SELECT ps.day, ps.symbol, ps.traders::text, pd.total::text
        FROM per_symbol ps
        JOIN per_day pd ON ps.day = pd.day
        ORDER BY ps.day ASC
      `);

      // Per-day per-coin dict. We keep `total` separately as an extra field.
      const dayMap = new Map<string, { coinValues: Record<string, number>; total: number }>();
      for (const row of fallback) {
        const dayStr = new Date(row.day).toISOString().split('T')[0];
        if (!dayMap.has(dayStr)) {
          dayMap.set(dayStr, { coinValues: zeroCoinDict(symbols), total: parseInt(row.total) });
        }
        const entry = dayMap.get(dayStr)!;
        const coin = coinFromSymbol(row.symbol);
        if (coin) entry.coinValues[coin] = parseInt(row.traders);
      }

      const chartData = Array.from(dayMap.entries())
        .map(([day, { coinValues, total }]) =>
          buildAdditiveRow(day, coinValues, { total })
        )
        .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));

      const result = { data: chartData };
      await setCache(cacheKey, result, 60); // Short cache for fallback
      return res.json(result);
    }

    // Build from pre-aggregated data (FAST)
    const totalMap = new Map<string, number>();
    for (const row of totals) {
      totalMap.set(new Date(row.day).toISOString().split('T')[0], parseInt(row.total_unique));
    }

    const dayMap = new Map<string, { coinValues: Record<string, number>; total: number }>();
    for (const row of data) {
      const dayStr = new Date(row.day).toISOString().split('T')[0];
      if (!dayMap.has(dayStr)) {
        dayMap.set(dayStr, {
          coinValues: zeroCoinDict(symbols),
          total: totalMap.get(dayStr) || 0,
        });
      }
      const entry = dayMap.get(dayStr)!;
      const coin = coinFromSymbol(row.symbol);
      if (coin) entry.coinValues[coin] = parseInt(row.unique_traders);
    }

    const chartData = Array.from(dayMap.entries())
      .map(([day, { coinValues, total }]) =>
        buildAdditiveRow(day, coinValues, { total })
      )
      .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));

    const result = { data: chartData };
    await setCache(cacheKey, result, 600); // 10 min cache
    res.json(result);
  } catch (error) {
    console.error('Error fetching unique traders by coin:', error);
    res.status(500).json({ error: 'Failed to fetch unique traders by coin' });
  }
});

// Daily Active Users - USES PRE-AGGREGATED TABLE
router.get('/daily-active-users', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720; // Default 30 days
  
  try {
    const cacheKey = `dau:${hours}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Fast query from pre-aggregated table
    const data = await query<{
      day: string;
      total_unique: string;
    }>(`
      SELECT day, total_unique
      FROM daily_unique_traders
      WHERE day > NOW() - INTERVAL '${hours} hours'
      ORDER BY day ASC
    `);

    // Fallback if table is empty
    if (data.length === 0) {
      console.log('⚠️ daily_unique_traders empty, falling back to direct query');
      const fallback = await query<{ day: string; dau: string }>(`
        SELECT DATE(timestamp) as day, COUNT(DISTINCT wallet_address) as dau
        FROM trades
        WHERE timestamp > NOW() - INTERVAL '${hours} hours'
          AND wallet_address IS NOT NULL
        GROUP BY DATE(timestamp)
        ORDER BY day ASC
      `);
      
      const chartData = fallback.map(row => ({
        timestamp: new Date(row.day).toISOString().split('T')[0],
        dau: parseInt(row.dau)
      }));
      
      const result = { data: chartData };
      await setCache(cacheKey, result, 60);
      return res.json(result);
    }

    const chartData = data.map(row => ({
      timestamp: new Date(row.day).toISOString().split('T')[0],
      dau: parseInt(row.total_unique)
    }));

    const result = { data: chartData };
    await setCache(cacheKey, result, 600);
    res.json(result);
  } catch (error) {
    console.error('Error fetching DAU:', error);
    res.status(500).json({ error: 'Failed to fetch DAU' });
  }
});

// Cumulative New Users - USES PRE-AGGREGATED TABLE
router.get('/cumulative-new-users', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720; // Default 30 days
  
  try {
    const cacheKey = `new-users:${hours}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Fast query from pre-aggregated table
    const data = await query<{
      day: string;
      new_users: string;
      cumulative_users: string;
    }>(`
      SELECT day, new_users, cumulative_users
      FROM daily_unique_traders
      WHERE day > NOW() - INTERVAL '${hours} hours'
        AND new_users IS NOT NULL
      ORDER BY day ASC
    `);

    // Fallback if table is empty
    if (data.length === 0) {
      console.log('⚠️ daily_unique_traders empty for new users, falling back to direct query');
      const fallback = await query<{ first_day: string; new_users: string; cumulative: string }>(`
        WITH first_trades AS (
          SELECT wallet_address, DATE(MIN(timestamp)) as first_trade_date
          FROM trades WHERE wallet_address IS NOT NULL GROUP BY wallet_address
        ),
        daily_new AS (
          SELECT first_trade_date as first_day, COUNT(*) as new_users
          FROM first_trades GROUP BY first_trade_date
        )
        SELECT first_day, new_users::text, SUM(new_users) OVER (ORDER BY first_day)::text as cumulative
        FROM daily_new
        WHERE first_day > NOW() - INTERVAL '${hours} hours'
        ORDER BY first_day ASC
      `);
      
      const chartData = fallback.map(row => ({
        timestamp: new Date(row.first_day).toISOString().split('T')[0],
        newUsers: parseInt(row.new_users),
        cumulative: parseInt(row.cumulative)
      }));
      
      const result = { data: chartData };
      await setCache(cacheKey, result, 60);
      return res.json(result);
    }

    const chartData = data.map(row => ({
      timestamp: new Date(row.day).toISOString().split('T')[0],
      newUsers: parseInt(row.new_users) || 0,
      cumulative: parseInt(row.cumulative_users) || 0
    }));

    const result = { data: chartData };
    await setCache(cacheKey, result, 600);
    res.json(result);
  } catch (error) {
    console.error('Error fetching cumulative new users:', error);
    res.status(500).json({ error: 'Failed to fetch cumulative new users' });
  }
});

// ============ LIQUIDATIONS DASHBOARD ENDPOINTS ============

// Treemap data - liquidations by coin and side
router.get('/liquidations/treemap', async (req: Request, res: Response) => {
  const period = req.query.period as string || '24h';
  
  const intervalMap: Record<string, string> = {
    '4h': '4 hours',
    '24h': '24 hours',
    '3d': '3 days',
    '7d': '7 days',
    'all': '365 days'
  };
  const interval = intervalMap[period] || '24 hours';
  
  try {
    const cacheKey = `liq-treemap:${period}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const data = await query<{
      symbol: string;
      side: string;
      total_value: string;
      count: string;
    }>(`
      SELECT 
        symbol,
        side,
        COALESCE(SUM(value), 0) as total_value,
        COUNT(*) as count
      FROM liquidations
      WHERE timestamp > NOW() - INTERVAL '${interval}'
      GROUP BY symbol, side
      ORDER BY total_value DESC
    `);

    const result = {
      period,
      data: data.map(row => ({
        symbol: row.symbol.replace('-USD', ''),
        side: row.side,
        value: parseFloat(row.total_value),
        count: parseInt(row.count)
      })),
      totalValue: data.reduce((sum, row) => sum + parseFloat(row.total_value), 0),
      assets: new Set(data.map(row => row.symbol)).size
    };

    await setCache(cacheKey, result, 60); // 1 min cache
    res.json(result);
  } catch (error) {
    console.error('Error fetching liquidation treemap:', error);
    res.status(500).json({ error: 'Failed to fetch liquidation treemap' });
  }
});

// Chart data - long vs short liquidations over time
// NOTE: Only shows data from BULK API launch (April 13, 2026 19:00 UTC) for chart alignment
router.get('/liquidations/chart', async (req: Request, res: Response) => {
  const period = req.query.period as string || 'all';
  
  const intervalMap: Record<string, { interval: string; bucket: string }> = {
    '4h': { interval: '4 hours', bucket: '15 minutes' },
    '24h': { interval: '24 hours', bucket: '1 hour' },
    '3d': { interval: '3 days', bucket: '4 hours' },
    '7d': { interval: '7 days', bucket: '12 hours' },
    'all': { interval: '365 days', bucket: '1 day' }
  };
  const { bucket } = intervalMap[period] || intervalMap['all'];
  const isAllTime = period === 'all';
  
  try {
    const cacheKey = `liq-chart:${period}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Get total liquidations ONLY from BULK API start date onwards
    const totalResult = await query<{ total: string }>(`
      SELECT COALESCE(SUM(value), 0) as total FROM liquidations WHERE timestamp >= '${BULK_API_START}'
    `);
    const totalAllTime = parseFloat(totalResult[0]?.total || '0');

    // Now get bucketed data for the visible period
    const intervalHours: Record<string, number> = {
      '4h': 4, '24h': 24, '3d': 72, '7d': 168, 'all': 8760
    };
    const hours = intervalHours[period] || 8760;
    
    // Get data for visible period with appropriate bucket (always filtered to BULK API start)
    const visibleData = await query<{
      time_bucket: string;
      long_value: string;
      short_value: string;
      long_count: string;
      short_count: string;
    }>(`
      SELECT 
        DATE_TRUNC('${bucket.split(' ')[1]}', timestamp) as time_bucket,
        COALESCE(SUM(CASE WHEN side = 'long' THEN value ELSE 0 END), 0) as long_value,
        COALESCE(SUM(CASE WHEN side = 'short' THEN value ELSE 0 END), 0) as short_value,
        COUNT(CASE WHEN side = 'long' THEN 1 END) as long_count,
        COUNT(CASE WHEN side = 'short' THEN 1 END) as short_count
      FROM liquidations
      WHERE timestamp >= GREATEST('${BULK_API_START}'::timestamp, ${isAllTime ? `'${BULK_API_START}'::timestamp` : `NOW() - INTERVAL '${hours} hours'`})
      GROUP BY time_bucket
      ORDER BY time_bucket ASC
    `);

    // Calculate the sum of visible data
    let visibleSum = 0;
    for (const row of visibleData) {
      visibleSum += parseFloat(row.long_value) + parseFloat(row.short_value);
    }
    
    // Historical cumulative = total (from BULK start) - visible period sum
    const historicalCumulative = totalAllTime - visibleSum;

    // Build result with cumulative starting from historical
    let cumulative = historicalCumulative;
    const result = {
      period,
      data: visibleData.map(row => {
        const longVal = parseFloat(row.long_value);
        const shortVal = parseFloat(row.short_value);
        cumulative += longVal + shortVal;
        return {
          timestamp: row.time_bucket,
          longValue: longVal,
          shortValue: shortVal,
          longCount: parseInt(row.long_count),
          shortCount: parseInt(row.short_count),
          Cumulative: cumulative
        };
      })
    };

    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching liquidation chart:', error);
    res.status(500).json({ error: 'Failed to fetch liquidation chart' });
  }
});

// Summary for a specific coin
router.get('/liquidations/summary/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const period = req.query.period as string || '7d';
  const dbSymbol = symbol.includes('-') ? symbol : `${symbol}-USD`;
  
  const intervalMap: Record<string, string> = {
    '4h': '4 hours',
    '24h': '24 hours',
    '3d': '3 days',
    '7d': '7 days',
    'all': '365 days'
  };
  const interval = intervalMap[period] || '7 days';
  
  try {
    const cacheKey = `liq-summary:${symbol}:${period}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const data = await query<{
      total_value: string;
      total_count: string;
      long_value: string;
      short_value: string;
      long_count: string;
      short_count: string;
      largest_value: string;
      largest_size: string;
    }>(`
      SELECT 
        COALESCE(SUM(value), 0) as total_value,
        COUNT(*) as total_count,
        COALESCE(SUM(CASE WHEN side = 'long' THEN value ELSE 0 END), 0) as long_value,
        COALESCE(SUM(CASE WHEN side = 'short' THEN value ELSE 0 END), 0) as short_value,
        COUNT(CASE WHEN side = 'long' THEN 1 END) as long_count,
        COUNT(CASE WHEN side = 'short' THEN 1 END) as short_count,
        COALESCE(MAX(value), 0) as largest_value,
        COALESCE(MAX(size), 0) as largest_size
      FROM liquidations
      WHERE symbol = $1
        AND timestamp > NOW() - INTERVAL '${interval}'
    `, [dbSymbol]);

    const row = data[0];
    const totalValue = parseFloat(row.total_value);
    const longValue = parseFloat(row.long_value);
    const shortValue = parseFloat(row.short_value);

    const result = {
      symbol: symbol.replace('-USD', ''),
      period,
      totalValue,
      totalCount: parseInt(row.total_count),
      longValue,
      shortValue,
      longCount: parseInt(row.long_count),
      shortCount: parseInt(row.short_count),
      longPercent: totalValue > 0 ? (longValue / totalValue) * 100 : 0,
      shortPercent: totalValue > 0 ? (shortValue / totalValue) * 100 : 0,
      largestValue: parseFloat(row.largest_value),
      largestSize: parseFloat(row.largest_size)
    };

    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching liquidation summary:', error);
    res.status(500).json({ error: 'Failed to fetch liquidation summary' });
  }
});

// Market summary for a specific coin
router.get('/liquidations/market/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const period = req.query.period as string || 'all';
  const dbSymbol = symbol.includes('-') ? symbol : `${symbol}-USD`;
  
  const intervalMap: Record<string, string> = {
    '4h': '4 hours',
    '24h': '24 hours',
    '3d': '3 days',
    '7d': '7 days',
    'all': '365 days'
  };
  const interval = intervalMap[period] || '365 days';
  
  try {
    const cacheKey = `liq-market:${symbol}:${period}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Get liquidation stats
    const liqData = await query<{
      total_value: string;
      long_value: string;
      short_value: string;
      long_count: string;
      short_count: string;
    }>(`
      SELECT 
        COALESCE(SUM(value), 0) as total_value,
        COALESCE(SUM(CASE WHEN side = 'long' THEN value ELSE 0 END), 0) as long_value,
        COALESCE(SUM(CASE WHEN side = 'short' THEN value ELSE 0 END), 0) as short_value,
        COUNT(CASE WHEN side = 'long' THEN 1 END) as long_count,
        COUNT(CASE WHEN side = 'short' THEN 1 END) as short_count
      FROM liquidations
      WHERE symbol = $1
        AND timestamp > NOW() - INTERVAL '${interval}'
    `, [dbSymbol]);

    // Get current price and 24h change from BULK API
    let markPrice = 0;
    let priceChange24h = 0;
    try {
      const tickerRes = await fetch(`${BULK_API_BASE}/ticker/${dbSymbol}`);
      if (tickerRes.ok) {
        const ticker = await tickerRes.json() as BulkTicker;
        markPrice = ticker.markPrice || ticker.lastPrice || 0;
        priceChange24h = ticker.priceChangePercent || 0;
      }
    } catch (e) {
      console.error('Failed to fetch ticker for market summary:', e);
    }

    const row = liqData[0];
    const totalValue = parseFloat(row.total_value);
    const longValue = parseFloat(row.long_value);
    const shortValue = parseFloat(row.short_value);
    const longCount = parseInt(row.long_count);
    const shortCount = parseInt(row.short_count);

    const result = {
      symbol: symbol.replace('-USD', ''),
      period,
      markPrice,
      priceChange24h,
      totalValue,
      longValue,
      shortValue,
      longCount,
      shortCount,
      longPercent: totalValue > 0 ? (longValue / totalValue) * 100 : 0,
      shortPercent: totalValue > 0 ? (shortValue / totalValue) * 100 : 0,
      dominant: longValue > shortValue ? 'LONGS' : shortValue > longValue ? 'SHORTS' : 'NEUTRAL'
    };

    await setCache(cacheKey, result, 30); // 30s cache (has live price)
    res.json(result);
  } catch (error) {
    console.error('Error fetching market summary:', error);
    res.status(500).json({ error: 'Failed to fetch market summary' });
  }
});

// Featured/Recent large liquidations
router.get('/liquidations/featured', async (req: Request, res: Response) => {
  const limit = parseInt(req.query.limit as string) || 10;
  const symbol = req.query.symbol as string;
  
  try {
    const cacheKey = `liq-featured:${symbol || 'all'}:${limit}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    let whereClause = 'WHERE timestamp > NOW() - INTERVAL \'7 days\'';
    const params: any[] = [];
    
    if (symbol && symbol !== 'ALL') {
      const dbSymbol = symbol.includes('-') ? symbol : `${symbol}-USD`;
      whereClause += ' AND symbol = $1';
      params.push(dbSymbol);
    }

    const data = await query<{
      id: number;
      wallet_address: string;
      symbol: string;
      side: string;
      size: string;
      price: string;
      value: string;
      timestamp: string;
    }>(`
      SELECT id, wallet_address, symbol, side, size, price, value, timestamp
      FROM liquidations
      ${whereClause}
      ORDER BY value DESC
      LIMIT ${limit}
    `, params);

    const result = {
      data: data.map(row => ({
        id: row.id,
        wallet: row.wallet_address,
        symbol: row.symbol.replace('-USD', ''),
        side: row.side,
        size: parseFloat(row.size),
        price: parseFloat(row.price),
        value: parseFloat(row.value),
        timestamp: row.timestamp,
        isHighImpact: parseFloat(row.value) > 100000 // > $100k is "high impact"
      }))
    };

    await setCache(cacheKey, result, 30);
    res.json(result);
  } catch (error) {
    console.error('Error fetching featured liquidations:', error);
    res.status(500).json({ error: 'Failed to fetch featured liquidations' });
  }
});

// ============ NEW: REGIME & SENTIMENT ENDPOINTS ============

// Live market regime data (from BULK API tickers)
router.get('/regime', async (req: Request, res: Response) => {
  const cacheKey = 'analytics:regime';

  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    // Fetch regime data for every market BULK has listed — new coins
    // automatically appear here once BULK starts returning regime fields.
    const symbols = await getActiveSymbols();
    const regimeData: any[] = [];

    for (const symbol of symbols) {
      try {
        const tickerRes = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
        if (tickerRes.ok) {
          const ticker = await tickerRes.json() as any;
          regimeData.push({
            symbol: coinFromSymbol(symbol),
            regime: ticker.regime ?? 0,
            regimeDt: ticker.regimeDt ?? 0,
            regimeVol: ticker.regimeVol ?? 0,
            fairBookPx: ticker.fairBookPx ?? 0,
            markPrice: ticker.markPrice ?? 0,
            fairBias: ticker.fairBias ?? 0,
            timestamp: ticker.timestamp
          });
        }
      } catch (e) {
        console.error(`Failed to fetch regime for ${symbol}:`, e);
      }
    }

    // Calculate aggregate regime (weighted by some factor or just average)
    const avgRegime = regimeData.length > 0
      ? regimeData.reduce((sum, d) => sum + (d.regime || 0), 0) / regimeData.length
      : 0;

    const result = {
      timestamp: Date.now(),
      aggregateRegime: avgRegime,
      markets: regimeData
    };

    await setCache(cacheKey, result, 10); // 10 second cache for live data
    res.json(result);
  } catch (error) {
    console.error('Error fetching regime data:', error);
    res.status(500).json({ error: 'Failed to fetch regime data' });
  }
});

// Volatility history chart data
router.get('/volatility-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const cacheKey = `analytics:volatility_chart:${hours}`;

  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    // Determine bucket interval for date_trunc (PostgreSQL format)
    let bucketInterval = 'hour';
    if (hours <= 24) bucketInterval = 'hour';
    else if (hours <= 168) bucketInterval = 'hour';
    else bucketInterval = 'day';

    // Pivot per-(time_bucket, symbol) rows into per-timestamp dicts. The SQL
    // now groups dynamically by symbol — no more hardcoded btc_vol/eth_vol
    // columns — so any market with regime_vol data appears automatically.
    const rows = await query<{
      time_bucket: string;
      symbol: string;
      vol: string;
    }>(`
      SELECT 
        date_trunc('${bucketInterval}', timestamp) as time_bucket,
        symbol,
        AVG(regime_vol) as vol
      FROM ticker_snapshots
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
        AND regime_vol IS NOT NULL
      GROUP BY time_bucket, symbol
      ORDER BY time_bucket ASC
    `);

    const symbols = await getActiveSymbols();
    const bucketMap = new Map<string, Record<string, number>>();
    for (const row of rows) {
      const ts = new Date(row.time_bucket).toISOString();
      if (!bucketMap.has(ts)) bucketMap.set(ts, zeroCoinDict(symbols));
      const coin = coinFromSymbol(row.symbol);
      if (coin) bucketMap.get(ts)![coin] = parseFloat(row.vol || '0');
    }

    const data = Array.from(bucketMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ts, coinValues]) => buildAdditiveRow(ts, coinValues));

    const result = { period: hours, data };

    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching volatility chart:', error);
    res.status(500).json({ error: 'Failed to fetch volatility chart' });
  }
});

// Fair price vs mark price spread chart
router.get('/fair-spread-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const symbol = (req.query.symbol as string) || 'BTC-USD';
  const cacheKey = `analytics:fair_spread:${symbol}:${hours}`;
  
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Determine bucket interval for date_trunc (PostgreSQL format)
    let bucketInterval = 'hour';
    if (hours <= 24) bucketInterval = 'hour';
    else if (hours <= 168) bucketInterval = 'hour';
    else bucketInterval = 'day';
    
    const data = await query<{
      time_bucket: string;
      avg_mark: string;
      avg_fair: string;
    }>(`
      SELECT 
        date_trunc('${bucketInterval}', timestamp) as time_bucket,
        AVG(mark_price) as avg_mark,
        AVG(fair_book_px) as avg_fair
      FROM ticker_snapshots
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
        AND symbol = $1
        AND fair_book_px IS NOT NULL
      GROUP BY time_bucket
      ORDER BY time_bucket ASC
    `, [symbol]);
    
    const result = {
      symbol,
      period: hours,
      data: data.map(row => {
        const markPrice = parseFloat(row.avg_mark || '0');
        const fairPrice = parseFloat(row.avg_fair || '0');
        const spread = fairPrice > 0 ? ((markPrice - fairPrice) / fairPrice) * 100 : 0;
        return {
          timestamp: row.time_bucket,
          markPrice,
          fairPrice,
          spreadBps: spread * 100 // in basis points
        };
      })
    };
    
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching fair spread chart:', error);
    res.status(500).json({ error: 'Failed to fetch fair spread chart' });
  }
});

// ============ FEE STATE ENDPOINTS ============

// Fee tiers (from BULK API)
router.get('/fee-tiers', async (req: Request, res: Response) => {
  const cacheKey = 'analytics:fee_tiers';
  
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    const feeRes = await fetch(`${BULK_API_BASE}/feeState`);
    if (!feeRes.ok) {
      throw new Error('Failed to fetch fee state from BULK API');
    }
    
    const feeState = await feeRes.json() as any;
    
    // Extract global fee tiers
    const globalScope = feeState.scopes?.find((s: any) => s.instrument === 'global');
    const tiers = globalScope?.active_policy?.tiers || [];
    
    const result = {
      timestamp: feeState.stamp,
      windowDays: globalScope?.active_policy?.window_days || 15,
      tiers: tiers.map((t: any) => ({
        thresholdVolume: t.threshold_volume,
        makerBps: t.maker_bps,
        takerBps: t.taker_bps
      })),
      totalMakerFees: feeState.total_maker_fees || 0,
      totalTakerFees: feeState.total_taker_fees || 0,
      totalProtocolSettlement: feeState.total_protocol_settlement || 0,
      settledFills: feeState.settled_fills || 0
    };
    
    await setCache(cacheKey, result, 300); // 5 min cache
    res.json(result);
  } catch (error) {
    console.error('Error fetching fee tiers:', error);
    res.status(500).json({ error: 'Failed to fetch fee tiers' });
  }
});

// Protocol revenue chart (requires fee_snapshots table - collected periodically)
router.get('/protocol-revenue-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 168; // Default 7 days
  const cacheKey = `analytics:protocol_revenue:${hours}`;
  
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Determine bucket interval for date_trunc (PostgreSQL format)
    let bucketInterval = 'hour';
    if (hours <= 24) bucketInterval = 'hour';
    else if (hours <= 168) bucketInterval = 'hour'; // Still hourly, just more data
    else bucketInterval = 'day';
    
    const data = await query<{
      time_bucket: string;
      protocol_revenue: string;
      maker_fees: string;
      taker_fees: string;
    }>(`
      SELECT 
        date_trunc('${bucketInterval}', timestamp) as time_bucket,
        MAX(total_protocol_settlement) as protocol_revenue,
        MAX(total_maker_fees) as maker_fees,
        MAX(total_taker_fees) as taker_fees
      FROM fee_snapshots
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY time_bucket
      ORDER BY time_bucket ASC
    `);
    
    // Calculate deltas (difference between consecutive snapshots).
    // fee_snapshots stores RUNNING TOTALS from BULK's /feeState endpoint, so to get
    // the period's actual flows we subtract the previous bucket's total.
    //
    // BULK's sign convention (verified from /feeState response):
    //   total_maker_fees        → POSITIVE  (rebates received by makers)
    //   total_taker_fees        → NEGATIVE  (fees paid by takers, from their POV)
    //   total_protocol_settlement → POSITIVE (protocol's cut)
    //   Identity: |total_taker_fees| = total_maker_fees + total_protocol_settlement
    // Deltas of each are returned as-is here; the frontend applies Math.abs() on display.
    const withDeltas = data.map((row, i) => {
      const prev = data[i - 1];
      const prevProtocol = i > 0 ? parseFloat(prev.protocol_revenue || '0') : 0;
      const prevMaker    = i > 0 ? parseFloat(prev.maker_fees       || '0') : 0;
      const prevTaker    = i > 0 ? parseFloat(prev.taker_fees       || '0') : 0;

      const curProtocol = parseFloat(row.protocol_revenue || '0');
      const curMaker    = parseFloat(row.maker_fees       || '0');
      const curTaker    = parseFloat(row.taker_fees       || '0');

      return {
        timestamp: row.time_bucket,
        cumulativeRevenue: curProtocol,                       // running total (for the line)
        periodRevenue:     i > 0 ? curProtocol - prevProtocol : 0, // delta for the bar
        makerFees:         i > 0 ? curMaker    - prevMaker    : 0, // delta for the bar
        takerFees:         i > 0 ? curTaker    - prevTaker    : 0, // delta for the bar
      };
    });
    
    const result = {
      period: hours,
      data: withDeltas
    };
    
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching protocol revenue chart:', error);
    res.status(500).json({ error: 'Failed to fetch protocol revenue chart' });
  }
});

// ============ ADL EVENTS CHART ============

// ADL events chart (already in DB)
router.get('/adl-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 168;
  const cacheKey = `analytics:adl_chart:${hours}`;

  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    // Determine bucket interval
    let bucketInterval = '1 hour';
    if (hours <= 24) bucketInterval = '1 hour';
    else if (hours <= 168) bucketInterval = '4 hours';
    else bucketInterval = '1 day';

    // Start date for BULK API data
    const BULK_API_START = '2026-04-13T19:00:00.000Z';

    // Group dynamically per symbol — no more hardcoded btc_value/eth_value
    // columns. Any new market BULK lists appears automatically once ADL
    // events for it land in the DB.
    const rows = await query<{
      time_bucket: string;
      symbol: string;
      bucket_value: string;
      bucket_count: string;
    }>(`
      SELECT 
        date_trunc('${bucketInterval.replace(' ', '_')}', timestamp) as time_bucket,
        symbol,
        COALESCE(SUM(value), 0) as bucket_value,
        COUNT(*) as bucket_count
      FROM adl_events
      WHERE timestamp >= GREATEST('${BULK_API_START}'::timestamp, NOW() - INTERVAL '${hours} hours')
      GROUP BY time_bucket, symbol
      ORDER BY time_bucket ASC
    `);

    const symbols = await getActiveSymbols();
    const bucketMap = new Map<string, { coinValues: Record<string, number>; count: number }>();
    for (const row of rows) {
      const ts = new Date(row.time_bucket).toISOString();
      if (!bucketMap.has(ts)) bucketMap.set(ts, { coinValues: zeroCoinDict(symbols), count: 0 });
      const entry = bucketMap.get(ts)!;
      const coin = coinFromSymbol(row.symbol);
      if (coin) entry.coinValues[coin] = parseFloat(row.bucket_value || '0');
      entry.count += parseInt(row.bucket_count || '0');
    }

    let cumulative = 0;
    const data = Array.from(bucketMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ts, { coinValues, count }]) => {
        const total = Object.values(coinValues).reduce((s, v) => s + v, 0);
        cumulative += total;
        return buildAdditiveRow(ts, coinValues, { total, count, Cumulative: cumulative });
      });

    const result = { period: hours, data };
    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching ADL chart:', error);
    res.status(500).json({ error: 'Failed to fetch ADL chart' });
  }
});

// ADL summary stats
router.get('/adl-summary', async (req: Request, res: Response) => {
  const period = req.query.period as string || '7d';
  const cacheKey = `analytics:adl_summary:${period}`;

  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  const intervalMap: Record<string, string> = {
    '24h': '24 hours',
    '7d': '7 days',
    '30d': '30 days',
    'all': '365 days'
  };
  const interval = intervalMap[period] || '7 days';

  try {
    // Totals plus per-symbol breakdown, dynamically grouped.
    const [totals, bySymbol] = await Promise.all([
      query<{ total_value: string; total_count: string }>(`
        SELECT
          COALESCE(SUM(value), 0) as total_value,
          COUNT(*) as total_count
        FROM adl_events
        WHERE timestamp > NOW() - INTERVAL '${interval}'
      `),
      query<{ symbol: string; sym_value: string }>(`
        SELECT symbol, COALESCE(SUM(value), 0) as sym_value
        FROM adl_events
        WHERE timestamp > NOW() - INTERVAL '${interval}'
        GROUP BY symbol
      `),
    ]);

    const symbols = await getActiveSymbols();
    const byAsset: Record<string, number> = zeroCoinDict(symbols);
    for (const r of bySymbol) {
      const coin = coinFromSymbol(r.symbol);
      if (coin) byAsset[coin] = parseFloat(r.sym_value || '0');
    }

    const row = totals[0];
    const result = {
      period,
      totalValue: parseFloat(row?.total_value || '0'),
      totalCount: parseInt(row?.total_count || '0'),
      byAsset, // full dict with every coin — old consumers reading .BTC/.ETH/.SOL still work
    };

    await setCache(cacheKey, result, 60);
    res.json(result);
  } catch (error) {
    console.error('Error fetching ADL summary:', error);
    res.status(500).json({ error: 'Failed to fetch ADL summary' });
  }
});

// ============ ORDER BOOK (live proxy of BULK /l2book) ============

// Live order book snapshot for a given market.
// This is a thin caching proxy over BULK's /l2book endpoint so the frontend can
// auto-refresh every few seconds without hammering BULK when many users are on
// the page. Cache TTL is deliberately short (2s) to keep data fresh.
//
// Only BTC-USD / ETH-USD / SOL-USD are allowed here to match the rest of the
// site; the BULK endpoint itself supports more markets.
router.get('/orderbook/:coin', async (req: Request, res: Response) => {
  const coinParam = String(req.params.coin || '').toUpperCase();

  // Whitelist input to prevent arbitrary proxying and inject-style abuse.
  const ALLOWED = new Set(['BTC-USD', 'ETH-USD', 'SOL-USD']);
  const coin = coinParam.endsWith('-USD') ? coinParam : `${coinParam}-USD`;
  if (!ALLOWED.has(coin)) {
    return res.status(400).json({ error: `Unsupported market: ${coinParam}` });
  }

  const nlevels = Math.max(1, Math.min(50, parseInt(String(req.query.nlevels ?? '20'), 10) || 20));
  const cacheKey = `analytics:orderbook:${coin}:${nlevels}`;

  const cached = await getCache<unknown>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    const url = `${BULK_API_BASE}/l2book?type=l2book&coin=${encodeURIComponent(coin)}&nlevels=${nlevels}`;
    const upstream = await fetch(url);
    if (!upstream.ok) {
      console.error(`BULK /l2book returned ${upstream.status} for ${coin}`);
      return res.status(502).json({ error: 'Upstream order book unavailable' });
    }
    const raw: any = await upstream.json();

    // Validate shape defensively — BULK's docs say `levels: [bids, asks]` and we
    // want to fail loudly rather than push a malformed payload to the client.
    if (!raw || !Array.isArray(raw.levels) || raw.levels.length !== 2) {
      console.error('Unexpected /l2book shape:', JSON.stringify(raw).slice(0, 200));
      return res.status(502).json({ error: 'Malformed upstream response' });
    }

    const bids = Array.isArray(raw.levels[0]) ? raw.levels[0] : [];
    const asks = Array.isArray(raw.levels[1]) ? raw.levels[1] : [];

    // Compute derived stats here so the frontend doesn't have to redo math on
    // every refresh. All values are USD-quoted (BULK markets are USD quote).
    const bestBid = bids[0] ? { px: Number(bids[0].px), sz: Number(bids[0].sz), n: Number(bids[0].n) } : null;
    const bestAsk = asks[0] ? { px: Number(asks[0].px), sz: Number(asks[0].sz), n: Number(asks[0].n) } : null;

    let mid: number | null = null;
    let spreadAbs: number | null = null;
    let spreadBps: number | null = null;
    if (bestBid && bestAsk) {
      mid = (bestBid.px + bestAsk.px) / 2;
      spreadAbs = bestAsk.px - bestBid.px;
      spreadBps = mid > 0 ? (spreadAbs / mid) * 10000 : null;
    }

    // Depth within ±2% of mid (notional USD).
    const depth2pct = (() => {
      if (!mid) return { bid: 0, ask: 0 };
      const lo = mid * 0.98, hi = mid * 1.02;
      const bidUsd = bids
        .filter((l: any) => Number(l.px) >= lo)
        .reduce((s: number, l: any) => s + Number(l.px) * Number(l.sz), 0);
      const askUsd = asks
        .filter((l: any) => Number(l.px) <= hi)
        .reduce((s: number, l: any) => s + Number(l.px) * Number(l.sz), 0);
      return { bid: bidUsd, ask: askUsd };
    })();

    // Book imbalance: fraction of total ±2% depth on the bid side, rebased to
    // [-1, +1] where +1 = all bids, -1 = all asks, 0 = balanced.
    const totalDepth = depth2pct.bid + depth2pct.ask;
    const imbalance = totalDepth > 0 ? (depth2pct.bid - depth2pct.ask) / totalDepth : 0;

    const result = {
      symbol: raw.symbol || coin,
      updateType: raw.updateType || 'snapshot',
      // BULK returns nanoseconds despite the docs saying ms. Normalize to ms
      // here so the client has a single time format to deal with.
      timestamp: typeof raw.timestamp === 'number'
        ? Math.floor(raw.timestamp / 1_000_000)
        : Date.now(),
      bids: bids.map((l: any) => ({ px: Number(l.px), sz: Number(l.sz), n: Number(l.n) })),
      asks: asks.map((l: any) => ({ px: Number(l.px), sz: Number(l.sz), n: Number(l.n) })),
      stats: {
        bestBid,
        bestAsk,
        mid,
        spreadAbs,
        spreadBps,
        bidDepth2pctUsd: depth2pct.bid,
        askDepth2pctUsd: depth2pct.ask,
        imbalance,
      },
    };

    // 2-second TTL — order books move fast, but not so fast that a cached copy
    // for 2 seconds will mislead anyone. Prevents thundering herd against BULK.
    await setCache(cacheKey, result, 2);
    res.json(result);
  } catch (error) {
    console.error('Error fetching order book:', error);
    res.status(500).json({ error: 'Failed to fetch order book' });
  }
});

// ============ EXCHANGE INFO (list of all markets from BULK) ============

// Proxy BULK's /exchangeInfo so the frontend doesn't have to hit the external
// API directly (avoids CORS, adds caching, normalizes the shape). This drives
// the dynamic coin list that powers every chart's coin selector — whenever
// BULK lists a new market, it shows up here automatically.
//
// Cached aggressively (5 minutes) because market metadata changes infrequently.
router.get('/exchange-info', async (_req: Request, res: Response) => {
  const cacheKey = 'analytics:exchange_info';

  const cached = await getCache<unknown>(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  try {
    const upstream = await fetch(`${BULK_API_BASE}/exchangeInfo`);
    if (!upstream.ok) {
      console.error(`BULK /exchangeInfo returned ${upstream.status}`);
      return res.status(502).json({ error: 'Upstream exchange info unavailable' });
    }
    const raw: any = await upstream.json();

    // BULK returns an array of market objects. Normalize each to the minimal
    // shape the frontend needs — we can add more fields later as needed.
    const markets = Array.isArray(raw)
      ? raw
          .filter((m: any) => m && typeof m.symbol === 'string')
          .map((m: any) => ({
            symbol: String(m.symbol),                              // e.g. "BTC-USD"
            coin: String(m.baseAsset || m.symbol.replace('-USD', '')), // e.g. "BTC"
            quoteAsset: String(m.quoteAsset || 'USDC'),
            status: String(m.status || 'TRADING'),
            maxLeverage: Number(m.maxLeverage || 0),
            tickSize: Number(m.tickSize || 0),
            lotSize: Number(m.lotSize || 0),
            minNotional: Number(m.minNotional || 0),
          }))
      : [];

    const result = { markets, count: markets.length, timestamp: Date.now() };
    await setCache(cacheKey, result, 300); // 5-minute TTL
    res.json(result);
  } catch (error) {
    console.error('Error fetching exchange info:', error);
    res.status(500).json({ error: 'Failed to fetch exchange info' });
  }
});

export default router;
