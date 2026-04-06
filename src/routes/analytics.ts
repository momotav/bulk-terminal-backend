import { Router, Request, Response } from 'express';
import { query } from '../db';
import { getCache, setCache } from '../services/cache';

const router = Router();

// BULK API base URL
const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

// All supported markets
const MARKETS = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'GOLD-USD', 'XRP-USD'];

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
async function fetchAllKlines(interval: string = '1h', limit: number = 100): Promise<{ symbol: string; klines: BulkKline[] }[]> {
  const results = await Promise.all(
    MARKETS.map(async (symbol) => ({
      symbol,
      klines: await fetchKlines(symbol, interval, limit)
    }))
  );
  return results;
}

// Helper: Fetch all tickers and sum volume/OI
async function fetchTickersForStats(): Promise<{ volume24h: number; openInterest: number; timestamp: number }> {
  let totalVolume = 0;
  let totalOI = 0;
  let timestamp = Date.now();

  const tickerPromises = MARKETS.map(symbol =>
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
    // Get recent trades from database
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
    
    // Get recent liquidations from database
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
    `, [limit]).catch(() => []);
    
    // Combine and sort by timestamp
    const combined = [...trades, ...liquidations]
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
      .slice(0, limit);
    
    res.json({ data: combined });
  } catch (error) {
    console.error('Error fetching recent activity:', error);
    res.status(500).json({ error: 'Failed to fetch recent activity' });
  }
});

// ============ BULK API KLINES FOR CHARTS ============

// Volume chart from BULK API klines (aggregated by symbol)
router.get('/volume-chart-api', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const now = Date.now();
    const startTime = now - (hours * 60 * 60 * 1000);
    
    // Determine interval based on hours
    let interval = '1h';
    if (hours <= 24) interval = '1h';
    else if (hours <= 168) interval = '4h';
    else if (hours <= 720) interval = '1d';
    else interval = '1d';
    
    const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
    const klinesPromises = symbols.map(symbol => 
      fetch(`${BULK_API_BASE}/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${now}`)
        .then(r => r.ok ? r.json() : [])
        .catch(() => [])
    );
    
    const [btcKlines, ethKlines, solKlines] = await Promise.all(klinesPromises);
    
    // Create a map of timestamp -> volumes
    const volumeMap = new Map<number, { BTC: number; ETH: number; SOL: number }>();
    
    // Process BTC
    (btcKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!volumeMap.has(ts)) volumeMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      volumeMap.get(ts)!.BTC = (k.v || 0) * (k.c || 0); // volume in USD
    });
    
    // Process ETH
    (ethKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!volumeMap.has(ts)) volumeMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      volumeMap.get(ts)!.ETH = (k.v || 0) * (k.c || 0);
    });
    
    // Process SOL
    (solKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!volumeMap.has(ts)) volumeMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      volumeMap.get(ts)!.SOL = (k.v || 0) * (k.c || 0);
    });
    
    // Convert to array and sort, add cumulative
    let cumulative = 0;
    const data = Array.from(volumeMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, vol]) => {
        const total = vol.BTC + vol.ETH + vol.SOL;
        cumulative += total;
        return {
          timestamp: new Date(ts).toISOString(),
          BTC: vol.BTC,
          ETH: vol.ETH,
          SOL: vol.SOL,
          total,
          Cumulative: cumulative,
        };
      });
    
    console.log(`📊 Volume chart: ${data.length} data points from BULK API klines`);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching volume chart from API:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart' });
  }
});

// Trades count chart from BULK API klines
router.get('/trades-chart-api', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const now = Date.now();
    const startTime = now - (hours * 60 * 60 * 1000);
    
    // Determine interval based on hours
    let interval = '1h';
    if (hours <= 24) interval = '1h';
    else if (hours <= 168) interval = '4h';
    else if (hours <= 720) interval = '1d';
    else interval = '1d';
    
    const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
    const klinesPromises = symbols.map(symbol => 
      fetch(`${BULK_API_BASE}/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${now}`)
        .then(r => r.ok ? r.json() : [])
        .catch(() => [])
    );
    
    const [btcKlines, ethKlines, solKlines] = await Promise.all(klinesPromises);
    
    // Create a map of timestamp -> trade counts
    const tradesMap = new Map<number, { BTC: number; ETH: number; SOL: number }>();
    
    // Process BTC
    (btcKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!tradesMap.has(ts)) tradesMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      tradesMap.get(ts)!.BTC = k.n || 0; // number of trades
    });
    
    // Process ETH
    (ethKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!tradesMap.has(ts)) tradesMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      tradesMap.get(ts)!.ETH = k.n || 0;
    });
    
    // Process SOL
    (solKlines as any[]).forEach((k: any) => {
      const ts = k.t;
      if (!tradesMap.has(ts)) tradesMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      tradesMap.get(ts)!.SOL = k.n || 0;
    });
    
    // Convert to array and sort, add cumulative
    let cumulative = 0;
    const data = Array.from(tradesMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, trades]) => {
        const total = trades.BTC + trades.ETH + trades.SOL;
        cumulative += total;
        return {
          timestamp: new Date(ts).toISOString(),
          BTC: trades.BTC,
          ETH: trades.ETH,
          SOL: trades.SOL,
          total,
          Cumulative: cumulative,
        };
      });
    
    console.log(`📊 Trades chart: ${data.length} data points from BULK API klines`);
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
    
    // Group by timestamp
    const dataMap = new Map<string, { BTC: number; ETH: number; SOL: number }>();
    
    for (const row of result) {
      const ts = new Date(row.timestamp).toISOString();
      if (!dataMap.has(ts)) {
        dataMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      }
      const coin = (row.symbol || '').split('-')[0] as 'BTC' | 'ETH' | 'SOL';
      if (coin in dataMap.get(ts)!) {
        dataMap.get(ts)![coin] = parseFloat(row.value || 0);
      }
    }
    
    let data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values,
      total: values.BTC + values.ETH + values.SOL
    }));
    
    // Filter out restart drops: remove points where total drops more than 80% from neighbors
    // or where total is 0 but neighbors have significant values
    data = data.filter((point, index, arr) => {
      const total = point.total;
      
      // If total is 0 or very small, check if it's a restart drop
      if (total < 1000) {
        // Check previous and next points
        const prev = arr[index - 1];
        const next = arr[index + 1];
        const prevTotal = prev?.total || 0;
        const nextTotal = next?.total || 0;
        
        // If both neighbors have significant values, this is likely a restart drop
        if (prevTotal > 100000 || nextTotal > 100000) {
          return false; // Filter out this point
        }
      }
      
      // Check for sudden drops more than 80%
      if (index > 0) {
        const prev = arr[index - 1];
        if (prev.total > 100000 && total < prev.total * 0.2) {
          // Check if it recovers in the next few points
          const next = arr[index + 1];
          if (next && next.total > prev.total * 0.5) {
            return false; // This is a temporary drop, filter it out
          }
        }
      }
      
      return true;
    });
    
    const response = { hours, dataPoints: data.length, data };
    
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
    
    const dataMap = new Map<string, { BTC: number; ETH: number; SOL: number }>();
    
    for (const row of result) {
      const ts = new Date(row.timestamp).toISOString();
      if (!dataMap.has(ts)) {
        dataMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      }
      const coin = (row.symbol || '').split('-')[0] as 'BTC' | 'ETH' | 'SOL';
      if (coin in dataMap.get(ts)!) {
        dataMap.get(ts)![coin] = parseFloat(row.value || 0);
      }
    }
    
    let data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values
    }));
    
    // Filter out restart drops: remove points where all funding rates are 0
    // but neighbors have non-zero values (indicates server restart)
    data = data.filter((point, index, arr) => {
      const allZero = point.BTC === 0 && point.ETH === 0 && point.SOL === 0;
      
      if (allZero) {
        // Check if neighbors have data
        const prev = arr[index - 1];
        const next = arr[index + 1];
        const prevHasData = prev && (prev.BTC !== 0 || prev.ETH !== 0 || prev.SOL !== 0);
        const nextHasData = next && (next.BTC !== 0 || next.ETH !== 0 || next.SOL !== 0);
        
        // If surrounded by data points, this is likely a restart gap
        if (prevHasData || nextHasData) {
          return false;
        }
      }
      
      return true;
    });
    
    const response = { hours, dataPoints: data.length, data };
    
    // Cache for 60 seconds
    await setCache(cacheKey, response, 60);
    
    res.json(response);
  } catch (error) {
    console.error('Error fetching funding chart:', error);
    res.json({ hours, dataPoints: 0, data: [], error: 'No funding data available yet' });
  }
});

// ============ DATABASE CHARTS (Volume, Trades, Liquidations, ADL) ============

// Helper function to transform raw DB rows to chart format
function transformToChartData(rows: any[]): { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }[] {
  const dataMap = new Map<string, { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }>();
  
  for (const row of rows) {
    const dateKey = new Date(row.day).toISOString();
    if (!dataMap.has(dateKey)) {
      dataMap.set(dateKey, { timestamp: dateKey, BTC: 0, ETH: 0, SOL: 0, total: 0 });
    }
    const entry = dataMap.get(dateKey)!;
    const value = parseFloat(row.volume || row.total_value || row.trade_count || row.liquidation_count || row.adl_count || 0);
    
    entry.total += value;
    
    const symbol = row.symbol || '';
    if (symbol.includes('BTC')) entry.BTC += value;
    else if (symbol.includes('ETH')) entry.ETH += value;
    else if (symbol.includes('SOL')) entry.SOL += value;
  }
  
  return Array.from(dataMap.values()).sort((a, b) => 
    new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );
}

// Get volume chart data from database
router.get('/volume-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  
  try {
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        SUM(value) as volume
      FROM trades
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `);
    
    const data = transformToChartData(rows);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching volume chart:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart data' });
  }
});

// Get trades chart data from database
router.get('/trades-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const cacheKey = `analytics:trades_chart:${hours}`;
  
  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    // Use Promise.race with timeout to prevent hanging
    const rows = await Promise.race([
      query(`
        SELECT 
          date_trunc('day', timestamp) as day,
          symbol,
          COUNT(*) as trade_count,
          SUM(value) as volume
        FROM trades
        WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
        GROUP BY date_trunc('day', timestamp), symbol
        ORDER BY day ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 8000))
    ]) as any[];
    
    const data = transformToChartData(rows);
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
router.get('/liquidations-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const cacheKey = `analytics:liquidations_chart:${hours}`;
  
  // Check cache first
  const cached = await getCache<any>(cacheKey);
  if (cached) {
    return res.json(cached);
  }
  
  try {
    const rows = await Promise.race([
      query(`
        SELECT 
          date_trunc('day', timestamp) as day,
          symbol,
          COUNT(*) as liquidation_count,
          SUM(value) as total_value
        FROM liquidations
        WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
        GROUP BY date_trunc('day', timestamp), symbol
        ORDER BY day ASC
      `),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Query timeout')), 5000))
    ]) as any[];
    
    const data = transformToChartData(rows);
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
router.get('/adl-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  
  try {
    // Try adl_events table first, return empty array if doesn't exist
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as adl_count,
        SUM(value) as total_value
      FROM adl_events
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `).catch(() => []);
    
    const data = transformToChartData(rows);
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
    // Fetch klines for all markets
    const allKlines = await fetchAllKlines(interval, 500);
    
    // Create a map of timestamp -> { total, BTC, ETH, SOL, ... }
    const dataMap = new Map<number, { timestamp: string; total: number; BTC: number; ETH: number; SOL: number; XRP: number; GOLD: number }>();
    
    for (const { symbol, klines } of allKlines) {
      for (const kline of klines) {
        const timestamp = kline.t;
        
        if (!dataMap.has(timestamp)) {
          dataMap.set(timestamp, {
            timestamp: new Date(timestamp).toISOString(),
            total: 0,
            BTC: 0,
            ETH: 0,
            SOL: 0,
            XRP: 0,
            GOLD: 0
          });
        }
        
        const entry = dataMap.get(timestamp)!;
        // Volume in quote (USD) = volume in base * close price
        const volumeUsd = kline.v * kline.c;
        
        entry.total += volumeUsd;
        
        if (symbol === 'BTC-USD') entry.BTC = volumeUsd;
        else if (symbol === 'ETH-USD') entry.ETH = volumeUsd;
        else if (symbol === 'SOL-USD') entry.SOL = volumeUsd;
        else if (symbol === 'XRP-USD') entry.XRP = volumeUsd;
        else if (symbol === 'GOLD-USD') entry.GOLD = volumeUsd;
      }
    }
    
    // Sort by timestamp
    const data = Array.from(dataMap.values()).sort((a, b) => 
      new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
    
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
    const tickers = await Promise.all(
      MARKETS.map(async (symbol) => {
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
      
      const dayMap = new Map<string, { BTC: number; ETH: number; SOL: number; total: number }>();
      for (const row of fallback) {
        const dayStr = new Date(row.day).toISOString().split('T')[0];
        if (!dayMap.has(dayStr)) {
          dayMap.set(dayStr, { BTC: 0, ETH: 0, SOL: 0, total: parseInt(row.total) });
        }
        const entry = dayMap.get(dayStr)!;
        const coin = row.symbol.replace('-USD', '') as 'BTC' | 'ETH' | 'SOL';
        if (coin in entry) entry[coin] = parseInt(row.traders);
      }
      
      const chartData = Array.from(dayMap.entries())
        .map(([day, values]) => ({ timestamp: day, ...values }))
        .sort((a, b) => a.timestamp.localeCompare(b.timestamp));
      
      const result = { data: chartData };
      await setCache(cacheKey, result, 60); // Short cache for fallback
      return res.json(result);
    }

    // Build from pre-aggregated data (FAST)
    const totalMap = new Map<string, number>();
    for (const row of totals) {
      totalMap.set(new Date(row.day).toISOString().split('T')[0], parseInt(row.total_unique));
    }
    
    const dayMap = new Map<string, { BTC: number; ETH: number; SOL: number; total: number }>();
    for (const row of data) {
      const dayStr = new Date(row.day).toISOString().split('T')[0];
      if (!dayMap.has(dayStr)) {
        dayMap.set(dayStr, { BTC: 0, ETH: 0, SOL: 0, total: totalMap.get(dayStr) || 0 });
      }
      const entry = dayMap.get(dayStr)!;
      const coin = row.symbol.replace('-USD', '') as 'BTC' | 'ETH' | 'SOL';
      if (coin in entry) entry[coin] = parseInt(row.unique_traders);
    }

    const chartData = Array.from(dayMap.entries())
      .map(([day, values]) => ({ timestamp: day, ...values }))
      .sort((a, b) => a.timestamp.localeCompare(b.timestamp));

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

export default router;
