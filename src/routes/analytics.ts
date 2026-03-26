import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

// BULK API base URL
const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

// All supported markets
const MARKETS = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'GOLD-USD', 'XRP-USD'];

// ============ SIMPLE IN-MEMORY CACHE ============
const analyticsCache: Map<string, { data: any; expiry: number }> = new Map();

function getCached<T>(key: string): T | null {
  const cached = analyticsCache.get(key);
  if (cached && cached.expiry > Date.now()) {
    return cached.data as T;
  }
  return null;
}

function setCache(key: string, data: any, ttlSeconds: number = 60): void {
  analyticsCache.set(key, { data, expiry: Date.now() + ttlSeconds * 1000 });
}
// ============ END CACHE ============

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
  const cacheKey = 'exchange_stats';
  
  // Check cache first (cache for 30 seconds)
  const cached = getCached<any>(cacheKey);
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
    setCache(cacheKey, result, 30);
    
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
  const cacheKey = `oi_chart_${hours}`;
  
  // Check cache first
  const cached = getCached<any>(cacheKey);
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
    
    const data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values,
      total: values.BTC + values.ETH + values.SOL
    }));
    
    const response = { hours, dataPoints: data.length, data };
    
    // Cache for 60 seconds
    setCache(cacheKey, response, 60);
    
    res.json(response);
  } catch (error) {
    console.error('Error fetching OI chart:', error);
    res.json({ hours, dataPoints: 0, data: [], error: 'No OI data available yet' });
  }
});

// Combined Funding Rate chart data for all symbols
router.get('/funding-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  const cacheKey = `funding_chart_${hours}`;
  
  // Check cache first
  const cached = getCached<any>(cacheKey);
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
    
    const data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values
    }));
    
    const response = { hours, dataPoints: data.length, data };
    
    // Cache for 60 seconds
    setCache(cacheKey, response, 60);
    
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
  const cacheKey = `trades_chart_${hours}`;
  
  // Check cache first
  const cached = getCached<any>(cacheKey);
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
    setCache(cacheKey, result, 60);
    
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
  const cacheKey = `liquidations_chart_${hours}`;
  
  // Check cache first
  const cached = getCached<any>(cacheKey);
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
    setCache(cacheKey, result, 60);
    
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
  const cacheKey = 'overall_stats';
  
  // Check cache first (60 second TTL)
  const cached = getCached<any>(cacheKey);
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
    setCache(cacheKey, result, 60);
    
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

export default router;
