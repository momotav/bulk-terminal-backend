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
    
    // Calculate median values for each coin to detect anomalies
    const btcValues = data.map(d => d.BTC).filter(v => v > 0).sort((a, b) => a - b);
    const ethValues = data.map(d => d.ETH).filter(v => v > 0).sort((a, b) => a - b);
    const solValues = data.map(d => d.SOL).filter(v => v > 0).sort((a, b) => a - b);
    
    const medianBTC = btcValues.length > 0 ? btcValues[Math.floor(btcValues.length / 2)] : 0;
    const medianETH = ethValues.length > 0 ? ethValues[Math.floor(ethValues.length / 2)] : 0;
    const medianSOL = solValues.length > 0 ? solValues[Math.floor(solValues.length / 2)] : 0;
    
    // Calculate what percentage of data points have each coin
    const btcCoverage = btcValues.length / data.length;
    const ethCoverage = ethValues.length / data.length;
    const solCoverage = solValues.length / data.length;
    
    // Filter out restart drops using multiple strategies
    data = data.filter((point, index, arr) => {
      // Strategy 1: If a coin is 0 but it normally has data (>80% coverage) and median is significant
      // This is likely a restart drop - filter it out
      if (btcCoverage > 0.8 && medianBTC > 10000000 && point.BTC === 0) {
        return false;
      }
      
      if (ethCoverage > 0.8 && medianETH > 10000000 && point.ETH === 0) {
        return false;
      }
      
      if (solCoverage > 0.8 && medianSOL > 10000000 && point.SOL === 0) {
        return false;
      }
      
      // Strategy 2: Detect sudden drops more than 90% for any coin
      if (index > 0) {
        const prev = arr[index - 1];
        const next = arr[index + 1];
        
        // BTC sudden drop (but not to exactly 0 - that's handled above)
        if (point.BTC > 0 && prev.BTC > medianBTC * 0.5 && point.BTC < prev.BTC * 0.1) {
          if (next && next.BTC > prev.BTC * 0.5) {
            return false;
          }
        }
        
        // ETH sudden drop
        if (point.ETH > 0 && prev.ETH > medianETH * 0.5 && point.ETH < prev.ETH * 0.1) {
          if (next && next.ETH > prev.ETH * 0.5) {
            return false;
          }
        }
        
        // SOL sudden drop
        if (point.SOL > 0 && prev.SOL > medianSOL * 0.5 && point.SOL < prev.SOL * 0.1) {
          if (next && next.SOL > prev.SOL * 0.5) {
            return false;
          }
        }
      }
      
      // Strategy 3: Total drops to near zero
      if (point.total < 1000) {
        const prev = arr[index - 1];
        const next = arr[index + 1];
        if ((prev && prev.total > 100000) || (next && next.total > 100000)) {
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
    
    // Count how many data points have non-zero values for each coin
    const btcCount = data.filter(d => d.BTC !== 0).length;
    const ethCount = data.filter(d => d.ETH !== 0).length;
    const solCount = data.filter(d => d.SOL !== 0).length;
    const totalPoints = data.length;
    
    // If a coin has data in most points, filter out points where it's suddenly 0
    const btcShouldHaveData = btcCount > totalPoints * 0.7;
    const ethShouldHaveData = ethCount > totalPoints * 0.7;
    const solShouldHaveData = solCount > totalPoints * 0.7;
    
    // Filter out restart drops
    data = data.filter((point, index, arr) => {
      const prev = arr[index - 1];
      const next = arr[index + 1];
      
      // Check if this point has missing data that neighbors have
      // BTC missing but should have data
      if (btcShouldHaveData && point.BTC === 0) {
        if ((prev && prev.BTC !== 0) || (next && next.BTC !== 0)) {
          // Check if multiple coins are missing - likely a restart
          const missingCount = (point.BTC === 0 ? 1 : 0) + (point.ETH === 0 ? 1 : 0) + (point.SOL === 0 ? 1 : 0);
          if (missingCount >= 2) {
            return false;
          }
        }
      }
      
      // All zeros is definitely a restart
      const allZero = point.BTC === 0 && point.ETH === 0 && point.SOL === 0;
      if (allZero) {
        const prevHasData = prev && (prev.BTC !== 0 || prev.ETH !== 0 || prev.SOL !== 0);
        const nextHasData = next && (next.BTC !== 0 || next.ETH !== 0 || next.SOL !== 0);
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
router.get('/liquidations/chart', async (req: Request, res: Response) => {
  const period = req.query.period as string || 'all';
  
  const intervalMap: Record<string, { interval: string; bucket: string }> = {
    '4h': { interval: '4 hours', bucket: '15 minutes' },
    '24h': { interval: '24 hours', bucket: '1 hour' },
    '3d': { interval: '3 days', bucket: '4 hours' },
    '7d': { interval: '7 days', bucket: '12 hours' },
    'all': { interval: '365 days', bucket: '1 day' }
  };
  const { interval, bucket } = intervalMap[period] || intervalMap['all'];
  
  try {
    const cacheKey = `liq-chart:${period}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const data = await query<{
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
      WHERE timestamp > NOW() - INTERVAL '${interval}'
      GROUP BY time_bucket
      ORDER BY time_bucket ASC
    `);

    const result = {
      period,
      data: data.map(row => ({
        timestamp: row.time_bucket,
        longValue: parseFloat(row.long_value),
        shortValue: parseFloat(row.short_value),
        longCount: parseInt(row.long_count),
        shortCount: parseInt(row.short_count)
      }))
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

export default router;
