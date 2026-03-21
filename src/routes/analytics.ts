import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

// BULK API base URL
const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

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
  volume?: { totalUsd?: number };
  openInterest?: { totalUsd?: number };
  markets?: Array<{
    symbol: string;
    quoteVolume?: number;
    openInterest?: number;
    markPrice?: number;
    fundingRate?: number;
  }>;
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

// Get exchange stats - uses BULK API /stats for volume and OI
router.get('/exchange-stats', async (req: Request, res: Response) => {
  const period = (req.query.period as string) || '1d';
  
  try {
    const response = await fetch(`${BULK_API_BASE}/stats?period=${period}`);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching exchange stats:', error);
    res.status(500).json({ error: 'Failed to fetch exchange stats' });
  }
});

// Exchange health endpoint - combines BULK API + DB data
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    // Fetch from BULK API /stats for official 24h volume and OI
    let bulkStats: BulkStatsResponse | null = null;
    let totalVolume24h = 0;
    let totalOI = 0;
    
    try {
      const statsRes = await fetch(`${BULK_API_BASE}/stats?period=1d`);
      if (statsRes.ok) {
        bulkStats = await statsRes.json() as BulkStatsResponse;
        
        // Calculate volume from markets if totalUsd is null
        if (bulkStats?.markets) {
          for (const market of bulkStats.markets) {
            totalVolume24h += market.quoteVolume || 0;
            totalOI += (market.openInterest || 0) * (market.markPrice || 0);
          }
        }
        
        // Use provided totals if available
        if (bulkStats?.volume?.totalUsd) {
          totalVolume24h = bulkStats.volume.totalUsd;
        }
        if (bulkStats?.openInterest?.totalUsd) {
          totalOI = bulkStats.openInterest.totalUsd;
        }
      }
    } catch (e) {
      console.error('Failed to fetch BULK stats:', e);
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
  
  try {
    const result = await query(`
      SELECT 
        date_trunc('minute', timestamp) as timestamp,
        symbol,
        AVG(open_interest_usd) as value
      FROM ticker_snapshots
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('minute', timestamp), symbol
      ORDER BY timestamp ASC
    `);
    
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
    
    res.json({ hours, dataPoints: data.length, data });
  } catch (error) {
    console.error('Error fetching OI chart:', error);
    res.status(500).json({ error: 'Failed to fetch OI chart' });
  }
});

// Combined Funding Rate chart data for all symbols
router.get('/funding-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const result = await query(`
      SELECT 
        date_trunc('minute', timestamp) as timestamp,
        symbol,
        AVG(funding_rate) as value
      FROM ticker_snapshots
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('minute', timestamp), symbol
      ORDER BY timestamp ASC
    `);
    
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
    
    res.json({ hours, dataPoints: data.length, data });
  } catch (error) {
    console.error('Error fetching funding chart:', error);
    res.status(500).json({ error: 'Failed to fetch funding chart' });
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
  
  try {
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as trade_count,
        SUM(value) as volume
      FROM trades
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `);
    
    const data = transformToChartData(rows);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching trades chart:', error);
    res.status(500).json({ error: 'Failed to fetch trades chart data' });
  }
});

// Get liquidations chart data from database
router.get('/liquidations-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  
  try {
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as liquidation_count,
        SUM(value) as total_value
      FROM liquidations
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `);
    
    const data = transformToChartData(rows);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching liquidations chart:', error);
    res.status(500).json({ error: 'Failed to fetch liquidations chart data' });
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
  try {
    // Get BULK API stats for volume
    let totalVolume = 0;
    try {
      const statsRes = await fetch(`${BULK_API_BASE}/stats?period=all`);
      if (statsRes.ok) {
        const bulkStats = await statsRes.json() as BulkStatsResponse;
        if (bulkStats?.markets) {
          for (const market of bulkStats.markets) {
            totalVolume += market.quoteVolume || 0;
          }
        }
        if (bulkStats?.volume?.totalUsd) {
          totalVolume = bulkStats.volume.totalUsd;
        }
      }
    } catch (e) {
      console.error('Failed to fetch BULK stats for volume:', e);
    }

    const [tradesResult, liqResult, tradersResult] = await Promise.all([
      query(`SELECT COUNT(*) as count FROM trades`).catch(() => [{ count: 0 }]),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations`).catch(() => [{ count: 0, volume: 0 }]),
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM traders`).catch(() => [{ count: 0 }])
    ]);
    
    res.json({
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
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
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

export default router;
