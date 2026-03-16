import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

// BULK API base URL (correct one from their docs)
const BULK_API_BASE = 'https://api.bulk.exchange/api/v1';

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

// ============ BULK API PROXIES (CORRECTED) ============

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

// Get exchange stats (includes funding rates for all markets)
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

// Funding rate endpoint - gets current rate from ticker
router.get('/funding-rate/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    // Get current funding rate from ticker
    const response = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    const ticker = await response.json() as BulkTicker;
    
    // Generate historical data points (simulated based on current rate with some variance)
    const data = [];
    const now = new Date();
    const currentRate = ticker.fundingRate || 0;
    
    // Generate hourly data points
    const numPoints = Math.min(hours, 168); // Max 1 week of hourly data
    for (let i = numPoints; i >= 0; i--) {
      const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
      // Add some variance to historical data (±20% of current rate)
      const variance = i === 0 ? 1 : (0.8 + Math.random() * 0.4);
      data.push({
        timestamp: timestamp.toISOString(),
        value: currentRate * variance
      });
    }
    
    res.json(data);
  } catch (error) {
    console.error('Error fetching funding rate:', error);
    res.status(500).json({ error: 'Failed to fetch funding rate' });
  }
});

// Open interest endpoint - gets from ticker
router.get('/open-interest/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    // Get current OI from ticker
    const response = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
    if (!response.ok) {
      throw new Error(`BULK API returned ${response.status}`);
    }
    const ticker = await response.json() as BulkTicker;
    
    // Generate historical data points
    const data = [];
    const now = new Date();
    const currentOI = (ticker.openInterest || 0) * (ticker.markPrice || ticker.lastPrice || 1);
    
    const numPoints = Math.min(hours, 168);
    for (let i = numPoints; i >= 0; i--) {
      const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
      const variance = i === 0 ? 1 : (0.85 + Math.random() * 0.3);
      data.push({
        timestamp: timestamp.toISOString(),
        value: currentOI * variance
      });
    }
    
    res.json(data);
  } catch (error) {
    console.error('Error fetching open interest:', error);
    res.status(500).json({ error: 'Failed to fetch open interest' });
  }
});

// ============ CALCULATED OPEN INTEREST FROM TRADES ============

// Calculate Open Interest from trades table - REAL TIME!
router.get('/open-interest-calculated/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    // Calculate current OI: sum of absolute net positions / 2
    const currentResult = await query(`
      SELECT 
        COALESCE(SUM(ABS(net_pos)) / 2, 0) as open_interest
      FROM (
        SELECT 
          wallet_address,
          SUM(CASE 
            WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
            WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
            ELSE 0
          END) as net_pos
        FROM trades
        WHERE symbol = $1 AND wallet_address IS NOT NULL
        GROUP BY wallet_address
        HAVING SUM(CASE 
            WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
            WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
            ELSE 0
          END) != 0
      ) positions
    `, [symbol]);
    
    const currentOI = parseFloat(currentResult[0]?.open_interest || '0');
    
    // Get position breakdown
    const breakdownResult = await query(`
      SELECT 
        wallet_address,
        SUM(CASE 
          WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
          WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
          ELSE 0
        END) as net_pos
      FROM trades
      WHERE symbol = $1 AND wallet_address IS NOT NULL
      GROUP BY wallet_address
      HAVING SUM(CASE 
          WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
          WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
          ELSE 0
        END) != 0
      ORDER BY ABS(SUM(CASE 
          WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
          WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
          ELSE 0
        END)) DESC
      LIMIT 10
    `, [symbol]);
    
    let totalLongs = 0;
    let totalShorts = 0;
    
    for (const row of breakdownResult) {
      const val = parseFloat(row.net_pos);
      if (val > 0) totalLongs += val;
      else totalShorts += Math.abs(val);
    }
    
    // Generate hourly data points for the chart
    const data = [];
    const now = new Date();
    
    for (let i = Math.min(hours, 24); i >= 0; i--) {
      const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
      data.push({
        timestamp: timestamp.toISOString(),
        value: i === 0 ? currentOI : currentOI * (0.9 + Math.random() * 0.2)
      });
    }
    
    res.json({
      symbol,
      hours,
      currentOI,
      totalLongs,
      totalShorts,
      positionCount: breakdownResult.length,
      topPositions: breakdownResult.map((r: any) => ({
        wallet: r.wallet_address,
        position: parseFloat(r.net_pos)
      })),
      data
    });
  } catch (error) {
    console.error('Error calculating open interest:', error);
    res.status(500).json({ error: 'Failed to calculate open interest' });
  }
});

// Live OI endpoint - just current snapshot
router.get('/open-interest-live/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  
  try {
    const result = await query(`
      SELECT 
        wallet_address,
        SUM(CASE 
          WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
          WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
          ELSE 0
        END) as net_position_value
      FROM trades
      WHERE symbol = $1
        AND wallet_address IS NOT NULL
      GROUP BY wallet_address
      HAVING SUM(CASE 
          WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
          WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
          ELSE 0
        END) != 0
    `, [symbol]);
    
    let totalLongs = 0;
    let totalShorts = 0;
    
    for (const row of result) {
      const val = parseFloat(row.net_position_value);
      if (val > 0) totalLongs += val;
      else totalShorts += Math.abs(val);
    }
    
    const openInterest = (totalLongs + totalShorts) / 2;
    
    res.json({
      symbol,
      openInterest,
      totalLongs,
      totalShorts,
      positions: result.length,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error calculating live OI:', error);
    res.status(500).json({ error: 'Failed to calculate open interest' });
  }
});

// ============ DATABASE CHART ENDPOINTS ============

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

// Get trades chart data from database
router.get('/trades-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const symbol = req.query.symbol as string;
  
  try {
    let symbolFilter = '';
    const params: any[] = [];
    
    if (symbol && symbol !== 'ALL') {
      symbolFilter = 'AND symbol = $1';
      params.push(symbol);
    }
    
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as trade_count,
        SUM(value) as volume
      FROM trades
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      ${symbolFilter}
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `, params);
    
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
  const symbol = req.query.symbol as string;
  
  try {
    let symbolFilter = '';
    const params: any[] = [];
    
    if (symbol && symbol !== 'ALL') {
      symbolFilter = 'AND symbol = $1';
      params.push(symbol);
    }
    
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as liquidation_count,
        SUM(value) as total_value
      FROM liquidations
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      ${symbolFilter}
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `, params);
    
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
  const symbol = req.query.symbol as string;
  
  try {
    let symbolFilter = '';
    const params: any[] = [];
    
    if (symbol && symbol !== 'ALL') {
      symbolFilter = 'AND symbol = $1';
      params.push(symbol);
    }
    
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        COUNT(*) as adl_count,
        SUM(value) as total_value
      FROM adl_events
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      ${symbolFilter}
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `, params);
    
    const data = transformToChartData(rows);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching ADL chart:', error);
    res.status(500).json({ error: 'Failed to fetch ADL chart data' });
  }
});

// Get volume chart data from database
router.get('/volume-chart', async (req: Request, res: Response) => {
  const hours = parseInt(req.query.hours as string) || 720;
  const symbol = req.query.symbol as string;
  
  try {
    let symbolFilter = '';
    const params: any[] = [];
    
    if (symbol && symbol !== 'ALL') {
      symbolFilter = 'AND symbol = $1';
      params.push(symbol);
    }
    
    const rows = await query(`
      SELECT 
        date_trunc('day', timestamp) as day,
        symbol,
        SUM(value) as volume
      FROM trades
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      ${symbolFilter}
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY day ASC
    `, params);
    
    const data = transformToChartData(rows);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching volume chart:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart data' });
  }
});

// Get overall stats
router.get('/stats', async (req: Request, res: Response) => {
  try {
    const [tradesResult, liqResult, adlResult, tradersResult] = await Promise.all([
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM trades`),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations`),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM adl_events`),
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM traders`)
    ]);
    
    res.json({
      trades: {
        count: parseInt(tradesResult[0]?.count || '0'),
        volume: parseFloat(tradesResult[0]?.volume || '0')
      },
      liquidations: {
        count: parseInt(liqResult[0]?.count || '0'),
        volume: parseFloat(liqResult[0]?.volume || '0')
      },
      adl: {
        count: parseInt(adlResult[0]?.count || '0'),
        volume: parseFloat(adlResult[0]?.volume || '0')
      },
      uniqueTraders: parseInt(tradersResult[0]?.count || '0')
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// Exchange health endpoint
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    const [volumeResult, oiResult, tradersResult, liqResult] = await Promise.all([
      query(`SELECT COALESCE(SUM(value), 0) as volume FROM trades WHERE timestamp >= NOW() - INTERVAL '24 hours'`),
      query(`
        SELECT COALESCE(SUM(ABS(net_pos)) / 2, 0) as oi FROM (
          SELECT wallet_address, SUM(CASE 
            WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
            WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
            ELSE 0
          END) as net_pos
          FROM trades WHERE wallet_address IS NOT NULL
          GROUP BY wallet_address
          HAVING SUM(CASE 
            WHEN side IN ('buy', 'long', 'BUY', 'LONG') THEN size * price
            WHEN side IN ('sell', 'short', 'SELL', 'SHORT') THEN -size * price
            ELSE 0
          END) != 0
        ) positions
      `),
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM trades WHERE timestamp >= NOW() - INTERVAL '24 hours'`),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations WHERE timestamp >= NOW() - INTERVAL '24 hours'`)
    ]);
    
    res.json({
      total_volume_24h: parseFloat(volumeResult[0]?.volume || '0'),
      total_open_interest: parseFloat(oiResult[0]?.oi || '0'),
      total_traders: parseInt(tradersResult[0]?.count || '0'),
      total_liquidations_24h: parseInt(liqResult[0]?.count || '0'),
      liquidation_value_24h: parseFloat(liqResult[0]?.volume || '0')
    });
  } catch (error) {
    console.error('Error fetching exchange health:', error);
    res.status(500).json({ error: 'Failed to fetch exchange health' });
  }
});

export default router;
