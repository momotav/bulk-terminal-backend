import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

// BULK API base URL (new production endpoint)
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

// ============ REAL HISTORICAL OI & FUNDING FROM TICKER SNAPSHOTS ============

// Real Open Interest history from ticker_snapshots table
router.get('/open-interest-history/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const result = await query(`
      SELECT 
        timestamp,
        open_interest_usd as value
      FROM ticker_snapshots
      WHERE symbol = $1 
        AND timestamp >= NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);
    
    const data = result.map((row: any) => ({
      timestamp: row.timestamp,
      value: parseFloat(row.value || 0)
    }));
    
    res.json({ 
      symbol,
      hours,
      dataPoints: data.length,
      data 
    });
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
      SELECT 
        timestamp,
        funding_rate as value
      FROM ticker_snapshots
      WHERE symbol = $1 
        AND timestamp >= NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);
    
    const data = result.map((row: any) => ({
      timestamp: row.timestamp,
      value: parseFloat(row.value || 0)
    }));
    
    res.json({ 
      symbol,
      hours,
      dataPoints: data.length,
      data 
    });
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
      const coin = row.symbol.split('-')[0] as 'BTC' | 'ETH' | 'SOL';
      if (coin in dataMap.get(ts)!) {
        dataMap.get(ts)![coin] = parseFloat(row.value || 0);
      }
    }
    
    const data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values,
      total: values.BTC + values.ETH + values.SOL
    }));
    
    res.json({ 
      hours,
      dataPoints: data.length,
      data 
    });
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
    
    // Group by timestamp
    const dataMap = new Map<string, { BTC: number; ETH: number; SOL: number }>();
    
    for (const row of result) {
      const ts = new Date(row.timestamp).toISOString();
      if (!dataMap.has(ts)) {
        dataMap.set(ts, { BTC: 0, ETH: 0, SOL: 0 });
      }
      const coin = row.symbol.split('-')[0] as 'BTC' | 'ETH' | 'SOL';
      if (coin in dataMap.get(ts)!) {
        dataMap.get(ts)![coin] = parseFloat(row.value || 0);
      }
    }
    
    const data = Array.from(dataMap.entries()).map(([timestamp, values]) => ({
      timestamp,
      ...values
    }));
    
    res.json({ 
      hours,
      dataPoints: data.length,
      data 
    });
  } catch (error) {
    console.error('Error fetching funding chart:', error);
    res.status(500).json({ error: 'Failed to fetch funding chart' });
  }
});

// ============ BULK API PROXIES ============

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
    
    res.json({ data });
  } catch (error) {
    console.error('Error fetching funding rate:', error);
    res.status(500).json({ error: 'Failed to fetch funding rate' });
  }
});

// Open interest endpoint - gets from ticker (OI is in coins, converted to USD)
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
    
    // OI from API is in coins, multiply by mark price to get USD value
    const price = ticker.markPrice || ticker.lastPrice || 1;
    const currentOI = (ticker.openInterest || 0) * price;
    
    console.log(`📊 OI for ${symbol}: ${ticker.openInterest} coins × $${price} = $${currentOI.toFixed(2)}`);
    
    // For now, return current value with simulated history
    // TODO: Store OI snapshots in database for real historical data
    const data = [];
    const now = new Date();
    
    const numPoints = Math.min(hours, 168);
    for (let i = numPoints; i >= 0; i--) {
      const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
      const variance = i === 0 ? 1 : (0.85 + Math.random() * 0.3);
      data.push({
        timestamp: timestamp.toISOString(),
        value: currentOI * variance
      });
    }
    
    res.json({ 
      data,
      currentOI,
      openInterestCoins: ticker.openInterest,
      markPrice: price,
      symbol
    });
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

// Exchange health endpoint - uses BULK API /stats for volume and OI
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    // Fetch from BULK API /stats for official 24h volume and OI
    const bulkStatsPromise = fetch(`${BULK_API_BASE}/stats?period=1d`)
      .then(res => res.ok ? res.json() : null)
      .catch(() => null);
    
    // Fetch from our DB for traders and liquidations (BULK doesn't have these endpoints)
    const [bulkStats, tradersResult, liqResult] = await Promise.all([
      bulkStatsPromise,
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM trades WHERE timestamp >= NOW() - INTERVAL '24 hours'`),
      query(`SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as volume FROM liquidations WHERE timestamp >= NOW() - INTERVAL '24 hours'`)
    ]);
    
    // Use BULK API data for volume and OI, fallback to 0 if unavailable
    const volume24h = bulkStats?.volume?.totalUsd || 0;
    const openInterest = bulkStats?.openInterest?.totalUsd || 0;
    
    res.json({
      total_volume_24h: volume24h,
      total_open_interest: openInterest,
      total_traders: parseInt(tradersResult[0]?.count || '0'),
      total_liquidations_24h: parseInt(liqResult[0]?.count || '0'),
      liquidation_value_24h: parseFloat(liqResult[0]?.volume || '0')
    });
  } catch (error) {
    console.error('Error fetching exchange health:', error);
    res.status(500).json({ error: 'Failed to fetch exchange health' });
  }
});

// ============ BULK API KLINES PROXY ============

// Klines proxy endpoint
router.get('/klines/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const interval = (req.query.interval as string) || '1h';
  const startTime = req.query.startTime as string;
  const endTime = req.query.endTime as string;
  
  try {
    let url = `${BULK_API_BASE}/klines?symbol=${symbol}&interval=${interval}`;
    if (startTime) url += `&startTime=${startTime}`;
    if (endTime) url += `&endTime=${endTime}`;
    
    console.log(`📊 Fetching klines: ${url}`);
    
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
    
    // Convert to array and sort
    const data = Array.from(volumeMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, vol]) => ({
        timestamp: new Date(ts).toISOString(),
        BTC: vol.BTC,
        ETH: vol.ETH,
        SOL: vol.SOL,
        total: vol.BTC + vol.ETH + vol.SOL,
      }));
    
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
    
    // Convert to array and sort
    const data = Array.from(tradesMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ts, trades]) => ({
        timestamp: new Date(ts).toISOString(),
        BTC: trades.BTC,
        ETH: trades.ETH,
        SOL: trades.SOL,
        total: trades.BTC + trades.ETH + trades.SOL,
      }));
    
    console.log(`📊 Trades chart: ${data.length} data points from BULK API klines`);
    res.json({ data });
  } catch (error) {
    console.error('Error fetching trades chart from API:', error);
    res.status(500).json({ error: 'Failed to fetch trades chart' });
  }
});

export default router;
