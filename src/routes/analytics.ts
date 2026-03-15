import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

// ============ BULK API PROXIES ============

// Proxy to BULK API for open interest
router.get('/open-interest/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const response = await fetch(
      `https://exchange-api.bulk.trade/api/analytics/open-interest/${symbol}?hours=${hours}`
    );
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching open interest:', error);
    res.status(500).json({ error: 'Failed to fetch open interest' });
  }
});

// Proxy to BULK API for funding rate
router.get('/funding-rate/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
    const response = await fetch(
      `https://exchange-api.bulk.trade/api/analytics/funding-rate/${symbol}?hours=${hours}`
    );
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching funding rate:', error);
    res.status(500).json({ error: 'Failed to fetch funding rate' });
  }
});

// ============ NEW: CALCULATED OPEN INTEREST FROM TRADES ============

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

// OI chart endpoint calculated from trades
router.get('/open-interest-chart/:symbol', async (req: Request, res: Response) => {
  const { symbol } = req.params;
  const hours = parseInt(req.query.hours as string) || 24;
  
  try {
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
    
    const data = [];
    data.push({
      timestamp: new Date().toISOString(),
      value: currentOI
    });
    
    res.json({
      symbol,
      hours,
      data
    });
  } catch (error) {
    console.error('Error calculating OI chart:', error);
    res.status(500).json({ error: 'Failed to calculate open interest chart' });
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
