import { Router, Request, Response } from 'express';
import { analyticsService } from '../services/analytics';
import { query } from '../db';

const router = Router();

// GET /analytics/open-interest/:symbol
router.get('/open-interest/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720); // Max 30 days
    
    const data = await analyticsService.getOpenInterestHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/funding-rate/:symbol
router.get('/funding-rate/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getFundingRateHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/volume/:symbol
router.get('/volume/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getVolumeHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/price/:symbol
router.get('/price/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getPriceHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/long-short-ratio/:symbol
router.get('/long-short-ratio/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getLongShortRatioHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/liquidation-heatmap/:symbol
router.get('/liquidation-heatmap/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    const bucketSize = parseInt(req.query.bucketSize as string) || 100;
    
    const data = await analyticsService.getLiquidationHeatmap(symbol, bucketSize, hours);
    res.json({ symbol, hours, bucketSize, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/correlation
router.get('/correlation', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getCorrelationMatrix(hours);
    res.json({ hours, ...data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/exchange-health
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    const data = await analyticsService.getExchangeHealth();
    res.json(data);
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// ============ REAL DATA FROM DATABASE (Testnet Activity) ============

// GET /analytics/trades-chart - Real trades from database grouped by day
router.get('/trades-chart', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 720, 8760);
    const symbol = req.query.symbol as string; // Optional filter
    
    let queryStr = `
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        symbol,
        COUNT(*) as trade_count,
        SUM(value) as volume
      FROM trades
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
    `;
    
    if (symbol && symbol !== 'all') {
      queryStr += ` AND symbol = '${symbol}'`;
    }
    
    queryStr += `
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY timestamp ASC
    `;
    
    const result = await query(queryStr);
    
    // Transform to chart format grouped by date with all symbols
    const dataMap = new Map<string, { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }>();
    
    for (const row of result.rows) {
      const dateKey = new Date(row.timestamp).toISOString();
      if (!dataMap.has(dateKey)) {
        dataMap.set(dateKey, { timestamp: dateKey, BTC: 0, ETH: 0, SOL: 0, total: 0 });
      }
      const entry = dataMap.get(dateKey)!;
      const count = parseInt(row.trade_count);
      entry.total += count;
      
      if (row.symbol?.includes('BTC')) entry.BTC += count;
      else if (row.symbol?.includes('ETH')) entry.ETH += count;
      else if (row.symbol?.includes('SOL')) entry.SOL += count;
    }
    
    const data = Array.from(dataMap.values()).sort((a, b) => 
      new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
    
    res.json({ hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch trades chart' });
  }
});

// GET /analytics/liquidations-chart - Real liquidations from database grouped by day
router.get('/liquidations-chart', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 720, 8760);
    const symbol = req.query.symbol as string;
    
    let queryStr = `
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        symbol,
        COUNT(*) as liq_count,
        SUM(value) as total_value
      FROM liquidations
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
    `;
    
    if (symbol && symbol !== 'all') {
      queryStr += ` AND symbol = '${symbol}'`;
    }
    
    queryStr += `
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY timestamp ASC
    `;
    
    const result = await query(queryStr);
    
    const dataMap = new Map<string, { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }>();
    
    for (const row of result.rows) {
      const dateKey = new Date(row.timestamp).toISOString();
      if (!dataMap.has(dateKey)) {
        dataMap.set(dateKey, { timestamp: dateKey, BTC: 0, ETH: 0, SOL: 0, total: 0 });
      }
      const entry = dataMap.get(dateKey)!;
      const value = parseFloat(row.total_value) || 0;
      entry.total += value;
      
      if (row.symbol?.includes('BTC')) entry.BTC += value;
      else if (row.symbol?.includes('ETH')) entry.ETH += value;
      else if (row.symbol?.includes('SOL')) entry.SOL += value;
    }
    
    const data = Array.from(dataMap.values()).sort((a, b) => 
      new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
    
    res.json({ hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch liquidations chart' });
  }
});

// GET /analytics/adl-chart - Real ADL events from database grouped by day
router.get('/adl-chart', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 720, 8760);
    const symbol = req.query.symbol as string;
    
    let queryStr = `
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        symbol,
        COUNT(*) as adl_count,
        SUM(value) as total_value
      FROM adl_events
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
    `;
    
    if (symbol && symbol !== 'all') {
      queryStr += ` AND symbol = '${symbol}'`;
    }
    
    queryStr += `
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY timestamp ASC
    `;
    
    const result = await query(queryStr);
    
    const dataMap = new Map<string, { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }>();
    
    for (const row of result.rows) {
      const dateKey = new Date(row.timestamp).toISOString();
      if (!dataMap.has(dateKey)) {
        dataMap.set(dateKey, { timestamp: dateKey, BTC: 0, ETH: 0, SOL: 0, total: 0 });
      }
      const entry = dataMap.get(dateKey)!;
      const value = parseFloat(row.total_value) || 0;
      entry.total += value;
      
      if (row.symbol?.includes('BTC')) entry.BTC += value;
      else if (row.symbol?.includes('ETH')) entry.ETH += value;
      else if (row.symbol?.includes('SOL')) entry.SOL += value;
    }
    
    const data = Array.from(dataMap.values()).sort((a, b) => 
      new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
    
    res.json({ hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch ADL chart' });
  }
});

// GET /analytics/volume-chart - Real volume from trades database grouped by day
router.get('/volume-chart', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 720, 8760);
    
    const result = await query(`
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        symbol,
        SUM(value) as volume
      FROM trades
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp), symbol
      ORDER BY timestamp ASC
    `);
    
    const dataMap = new Map<string, { timestamp: string; BTC: number; ETH: number; SOL: number; total: number }>();
    
    for (const row of result.rows) {
      const dateKey = new Date(row.timestamp).toISOString();
      if (!dataMap.has(dateKey)) {
        dataMap.set(dateKey, { timestamp: dateKey, BTC: 0, ETH: 0, SOL: 0, total: 0 });
      }
      const entry = dataMap.get(dateKey)!;
      const value = parseFloat(row.volume) || 0;
      entry.total += value;
      
      if (row.symbol?.includes('BTC')) entry.BTC += value;
      else if (row.symbol?.includes('ETH')) entry.ETH += value;
      else if (row.symbol?.includes('SOL')) entry.SOL += value;
    }
    
    const data = Array.from(dataMap.values()).sort((a, b) => 
      new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
    
    res.json({ hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch volume chart' });
  }
});

// GET /analytics/stats - Overall stats from database
router.get('/stats', async (req: Request, res: Response) => {
  try {
    const [tradesResult, liqResult, adlResult, tradersResult] = await Promise.all([
      query(`SELECT COUNT(*) as count, SUM(value) as volume FROM trades`),
      query(`SELECT COUNT(*) as count, SUM(value) as volume FROM liquidations`),
      query(`SELECT COUNT(*) as count, SUM(value) as volume FROM adl_events`),
      query(`SELECT COUNT(DISTINCT wallet_address) as count FROM traders`),
    ]);
    
    res.json({
      trades: {
        count: parseInt(tradesResult.rows[0]?.count || '0'),
        volume: parseFloat(tradesResult.rows[0]?.volume || '0'),
      },
      liquidations: {
        count: parseInt(liqResult.rows[0]?.count || '0'),
        volume: parseFloat(liqResult.rows[0]?.volume || '0'),
      },
      adl: {
        count: parseInt(adlResult.rows[0]?.count || '0'),
        volume: parseFloat(adlResult.rows[0]?.volume || '0'),
      },
      uniqueTraders: parseInt(tradersResult.rows[0]?.count || '0'),
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch stats' });
  }
});

export default router;
