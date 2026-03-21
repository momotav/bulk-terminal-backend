import { Router, Request, Response } from 'express';
import { query } from '../db';

const router = Router();

const BULK_API = 'https://exchange-api.bulk.trade/api/v1';

// Type definitions for BULK API response
interface BulkMarket {
  symbol: string;
  volume: number;
  quoteVolume: number | null;
  openInterest: number;
  fundingRate: number;
  fundingRateAnnualized: number;
  lastPrice: number | null;
  markPrice: number | null;
}

interface BulkStatsResponse {
  timestamp: number;
  period: string;
  volume: { totalUsd: number | null };
  openInterest: { totalUsd: number };
  funding: {
    rates: Record<string, { current: number; annualized: number }>;
  };
  markets: BulkMarket[];
}

// Helper to fetch from BULK API
async function fetchBulkAPI(endpoint: string): Promise<any> {
  try {
    const response = await fetch(`${BULK_API}${endpoint}`);
    if (!response.ok) {
      throw new Error(`BULK API error: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error(`Failed to fetch ${endpoint}:`, error);
    return null;
  }
}

// GET /api/analytics/exchange-stats - Get live exchange stats from BULK API
router.get('/exchange-stats', async (req: Request, res: Response) => {
  try {
    // Fetch from new BULK /stats endpoint
    const stats: BulkStatsResponse | null = await fetchBulkAPI('/stats?period=1d');
    
    if (!stats) {
      return res.status(503).json({ error: 'BULK API unavailable' });
    }

    // Calculate total 24h volume from all markets
    let totalVolume24h = 0;
    let totalOpenInterest = 0;
    const marketStats: any[] = [];

    if (stats.markets && Array.isArray(stats.markets)) {
      for (const market of stats.markets) {
        const quoteVolume = market.quoteVolume || 0;
        const oi = market.openInterest || 0;
        const markPrice = market.markPrice || 0;
        
        totalVolume24h += quoteVolume;
        totalOpenInterest += oi * markPrice;
        
        marketStats.push({
          symbol: market.symbol,
          volume24h: quoteVolume,
          openInterest: oi * markPrice,
          fundingRate: market.fundingRate || 0,
          fundingRateAnnualized: market.fundingRateAnnualized || 0,
          lastPrice: market.lastPrice,
          markPrice: market.markPrice,
        });
      }
    }

    // Use OI from stats response if available
    if (stats.openInterest?.totalUsd) {
      totalOpenInterest = stats.openInterest.totalUsd;
    }

    // Get unique traders count from traders table (last 24h)
    let activeTraders = 0;
    try {
      const tradersResult = await query(`
        SELECT COUNT(DISTINCT wallet_address) as count 
        FROM traders 
        WHERE last_seen > NOW() - INTERVAL '24 hours'
      `);
      activeTraders = parseInt(tradersResult[0]?.count || '0');
    } catch (e) {
      // Fallback: count from trades table
      try {
        const tradersResult = await query(`
          SELECT COUNT(DISTINCT wallet_address) as count 
          FROM trades 
          WHERE timestamp > NOW() - INTERVAL '24 hours'
        `);
        activeTraders = parseInt(tradersResult[0]?.count || '0');
      } catch (e2) {
        console.error('Failed to get active traders:', e2);
      }
    }

    // Get liquidations from liquidations table (last 24h)
    let liquidations24h = 0;
    try {
      const liqResult = await query(`
        SELECT COALESCE(SUM(value), 0) as total
        FROM liquidations 
        WHERE timestamp > NOW() - INTERVAL '24 hours'
      `);
      liquidations24h = parseFloat(liqResult[0]?.total || '0');
    } catch (e) {
      console.error('Failed to get liquidations:', e);
    }

    res.json({
      timestamp: stats.timestamp || Date.now(),
      volume24h: totalVolume24h,
      openInterest: totalOpenInterest,
      activeTraders: activeTraders,
      liquidations24h: liquidations24h,
      fundingRates: stats.funding?.rates || {},
      markets: marketStats,
    });
  } catch (error) {
    console.error('Error fetching exchange stats:', error);
    res.status(500).json({ error: 'Failed to fetch exchange stats' });
  }
});

// GET /api/analytics/stats - Get stats from our database
router.get('/stats', async (req: Request, res: Response) => {
  try {
    // Get trade stats
    const tradeStats = await query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(value), 0) as volume
      FROM trades
    `);

    // Get liquidation stats
    const liqStats = await query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(value), 0) as volume
      FROM liquidations
    `);

    // Get unique traders from traders table
    const uniqueTraders = await query(`
      SELECT COUNT(*) as count
      FROM traders
    `);

    res.json({
      trades: {
        count: parseInt(tradeStats[0]?.count || '0'),
        volume: parseFloat(tradeStats[0]?.volume || '0'),
      },
      liquidations: {
        count: parseInt(liqStats[0]?.count || '0'),
        volume: parseFloat(liqStats[0]?.volume || '0'),
      },
      adl: {
        count: 0,
        volume: 0,
      },
      uniqueTraders: parseInt(uniqueTraders[0]?.count || '0'),
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// GET /api/analytics/markets - Get market data
router.get('/markets', async (req: Request, res: Response) => {
  try {
    const stats: BulkStatsResponse | null = await fetchBulkAPI('/stats?period=1d');
    
    if (!stats || !stats.markets) {
      return res.status(503).json({ error: 'BULK API unavailable' });
    }

    const markets = stats.markets.map((m: BulkMarket) => ({
      symbol: m.symbol,
      lastPrice: m.lastPrice,
      markPrice: m.markPrice,
      volume24h: m.quoteVolume || 0,
      openInterest: (m.openInterest || 0) * (m.markPrice || 0),
      fundingRate: m.fundingRate || 0,
      fundingRateAnnualized: m.fundingRateAnnualized || 0,
    }));

    res.json({ markets });
  } catch (error) {
    console.error('Error fetching markets:', error);
    res.status(500).json({ error: 'Failed to fetch markets' });
  }
});

// GET /api/analytics/oi-chart - Open Interest chart data
router.get('/oi-chart', async (req: Request, res: Response) => {
  try {
    const hours = parseInt(req.query.hours as string) || 24;
    
    const result = await query(`
      SELECT 
        date_trunc('hour', timestamp) as timestamp,
        SUM(CASE WHEN symbol = 'BTC-USD' THEN open_interest_usd ELSE 0 END) as "BTC",
        SUM(CASE WHEN symbol = 'ETH-USD' THEN open_interest_usd ELSE 0 END) as "ETH",
        SUM(CASE WHEN symbol = 'SOL-USD' THEN open_interest_usd ELSE 0 END) as "SOL",
        SUM(open_interest_usd) as total
      FROM ticker_snapshots
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('hour', timestamp)
      ORDER BY timestamp ASC
    `);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching OI chart:', error);
    res.status(500).json({ error: 'Failed to fetch OI chart' });
  }
});

// GET /api/analytics/funding-chart - Funding rate chart data
router.get('/funding-chart', async (req: Request, res: Response) => {
  try {
    const hours = parseInt(req.query.hours as string) || 24;
    
    const result = await query(`
      SELECT 
        date_trunc('hour', timestamp) as timestamp,
        AVG(CASE WHEN symbol = 'BTC-USD' THEN funding_rate ELSE NULL END) as "BTC",
        AVG(CASE WHEN symbol = 'ETH-USD' THEN funding_rate ELSE NULL END) as "ETH",
        AVG(CASE WHEN symbol = 'SOL-USD' THEN funding_rate ELSE NULL END) as "SOL"
      FROM ticker_snapshots
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('hour', timestamp)
      ORDER BY timestamp ASC
    `);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching funding chart:', error);
    res.status(500).json({ error: 'Failed to fetch funding chart' });
  }
});

// GET /api/analytics/volume-chart - Volume chart data from trades
router.get('/volume-chart', async (req: Request, res: Response) => {
  try {
    const hours = parseInt(req.query.hours as string) || 720;
    
    const result = await query(`
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        SUM(CASE WHEN symbol = 'BTC-USD' THEN value ELSE 0 END) as "BTC",
        SUM(CASE WHEN symbol = 'ETH-USD' THEN value ELSE 0 END) as "ETH",
        SUM(CASE WHEN symbol = 'SOL-USD' THEN value ELSE 0 END) as "SOL",
        SUM(value) as total
      FROM trades
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp)
      ORDER BY timestamp ASC
    `);

    // Add cumulative
    let cumulative = 0;
    const dataWithCumulative = result.map((row: any) => {
      cumulative += parseFloat(row.total || 0);
      return {
        ...row,
        BTC: parseFloat(row.BTC || 0),
        ETH: parseFloat(row.ETH || 0),
        SOL: parseFloat(row.SOL || 0),
        total: parseFloat(row.total || 0),
        Cumulative: cumulative,
      };
    });

    res.json({ data: dataWithCumulative });
  } catch (error) {
    console.error('Error fetching volume chart:', error);
    res.status(500).json({ error: 'Failed to fetch volume chart' });
  }
});

// GET /api/analytics/liquidations-chart - Liquidations chart data
router.get('/liquidations-chart', async (req: Request, res: Response) => {
  try {
    const hours = parseInt(req.query.hours as string) || 720;
    
    const result = await query(`
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        SUM(CASE WHEN symbol = 'BTC-USD' THEN value ELSE 0 END) as "BTC",
        SUM(CASE WHEN symbol = 'ETH-USD' THEN value ELSE 0 END) as "ETH",
        SUM(CASE WHEN symbol = 'SOL-USD' THEN value ELSE 0 END) as "SOL",
        SUM(value) as total
      FROM liquidations
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp)
      ORDER BY timestamp ASC
    `);

    // Add cumulative
    let cumulative = 0;
    const dataWithCumulative = result.map((row: any) => {
      cumulative += parseFloat(row.total || 0);
      return {
        ...row,
        BTC: parseFloat(row.BTC || 0),
        ETH: parseFloat(row.ETH || 0),
        SOL: parseFloat(row.SOL || 0),
        total: parseFloat(row.total || 0),
        Cumulative: cumulative,
      };
    });

    res.json({ data: dataWithCumulative });
  } catch (error) {
    console.error('Error fetching liquidations chart:', error);
    res.status(500).json({ error: 'Failed to fetch liquidations chart' });
  }
});

// GET /api/analytics/trades-chart - Trades count chart
router.get('/trades-chart', async (req: Request, res: Response) => {
  try {
    const hours = parseInt(req.query.hours as string) || 720;
    
    const result = await query(`
      SELECT 
        date_trunc('day', timestamp) as timestamp,
        COUNT(CASE WHEN symbol = 'BTC-USD' THEN 1 END) as "BTC",
        COUNT(CASE WHEN symbol = 'ETH-USD' THEN 1 END) as "ETH",
        COUNT(CASE WHEN symbol = 'SOL-USD' THEN 1 END) as "SOL",
        COUNT(*) as total
      FROM trades
      WHERE timestamp > NOW() - INTERVAL '${hours} hours'
      GROUP BY date_trunc('day', timestamp)
      ORDER BY timestamp ASC
    `);

    // Add cumulative
    let cumulative = 0;
    const dataWithCumulative = result.map((row: any) => {
      cumulative += parseInt(row.total || 0);
      return {
        ...row,
        BTC: parseInt(row.BTC || 0),
        ETH: parseInt(row.ETH || 0),
        SOL: parseInt(row.SOL || 0),
        total: parseInt(row.total || 0),
        Cumulative: cumulative,
      };
    });

    res.json({ data: dataWithCumulative });
  } catch (error) {
    console.error('Error fetching trades chart:', error);
    res.status(500).json({ error: 'Failed to fetch trades chart' });
  }
});

// GET /api/analytics/adl-chart - ADL chart (placeholder - BULK tracks this via trades with reason='adl')
router.get('/adl-chart', async (req: Request, res: Response) => {
  try {
    // ADL events would come through trades with reason='adl' from the WebSocket
    // For now, return empty data since we don't have a separate ADL table
    res.json({ data: [] });
  } catch (error) {
    console.error('Error fetching ADL chart:', error);
    res.status(500).json({ error: 'Failed to fetch ADL chart' });
  }
});

// GET /api/analytics/activity - Get recent activity
router.get('/activity', async (req: Request, res: Response) => {
  try {
    const { limit = '50', type } = req.query;
    const limitNum = Math.min(parseInt(limit as string) || 50, 100);

    if (type === 'liquidations') {
      const result = await query(`
        SELECT 
          id,
          wallet_address,
          symbol,
          side,
          'liquidation' as type,
          size,
          price,
          value,
          timestamp
        FROM liquidations
        ORDER BY timestamp DESC
        LIMIT $1
      `, [limitNum]);
      return res.json({ data: result });
    }

    // Default: return trades
    const result = await query(`
      SELECT 
        id,
        wallet_address,
        symbol,
        side,
        'trade' as type,
        size,
        price,
        value,
        timestamp
      FROM trades
      ORDER BY timestamp DESC
      LIMIT $1
    `, [limitNum]);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching activity:', error);
    res.status(500).json({ error: 'Failed to fetch activity' });
  }
});

// GET /api/analytics/open-interest-history/:symbol
router.get('/open-interest-history/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = parseInt(req.query.hours as string) || 24;
    
    const result = await query(`
      SELECT timestamp, open_interest_usd as value
      FROM ticker_snapshots
      WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching OI history:', error);
    res.status(500).json({ error: 'Failed to fetch OI history' });
  }
});

// GET /api/analytics/funding-rate-history/:symbol
router.get('/funding-rate-history/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = parseInt(req.query.hours as string) || 24;
    
    const result = await query(`
      SELECT timestamp, funding_rate as value
      FROM ticker_snapshots
      WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
      ORDER BY timestamp ASC
    `, [symbol]);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching funding rate history:', error);
    res.status(500).json({ error: 'Failed to fetch funding rate history' });
  }
});

export default router;
