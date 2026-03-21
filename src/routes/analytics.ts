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
        totalOpenInterest += oi * markPrice; // Convert to USD
        
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

    // Get unique traders count from our database
    const tradersResult = await query(`
      SELECT COUNT(DISTINCT wallet_address) as count 
      FROM tracked_wallets 
      WHERE last_trade_at > NOW() - INTERVAL '24 hours'
    `);
    const activeTraders = parseInt(tradersResult[0]?.count || '0');

    // Get liquidations from our database (last 24h)
    const liqResult = await query(`
      SELECT COALESCE(SUM(value), 0) as total
      FROM trades 
      WHERE type = 'liquidation' 
      AND timestamp > NOW() - INTERVAL '24 hours'
    `);
    const liquidations24h = parseFloat(liqResult[0]?.total || '0');

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
      WHERE type = 'trade'
    `);

    // Get liquidation stats
    const liqStats = await query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(value), 0) as volume
      FROM trades
      WHERE type = 'liquidation'
    `);

    // Get ADL stats
    const adlStats = await query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(value), 0) as volume
      FROM trades
      WHERE type = 'adl'
    `);

    // Get unique traders
    const uniqueTraders = await query(`
      SELECT COUNT(DISTINCT wallet_address) as count
      FROM tracked_wallets
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
        count: parseInt(adlStats[0]?.count || '0'),
        volume: parseFloat(adlStats[0]?.volume || '0'),
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

// GET /api/analytics/leaderboard/:type - Get leaderboard data
router.get('/leaderboard/:type', async (req: Request, res: Response) => {
  try {
    const { type } = req.params;
    const { timeframe = '24h', limit = '50' } = req.query;
    
    let interval = '24 hours';
    if (timeframe === '7d') interval = '7 days';
    else if (timeframe === '30d') interval = '30 days';
    else if (timeframe === 'all') interval = '10 years';

    let queryStr = '';
    
    if (type === 'pnl') {
      queryStr = `
        SELECT 
          wallet_address,
          total_pnl as value,
          total_volume as volume,
          total_trades as trades
        FROM tracked_wallets
        WHERE last_trade_at > NOW() - INTERVAL '${interval}'
        ORDER BY total_pnl DESC
        LIMIT $1
      `;
    } else if (type === 'volume' || type === 'whales') {
      queryStr = `
        SELECT 
          wallet_address,
          total_volume as value,
          total_pnl as pnl,
          total_trades as trades
        FROM tracked_wallets
        ORDER BY total_volume DESC
        LIMIT $1
      `;
    } else if (type === 'liquidated') {
      queryStr = `
        SELECT 
          wallet_address,
          total_liquidations as value,
          total_volume as volume,
          total_trades as trades
        FROM tracked_wallets
        WHERE total_liquidations > 0
        ORDER BY total_liquidations DESC
        LIMIT $1
      `;
    } else {
      return res.status(400).json({ error: 'Invalid leaderboard type' });
    }

    const result = await query(queryStr, [parseInt(limit as string)]);
    
    res.json({
      type,
      timeframe,
      data: result,
    });
  } catch (error) {
    console.error('Error fetching leaderboard:', error);
    res.status(500).json({ error: 'Failed to fetch leaderboard' });
  }
});

// GET /api/analytics/activity - Get recent activity
router.get('/activity', async (req: Request, res: Response) => {
  try {
    const { limit = '50', type } = req.query;
    
    let whereClause = '';
    if (type === 'trades') whereClause = "WHERE type = 'trade'";
    else if (type === 'liquidations') whereClause = "WHERE type = 'liquidation'";
    else if (type === 'adl') whereClause = "WHERE type = 'adl'";

    const result = await query(`
      SELECT 
        id,
        wallet_address,
        symbol,
        side,
        type,
        size,
        price,
        value,
        timestamp
      FROM trades
      ${whereClause}
      ORDER BY timestamp DESC
      LIMIT $1
    `, [parseInt(limit as string)]);

    res.json({ data: result });
  } catch (error) {
    console.error('Error fetching activity:', error);
    res.status(500).json({ error: 'Failed to fetch activity' });
  }
});

export default router;
