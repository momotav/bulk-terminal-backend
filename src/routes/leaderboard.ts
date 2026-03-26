import { Router, Request, Response } from 'express';
import { leaderboardService, TimeFrame } from '../services/leaderboard';

const router = Router();

// Validate timeframe parameter
function validateTimeframe(tf: string | undefined): TimeFrame {
  const valid: TimeFrame[] = ['24h', '7d', '30d', 'all'];
  return valid.includes(tf as TimeFrame) ? (tf as TimeFrame) : 'all';
}

// Helper to wrap service calls with timeout
async function withTimeout<T>(promise: Promise<T>, ms: number = 5000): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error('Timeout')), ms))
  ]);
}

// GET /leaderboard/pnl - Top traders by PnL
router.get('/pnl', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getTopTradersByPnL(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard PnL error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No PnL data available yet' });
  }
});

// GET /leaderboard/liquidated - Most liquidated traders
router.get('/liquidated', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getMostLiquidated(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard liquidated error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No liquidation data available yet' });
  }
});

// GET /leaderboard/whales - Biggest current positions
router.get('/whales', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getBiggestPositions(limit));
    res.json({ data });
  } catch (error: any) {
    console.error('Leaderboard whales error:', error.message);
    res.json({ data: [], error: 'No whale data available yet' });
  }
});

// GET /leaderboard/active - Most active traders
router.get('/active', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getMostActive(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard active error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No activity data available yet' });
  }
});

// GET /leaderboard/volume - Top volume traders
router.get('/volume', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await withTimeout(leaderboardService.getTopVolume(timeframe, limit));
    res.json({ timeframe, data });
  } catch (error: any) {
    console.error('Leaderboard volume error:', error.message);
    res.json({ timeframe: 'all', data: [], error: 'No volume data available yet' });
  }
});

// GET /leaderboard/liquidations/recent - Recent liquidation events
router.get('/liquidations/recent', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getRecentLiquidations(limit);
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch liquidations' });
  }
});

// GET /leaderboard/trades/recent - Recent big trades
router.get('/trades/recent', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    const minValue = parseInt(req.query.minValue as string) || 10000;
    
    const data = await leaderboardService.getRecentTrades(limit, minValue);
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch trades' });
  }
});

// GET /leaderboard/rank/:wallet - Get wallet's rank across all leaderboards
router.get('/rank/:wallet', async (req: Request, res: Response) => {
  try {
    const wallet = req.params.wallet;
    
    if (!wallet || wallet.length < 32) {
      return res.status(400).json({ error: 'Invalid wallet address' });
    }
    
    const data = await leaderboardService.getWalletRank(wallet);
    res.json(data);
  } catch (error: any) {
    console.error('Wallet rank error:', error.message);
    res.status(500).json({ error: error.message || 'Failed to fetch wallet rank' });
  }
});

export default router;
