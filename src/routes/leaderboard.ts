import { Router, Request, Response } from 'express';
import { leaderboardService, TimeFrame } from '../services/leaderboard';

const router = Router();

// Validate timeframe parameter
function validateTimeframe(tf: string | undefined): TimeFrame {
  const valid: TimeFrame[] = ['24h', '7d', '30d', 'all'];
  return valid.includes(tf as TimeFrame) ? (tf as TimeFrame) : 'all';
}

// GET /leaderboard/pnl - Top traders by PnL
router.get('/pnl', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getTopTradersByPnL(timeframe, limit);
    res.json({ timeframe, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch leaderboard' });
  }
});

// GET /leaderboard/liquidated - Most liquidated traders
router.get('/liquidated', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getMostLiquidated(timeframe, limit);
    res.json({ timeframe, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch leaderboard' });
  }
});

// GET /leaderboard/whales - Biggest current positions
router.get('/whales', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getBiggestPositions(limit);
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch leaderboard' });
  }
});

// GET /leaderboard/active - Most active traders
router.get('/active', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getMostActive(timeframe, limit);
    res.json({ timeframe, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch leaderboard' });
  }
});

// GET /leaderboard/volume - Top volume traders
router.get('/volume', async (req: Request, res: Response) => {
  try {
    const timeframe = validateTimeframe(req.query.timeframe as string);
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    
    const data = await leaderboardService.getTopVolume(timeframe, limit);
    res.json({ timeframe, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch leaderboard' });
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

export default router;
