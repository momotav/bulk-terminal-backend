import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { bulkApi } from '../services/bulkApi';
import { addWalletToTrack } from '../jobs/dataCollector';
import { requireAuth } from '../middleware/auth';

const router = Router();

// GET /wallet/:address - Get wallet info (live from BULK API + our data)
router.get('/:address', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    // Get live account data from BULK
    const account = await bulkApi.getFullAccount(address);
    
    console.log(`🔍 Wallet API for ${address.slice(0,8)}:`, JSON.stringify(account));
    
    // Get our tracked data
    const trader = await queryOne(
      'SELECT * FROM traders WHERE wallet_address = $1',
      [address]
    );
    
    // Get recent snapshots for history
    const snapshots = await query(
      `SELECT timestamp, pnl, unrealized_pnl, positions_count, total_notional
       FROM trader_snapshots
       WHERE wallet_address = $1
       ORDER BY timestamp DESC
       LIMIT 168`, // Last 7 days hourly
      [address]
    );
    
    res.json({
      address,
      live: account,
      tracked: trader,
      history: snapshots.reverse(),
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch wallet data' });
  }
});

// POST /wallet/:address/track - Start tracking a wallet
router.post('/:address/track', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    const success = await addWalletToTrack(address);
    
    if (success) {
      res.json({ success: true, message: 'Wallet is now being tracked' });
    } else {
      res.status(400).json({ error: 'Wallet has no activity or is invalid' });
    }
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to track wallet' });
  }
});

// GET /wallet/:address/trades - Get trade history
router.get('/:address/trades', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    
    const trades = await query(
      `SELECT * FROM trades 
       WHERE wallet_address = $1 
       ORDER BY timestamp DESC 
       LIMIT $2`,
      [address, limit]
    );
    
    res.json({ data: trades });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch trades' });
  }
});

// GET /wallet/:address/liquidations - Get liquidation history
router.get('/:address/liquidations', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    
    const liquidations = await query(
      `SELECT * FROM liquidations 
       WHERE wallet_address = $1 
       ORDER BY timestamp DESC 
       LIMIT $2`,
      [address, limit]
    );
    
    res.json({ data: liquidations });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch liquidations' });
  }
});

// ============ WATCHLIST (requires auth) ============

// GET /wallet/watchlist - Get user's watchlist
router.get('/user/watchlist', requireAuth, async (req: Request, res: Response) => {
  try {
    const watchlist = await query(
      `SELECT w.wallet_address, w.nickname, w.created_at, t.total_pnl, t.total_volume
       FROM watchlist w
       LEFT JOIN traders t ON w.wallet_address = t.wallet_address
       WHERE w.user_id = $1
       ORDER BY w.created_at DESC`,
      [req.userId]
    );
    
    res.json({ data: watchlist });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch watchlist' });
  }
});

// POST /wallet/watchlist/:address - Add to watchlist
router.post('/watchlist/:address', requireAuth, async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const { nickname } = req.body;
    
    // Start tracking if not already
    await addWalletToTrack(address);
    
    await query(
      `INSERT INTO watchlist (user_id, wallet_address, nickname)
       VALUES ($1, $2, $3)
       ON CONFLICT (user_id, wallet_address) DO UPDATE SET nickname = $3`,
      [req.userId, address, nickname || null]
    );
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to add to watchlist' });
  }
});

// DELETE /wallet/watchlist/:address - Remove from watchlist
router.delete('/watchlist/:address', requireAuth, async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    await query(
      'DELETE FROM watchlist WHERE user_id = $1 AND wallet_address = $2',
      [req.userId, address]
    );
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to remove from watchlist' });
  }
});

export default router;
