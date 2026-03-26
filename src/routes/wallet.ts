import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { bulkApi } from '../services/bulkApi';
import { addWalletToTrack } from '../jobs/dataCollector';
import { requireAuth } from '../middleware/auth';
import { getCache, setCache } from '../services/cache';

const router = Router();

// GET /wallet/:address - Get wallet info (live from BULK API + our data)
router.get('/:address', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    // Check cache first (30 second TTL)
    const cacheKey = `wallet:profile:${address}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) {
      return res.json(cached);
    }
    
    // Get live account data from BULK
    const account = await bulkApi.getFullAccount(address);
    
    // Get current mark prices for all symbols
    const tickers = await bulkApi.getAllTickers();
    const markPrices: Record<string, number> = {};
    for (const ticker of tickers) {
      markPrices[ticker.symbol] = ticker.markPrice;
    }
    
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
    
    const result = {
      address,
      live: account,
      markPrices,
      tracked: trader,
      history: snapshots.reverse(),
    };
    
    // Cache for 30 seconds
    await setCache(cacheKey, result, 30);
    
    res.json(result);
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

// ============ NOTIFICATIONS ============

// GET /wallet/notifications - Get user's notifications
router.get('/user/notifications', requireAuth, async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const unreadOnly = req.query.unread === 'true';
    
    let sql = `
      SELECT n.*, w.nickname
      FROM notifications n
      LEFT JOIN watchlist w ON n.wallet_address = w.wallet_address AND w.user_id = n.user_id
      WHERE n.user_id = $1
    `;
    
    if (unreadOnly) {
      sql += ' AND n.read = false';
    }
    
    sql += ' ORDER BY n.created_at DESC LIMIT $2';
    
    const notifications = await query(sql, [req.userId, limit]);
    
    // Get unread count
    const [countResult] = await query<{ count: string }>(
      'SELECT COUNT(*) as count FROM notifications WHERE user_id = $1 AND read = false',
      [req.userId]
    );
    
    res.json({ 
      data: notifications,
      unread_count: parseInt(countResult?.count || '0')
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch notifications' });
  }
});

// POST /wallet/notifications/read - Mark notifications as read
router.post('/user/notifications/read', requireAuth, async (req: Request, res: Response) => {
  try {
    const { ids } = req.body; // Array of notification IDs, or empty for all
    
    if (ids && Array.isArray(ids) && ids.length > 0) {
      await query(
        'UPDATE notifications SET read = true WHERE user_id = $1 AND id = ANY($2)',
        [req.userId, ids]
      );
    } else {
      // Mark all as read
      await query(
        'UPDATE notifications SET read = true WHERE user_id = $1 AND read = false',
        [req.userId]
      );
    }
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to mark notifications as read' });
  }
});

// DELETE /wallet/notifications - Clear all notifications
router.delete('/user/notifications', requireAuth, async (req: Request, res: Response) => {
  try {
    await query(
      'DELETE FROM notifications WHERE user_id = $1',
      [req.userId]
    );
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to clear notifications' });
  }
});

export default router;
