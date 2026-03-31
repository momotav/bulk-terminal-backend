import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { bulkApi } from '../services/bulkApi';
import { requireAuth } from '../middleware/auth';
import { getCache, setCache } from '../services/cache';

const router = Router();

// Helper: Fetch wallet data from BULK API and store snapshot
async function fetchAndStoreWalletSnapshot(walletAddress: string): Promise<void> {
  try {
    const account = await bulkApi.getFullAccount(walletAddress);
    if (!account) return;
    
    // Calculate totals from positions
    let totalNotional = 0;
    let totalRealizedPnl = 0;
    let totalUnrealizedPnl = 0;
    
    for (const p of account.positions) {
      totalNotional += Math.abs(p.notional || 0);
      totalRealizedPnl += p.realizedPnl || 0;
      totalUnrealizedPnl += p.unrealizedPnl || 0;
    }
    
    // Use margin totals if available
    const marginRealizedPnl = account.margin?.realizedPnl || 0;
    const marginUnrealizedPnl = account.margin?.unrealizedPnl || 0;
    
    const realizedPnl = marginRealizedPnl !== 0 ? marginRealizedPnl : totalRealizedPnl;
    const unrealizedPnl = marginUnrealizedPnl !== 0 ? marginUnrealizedPnl : totalUnrealizedPnl;
    const totalPnl = realizedPnl + unrealizedPnl;
    
    // Update trader with current PnL
    await query(
      `INSERT INTO traders (wallet_address, total_pnl, last_seen)
       VALUES ($1, $2, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET
         total_pnl = $2,
         last_seen = NOW()`,
      [walletAddress, totalPnl]
    );
    
    // Store snapshot for history (only if they have positions OR PnL != 0)
    if (account.positions.length > 0 || totalPnl !== 0) {
      await query(
        `INSERT INTO trader_snapshots 
         (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
         VALUES ($1, $2, $3, $4, $5)`,
        [walletAddress, realizedPnl, unrealizedPnl, account.positions.length, totalNotional]
      );
    }
  } catch (error) {
    console.error(`Failed to store wallet snapshot for ${walletAddress.slice(0, 8)}:`, error);
  }
}

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
    
    // Store snapshot BEFORE fetching history (so new snapshot appears in history)
    // But don't block on it too long - use a short timeout
    try {
      await Promise.race([
        fetchAndStoreWalletSnapshot(address),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
      ]);
    } catch {
      // Timeout or error, continue anyway
    }
    
    // Get our tracked data (now includes updated PnL)
    const trader = await queryOne(
      'SELECT * FROM traders WHERE wallet_address = $1',
      [address]
    );
    
    // Get recent snapshots for history (now includes new snapshot)
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
    
    // Actually fetch and store data
    await fetchAndStoreWalletSnapshot(address);
    
    res.json({ success: true, message: 'Wallet is now being tracked' });
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
    await fetchAndStoreWalletSnapshot(address);
    
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
