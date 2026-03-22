import { Router, Request, Response } from 'express';
import { query } from '../db';
import { PrivyClient } from '@privy-io/server-auth';

const router = Router();

// Check if Privy credentials are configured
const PRIVY_APP_ID = process.env.PRIVY_APP_ID || '';
const PRIVY_APP_SECRET = process.env.PRIVY_APP_SECRET || '';

console.log('[Privy] Initializing with App ID:', PRIVY_APP_ID ? `${PRIVY_APP_ID.slice(0, 8)}...` : 'NOT SET');
console.log('[Privy] App Secret:', PRIVY_APP_SECRET ? 'SET' : 'NOT SET');

// Initialize Privy client for server-side verification
const privy = new PrivyClient(PRIVY_APP_ID, PRIVY_APP_SECRET);

// Middleware to verify Privy token
async function verifyPrivyToken(req: Request, res: Response, next: Function) {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.log('[Privy] Missing authorization header');
      return res.status(401).json({ error: 'Missing authorization token' });
    }

    const token = authHeader.substring(7);
    console.log('[Privy] Verifying token:', token.slice(0, 20) + '...');
    
    // Check if Privy is configured
    if (!PRIVY_APP_ID || !PRIVY_APP_SECRET) {
      console.error('[Privy] ERROR: Missing PRIVY_APP_ID or PRIVY_APP_SECRET environment variables');
      return res.status(500).json({ error: 'Privy not configured on server' });
    }
    
    // Verify token with Privy
    const verifiedClaims = await privy.verifyAuthToken(token);
    console.log('[Privy] Token verified successfully for user:', verifiedClaims.userId);
    
    // Attach user info to request
    (req as any).privyUserId = verifiedClaims.userId;
    (req as any).privyUser = verifiedClaims;
    
    next();
  } catch (error: any) {
    console.error('[Privy] Token verification failed:', {
      message: error.message,
      name: error.name,
      code: error.code,
    });
    
    // Provide more specific error messages
    if (error.message?.includes('expired')) {
      return res.status(401).json({ error: 'Token expired, please reconnect' });
    }
    if (error.message?.includes('invalid')) {
      return res.status(401).json({ error: 'Invalid token format' });
    }
    
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// POST /api/users/auth - Authenticate/create user after Privy login
router.post('/auth', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress } = req.body;

    if (!walletAddress) {
      return res.status(400).json({ error: 'Wallet address required' });
    }

    console.log('[Auth] Creating/updating user:', { privyUserId, walletAddress });

    // Upsert user
    const users = await query(`
      INSERT INTO users (wallet_address, privy_id, email, password_hash, last_login_at)
      VALUES ($1, $2, $1, 'privy_auth', NOW())
      ON CONFLICT (wallet_address) 
      DO UPDATE SET 
        privy_id = COALESCE(users.privy_id, $2),
        last_login_at = NOW()
      RETURNING id, wallet_address, privy_id, twitter_handle, twitter_name, twitter_avatar,
                telegram_handle, display_name, avatar_url, created_at
    `, [walletAddress, privyUserId]);

    const user = users[0];
    console.log('[Auth] User upserted:', user.id);

    // Get following count
    const followsResult = await query(
      'SELECT COUNT(*) as count FROM wallet_follows WHERE user_id = $1',
      [user.id]
    );

    // Get trading stats
    const statsResult = await query(
      'SELECT total_trades as trade_count, total_volume, total_pnl, win_rate FROM traders WHERE wallet_address = $1',
      [walletAddress]
    );

    res.json({
      user: {
        ...user,
        following_count: parseInt(followsResult[0]?.count || '0'),
        stats: statsResult[0] || null
      }
    });
  } catch (error) {
    console.error('Auth error:', error);
    res.status(500).json({ error: 'Authentication failed' });
  }
});

// GET /api/users/me - Get current user profile
router.get('/me', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    const users = await query(`
      SELECT u.id, u.wallet_address, u.privy_id, u.twitter_handle, u.twitter_name, 
             u.twitter_avatar, u.telegram_handle, u.display_name, u.avatar_url, u.created_at,
             COUNT(wf.id) as following_count
      FROM users u
      LEFT JOIN wallet_follows wf ON u.id = wf.user_id
      WHERE u.privy_id = $1
      GROUP BY u.id
    `, [privyUserId]);

    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Get trading stats
    const statsResult = await query(
      'SELECT total_trades as trade_count, total_volume, total_pnl, win_rate FROM traders WHERE wallet_address = $1',
      [users[0].wallet_address]
    );

    res.json({ 
      user: {
        ...users[0],
        stats: statsResult[0] || null
      }
    });
  } catch (error) {
    console.error('Get user error:', error);
    res.status(500).json({ error: 'Failed to get user' });
  }
});

// POST /api/users/link/twitter - Link Twitter account
router.post('/link/twitter', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { twitterId, twitterHandle, twitterName, twitterAvatar } = req.body;

    if (!twitterId || !twitterHandle) {
      return res.status(400).json({ error: 'Twitter info required' });
    }

    const users = await query(`
      UPDATE users 
      SET twitter_id = $1, 
          twitter_handle = $2, 
          twitter_name = $3,
          twitter_avatar = $4,
          updated_at = NOW()
      WHERE privy_id = $5
      RETURNING *
    `, [twitterId, twitterHandle, twitterName, twitterAvatar, privyUserId]);

    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json({ user: users[0], message: 'Twitter linked successfully' });
  } catch (error) {
    console.error('Link Twitter error:', error);
    res.status(500).json({ error: 'Failed to link Twitter' });
  }
});

// DELETE /api/users/link/twitter - Unlink Twitter account
router.delete('/link/twitter', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    const users = await query(`
      UPDATE users 
      SET twitter_id = NULL, 
          twitter_handle = NULL, 
          twitter_name = NULL,
          twitter_avatar = NULL,
          updated_at = NOW()
      WHERE privy_id = $1
      RETURNING *
    `, [privyUserId]);

    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json({ user: users[0], message: 'Twitter unlinked' });
  } catch (error) {
    console.error('Unlink Twitter error:', error);
    res.status(500).json({ error: 'Failed to unlink Twitter' });
  }
});

// GET /api/users/following - Get wallets the user follows
router.get('/following', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    const userId = userResult[0].id;

    const following = await query(`
      SELECT wf.followed_wallet as wallet_address,
             wf.nickname,
             wf.created_at as followed_at,
             t.total_trades as trade_count,
             t.total_volume,
             t.total_pnl,
             t.win_rate,
             t.last_seen as last_trade_time
      FROM wallet_follows wf
      LEFT JOIN traders t ON wf.followed_wallet = t.wallet_address
      WHERE wf.user_id = $1
      ORDER BY wf.created_at DESC
    `, [userId]);

    res.json({ following });
  } catch (error) {
    console.error('Get following error:', error);
    res.status(500).json({ error: 'Failed to get following' });
  }
});

// POST /api/users/follow - Follow a wallet
router.post('/follow', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress, nickname } = req.body;

    console.log('[Follow] Request:', { privyUserId, walletAddress, nickname });

    if (!walletAddress) {
      return res.status(400).json({ error: 'Wallet address required' });
    }

    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      console.log('[Follow] User not found for privy_id:', privyUserId);
      return res.status(404).json({ error: 'User not found' });
    }
    const userId = userResult[0].id;

    const result = await query(`
      INSERT INTO wallet_follows (user_id, followed_wallet, nickname)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_id, followed_wallet) 
      DO UPDATE SET nickname = COALESCE($3, wallet_follows.nickname)
      RETURNING *
    `, [userId, walletAddress, nickname || null]);

    console.log('[Follow] Success:', result[0]);
    res.json({ follow: result[0], message: 'Wallet followed' });
  } catch (error) {
    console.error('Follow error:', error);
    res.status(500).json({ error: 'Failed to follow wallet' });
  }
});

// DELETE /api/users/follow/:walletAddress - Unfollow a wallet
router.delete('/follow/:walletAddress', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress } = req.params;

    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    const userId = userResult[0].id;

    await query(
      'DELETE FROM wallet_follows WHERE user_id = $1 AND followed_wallet = $2',
      [userId, walletAddress]
    );

    res.json({ message: 'Wallet unfollowed' });
  } catch (error) {
    console.error('Unfollow error:', error);
    res.status(500).json({ error: 'Failed to unfollow wallet' });
  }
});

// GET /api/users/is-following/:walletAddress - Check if following
router.get('/is-following/:walletAddress', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress } = req.params;

    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.json({ isFollowing: false });
    }
    const userId = userResult[0].id;

    const result = await query(
      'SELECT id FROM wallet_follows WHERE user_id = $1 AND followed_wallet = $2',
      [userId, walletAddress]
    );

    res.json({ isFollowing: result.length > 0 });
  } catch (error) {
    console.error('Is following error:', error);
    res.status(500).json({ error: 'Failed to check follow status' });
  }
});

// GET /api/users/wallet/:address - Get public wallet profile (NO AUTH REQUIRED)
router.get('/wallet/:address', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;

    const users = await query(`
      SELECT u.wallet_address, u.twitter_handle, u.twitter_name, u.twitter_avatar,
             u.display_name, u.avatar_url, u.created_at
      FROM users u
      WHERE u.wallet_address = $1
    `, [address]);

    const statsResult = await query(
      'SELECT total_trades as trade_count, total_volume, total_pnl, win_rate FROM traders WHERE wallet_address = $1',
      [address]
    );

    if (users.length === 0) {
      return res.json({ 
        profile: null, 
        stats: statsResult[0] || null 
      });
    }

    res.json({ 
      profile: users[0],
      stats: statsResult[0] || null
    });
  } catch (error) {
    console.error('Get wallet profile error:', error);
    res.status(500).json({ error: 'Failed to get wallet profile' });
  }
});

export default router;
