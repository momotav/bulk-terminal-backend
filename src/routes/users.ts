import { Router, Request, Response } from 'express';
import { query } from '../db';
import { PrivyClient } from '@privy-io/server-auth';

const router = Router();

// Check if Privy credentials are configured
const PRIVY_APP_ID = process.env.PRIVY_APP_ID || '';
const PRIVY_APP_SECRET = process.env.PRIVY_APP_SECRET || '';
// Optional: Get verification key from dashboard to avoid API calls
// Go to Privy Dashboard > Configuration > App Settings > Verification Key
const PRIVY_VERIFICATION_KEY = process.env.PRIVY_VERIFICATION_KEY || '';

console.log('[Privy] =========================================');
console.log('[Privy] Initializing Privy Server Auth');
console.log('[Privy] App ID:', PRIVY_APP_ID ? `${PRIVY_APP_ID.slice(0, 12)}...` : 'NOT SET');
console.log('[Privy] App Secret:', PRIVY_APP_SECRET ? `SET (${PRIVY_APP_SECRET.length} chars)` : 'NOT SET');
console.log('[Privy] Verification Key:', PRIVY_VERIFICATION_KEY ? 'SET' : 'NOT SET (will fetch from API)');
console.log('[Privy] =========================================');

// Initialize Privy client for server-side verification
const privy = new PrivyClient(PRIVY_APP_ID, PRIVY_APP_SECRET);

// Middleware to verify Privy token
async function verifyPrivyToken(req: Request, res: Response, next: Function) {
  const startTime = Date.now();
  
  try {
    const authHeader = req.headers.authorization;
    console.log('[Privy] Auth header present:', !!authHeader);
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.log('[Privy] Missing or malformed authorization header');
      return res.status(401).json({ error: 'Missing authorization token' });
    }

    const token = authHeader.substring(7); // Remove 'Bearer ' prefix
    console.log('[Privy] Token preview:', token.slice(0, 30) + '...' + token.slice(-10));
    console.log('[Privy] Token length:', token.length);
    
    // Basic JWT structure check
    const parts = token.split('.');
    console.log('[Privy] Token parts:', parts.length, '(expected: 3 for JWT)');
    
    if (parts.length !== 3) {
      console.error('[Privy] Invalid JWT structure - not a valid token');
      return res.status(401).json({ error: 'Invalid token format' });
    }
    
    // Try to decode header to see algorithm
    try {
      const headerJson = Buffer.from(parts[0], 'base64').toString();
      console.log('[Privy] Token header:', headerJson);
    } catch (e) {
      console.log('[Privy] Could not decode token header');
    }

    console.log('[Privy] Calling verifyAuthToken...');
    
    // Verify the token with Privy
    const verifiedClaims = await privy.verifyAuthToken(token);
    
    const duration = Date.now() - startTime;
    console.log(`[Privy] ✓ Token verified successfully in ${duration} ms`);
    console.log('[Privy] User ID:', verifiedClaims.userId);
    
    // Add the verified user ID to the request
    (req as any).privyUserId = verifiedClaims.userId;
    next();
  } catch (error: any) {
    const duration = Date.now() - startTime;
    console.error(`[Privy] ✗ Token verification FAILED in ${duration} ms`);
    console.error('[Privy] Error type:', error?.constructor?.name || typeof error);
    console.error('[Privy] Error message:', error?.message);
    console.error('[Privy] Error code:', error?.code);
    console.error('[Privy] Error status:', error?.status);
    console.error('[Privy] Full error:', JSON.stringify(error, Object.getOwnPropertyNames(error), 2));
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// POST /api/users/auth - Authenticate after Privy login
router.post('/auth', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress } = req.body;

    console.log('[Users] Auth request:', { privyUserId, walletAddress });

    if (!walletAddress) {
      return res.status(400).json({ error: 'Wallet address required' });
    }

    // Check if user exists
    const existingUser = await query(
      'SELECT * FROM users WHERE privy_id = $1',
      [privyUserId]
    );

    let user;
    if (existingUser.length > 0) {
      // Update wallet address if changed
      if (existingUser[0].wallet_address !== walletAddress) {
        await query(
          'UPDATE users SET wallet_address = $1 WHERE privy_id = $2',
          [walletAddress, privyUserId]
        );
      }
      user = existingUser[0];
      user.wallet_address = walletAddress;
      console.log('[Users] Existing user found:', user.id);
    } else {
      // Create new user
      const result = await query(
        `INSERT INTO users (privy_id, wallet_address, created_at) 
         VALUES ($1, $2, NOW()) 
         RETURNING *`,
        [privyUserId, walletAddress]
      );
      user = result[0];
      console.log('[Users] New user created:', user.id);
    }

    // Get following count
    const followingResult = await query(
      'SELECT COUNT(*) as count FROM wallet_follows WHERE user_id = $1',
      [user.id]
    );

    res.json({ 
      user: {
        ...user,
        following_count: parseInt(followingResult[0]?.count || '0')
      }
    });
  } catch (error) {
    console.error('Auth error:', error);
    res.status(500).json({ error: 'Authentication failed' });
  }
});

// GET /api/users/me - Get current user
router.get('/me', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    const users = await query(
      'SELECT * FROM users WHERE privy_id = $1',
      [privyUserId]
    );

    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    const user = users[0];

    // Get following count
    const followingResult = await query(
      'SELECT COUNT(*) as count FROM wallet_follows WHERE user_id = $1',
      [user.id]
    );

    // Get wallet stats from traders table
    const statsResult = await query(
      'SELECT total_trades as trade_count, total_volume, total_pnl FROM traders WHERE wallet_address = $1',
      [user.wallet_address]
    );

    res.json({ 
      user: {
        ...user,
        following_count: parseInt(followingResult[0]?.count || '0'),
        stats: statsResult[0] || null
      }
    });
  } catch (error) {
    console.error('Get me error:', error);
    res.status(500).json({ error: 'Failed to get user' });
  }
});

// POST /api/users/link/twitter - Link Twitter account
router.post('/link/twitter', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { twitterId, twitterHandle, twitterName, twitterAvatar } = req.body;

    console.log('[Users] Link Twitter:', { privyUserId, twitterHandle });

    await query(
      `UPDATE users 
       SET twitter_id = $1, twitter_handle = $2, twitter_name = $3, twitter_avatar = $4 
       WHERE privy_id = $5`,
      [twitterId, twitterHandle, twitterName, twitterAvatar, privyUserId]
    );

    const users = await query('SELECT * FROM users WHERE privy_id = $1', [privyUserId]);

    res.json({ user: users[0] });
  } catch (error) {
    console.error('Link Twitter error:', error);
    res.status(500).json({ error: 'Failed to link Twitter' });
  }
});

// DELETE /api/users/link/twitter - Unlink Twitter account
router.delete('/link/twitter', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    await query(
      `UPDATE users 
       SET twitter_id = NULL, twitter_handle = NULL, twitter_name = NULL, twitter_avatar = NULL 
       WHERE privy_id = $1`,
      [privyUserId]
    );

    const users = await query('SELECT * FROM users WHERE privy_id = $1', [privyUserId]);

    res.json({ user: users[0] });
  } catch (error) {
    console.error('Unlink Twitter error:', error);
    res.status(500).json({ error: 'Failed to unlink Twitter' });
  }
});

// GET /api/users/following - Get followed wallets
router.get('/following', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    // Get user ID
    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.json({ following: [] });
    }
    const userId = userResult[0].id;

    // Get followed wallets with their stats
    const result = await query(`
      SELECT 
        wf.followed_wallet as wallet_address,
        wf.nickname,
        wf.created_at as followed_at,
        t.total_trades as trade_count,
        t.total_volume,
        t.total_pnl,
        t.last_seen as last_trade_time
      FROM wallet_follows wf
      LEFT JOIN traders t ON t.wallet_address = wf.followed_wallet
      WHERE wf.user_id = $1
      ORDER BY wf.created_at DESC
    `, [userId]);

    res.json({ following: result });
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

    console.log('[Users] Follow wallet:', { privyUserId, walletAddress });

    if (!walletAddress) {
      return res.status(400).json({ error: 'Wallet address required' });
    }

    // Get user ID
    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    const userId = userResult[0].id;

    // Check if already following
    const existing = await query(
      'SELECT id FROM wallet_follows WHERE user_id = $1 AND followed_wallet = $2',
      [userId, walletAddress]
    );

    if (existing.length > 0) {
      return res.status(400).json({ error: 'Already following this wallet' });
    }

    // Add follow
    const result = await query(
      `INSERT INTO wallet_follows (user_id, followed_wallet, nickname, created_at) 
       VALUES ($1, $2, $3, NOW()) 
       RETURNING *`,
      [userId, walletAddress, nickname || null]
    );

    console.log('[Users] Follow created:', result[0]);

    res.json({ follow: result[0] });
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

    // Get user ID
    const userResult = await query('SELECT id FROM users WHERE privy_id = $1', [privyUserId]);
    if (userResult.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    const userId = userResult[0].id;

    // Remove follow
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
    console.log('[Users] Getting wallet profile for:', address);

    // Get user profile if they've registered
    const users = await query(`
      SELECT wallet_address, twitter_handle, twitter_name, twitter_avatar,
             display_name, created_at
      FROM users
      WHERE wallet_address = $1
    `, [address]);

    console.log('[Users] User query result:', users.length > 0 ? 'Found' : 'Not found');

    // Get wallet stats from traders table (without win_rate which may not exist)
    let stats = null;
    try {
      const statsResult = await query(`
        SELECT 
          total_trades as trade_count, 
          total_volume, 
          total_pnl
        FROM traders 
        WHERE wallet_address = $1
      `, [address]);
      stats = statsResult[0] || null;
      console.log('[Users] Stats query result:', stats ? 'Found' : 'Not found');
    } catch (statsError) {
      console.error('[Users] Stats query error (non-fatal):', statsError);
      // Continue without stats
    }

    if (users.length === 0) {
      console.log('[Users] No user profile found, returning null profile with stats');
      return res.json({ 
        profile: null, 
        stats: stats 
      });
    }

    console.log('[Users] Returning profile:', {
      wallet: users[0].wallet_address,
      twitter: users[0].twitter_handle || 'none'
    });

    res.json({ 
      profile: users[0],
      stats: stats
    });
  } catch (error) {
    console.error('[Users] Get wallet profile error:', error);
    res.status(500).json({ error: 'Failed to get wallet profile' });
  }
});

// GET /api/users/debug - Debug endpoint to check Privy config (remove in production)
router.get('/debug', async (req: Request, res: Response) => {
  res.json({
    privy_app_id_set: !!PRIVY_APP_ID,
    privy_app_id_preview: PRIVY_APP_ID ? PRIVY_APP_ID.slice(0, 8) + '...' : null,
    privy_secret_set: !!PRIVY_APP_SECRET,
    privy_secret_length: PRIVY_APP_SECRET?.length || 0,
    privy_verification_key_set: !!PRIVY_VERIFICATION_KEY,
    timestamp: new Date().toISOString()
  });
});

export default router;
