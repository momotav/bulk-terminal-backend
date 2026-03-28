import { Router, Request, Response } from 'express';
import { query } from '../db';
import { PrivyClient } from '@privy-io/server-auth';
import { getCache, setCache } from '../services/cache';

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
    const { walletAddress, email } = req.body;

    console.log('[Users] Auth request:', { privyUserId, walletAddress, email });

    // For email-only users, walletAddress may be null
    const hasWallet = !!walletAddress;

    // First, check if user exists by privy_id
    let userResult = await query(
      'SELECT * FROM users WHERE privy_id = $1',
      [privyUserId]
    );

    let user = userResult[0];

    if (user) {
      // User exists by privy_id - update wallet/email if provided
      const updates: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      if (walletAddress && user.wallet_address !== walletAddress) {
        updates.push(`wallet_address = $${paramIndex++}`);
        values.push(walletAddress);
      }
      if (email && user.email !== email) {
        updates.push(`email = $${paramIndex++}`);
        values.push(email);
      }

      if (updates.length > 0) {
        values.push(user.id);
        await query(
          `UPDATE users SET ${updates.join(', ')} WHERE id = $${paramIndex}`,
          values
        );
        if (walletAddress) user.wallet_address = walletAddress;
        if (email) user.email = email;
      }
      console.log('[Users] Existing user found by privy_id:', user.id);
    } else if (hasWallet) {
      // Check if wallet already exists (different privy account)
      const walletCheck = await query(
        'SELECT * FROM users WHERE wallet_address = $1',
        [walletAddress]
      );

      if (walletCheck[0]) {
        // Wallet exists with different privy_id - update privy_id
        console.log('[Users] Wallet exists, updating privy_id');
        await query(
          'UPDATE users SET privy_id = $1, email = COALESCE($2, email) WHERE wallet_address = $3',
          [privyUserId, email, walletAddress]
        );
        user = walletCheck[0];
        user.privy_id = privyUserId;
        if (email) user.email = email;
      } else {
        // Create new user with wallet
        console.log('[Users] Creating new user with wallet');
        const insertResult = await query(
          `INSERT INTO users (privy_id, wallet_address, email, created_at) 
           VALUES ($1, $2, $3, NOW()) 
           RETURNING *`,
          [privyUserId, walletAddress, email]
        );
        user = insertResult[0];
        console.log('[Users] New user created:', user?.id);
      }
    } else {
      // Email-only user (no wallet)
      console.log('[Users] Creating new email-only user');
      const insertResult = await query(
        `INSERT INTO users (privy_id, email, created_at) 
         VALUES ($1, $2, NOW()) 
         RETURNING *`,
        [privyUserId, email]
      );
      user = insertResult[0];
      console.log('[Users] New email user created:', user?.id);
    }

    if (!user) {
      console.error('[Users] Failed to create or find user');
      return res.status(500).json({ error: 'Failed to create user' });
    }

    console.log('[Users] User authenticated:', user.id, user.wallet_address || user.email);

    // Get following count
    const followingResult = await query(
      'SELECT COUNT(*) as count FROM wallet_follows WHERE user_id = $1',
      [user.id]
    );

    // Get trader stats if user has a wallet (connected or claimed)
    const effectiveWallet = user.wallet_address || user.claimed_wallet;
    let stats = null;
    if (effectiveWallet) {
      const statsResult = await query(
        'SELECT total_trades as trade_count, total_volume, total_pnl FROM traders WHERE wallet_address = $1',
        [effectiveWallet]
      );
      stats = statsResult[0] || null;
    }

    res.json({ 
      user: {
        ...user,
        following_count: parseInt(followingResult[0]?.count || '0'),
        stats
      }
    });
  } catch (error: any) {
    console.error('[Users] Auth error:', error);
    console.error('[Users] Auth error message:', error?.message);
    console.error('[Users] Auth error code:', error?.code);
    console.error('[Users] Auth error detail:', error?.detail);
    res.status(500).json({ error: 'Authentication failed', detail: error?.message });
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

// POST /api/users/claim-wallet - Claim a wallet (for email users)
router.post('/claim-wallet', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;
    const { walletAddress } = req.body;

    if (!walletAddress) {
      return res.status(400).json({ error: 'Wallet address required' });
    }

    console.log('[Users] Claiming wallet:', walletAddress, 'for user:', privyUserId);

    // Update user's claimed_wallet
    await query(
      `UPDATE users 
       SET claimed_wallet = $1, wallet_address = COALESCE(wallet_address, $1)
       WHERE privy_id = $2`,
      [walletAddress, privyUserId]
    );

    const users = await query('SELECT * FROM users WHERE privy_id = $1', [privyUserId]);

    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    console.log('[Users] Wallet claimed successfully');
    res.json({ user: users[0], success: true });
  } catch (error) {
    console.error('Claim wallet error:', error);
    res.status(500).json({ error: 'Failed to claim wallet' });
  }
});

// DELETE /api/users/claim-wallet - Unclaim wallet
router.delete('/claim-wallet', verifyPrivyToken, async (req: Request, res: Response) => {
  try {
    const privyUserId = (req as any).privyUserId;

    console.log('[Users] Unclaiming wallet for user:', privyUserId);

    await query(
      `UPDATE users SET claimed_wallet = NULL WHERE privy_id = $1`,
      [privyUserId]
    );

    const users = await query('SELECT * FROM users WHERE privy_id = $1', [privyUserId]);

    res.json({ user: users[0], success: true });
  } catch (error) {
    console.error('Unclaim wallet error:', error);
    res.status(500).json({ error: 'Failed to unclaim wallet' });
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

    // Get followed wallets with their stats AND Twitter profile if they have one
    const result = await query(`
      SELECT 
        wf.followed_wallet as wallet_address,
        wf.nickname,
        wf.created_at as followed_at,
        t.total_trades as trade_count,
        t.total_volume,
        t.total_pnl,
        t.last_seen as last_trade_time,
        u.twitter_handle,
        u.twitter_name,
        u.twitter_avatar
      FROM wallet_follows wf
      LEFT JOIN traders t ON t.wallet_address = wf.followed_wallet
      LEFT JOIN users u ON u.wallet_address = wf.followed_wallet
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

// GET /api/users/search - Search users by Twitter handle or wallet address (NO AUTH REQUIRED)
router.get('/search', async (req: Request, res: Response) => {
  try {
    const { q } = req.query;
    
    if (!q || typeof q !== 'string' || q.length < 2) {
      return res.json({ results: [] });
    }

    const searchTerm = q.trim().toLowerCase().replace('@', ''); // Remove @ if present
    console.log('[Users] Searching for:', searchTerm);

    // Search by Twitter handle, Twitter name, or wallet address
    const results = await query(`
      SELECT 
        u.wallet_address, 
        u.twitter_handle, 
        u.twitter_name, 
        u.twitter_avatar,
        u.display_name,
        t.total_pnl,
        t.total_volume,
        t.total_trades as trade_count
      FROM users u
      LEFT JOIN traders t ON t.wallet_address = u.wallet_address
      WHERE 
        LOWER(u.twitter_handle) LIKE $1
        OR LOWER(u.twitter_name) LIKE $1
        OR LOWER(u.wallet_address) LIKE $1
      ORDER BY 
        CASE 
          WHEN LOWER(u.twitter_handle) = $2 THEN 0
          WHEN LOWER(u.twitter_handle) LIKE $3 THEN 1
          ELSE 2
        END,
        t.total_volume DESC NULLS LAST
      LIMIT 10
    `, [`%${searchTerm}%`, searchTerm, `${searchTerm}%`]);

    console.log('[Users] Search results:', results.length);

    res.json({ results });
  } catch (error) {
    console.error('[Users] Search error:', error);
    res.status(500).json({ error: 'Search failed' });
  }
});

// GET /api/users/wallet/:address - Get public wallet profile (NO AUTH REQUIRED)
router.get('/wallet/:address', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;

    // Get user profile if they've registered
    const users = await query(`
      SELECT wallet_address, twitter_handle, twitter_name, twitter_avatar,
             display_name, created_at
      FROM users
      WHERE wallet_address = $1
    `, [address]);

    // Get wallet stats from traders table
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
    } catch (statsError) {
      // Continue without stats
    }

    if (users.length === 0) {
      return res.json({ 
        profile: null, 
        stats: stats 
      });
    }

    res.json({ 
      profile: users[0],
      stats: stats
    });
  } catch (error) {
    console.error('[Users] Get wallet profile error:', error);
    res.status(500).json({ error: 'Failed to get wallet profile' });
  }
});

// POST /api/users/wallets/batch - Get multiple wallet profiles in one request
router.post('/wallets/batch', async (req: Request, res: Response) => {
  try {
    const { addresses } = req.body;
    
    if (!addresses || !Array.isArray(addresses) || addresses.length === 0) {
      return res.json({ profiles: {} });
    }
    
    // Limit to 50 addresses per request
    const limitedAddresses = addresses.slice(0, 50);
    
    // Get all profiles in one query
    const users = await query(`
      SELECT wallet_address, twitter_handle, twitter_name, twitter_avatar,
             display_name, created_at
      FROM users
      WHERE wallet_address = ANY($1)
    `, [limitedAddresses]);
    
    // Get all stats in one query
    const stats = await query(`
      SELECT 
        wallet_address,
        total_trades as trade_count, 
        total_volume, 
        total_pnl
      FROM traders 
      WHERE wallet_address = ANY($1)
    `, [limitedAddresses]);
    
    // Build response map
    const profiles: Record<string, any> = {};
    
    for (const addr of limitedAddresses) {
      const user = users.find(u => u.wallet_address === addr);
      const stat = stats.find(s => s.wallet_address === addr);
      
      profiles[addr] = {
        profile: user || null,
        stats: stat || null,
      };
    }
    
    res.json({ profiles });
  } catch (error) {
    console.error('[Users] Batch wallet profiles error:', error);
    res.status(500).json({ error: 'Failed to get wallet profiles' });
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
