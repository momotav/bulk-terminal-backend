import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

import { testConnection, initializeDatabase, query } from './db';
import { startDataCollector } from './jobs/dataCollector';
import { startWebSocketListener, getWebSocketStats } from './jobs/wsListener';

// Import routes
import authRoutes from './routes/auth';
import leaderboardRoutes from './routes/leaderboard';
import analyticsRoutes from './routes/analytics';
import walletRoutes from './routes/wallet';

const app = express();
const PORT = process.env.PORT || 3001;

// Allowed origins
const allowedOrigins = [
  'http://localhost:3000',
  'https://bulkstats.com',
  'https://www.bulkstats.com',
  'https://bulk-terminal.vercel.app',
  process.env.FRONTEND_URL,
].filter(Boolean);

// Middleware
app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (mobile apps, curl, etc.)
    if (!origin) return callback(null, true);
    
    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      console.log('Blocked by CORS:', origin);
      callback(null, true); // Allow all for now, log blocked ones
    }
  },
  credentials: true,
}));
app.use(express.json());

// Request logging
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
  next();
});

// Health check
app.get('/health', (req, res) => {
  const wsStats = getWebSocketStats();
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    websocket: wsStats,
  });
});

// Debug endpoint to check actual database values
app.get('/debug/db', async (req, res) => {
  try {
    const traders = await query('SELECT wallet_address, total_pnl, total_volume, total_trades, total_liquidations, liquidation_value FROM traders ORDER BY last_seen DESC LIMIT 10');
    const snapshots = await query('SELECT wallet_address, pnl, unrealized_pnl, total_notional, positions_count, timestamp FROM trader_snapshots ORDER BY timestamp DESC LIMIT 10');
    const marketStats = await query('SELECT symbol, price, open_interest, volume_24h, funding_rate, timestamp FROM market_stats ORDER BY timestamp DESC LIMIT 20');
    const marketStatsCount = await query('SELECT COUNT(*) as count FROM market_stats');
    const liquidations = await query('SELECT * FROM liquidations ORDER BY timestamp DESC LIMIT 10');
    const liquidationsCount = await query('SELECT COUNT(*) as count FROM liquidations');
    const trades = await query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10');
    const tradesCount = await query('SELECT COUNT(*) as count FROM trades');
    res.json({ traders, snapshots, marketStats, marketStatsCount, liquidations, liquidationsCount, trades, tradesCount });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// Debug endpoint to check WebSocket status
app.get('/debug/ws', async (req, res) => {
  const wsStats = getWebSocketStats();
  res.json(wsStats);
});

// Debug endpoint to clear test liquidations
app.get('/debug/clear-test-liquidations', async (req, res) => {
  try {
    await query(`DELETE FROM liquidations WHERE wallet_address LIKE 'TEST_WALLET_%'`);
    await query(`DELETE FROM traders WHERE wallet_address LIKE 'TEST_WALLET_%'`);
    res.json({ message: 'Test liquidations cleared' });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// Debug endpoint to insert a test liquidation (for testing UI)
app.get('/debug/test-liquidation', async (req, res) => {
  try {
    const testWallet = 'TEST_WALLET_' + Math.random().toString(36).substring(7);
    const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
    const symbol = symbols[Math.floor(Math.random() * symbols.length)];
    const side = Math.random() > 0.5 ? 'long' : 'short';
    const price = symbol === 'BTC-USD' ? 71000 + Math.random() * 1000 : symbol === 'ETH-USD' ? 2100 + Math.random() * 50 : 88 + Math.random() * 2;
    const size = Math.random() * 10;
    const value = price * size;
    
    await query(
      `INSERT INTO liquidations (wallet_address, symbol, side, size, price, value, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
      [testWallet, symbol, side, size, price, value]
    );
    
    await query(
      `INSERT INTO traders (wallet_address, total_liquidations, liquidation_value, last_seen)
       VALUES ($1, 1, $2, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET
         total_liquidations = traders.total_liquidations + 1,
         liquidation_value = traders.liquidation_value + $2,
         last_seen = NOW()`,
      [testWallet, value]
    );
    
    res.json({ 
      message: 'Test liquidation inserted',
      liquidation: { wallet: testWallet, symbol, side, size, price, value }
    });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// Debug endpoint to manually trigger market stats collection
app.get('/debug/collect', async (req, res) => {
  try {
    const { bulkApi } = await import('./services/bulkApi');
    const tickers = await bulkApi.getAllTickers();
    res.json({ 
      message: 'Fetched tickers',
      count: tickers.length,
      tickers 
    });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/leaderboard', leaderboardRoutes);
app.use('/api/analytics', analyticsRoutes);
app.use('/api/wallet', walletRoutes);

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// Error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
async function start() {
  console.log('🚀 Starting BULK Terminal Backend...');
  
  // Test database connection
  const dbConnected = await testConnection();
  if (!dbConnected) {
    console.error('❌ Cannot start without database connection');
    process.exit(1);
  }
  
  // Initialize database schema
  await initializeDatabase();
  
  // Start data collector cron jobs
  startDataCollector();
  
  // Start WebSocket listener for live trades/liquidations
  startWebSocketListener();
  
  // Start HTTP server
  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`   Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:3000'}`);
    console.log(`   Environment: ${process.env.NODE_ENV || 'development'}`);
  });
}

start().catch(error => {
  console.error('Failed to start server:', error);
  process.exit(1);
});
