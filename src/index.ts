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

// Middleware
app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
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
    const traders = await query('SELECT wallet_address, total_pnl, total_volume, total_trades FROM traders ORDER BY last_seen DESC LIMIT 10');
    const snapshots = await query('SELECT wallet_address, pnl, unrealized_pnl, total_notional, positions_count, timestamp FROM trader_snapshots ORDER BY timestamp DESC LIMIT 10');
    const marketStats = await query('SELECT symbol, price, open_interest, volume_24h, funding_rate, timestamp FROM market_stats ORDER BY timestamp DESC LIMIT 20');
    const marketStatsCount = await query('SELECT COUNT(*) as count FROM market_stats');
    res.json({ traders, snapshots, marketStats, marketStatsCount });
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
