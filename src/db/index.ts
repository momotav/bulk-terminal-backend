import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

// Database connection pool
export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// Test connection
export async function testConnection(): Promise<boolean> {
  try {
    const client = await pool.connect();
    await client.query('SELECT NOW()');
    client.release();
    console.log('✅ Database connected');
    return true;
  } catch (error) {
    console.error('❌ Database connection failed:', error);
    return false;
  }
}

// Initialize database schema
export async function initializeDatabase(): Promise<void> {
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');

    // Users table (for auth)
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        username VARCHAR(50) UNIQUE,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );
    `);

    // Traders table (tracked wallets)
    await client.query(`
      CREATE TABLE IF NOT EXISTS traders (
        wallet_address VARCHAR(64) PRIMARY KEY,
        first_seen TIMESTAMP DEFAULT NOW(),
        last_seen TIMESTAMP DEFAULT NOW(),
        total_volume DECIMAL(20, 2) DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        total_pnl DECIMAL(20, 2) DEFAULT 0,
        total_liquidations INTEGER DEFAULT 0,
        liquidation_value DECIMAL(20, 2) DEFAULT 0,
        total_adl INTEGER DEFAULT 0,
        adl_value DECIMAL(20, 2) DEFAULT 0
      );
    `);

    // Add missing columns if they don't exist (for existing databases)
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='traders' AND column_name='total_adl') THEN
          ALTER TABLE traders ADD COLUMN total_adl INTEGER DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='traders' AND column_name='adl_value') THEN
          ALTER TABLE traders ADD COLUMN adl_value DECIMAL(20, 2) DEFAULT 0;
        END IF;
      END $$;
    `);

    // Trader snapshots (for historical tracking)
    await client.query(`
      CREATE TABLE IF NOT EXISTS trader_snapshots (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(64) NOT NULL,
        timestamp TIMESTAMP DEFAULT NOW(),
        pnl DECIMAL(20, 2) DEFAULT 0,
        unrealized_pnl DECIMAL(20, 2) DEFAULT 0,
        volume_24h DECIMAL(20, 2) DEFAULT 0,
        positions_count INTEGER DEFAULT 0,
        total_notional DECIMAL(20, 2) DEFAULT 0,
        FOREIGN KEY (wallet_address) REFERENCES traders(wallet_address)
      );
      
      CREATE INDEX IF NOT EXISTS idx_snapshots_wallet_time 
      ON trader_snapshots(wallet_address, timestamp DESC);
    `);

    // Liquidations table
    await client.query(`
      CREATE TABLE IF NOT EXISTS liquidations (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(64),
        symbol VARCHAR(20) NOT NULL,
        side VARCHAR(10) NOT NULL,
        size DECIMAL(20, 8) NOT NULL,
        price DECIMAL(20, 2) NOT NULL,
        value DECIMAL(20, 2) NOT NULL,
        timestamp TIMESTAMP DEFAULT NOW()
      );
      
      CREATE INDEX IF NOT EXISTS idx_liquidations_time 
      ON liquidations(timestamp DESC);
      
      CREATE INDEX IF NOT EXISTS idx_liquidations_wallet 
      ON liquidations(wallet_address);
    `);

    // Trades table
    await client.query(`
      CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(64),
        symbol VARCHAR(20) NOT NULL,
        side VARCHAR(10) NOT NULL,
        size DECIMAL(20, 8) NOT NULL,
        price DECIMAL(20, 2) NOT NULL,
        value DECIMAL(20, 2) NOT NULL,
        timestamp TIMESTAMP DEFAULT NOW()
      );
      
      CREATE INDEX IF NOT EXISTS idx_trades_time 
      ON trades(timestamp DESC);
      
      CREATE INDEX IF NOT EXISTS idx_trades_value 
      ON trades(value DESC);
      
      CREATE INDEX IF NOT EXISTS idx_trades_wallet 
      ON trades(wallet_address);
      
      CREATE INDEX IF NOT EXISTS idx_trades_symbol 
      ON trades(symbol);
    `);

    // ADL (Auto-Deleveraging) events table
    await client.query(`
      CREATE TABLE IF NOT EXISTS adl_events (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(64),
        counterparty VARCHAR(64),
        symbol VARCHAR(20) NOT NULL,
        side VARCHAR(10) NOT NULL,
        size DECIMAL(20, 8) NOT NULL,
        price DECIMAL(20, 2) NOT NULL,
        value DECIMAL(20, 2) NOT NULL,
        timestamp TIMESTAMP DEFAULT NOW()
      );
      
      CREATE INDEX IF NOT EXISTS idx_adl_events_time 
      ON adl_events(timestamp DESC);
      
      CREATE INDEX IF NOT EXISTS idx_adl_events_wallet 
      ON adl_events(wallet_address);
      
      CREATE INDEX IF NOT EXISTS idx_adl_events_symbol 
      ON adl_events(symbol);
    `);

    // Market stats (for analytics charts)
    await client.query(`
      CREATE TABLE IF NOT EXISTS market_stats (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT NOW(),
        symbol VARCHAR(20) NOT NULL,
        price DECIMAL(20, 2),
        open_interest DECIMAL(20, 2),
        volume_24h DECIMAL(20, 2),
        funding_rate DECIMAL(20, 8),
        long_open_interest DECIMAL(20, 2),
        short_open_interest DECIMAL(20, 2)
      );
      
      CREATE INDEX IF NOT EXISTS idx_market_stats_symbol_time 
      ON market_stats(symbol, timestamp DESC);
    `);

    // Watchlist (users following wallets)
    await client.query(`
      CREATE TABLE IF NOT EXISTS watchlist (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        wallet_address VARCHAR(64) NOT NULL,
        nickname VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, wallet_address)
      );
    `);

    // Notifications for followed wallets activity
    await client.query(`
      CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        wallet_address VARCHAR(64) NOT NULL,
        type VARCHAR(20) NOT NULL,
        symbol VARCHAR(20),
        side VARCHAR(10),
        size DECIMAL(20, 8),
        price DECIMAL(20, 2),
        value DECIMAL(20, 2),
        read BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      );
      
      CREATE INDEX IF NOT EXISTS idx_notifications_user_time 
      ON notifications(user_id, created_at DESC);
      
      CREATE INDEX IF NOT EXISTS idx_notifications_unread 
      ON notifications(user_id, read) WHERE read = false;
    `);

    // Alerts
    await client.query(`
      CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        config JSONB NOT NULL,
        enabled BOOLEAN DEFAULT true,
        last_triggered TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      );
    `);

    // Comments
    await client.query(`
      CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        target_type VARCHAR(50) NOT NULL,
        target_id VARCHAR(100) NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      );
      
      CREATE INDEX IF NOT EXISTS idx_comments_target 
      ON comments(target_type, target_id);
    `);

    await client.query('COMMIT');
    console.log('✅ Database schema initialized');
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ Database initialization failed:', error);
    throw error;
  } finally {
    client.release();
  }
}

// Helper function to run queries
export async function query<T = any>(text: string, params?: any[]): Promise<T[]> {
  const result = await pool.query(text, params);
  return result.rows as T[];
}

export async function queryOne<T = any>(text: string, params?: any[]): Promise<T | null> {
  const rows = await query<T>(text, params);
  return rows[0] || null;
}
