import cron from 'node-cron';
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';

// ============================================
// OPTIMIZED DATA COLLECTOR
// - Market stats: every 1 minute
// - Trader snapshots: every 5 minutes, but only TOP 20 active wallets
// - Cleanup: daily
// ============================================

// Collect market stats every minute
async function collectMarketStats(): Promise<void> {
  try {
    const tickers = await bulkApi.getAllTickers();
    
    if (tickers.length === 0) {
      console.log('⚠️ No tickers returned from BULK API');
      return;
    }
    
    for (const ticker of tickers) {
      const price = ticker.markPrice || ticker.lastPrice || 0;
      const openInterest = ticker.openInterest || 0;
      const volume = ticker.quoteVolume || ticker.volume || 0;
      const fundingRate = ticker.fundingRate || 0;
      
      // Only insert if we have valid data
      if (price > 0) {
        await query(
          `INSERT INTO market_stats 
           (symbol, price, open_interest, volume_24h, funding_rate, long_open_interest, short_open_interest)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            ticker.symbol,
            price,
            openInterest,
            volume,
            fundingRate,
            openInterest * 0.5,
            openInterest * 0.5,
          ]
        );
      }
    }
    
    console.log(`📊 Collected market stats for ${tickers.length} symbols`);
  } catch (error) {
    console.error('❌ Failed to collect market stats:', error);
  }
}

// Update trader snapshots - wallets viewed in last 24 hours
async function updateTraderSnapshots(): Promise<void> {
  try {
    // Track wallets that have been viewed/active in last 24 hours
    // This ensures anyone who checked their wallet gets hourly snapshots
    const activeWallets = await query<{ wallet_address: string }>(
      `SELECT wallet_address FROM traders 
       WHERE last_seen > NOW() - INTERVAL '24 hours'
       ORDER BY last_seen DESC 
       LIMIT 100`
    );
    
    if (activeWallets.length === 0) {
      console.log('👤 No recently active wallets to update');
      return;
    }
    
    let updated = 0;
    
    for (const row of activeWallets) {
      const wallet = row.wallet_address;
      
      try {
        const account = await bulkApi.getFullAccount(wallet);
        if (!account) continue;
        
        const totalNotional = account.positions.reduce(
          (sum, p) => sum + Math.abs(p.notional || 0), 0
        );
        
        const realizedPnl = account.margin?.realizedPnl || 0;
        const unrealizedPnl = account.margin?.unrealizedPnl || 0;
        const totalPnl = realizedPnl + unrealizedPnl;
        
        // Update trader with current PnL
        await query(
          `UPDATE traders SET 
             total_pnl = $2
           WHERE wallet_address = $1`,
          [wallet, totalPnl]
        );
        
        // Create snapshot (even if no positions, to track PnL changes)
        if (account.positions.length > 0 || totalPnl !== 0) {
          await query(
            `INSERT INTO trader_snapshots 
             (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
             VALUES ($1, $2, $3, $4, $5)`,
            [wallet, realizedPnl, unrealizedPnl, account.positions.length, totalNotional]
          );
        }
        
        updated++;
        
        // Rate limit: 500ms between requests
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (err) {
        // Skip wallets that fail
      }
    }
    
    console.log(`👤 Hourly snapshot: Updated ${updated}/${activeWallets.length} wallets`);
  } catch (error) {
    console.error('❌ Failed to update trader snapshots:', error);
  }
}

// Record a liquidation event
export async function recordLiquidation(
  wallet: string | null,
  symbol: string,
  side: string,
  size: number,
  price: number
): Promise<void> {
  const value = size * price;
  
  await query(
    `INSERT INTO liquidations (wallet_address, symbol, side, size, price, value)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [wallet, symbol, side, size, price, value]
  );
  
  if (wallet) {
    await query(
      `UPDATE traders 
       SET total_liquidations = total_liquidations + 1,
           liquidation_value = liquidation_value + $1
       WHERE wallet_address = $2`,
      [value, wallet]
    );
  }
}

// Record a trade event
export async function recordTrade(
  wallet: string | null,
  symbol: string,
  side: string,
  size: number,
  price: number
): Promise<void> {
  const value = size * price;
  
  if (value < 1000) return;
  
  await query(
    `INSERT INTO trades (wallet_address, symbol, side, size, price, value)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [wallet, symbol, side, size, price, value]
  );
  
  if (wallet) {
    await query(
      `UPDATE traders 
       SET total_trades = total_trades + 1,
           total_volume = total_volume + $1
       WHERE wallet_address = $2`,
      [value, wallet]
    );
  }
}

// Clean up old data
async function cleanupOldData(): Promise<void> {
  try {
    await query(`DELETE FROM market_stats WHERE timestamp < NOW() - INTERVAL '30 days'`);
    await query(`DELETE FROM trader_snapshots WHERE timestamp < NOW() - INTERVAL '30 days'`);
    console.log('🧹 Cleaned up old data');
  } catch (error) {
    console.error('❌ Failed to cleanup old data:', error);
  }
}

// Add a wallet to track (called from wallet routes)
export async function addWalletToTrack(wallet: string): Promise<boolean> {
  try {
    // Just add to database, don't fetch from API immediately
    await query(
      `INSERT INTO traders (wallet_address, last_seen)
       VALUES ($1, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET last_seen = NOW()`,
      [wallet]
    );
    return true;
  } catch (error) {
    console.error(`Failed to add wallet ${wallet}:`, error);
    return false;
  }
}

// Aggregate daily statistics (runs every hour, updates today's stats)
async function aggregateDailyStats(): Promise<void> {
  try {
    console.log('📊 Aggregating daily statistics...');
    
    // Get today and yesterday (to ensure yesterday is finalized)
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    
    // Aggregate per-symbol stats for today and yesterday
    await query(`
      INSERT INTO daily_stats (day, symbol, unique_traders, trade_count, volume)
      SELECT 
        DATE(timestamp) as day,
        symbol,
        COUNT(DISTINCT wallet_address) as unique_traders,
        COUNT(*) as trade_count,
        SUM(value) as volume
      FROM trades
      WHERE DATE(timestamp) >= $1
        AND wallet_address IS NOT NULL
      GROUP BY DATE(timestamp), symbol
      ON CONFLICT (day, symbol) DO UPDATE SET
        unique_traders = EXCLUDED.unique_traders,
        trade_count = EXCLUDED.trade_count,
        volume = EXCLUDED.volume
    `, [yesterday]);
    
    // Aggregate total unique traders per day
    await query(`
      INSERT INTO daily_unique_traders (day, total_unique)
      SELECT 
        DATE(timestamp) as day,
        COUNT(DISTINCT wallet_address) as total_unique
      FROM trades
      WHERE DATE(timestamp) >= $1
        AND wallet_address IS NOT NULL
      GROUP BY DATE(timestamp)
      ON CONFLICT (day) DO UPDATE SET
        total_unique = EXCLUDED.total_unique
    `, [yesterday]);
    
    // Calculate new users (first-time traders) for recent days
    await query(`
      WITH first_trades AS (
        SELECT wallet_address, DATE(MIN(timestamp)) as first_day
        FROM trades
        WHERE wallet_address IS NOT NULL
        GROUP BY wallet_address
      ),
      daily_new AS (
        SELECT first_day, COUNT(*) as new_users
        FROM first_trades
        WHERE first_day >= $1
        GROUP BY first_day
      )
      UPDATE daily_unique_traders d
      SET new_users = dn.new_users
      FROM daily_new dn
      WHERE d.day = dn.first_day
    `, [yesterday]);
    
    // Calculate cumulative users
    await query(`
      WITH running_total AS (
        SELECT 
          day,
          SUM(new_users) OVER (ORDER BY day) as cumulative
        FROM daily_unique_traders
      )
      UPDATE daily_unique_traders d
      SET cumulative_users = rt.cumulative
      FROM running_total rt
      WHERE d.day = rt.day
    `);
    
    console.log('✅ Daily stats aggregation complete');
  } catch (error) {
    console.error('❌ Failed to aggregate daily stats:', error);
  }
}

// Backfill historical daily stats (run once on startup if tables are empty)
async function backfillDailyStats(): Promise<void> {
  try {
    const existing = await query<{ count: string }>('SELECT COUNT(*) as count FROM daily_stats');
    if (parseInt(existing[0]?.count || '0') > 0) {
      console.log('📊 Daily stats already populated, skipping backfill');
      return;
    }
    
    console.log('📊 Backfilling historical daily stats (this may take a minute)...');
    
    // Backfill per-symbol stats
    await query(`
      INSERT INTO daily_stats (day, symbol, unique_traders, trade_count, volume)
      SELECT 
        DATE(timestamp) as day,
        symbol,
        COUNT(DISTINCT wallet_address) as unique_traders,
        COUNT(*) as trade_count,
        SUM(value) as volume
      FROM trades
      WHERE wallet_address IS NOT NULL
      GROUP BY DATE(timestamp), symbol
      ON CONFLICT (day, symbol) DO NOTHING
    `);
    
    // Backfill total unique per day
    await query(`
      INSERT INTO daily_unique_traders (day, total_unique)
      SELECT 
        DATE(timestamp) as day,
        COUNT(DISTINCT wallet_address) as total_unique
      FROM trades
      WHERE wallet_address IS NOT NULL
      GROUP BY DATE(timestamp)
      ON CONFLICT (day) DO NOTHING
    `);
    
    // Calculate new users
    await query(`
      WITH first_trades AS (
        SELECT wallet_address, DATE(MIN(timestamp)) as first_day
        FROM trades
        WHERE wallet_address IS NOT NULL
        GROUP BY wallet_address
      ),
      daily_new AS (
        SELECT first_day, COUNT(*) as new_users
        FROM first_trades
        GROUP BY first_day
      )
      UPDATE daily_unique_traders d
      SET new_users = COALESCE(dn.new_users, 0)
      FROM daily_new dn
      WHERE d.day = dn.first_day
    `);
    
    // Calculate cumulative
    await query(`
      WITH running_total AS (
        SELECT 
          day,
          SUM(COALESCE(new_users, 0)) OVER (ORDER BY day) as cumulative
        FROM daily_unique_traders
      )
      UPDATE daily_unique_traders d
      SET cumulative_users = rt.cumulative
      FROM running_total rt
      WHERE d.day = rt.day
    `);
    
    console.log('✅ Historical daily stats backfill complete');
  } catch (error) {
    console.error('❌ Failed to backfill daily stats:', error);
  }
}

// ============ FEE SNAPSHOT COLLECTION ============

const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

// Collect fee state snapshot (for protocol revenue tracking)
async function collectFeeSnapshot(): Promise<void> {
  try {
    const res = await fetch(`${BULK_API_BASE}/feeState`);
    if (!res.ok) {
      console.error('Failed to fetch fee state from BULK API');
      return;
    }
    
    const feeState = await res.json() as any;
    
    const totalMakerFees = feeState.total_maker_fees || 0;
    const totalTakerFees = feeState.total_taker_fees || 0;
    const totalProtocolSettlement = feeState.total_protocol_settlement || 0;
    const settledFills = feeState.settled_fills || 0;
    
    await query(
      `INSERT INTO fee_snapshots (total_maker_fees, total_taker_fees, total_protocol_settlement, settled_fills, timestamp)
       VALUES ($1, $2, $3, $4, NOW())`,
      [totalMakerFees, totalTakerFees, totalProtocolSettlement, settledFills]
    );
    
    console.log(`💰 Fee snapshot: Protocol revenue $${totalProtocolSettlement.toFixed(2)} | Fills: ${settledFills}`);
  } catch (error) {
    console.error('❌ Failed to collect fee snapshot:', error);
  }
}

// Start all cron jobs
export function startDataCollector(): void {
  console.log('🚀 Starting data collector...');
  
  // Collect market stats every minute
  cron.schedule('* * * * *', () => {
    collectMarketStats();
  });
  
  // Update trader snapshots every hour (wallets viewed in last 24h)
  cron.schedule('0 * * * *', () => {
    updateTraderSnapshots();
  });
  
  // Aggregate daily stats every hour at :05 (after trader snapshots)
  cron.schedule('5 * * * *', () => {
    aggregateDailyStats();
  });
  
  // Collect fee snapshots every 10 minutes
  cron.schedule('*/10 * * * *', () => {
    collectFeeSnapshot();
  });
  
  // Clean up old data daily at 3am
  cron.schedule('0 3 * * *', () => {
    cleanupOldData();
  });
  
  // Run initial collection
  collectMarketStats();
  collectFeeSnapshot();
  
  // Backfill daily stats on startup (only if empty)
  setTimeout(() => backfillDailyStats(), 5000);
  
  console.log('✅ Data collector started (hourly snapshots + daily stats aggregation + fee tracking)');
}
