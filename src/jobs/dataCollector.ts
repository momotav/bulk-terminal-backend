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

// Update trader snapshots - ONLY top active wallets, with rate limiting
async function updateTraderSnapshots(): Promise<void> {
  try {
    // Only fetch TOP 20 most recently active wallets (not all 1000!)
    const activeWallets = await query<{ wallet_address: string }>(
      `SELECT wallet_address FROM traders 
       WHERE last_seen > NOW() - INTERVAL '1 hour'
       ORDER BY last_seen DESC 
       LIMIT 20`
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
        
        const realizedPnl = account.margin.realizedPnl || 0;
        const unrealizedPnl = account.margin.unrealizedPnl || 0;
        const totalPnl = realizedPnl + unrealizedPnl;
        
        // Update trader with current PnL
        await query(
          `UPDATE traders SET 
             last_seen = NOW(),
             total_pnl = $2
           WHERE wallet_address = $1`,
          [wallet, totalPnl]
        );
        
        // Create snapshot only if they have positions
        if (account.positions.length > 0) {
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
    
    console.log(`👤 Updated snapshots for ${updated}/${activeWallets.length} active traders`);
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

// Start all cron jobs
export function startDataCollector(): void {
  console.log('🚀 Starting data collector...');
  
  // Collect market stats every minute
  cron.schedule('* * * * *', () => {
    collectMarketStats();
  });
  
  // Update trader snapshots every 5 minutes (only top 20 active)
  cron.schedule('*/5 * * * *', () => {
    updateTraderSnapshots();
  });
  
  // Clean up old data daily at 3am
  cron.schedule('0 3 * * *', () => {
    cleanupOldData();
  });
  
  // Run initial collection
  collectMarketStats();
  
  console.log('✅ Data collector started (optimized: max 20 wallet fetches per 5 min)');
}
