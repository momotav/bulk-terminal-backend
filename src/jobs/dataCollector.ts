import cron from 'node-cron';
import { query, queryOne } from '../db';
import { bulkApi } from '../services/bulkApi';

// List of known active wallets to track (will grow over time)
let trackedWallets: string[] = [];

// Collect market stats every minute
async function collectMarketStats(): Promise<void> {
  try {
    const tickers = await bulkApi.getAllTickers();
    
    if (tickers.length === 0) {
      console.log('⚠️ No tickers returned from BULK API');
      return;
    }
    
    for (const ticker of tickers) {
      // Log first few times to debug
      console.log(`📊 Ticker ${ticker.symbol}:`, JSON.stringify(ticker));
      
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
            // Estimate long/short OI (50/50 split as placeholder)
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

// Update trader snapshots every 5 minutes
async function updateTraderSnapshots(): Promise<void> {
  try {
    // Also load wallets from recent trades that aren't being tracked yet
    const newWallets = await query<{ wallet_address: string }>(
      `SELECT DISTINCT wallet_address FROM trades 
       WHERE wallet_address IS NOT NULL 
       AND wallet_address NOT IN (SELECT wallet_address FROM traders)
       LIMIT 50`
    );
    
    for (const row of newWallets) {
      if (!trackedWallets.includes(row.wallet_address)) {
        trackedWallets.push(row.wallet_address);
        await query(
          `INSERT INTO traders (wallet_address) VALUES ($1) ON CONFLICT DO NOTHING`,
          [row.wallet_address]
        );
      }
    }
    
    let updated = 0;
    
    for (const wallet of trackedWallets) {
      try {
        const account = await bulkApi.getFullAccount(wallet);
        if (!account) continue;
        
        const totalNotional = account.positions.reduce(
          (sum, p) => sum + Math.abs(p.notional || 0), 0
        );
        
        const realizedPnl = account.margin.realizedPnl || 0;
        const unrealizedPnl = account.margin.unrealizedPnl || 0;
        const totalPnl = realizedPnl + unrealizedPnl;
        
        // Update trader with current PnL and notional
        await query(
          `UPDATE traders SET 
             last_seen = NOW(),
             total_pnl = $2
           WHERE wallet_address = $1`,
          [wallet, totalPnl]
        );
        
        // Create snapshot
        await query(
          `INSERT INTO trader_snapshots 
           (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            wallet,
            realizedPnl,
            unrealizedPnl,
            account.positions.length,
            totalNotional,
          ]
        );
        
        updated++;
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (err) {
        // Skip wallets that fail (might not exist on BULK)
        console.error(`Failed to update wallet ${wallet.slice(0, 8)}:`, err);
      }
    }
    
    console.log(`👤 Updated snapshots for ${updated} traders`);
  } catch (error) {
    console.error('❌ Failed to update trader snapshots:', error);
  }
}

// Discover new wallets from recent trades (placeholder - needs WebSocket in production)
async function discoverNewWallets(): Promise<void> {
  // In production, this would listen to WebSocket trades
  // For now, we can manually add wallets or import from a source
  
  // Example: Add some test wallets if list is empty
  if (trackedWallets.length === 0) {
    // These would be real wallets discovered from trading activity
    console.log('📍 No wallets to track yet. Add wallets via API or WebSocket discovery.');
  }
}

// Load tracked wallets from database
async function loadTrackedWallets(): Promise<void> {
  try {
    const rows = await query<{ wallet_address: string }>(
      'SELECT wallet_address FROM traders ORDER BY last_seen DESC LIMIT 1000'
    );
    trackedWallets = rows.map(r => r.wallet_address);
    console.log(`📋 Loaded ${trackedWallets.length} wallets to track`);
  } catch (error) {
    console.error('❌ Failed to load tracked wallets:', error);
  }
}

// Add a wallet to track
export async function addWalletToTrack(wallet: string): Promise<boolean> {
  try {
    // Verify wallet has activity
    const account = await bulkApi.getFullAccount(wallet);
    if (!account) {
      return false;
    }
    
    // Add to database
    await query(
      `INSERT INTO traders (wallet_address)
       VALUES ($1)
       ON CONFLICT (wallet_address) DO NOTHING`,
      [wallet]
    );
    
    // Add to in-memory list
    if (!trackedWallets.includes(wallet)) {
      trackedWallets.push(wallet);
    }
    
    return true;
  } catch (error) {
    console.error(`Failed to add wallet ${wallet}:`, error);
    return false;
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
  
  // Update trader stats if wallet known
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
  
  // Only record significant trades (>$1000)
  if (value < 1000) return;
  
  await query(
    `INSERT INTO trades (wallet_address, symbol, side, size, price, value)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [wallet, symbol, side, size, price, value]
  );
  
  // Update trader stats if wallet known
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

// Clean up old data (keep 30 days of detailed data)
async function cleanupOldData(): Promise<void> {
  try {
    // Keep market stats for 30 days
    await query(
      `DELETE FROM market_stats WHERE timestamp < NOW() - INTERVAL '30 days'`
    );
    
    // Keep snapshots for 30 days
    await query(
      `DELETE FROM trader_snapshots WHERE timestamp < NOW() - INTERVAL '30 days'`
    );
    
    // Keep liquidations forever (or limit to 90 days if needed)
    // await query(`DELETE FROM liquidations WHERE timestamp < NOW() - INTERVAL '90 days'`);
    
    console.log('🧹 Cleaned up old data');
  } catch (error) {
    console.error('❌ Failed to cleanup old data:', error);
  }
}

// Start all cron jobs
export function startDataCollector(): void {
  console.log('🚀 Starting data collector...');
  
  // Load existing wallets
  loadTrackedWallets();
  
  // Collect market stats every minute
  cron.schedule('* * * * *', () => {
    collectMarketStats();
  });
  
  // Update trader snapshots every 5 minutes
  cron.schedule('*/5 * * * *', () => {
    updateTraderSnapshots();
  });
  
  // Discover new wallets every 10 minutes
  cron.schedule('*/10 * * * *', () => {
    discoverNewWallets();
  });
  
  // Clean up old data daily at 3am
  cron.schedule('0 3 * * *', () => {
    cleanupOldData();
  });
  
  // Run initial collection
  collectMarketStats();
  
  console.log('✅ Data collector started');
}
