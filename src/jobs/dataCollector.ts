import cron from 'node-cron';
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';
import { bulkFetch } from '../services/bulkAuth';

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
//
// Inserts the trade row into the `trades` table for daily-aggregate
// computation (powers the unique-traders / new-users charts on the
// analytics page). Old rows are pruned by `cleanupOldData` after
// aggregation, so this table stays small.
//
// We used to ALSO update `traders.total_trades` and `traders.total_volume`
// here on every event, but the wallet page no longer reads those columns
// (it reads volume/trades from BULK's indexer directly). Those two writes
// were responsible for most of the per-trade DB load and added zero user
// value. Dropped.
//
// We DO maintain a single global counter (`global_stats.total_trades_since_baseline`)
// because the analytics page's "Total Trades" stat used to read
// `COUNT(*) FROM trades` — which became inaccurate when we added 2-day
// retention. The persistent counter + a frozen baseline (set once at
// rollout to 35,160,034) lets us show the true ever-growing count
// without depending on the trades table size.
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

  // Increment the persistent global trade counter. Single row in
  // global_stats with id=1 (enforced by a CHECK constraint). We use
  // ON CONFLICT to make this idempotent during rollout: if the row
  // doesn't exist yet (first run before the migration was applied),
  // we silently no-op rather than crash the WS listener.
  await query(
    `UPDATE global_stats
     SET total_trades_since_baseline = total_trades_since_baseline + 1
     WHERE id = 1`
  ).catch((err: unknown) => {
    // Table might not exist yet during initial deploy. Don't crash;
    // the next deploy after migration is applied will start counting.
    console.error('global_stats increment failed (migration pending?):', err);
  });
}

// Clean up old data
async function cleanupOldData(): Promise<void> {
  try {
    await query(`DELETE FROM market_stats WHERE timestamp < NOW() - INTERVAL '30 days'`);
    await query(`DELETE FROM trader_snapshots WHERE timestamp < NOW() - INTERVAL '30 days'`);

    // Trades retention: 2 days.
    //
    // The trades table feeds (a) live activity feed (last ~50 rows) and
    // (b) daily aggregates (`aggregateDailyStats`) which compute unique
    // traders / new users per day. Once a day's trades have been
    // aggregated into `daily_stats` and `daily_unique_traders`, the
    // raw rows have no further purpose.
    //
    // Two days = "today still aggregating" + "yesterday safety buffer
    // in case the cron missed a run." Wallet page stats no longer read
    // from this table at all (those moved to BULK indexer in Phase 1).
    //
    // Expected impact: trades table shrinks from ~4 GB → ~250 MB after
    // first run. Index storage shrinks proportionally (~5 GB freed).
    //
    // Batched delete: the first run will need to delete ~30M rows
    // (we're catching up on weeks of accumulated data). Doing it in
    // one DELETE would lock the table and stall WS-listener writes.
    // 50k rows per batch keeps each transaction short while still
    // making progress quickly. Loops until no more old rows remain
    // OR the safety cap is reached.
    //
    // Safety cap: 1000 batches * 50k = 50M rows per run. Today's
    // table only has ~35M rows total, so the cap is well above what
    // we'd ever need. Mostly it's there to guarantee the cron doesn't
    // hang forever if something goes wrong.
    let totalDeleted = 0;
    for (let i = 0; i < 1000; i++) {
      const batch = await query<{ count: string }>(
        `WITH deleted AS (
           DELETE FROM trades
           WHERE id IN (
             SELECT id FROM trades
             WHERE timestamp < NOW() - INTERVAL '2 days'
             LIMIT 50000
           )
           RETURNING 1
         )
         SELECT COUNT(*) AS count FROM deleted`
      );
      const batchCount = parseInt(batch[0]?.count || '0');
      totalDeleted += batchCount;
      if (batchCount < 50000) break;  // last batch was partial → done
    }
    console.log(`🧹 Cleaned up old data (deleted ${totalDeleted} trade rows older than 2d)`);
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
    const res = await bulkFetch(`${BULK_API_BASE}/feeState`);
    if (!res.ok) {
      console.error('Failed to fetch fee state from BULK API');
      return;
    }
    
    const feeState = await res.json() as any;
    
    // BULK changed `/feeState` field names to camelCase in late April 2026.
    // The cron silently inserted zero rows for 9 days (April 27 → May 6)
    // because these reads returned undefined → fallback to 0. We now try
    // both camelCase first (current) and snake_case as fallback (so this
    // continues working if BULK ever reverts or proxies cache stale data).
    const totalMakerFees = feeState.totalMakerFees ?? feeState.total_maker_fees ?? 0;
    const totalTakerFees = feeState.totalTakerFees ?? feeState.total_taker_fees ?? 0;
    const totalProtocolSettlement =
      feeState.totalProtocolSettlement ?? feeState.total_protocol_settlement ?? 0;
    const settledFills = feeState.settledFills ?? feeState.settled_fills ?? 0;
    
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

  // [Removed] updateTraderSnapshots hourly cron — the PnL history chart
  // is now derived from BULK closed-positions at query time, so we no
  // longer write to the `trader_snapshots` table. See deriveHistory-
  // FromClosedPositions in routes/wallet.ts. The DELETE cleanup below
  // still ages out rows that the OLD path wrote so the table doesn't
  // bloat indefinitely.

  // Aggregate daily stats every hour at :05
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
  
  console.log('✅ Data collector started (market stats + daily aggregation + fee tracking)');
}
