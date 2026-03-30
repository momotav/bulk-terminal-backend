import WebSocket from 'ws';
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';

const WS_URL = process.env.BULK_WS_URL || 'wss://exchange-ws1.bulk.trade';
const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

let ws: WebSocket | null = null;
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
let heartbeatInterval: ReturnType<typeof setInterval> | null = null;
let tickerSnapshotInterval: ReturnType<typeof setInterval> | null = null;
let isConnected = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

// Cache to avoid fetching same wallet too often (wallet -> last fetch time)
const walletFetchCache: Map<string, number> = new Map();
const FETCH_COOLDOWN = 60000; // Only fetch wallet data once per minute

// Stats for logging
const stats = {
  tradesReceived: 0,
  liquidationsReceived: 0,
  adlReceived: 0,
  tickerSnapshots: 0,
  lastTradeTime: null as Date | null,
  lastLiquidationTime: null as Date | null,
  lastAdlTime: null as Date | null,
  lastTickerSnapshot: null as Date | null,
};

// ============ TICKER SNAPSHOTS FOR REAL OI/FUNDING HISTORY ============

// Fetch and store ticker snapshots (OI, funding rate) every minute
interface TickerData {
  openInterest?: string | number;
  markPrice?: string | number;
  lastPrice?: string | number;
  fundingRate?: string | number;
}

// Track last ticker snapshot time per symbol to avoid duplicates
const lastTickerSnapshotTime: Map<string, number> = new Map();
const TICKER_SNAPSHOT_INTERVAL = 60000; // Save max 1 snapshot per minute per symbol

// Record ticker snapshot from WebSocket (real-time)
async function recordTickerSnapshot(
  symbol: string, 
  openInterestCoins: number, 
  openInterestUsd: number, 
  fundingRate: number, 
  markPrice: number
): Promise<void> {
  // Rate limit: only save 1 snapshot per minute per symbol
  const lastTime = lastTickerSnapshotTime.get(symbol) || 0;
  if (Date.now() - lastTime < TICKER_SNAPSHOT_INTERVAL) {
    return; // Skip, too recent
  }
  
  try {
    await query(
      `INSERT INTO ticker_snapshots (symbol, open_interest_coins, open_interest_usd, funding_rate, mark_price, timestamp)
       VALUES ($1, $2, $3, $4, $5, NOW())`,
      [symbol, openInterestCoins, openInterestUsd, fundingRate, markPrice]
    );
    
    lastTickerSnapshotTime.set(symbol, Date.now());
    stats.tickerSnapshots++;
    stats.lastTickerSnapshot = new Date();
    
    console.log(`📊 Ticker (WS): ${symbol} | OI: $${openInterestUsd.toFixed(0)} | Funding: ${(fundingRate * 100).toFixed(4)}%`);
  } catch (error) {
    console.error(`Failed to save ticker snapshot for ${symbol}:`, error);
  }
}

// FALLBACK: Fetch tickers via REST API (used if WebSocket ticker subscription fails)
async function snapshotTickersFallback(): Promise<void> {
  // All available BULK markets
  const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'GOLD-USD', 'XRP-USD'];
  
  for (const symbol of symbols) {
    // Skip if we got a recent WebSocket update
    const lastTime = lastTickerSnapshotTime.get(symbol) || 0;
    if (Date.now() - lastTime < TICKER_SNAPSHOT_INTERVAL) {
      continue; // Already have recent data from WebSocket
    }
    
    try {
      const res = await fetch(`${BULK_API_BASE}/ticker/${symbol}`);
      if (!res.ok) continue;
      
      const ticker = await res.json() as TickerData;
      
      const openInterestCoins = parseFloat(String(ticker.openInterest || 0));
      const markPrice = parseFloat(String(ticker.markPrice || ticker.lastPrice || 0));
      const openInterestUsd = openInterestCoins * markPrice;
      const fundingRate = parseFloat(String(ticker.fundingRate || 0));
      
      // Store snapshot in database
      await query(
        `INSERT INTO ticker_snapshots (symbol, open_interest_coins, open_interest_usd, funding_rate, mark_price, timestamp)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [symbol, openInterestCoins, openInterestUsd, fundingRate, markPrice]
      );
      
      lastTickerSnapshotTime.set(symbol, Date.now());
      stats.tickerSnapshots++;
      stats.lastTickerSnapshot = new Date();
      
      console.log(`📊 Ticker (REST fallback): ${symbol} | OI: $${openInterestUsd.toFixed(0)} | Funding: ${(fundingRate * 100).toFixed(4)}%`);
    } catch (error) {
      console.error(`Failed to snapshot ticker for ${symbol}:`, error);
    }
  }
}

// Start ticker snapshot collection (fallback for when WebSocket is down)
function startTickerSnapshots(): void {
  // Take initial snapshot via REST API
  snapshotTickersFallback();
  
  // Then every minute as fallback (WebSocket should provide real-time updates)
  tickerSnapshotInterval = setInterval(snapshotTickersFallback, 60 * 1000);
  console.log('📊 Started ticker snapshot fallback (REST API every 1 minute)');
  console.log('📊 Primary source: WebSocket ticker subscription (real-time)');
}

// Stop ticker snapshot collection
function stopTickerSnapshots(): void {
  if (tickerSnapshotInterval) {
    clearInterval(tickerSnapshotInterval);
    tickerSnapshotInterval = null;
  }
}

// Start ticker polling via REST API (primary mode when WebSocket is unstable)
function startTickerPolling(): void {
  console.log('📊 Starting REST API ticker polling (every 30 seconds)');
  
  // Take initial snapshot immediately
  snapshotTickersFallback();
  
  // Poll every 30 seconds (more frequent than fallback since it's primary)
  if (!tickerSnapshotInterval) {
    tickerSnapshotInterval = setInterval(snapshotTickersFallback, 30 * 1000);
  }
}

// Fetch wallet PnL from BULK API and store it
async function fetchAndStoreWalletData(walletAddress: string): Promise<void> {
  // Check cache to avoid spamming API
  const lastFetch = walletFetchCache.get(walletAddress) || 0;
  if (Date.now() - lastFetch < FETCH_COOLDOWN) {
    return; // Skip, fetched recently
  }
  
  walletFetchCache.set(walletAddress, Date.now());
  
  try {
    const account = await bulkApi.getFullAccount(walletAddress);
    if (!account) {
      console.log(`⚠️ No account data for ${walletAddress.slice(0, 8)}...`);
      return;
    }
    
    // Calculate totals from positions (the actual data source)
    let totalNotional = 0;
    let totalRealizedPnl = 0;
    let totalUnrealizedPnl = 0;
    
    for (const p of account.positions) {
      // Notional is already provided, use absolute value
      totalNotional += Math.abs(p.notional || 0);
      totalRealizedPnl += p.realizedPnl || 0;
      totalUnrealizedPnl += p.unrealizedPnl || 0;
    }
    
    // Also check margin object if it has better totals
    const marginRealizedPnl = account.margin?.realizedPnl || 0;
    const marginUnrealizedPnl = account.margin?.unrealizedPnl || 0;
    
    // Use whichever source has data (prefer margin totals if available)
    const realizedPnl = marginRealizedPnl !== 0 ? marginRealizedPnl : totalRealizedPnl;
    const unrealizedPnl = marginUnrealizedPnl !== 0 ? marginUnrealizedPnl : totalUnrealizedPnl;
    const totalPnl = realizedPnl + unrealizedPnl;
    
    // Upsert trader with PnL data
    await query(
      `INSERT INTO traders (wallet_address, total_pnl, last_seen)
       VALUES ($1, $2, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET
         total_pnl = $2,
         last_seen = NOW()`,
      [walletAddress, totalPnl]
    );
    
    // Store snapshot for history
    await query(
      `INSERT INTO trader_snapshots 
       (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
       VALUES ($1, $2, $3, $4, $5)`,
      [walletAddress, realizedPnl, unrealizedPnl, account.positions.length, totalNotional]
    );
    
    console.log(`💰 ${walletAddress.slice(0, 8)}...: PnL=$${totalPnl.toFixed(2)} | Notional=$${totalNotional.toFixed(2)} | Positions=${account.positions.length}`);
  } catch (error) {
    console.error(`❌ Error fetching ${walletAddress.slice(0, 8)}...:`, error);
  }
}

// Export for use in dataCollector
export function addWalletToTrack(wallet: string): Promise<void> {
  return fetchAndStoreWalletData(wallet);
}

// Record a trade to database
// ============================================
// PRODUCTION-GRADE TRADE BATCHING SYSTEM
// ============================================

// Trade queue for batching inserts
const tradeQueue: Array<{
  symbol: string;
  price: number;
  size: number;
  side: string;
  walletAddress: string | null;
  value: number;
  time: number;
}> = [];

// Prevent concurrent batch processing
let isProcessingBatch = false;

// Configuration
const BATCH_CONFIG = {
  MAX_BATCH_SIZE: 100,        // Max trades per batch insert
  FLUSH_INTERVAL_MS: 5000,    // Flush every 5 seconds
  MAX_QUEUE_SIZE: 5000,       // Drop oldest if queue exceeds this
  SKIP_TRADER_UPDATES: false, // Set true if DB is overwhelmed
};

// Aggregate trader stats in memory, flush periodically
const traderStatsBuffer: Map<string, { trades: number; volume: number }> = new Map();
let lastTraderFlush = Date.now();
const TRADER_FLUSH_INTERVAL = 30000; // Flush trader stats every 30 seconds

// Process trade batch with size limits
async function processTradeBatch(): Promise<void> {
  if (isProcessingBatch || tradeQueue.length === 0) return;
  
  isProcessingBatch = true;
  
  try {
    // Take only up to MAX_BATCH_SIZE trades
    const batchSize = Math.min(tradeQueue.length, BATCH_CONFIG.MAX_BATCH_SIZE);
    const trades = tradeQueue.splice(0, batchSize);
    
    if (trades.length === 0) return;
    
    // Build batch insert query
    const values = trades.map((t, i) => {
      const offset = i * 7;
      return `($${offset + 1}::varchar, $${offset + 2}::varchar, $${offset + 3}::varchar, $${offset + 4}, $${offset + 5}, $${offset + 6}, to_timestamp($${offset + 7}/1000.0))`;
    }).join(', ');
    
    const params = trades.flatMap(t => [
      t.walletAddress, t.symbol, t.side, Math.abs(t.size), t.price, t.value, t.time
    ]);
    
    // Single batch insert for all trades
    await query(
      `INSERT INTO trades (wallet_address, symbol, side, size, price, value, timestamp) VALUES ${values}`,
      params
    );
    
    // Aggregate trader stats in memory (don't write to DB yet)
    for (const t of trades) {
      if (t.walletAddress) {
        const existing = traderStatsBuffer.get(t.walletAddress) || { trades: 0, volume: 0 };
        existing.trades++;
        existing.volume += t.value;
        traderStatsBuffer.set(t.walletAddress, existing);
      }
    }
    
    console.log(`💾 Inserted ${trades.length} trades | Queue: ${tradeQueue.length} | Traders buffered: ${traderStatsBuffer.size}`);
    
  } catch (error) {
    console.error('❌ Batch insert failed:', error);
  } finally {
    isProcessingBatch = false;
  }
}

// Flush aggregated trader stats to DB (runs less frequently)
async function flushTraderStats(): Promise<void> {
  if (traderStatsBuffer.size === 0) return;
  
  const now = Date.now();
  if (now - lastTraderFlush < TRADER_FLUSH_INTERVAL) return;
  
  lastTraderFlush = now;
  
  // Take a snapshot and clear buffer
  const updates = Array.from(traderStatsBuffer.entries());
  traderStatsBuffer.clear();
  
  console.log(`📊 Flushing ${updates.length} trader stats...`);
  
  // Process in small chunks to avoid overwhelming DB
  const CHUNK_SIZE = 20;
  for (let i = 0; i < updates.length; i += CHUNK_SIZE) {
    const chunk = updates.slice(i, i + CHUNK_SIZE);
    
    // Use a single query with UNNEST for bulk upsert
    try {
      const wallets = chunk.map(([w]) => w);
      const tradeCounts = chunk.map(([, d]) => d.trades);
      const volumes = chunk.map(([, d]) => d.volume);
      
      await query(
        `INSERT INTO traders (wallet_address, total_trades, total_volume, last_seen)
         SELECT unnest($1::varchar[]), unnest($2::int[]), unnest($3::numeric[]), NOW()
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_trades = traders.total_trades + EXCLUDED.total_trades,
           total_volume = traders.total_volume + EXCLUDED.total_volume,
           last_seen = NOW()`,
        [wallets, tradeCounts, volumes]
      );
    } catch (error) {
      console.error('❌ Trader stats chunk failed:', error);
    }
    
    // Small delay between chunks
    await new Promise(r => setTimeout(r, 100));
  }
  
  console.log(`✅ Flushed ${updates.length} trader stats`);
}

// Main flush interval - process trades
setInterval(() => {
  processTradeBatch().catch(console.error);
}, BATCH_CONFIG.FLUSH_INTERVAL_MS);

// Separate interval for trader stats (less frequent)
setInterval(() => {
  flushTraderStats().catch(console.error);
}, TRADER_FLUSH_INTERVAL);

// Queue size protection - drop oldest if too large
setInterval(() => {
  if (tradeQueue.length > BATCH_CONFIG.MAX_QUEUE_SIZE) {
    const dropped = tradeQueue.length - BATCH_CONFIG.MAX_QUEUE_SIZE;
    tradeQueue.splice(0, dropped);
    console.warn(`⚠️ Queue overflow! Dropped ${dropped} oldest trades`);
  }
}, 10000);

async function recordTrade(trade: {
  symbol: string;
  price: number;
  size: number;
  side: string;
  maker?: string;
  taker?: string;
  time: number;
}): Promise<void> {
  const value = trade.price * Math.abs(trade.size);
  
  // Filter dust trades
  if (value < 1) return;

  const walletAddress = trade.taker || trade.maker || null;
  
  // Add to queue (non-blocking)
  tradeQueue.push({
    symbol: trade.symbol,
    price: trade.price,
    size: trade.size,
    side: trade.side,
    walletAddress,
    value,
    time: trade.time
  });

  stats.tradesReceived++;
  stats.lastTradeTime = new Date();
}

// Record a liquidation to database
async function recordLiquidation(liq: {
  symbol: string;
  price: number;
  size: number;
  side: string;
  wallet?: string;
  time: number;
}): Promise<void> {
  const value = liq.price * Math.abs(liq.size);
  const walletAddress = liq.wallet || null;

  try {
    // Insert liquidation
    await query(
      `INSERT INTO liquidations (wallet_address, symbol, side, size, price, value, timestamp)
       VALUES ($1::varchar, $2::varchar, $3::varchar, $4, $5, $6, to_timestamp($7/1000.0))
       ON CONFLICT DO NOTHING`,
      [walletAddress, liq.symbol, liq.side, Math.abs(liq.size), liq.price, value, liq.time]
    );

    // Update trader stats if wallet known (single query, no notifications to save connections)
    if (walletAddress) {
      await query(
        `INSERT INTO traders (wallet_address, total_liquidations, liquidation_value, last_seen)
         VALUES ($1::varchar, 1, $2, NOW())
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_liquidations = traders.total_liquidations + 1,
           liquidation_value = traders.liquidation_value + $2,
           last_seen = NOW()`,
        [walletAddress, value]
      );
    }

    stats.liquidationsReceived++;
    stats.lastLiquidationTime = new Date();
    
    console.log(`🔥 LIQUIDATION: ${liq.side} ${liq.symbol} | $${value.toFixed(2)} | ${walletAddress || 'unknown'}`);
  } catch (error) {
    console.error('Failed to record liquidation:', error);
  }
}

// Record an ADL (Auto-Deleveraging) event to database
async function recordADL(adl: {
  symbol: string;
  price: number;
  size: number;
  side: string;
  wallet?: string;
  counterparty?: string;
  time: number;
}): Promise<void> {
  const value = adl.price * Math.abs(adl.size);
  const walletAddress = adl.wallet || null;
  const counterparty = adl.counterparty || null;

  try {
    // Insert ADL event
    await query(
      `INSERT INTO adl_events (wallet_address, counterparty, symbol, side, size, price, value, timestamp)
       VALUES ($1::varchar, $2::varchar, $3::varchar, $4::varchar, $5, $6, $7, to_timestamp($8/1000.0))
       ON CONFLICT DO NOTHING`,
      [walletAddress, counterparty, adl.symbol, adl.side, Math.abs(adl.size), adl.price, value, adl.time]
    );

    // Update trader stats if wallet known
    if (walletAddress) {
      await query(
        `INSERT INTO traders (wallet_address, total_adl, adl_value, last_seen)
         VALUES ($1::varchar, 1, $2, NOW())
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_adl = COALESCE(traders.total_adl, 0) + 1,
           adl_value = COALESCE(traders.adl_value, 0) + $2,
           last_seen = NOW()`,
        [walletAddress, value]
      );
      
      // Create notifications for users following this wallet (use separate params)
      await query(
        `INSERT INTO notifications (user_id, wallet_address, type, symbol, side, size, price, value)
         SELECT user_id, $1::varchar, 'adl', $2::varchar, $3::varchar, $4, $5, $6
         FROM watchlist WHERE wallet_address = $7::varchar`,
        [walletAddress, adl.symbol, adl.side, Math.abs(adl.size), adl.price, value, walletAddress]
      );
    }

    stats.adlReceived++;
    stats.lastAdlTime = new Date();
    
    console.log(`⚡ ADL: ${adl.side} ${adl.symbol} | $${value.toFixed(2)} | ${walletAddress || 'unknown'} -> ${counterparty || 'unknown'}`);
  } catch (error) {
    console.error('Failed to record ADL:', error);
  }
}

// Process incoming WebSocket message
function processMessage(data: WebSocket.Data): void {
  try {
    const message = JSON.parse(data.toString());
    
    // FILTER 1: Ignore order book data (arrays of resting orders)
    // These have "status": "resting" and "filledSize": 0
    if (Array.isArray(message)) {
      // Check if this looks like order book data
      if (message.length > 0 && message[0]?.status === 'resting') {
        // Silently ignore order book snapshots - these are not trades
        return;
      }
    }
    
    // FILTER 2: Ignore single order objects with status "resting"
    if (message.status === 'resting' || message.filledSize === 0) {
      return;
    }
    
    // Log all messages for first 20 to help debug format
    if (stats.tradesReceived + stats.tickerSnapshots < 20) {
      console.log(`📨 Raw message (type=${message.type}, topic=${message.topic}):`, JSON.stringify(message).slice(0, 800));
    }

    // ============ HANDLE TICKER MESSAGES (Real-time OI, Funding, Price) ============
    // Format: { type: 'ticker', topic: 'ticker.BTC-USD', data: { ticker: {...} } }
    if (message.type === 'ticker' || (message.topic && message.topic.startsWith('ticker.'))) {
      const tickerData = message.data?.ticker || message.data || message;
      const symbol = tickerData.symbol || message.topic?.replace('ticker.', '') || 'UNKNOWN';
      
      const openInterestCoins = parseFloat(String(tickerData.openInterest || 0));
      const markPrice = parseFloat(String(tickerData.markPrice || tickerData.lastPrice || 0));
      const openInterestUsd = openInterestCoins * markPrice;
      const fundingRate = parseFloat(String(tickerData.fundingRate || 0));
      
      if (markPrice > 0) {
        // Save to ticker_snapshots (same as before, but now real-time!)
        recordTickerSnapshot(symbol, openInterestCoins, openInterestUsd, fundingRate, markPrice);
      }
      return;
    }

    // ============ HANDLE TRADES MESSAGES ============
    // Handle BULK trades format: { type: 'trades', topic: 'trades.BTC-USD', data: { trades: [...] } }
    // OR: { type: 'trades', data: [...] } (array directly in data)
    if (message.type === 'trades' || (message.topic && message.topic.startsWith('trades.'))) {
      // Extract trades array - handle both formats
      let trades: any[] = [];
      
      if (message.data?.trades && Array.isArray(message.data.trades)) {
        trades = message.data.trades;
      } else if (Array.isArray(message.data)) {
        trades = message.data;
      } else if (message.trades && Array.isArray(message.trades)) {
        trades = message.trades;
      } else if (message.data && typeof message.data === 'object' && !Array.isArray(message.data)) {
        // Single trade object
        trades = [message.data];
      }
      
      // Get symbol from topic if not in trades
      const topicSymbol = message.topic?.replace('trades.', '') || null;
      
      if (trades.length === 0) {
        console.log(`⚠️ Trades message but no trades found:`, JSON.stringify(message).slice(0, 500));
        return;
      }
      
      console.log(`📊 Processing ${trades.length} trades from ${topicSymbol || 'unknown'}`);
      
      for (const trade of trades) {
        // Skip if this is a resting order, not a fill
        if (trade.status === 'resting' || trade.filledSize === 0) {
          continue;
        }
        
        // Extract data from BULK format
        const symbol = trade.s || trade.symbol || topicSymbol || 'UNKNOWN';
        const price = parseFloat(trade.px || trade.price || trade.p || 0);
        const size = parseFloat(trade.sz || trade.size || trade.q || trade.qty || 0);
        const time = trade.time || trade.T || trade.timestamp || Date.now();
        
        // Handle side - BULK might use boolean, string, or 'B'/'S'
        let side = 'buy';
        if (trade.side === false || trade.side === 'S' || trade.side === 'sell' || trade.side === 'short') {
          side = 'sell';
        }
        
        const maker = trade.maker || null;
        const taker = trade.taker || null;
        const walletAddress = taker || maker || null;
        
        // Check reason field (BULK API v1.0.12)
        const reason = trade.reason || null;
        
        // Check for liquidation
        // Per BULK API docs: reason="liquidation" and/or liq=true
        const isLiquidation = reason === 'liquidation' || 
                              trade.liq === true ||
                              trade.liquidation || trade.isLiquidation || 
                              trade.orderType === 'liquidation' || 
                              trade.type === 'liquidation' ||
                              (trade.reduceOnly && trade.forcedLiquidation);
        
        // Check for ADL
        const isADL = reason === 'adl' ||
                      trade.adl || trade.isAdl || trade.isADL || 
                      trade.orderType === 'adl' || trade.type === 'adl' ||
                      trade.autoDeleverage || trade.auto_deleverage;
        
        if (price <= 0 || size <= 0) {
          console.log(`⚠️ Invalid trade data: price=${price}, size=${size}`);
          continue;
        }
        
        if (isADL) {
          console.log(`⚡ ADL detected: ${side} ${symbol} | $${(price * size).toFixed(2)}`);
          recordADL({
            symbol,
            price,
            size,
            side: side === 'buy' ? 'short' : 'long',
            wallet: walletAddress,
            counterparty: trade.counterparty || trade.reducer || (maker === walletAddress ? taker : maker),
            time,
          });
        } else if (isLiquidation) {
          // IMPORTANT: Only record the TAKER - they are the liquidated party
          // The maker is just a counterparty filling the liquidation order
          // Per BULK API docs: taker = liquidated user, reason = "liquidation"
          if (taker) {
            console.log(`🔥 LIQUIDATION detected: ${side} ${symbol} | $${(price * size).toFixed(2)} | taker=${taker}`);
            recordLiquidation({
              symbol,
              price,
              size,
              side: side === 'buy' ? 'short' : 'long',
              wallet: taker,  // Always use taker, not walletAddress
              time,
            });
          } else {
            console.log(`⚠️ LIQUIDATION skipped (no taker): ${side} ${symbol} | $${(price * size).toFixed(2)}`);
          }
        } else {
          recordTrade({
            symbol,
            price,
            size,
            side,
            maker,
            taker: walletAddress,
            time,
          });
        }
      }
      return;
    }

    // LEGACY: Handle BULK format: { type: 'trades', data: { trades: [...] } }
    if (message.type === 'trades' && message.data?.trades) {
      const trades = message.data.trades;
      
      // Log first few trades to see all fields (for debugging ADL/liquidation detection)
      if (stats.tradesReceived < 5) {
        console.log(`🔍 Trade fields:`, JSON.stringify(trades[0], null, 2));
      }
      
      for (const trade of trades) {
        // Skip if this is a resting order, not a fill
        if (trade.status === 'resting' || trade.filledSize === 0) {
          continue;
        }
        
        // Extract data from BULK format
        const symbol = trade.s || trade.symbol || 'UNKNOWN';  // "ETH-USD"
        const price = parseFloat(trade.px || trade.price);    // 2087.25
        const size = parseFloat(trade.sz || trade.size);     // 0.024
        const time = trade.time || Date.now();
        const side = trade.side === true ? 'buy' : 'sell';
        const maker = trade.maker || null;
        const taker = trade.taker || null;
        
        // Use taker as the primary wallet (they initiated the trade)
        const walletAddress = taker || maker || null;
        
        // Check if this is a liquidation trade
        // BULK may mark liquidations with: liquidation, isLiquidation, reduceOnly flags, or special order types
        const isLiquidation = trade.liquidation || trade.isLiquidation || 
                              trade.orderType === 'liquidation' || 
                              trade.type === 'liquidation' ||
                              (trade.reduceOnly && trade.forcedLiquidation);
        
        // Check if this is an ADL (Auto-Deleveraging) trade
        // ADL is sent via trades with adl flag per Bulk dev
        const isADL = trade.adl || trade.isAdl || trade.isADL || 
                      trade.orderType === 'adl' || trade.type === 'adl' ||
                      trade.autoDeleverage || trade.auto_deleverage;
        
        if (isADL) {
          console.log(`⚡ ADL detected in trade: ${side} ${symbol} | $${(price * size).toFixed(2)}`);
          recordADL({
            symbol,
            price,
            size,
            side: side === 'buy' ? 'short' : 'long',
            wallet: walletAddress,
            counterparty: trade.counterparty || trade.reducer || (maker === walletAddress ? taker : maker),
            time,
          });
        } else if (isLiquidation) {
          // IMPORTANT: Only record the TAKER - they are the liquidated party
          if (taker) {
            console.log(`🔥 LIQUIDATION detected in trade: ${side} ${symbol} | $${(price * size).toFixed(2)} | taker=${taker}`);
            recordLiquidation({
              symbol,
              price,
              size,
              side: side === 'buy' ? 'short' : 'long', // If liquidation buys, it's closing a short
              wallet: taker,  // Always use taker, not walletAddress
              time,
            });
          } else {
            console.log(`⚠️ LIQUIDATION skipped (no taker): ${side} ${symbol} | $${(price * size).toFixed(2)}`);
          }
        } else {
          console.log(`🔍 Trade: ${side} ${symbol} | price=${price} size=${size} | wallet=${walletAddress?.slice(0,8)}`);
          
          recordTrade({
            symbol,
            price,
            size,
            side,
            maker,
            taker: walletAddress,
            time,
          });
        }
      }
      return;
    }

    // Handle Hyperliquid-style format (channel instead of type)
    if (message.channel === 'trades' && message.data) {
      const trades = Array.isArray(message.data) ? message.data : [message.data];
      
      for (const trade of trades) {
        // Check for ADL first
        const isADL = trade.adl || trade.isAdl || trade.isADL || 
                      trade.orderType === 'adl' || trade.type === 'adl' ||
                      trade.autoDeleverage || trade.auto_deleverage;
        
        if (isADL) {
          recordADL({
            symbol: trade.coin || trade.symbol || 'UNKNOWN',
            price: parseFloat(trade.px) || trade.price,
            size: parseFloat(trade.sz) || trade.size,
            side: trade.side === 'B' ? 'long' : 'short',
            wallet: trade.users?.[0] || trade.user || trade.wallet,
            counterparty: trade.users?.[1] || trade.counterparty,
            time: trade.time || Date.now(),
          });
        } else if (trade.liquidation || trade.isLiquidation) {
          recordLiquidation({
            symbol: trade.coin || trade.symbol || 'UNKNOWN',
            price: parseFloat(trade.px) || trade.price,
            size: parseFloat(trade.sz) || trade.size,
            side: trade.side === 'B' ? 'long' : 'short',
            wallet: trade.users?.[0] || trade.user || trade.wallet,
            time: trade.time || Date.now(),
          });
        } else {
          recordTrade({
            symbol: trade.coin || trade.symbol || 'UNKNOWN',
            price: parseFloat(trade.px) || trade.price,
            size: parseFloat(trade.sz) || trade.size,
            side: trade.side === 'B' ? 'buy' : 'sell',
            taker: trade.users?.[0] || trade.user,
            maker: trade.users?.[1],
            time: trade.time || Date.now(),
          });
        }
      }
      return;
    }

    // Handle generic trade messages
    // BULK API v1.0.12: trades now have optional "reason" field: "liquidation" or "adl"
    if (message.type === 'trades' || message.e === 'trade') {
      const trades = message.data?.trades || message.trades || [message];
      for (const trade of trades) {
        const symbol = trade.s || trade.symbol || message.symbol || 'UNKNOWN';
        const price = parseFloat(trade.px || trade.p || trade.price);
        const size = parseFloat(trade.sz || trade.q || trade.size || trade.qty);
        const side = trade.side === true || trade.side === 'B' || trade.side === 'buy' ? 'buy' : 'sell';
        const reason = trade.reason; // NEW: "liquidation", "adl", or undefined for normal trades
        const maker = trade.maker;
        const taker = trade.taker;
        const time = trade.time || trade.T || Date.now();
        
        // Route based on reason field
        if (reason === 'liquidation' || trade.liq === true) {
          // IMPORTANT: Only record the TAKER - they are the liquidated party
          if (taker) {
            console.log(`🔥 LIQUIDATION (from trades): ${symbol} | $${(price * size).toFixed(2)} | taker=${taker}`);
            recordLiquidation({
              symbol,
              price,
              size,
              side,
              wallet: taker,  // Always use taker, not taker || maker
              time,
            });
          } else {
            console.log(`⚠️ LIQUIDATION skipped (no taker): ${symbol} | $${(price * size).toFixed(2)}`);
          }
        } else if (reason === 'adl') {
          console.log(`⚡ ADL (from trades): ${symbol} | $${(price * size).toFixed(2)}`);
          recordADL({
            symbol,
            price,
            size,
            side,
            wallet: taker,
            counterparty: maker,
            time,
          });
        } else {
          // Normal trade
          recordTrade({
            symbol,
            price,
            size,
            side,
            maker,
            taker,
            time,
          });
        }
      }
      return;
    }

    // Handle liquidation messages (various formats BULK might use)
    if (message.channel === 'liquidation' || message.channel === 'liquidations' ||
        message.type === 'liquidation' || message.type === 'liquidations') {
      
      const liquidations = message.data?.liquidations || message.data || [message];
      const liqArray = Array.isArray(liquidations) ? liquidations : [liquidations];
      
      for (const liq of liqArray) {
        const symbol = liq.s || liq.coin || liq.symbol || 'UNKNOWN';
        const price = parseFloat(liq.px || liq.price || 0);
        const size = parseFloat(liq.sz || liq.size || liq.qty || 0);
        
        if (price > 0 && size > 0) {
          console.log(`🔥 LIQUIDATION message: ${symbol} | $${(price * size).toFixed(2)}`);
          recordLiquidation({
            symbol,
            price,
            size,
            side: liq.side || 'unknown',
            wallet: liq.wallet || liq.user || liq.account || liq.trader,
            time: liq.time || liq.timestamp || Date.now(),
          });
        }
      }
      return;
    }

    // Handle BULK account channel messages (liquidations come through here!)
    // Format: { "type": "account", "data": { "type": "liquidation", ... }, "topic": "account.{wallet}" }
    if (message.type === 'account' && message.data?.type === 'liquidation') {
      const liq = message.data;
      const walletAddress = message.topic?.replace('account.', '') || null;
      const symbol = liq.symbol || 'UNKNOWN';
      const price = parseFloat(liq.price || 0);
      const size = parseFloat(liq.size || 0);
      const side = liq.isBuy ? 'long' : 'short';
      
      if (price > 0 && size > 0) {
        console.log(`🔥 BULK LIQUIDATION: ${walletAddress?.slice(0,8)}... | ${symbol} | $${(price * size).toFixed(2)}`);
        recordLiquidation({
          symbol,
          price,
          size,
          side,
          wallet: walletAddress,
          time: liq.timestamp ? Math.floor(liq.timestamp / 1000000) : Date.now(), // BULK uses nanoseconds
        });
      }
      return;
    }

    // Handle BULK account channel ADL events
    // Format: { "type": "account", "data": { "type": "ADL", ... }, "topic": "account.{wallet}" }
    if (message.type === 'account' && (message.data?.type === 'ADL' || message.data?.type === 'adl')) {
      const adl = message.data;
      const walletAddress = message.topic?.replace('account.', '') || null;
      const symbol = adl.symbol || 'UNKNOWN';
      const price = parseFloat(adl.price || 0);
      const size = parseFloat(adl.size || 0);
      const side = adl.isBuy ? 'long' : 'short';
      
      if (price > 0 && size > 0) {
        console.log(`⚡ BULK ADL: ${walletAddress?.slice(0,8)}... | ${symbol} | $${(price * size).toFixed(2)}`);
        recordADL({
          symbol,
          price,
          size,
          side,
          wallet: walletAddress,
          time: adl.timestamp ? Math.floor(adl.timestamp / 1000000) : Date.now(), // BULK uses nanoseconds
        });
      }
      return;
    }

    // Handle subscription confirmations
    // BULK API v1.0.12: returns { type: "subscriptionResponse", topics: ["ticker.BTC-USD", ...] }
    if (message.type === 'subscriptionResponse' || message.channel === 'subscriptionResponse') {
      const topics = message.topics || message.data?.topics || message.data?.subscription;
      console.log(`✅ Subscription confirmed:`, JSON.stringify(topics || message));
      return;
    }

    // Handle ADL (Auto-Deleveraging) messages
    if (message.channel === 'adl' || message.type === 'adl' || 
        message.channel === 'auto_deleverage' || message.type === 'auto_deleverage') {
      
      const adlEvents = message.data?.adl || message.data || [message];
      const adlArray = Array.isArray(adlEvents) ? adlEvents : [adlEvents];
      
      for (const adl of adlArray) {
        const symbol = adl.s || adl.coin || adl.symbol || 'UNKNOWN';
        const price = parseFloat(adl.px || adl.price || 0);
        const size = parseFloat(adl.sz || adl.size || adl.qty || 0);
        
        if (price > 0 && size > 0) {
          console.log(`⚡ ADL message: ${symbol} | $${(price * size).toFixed(2)}`);
          recordADL({
            symbol,
            price,
            size,
            side: adl.side || 'unknown',
            wallet: adl.wallet || adl.user || adl.account || adl.deleveraged,
            counterparty: adl.counterparty || adl.reducer,
            time: adl.time || adl.timestamp || Date.now(),
          });
        }
      }
      return;
    }

    // Also check if trade has ADL flag (BULK might send ADL as trades with a flag)
    // This is common in perp exchanges

    // Handle pong
    if (message.channel === 'pong' || message.type === 'pong') {
      return;
    }

    // Handle errors
    if (message.error || message.channel === 'error') {
      console.error('WebSocket error message:', message);
      return;
    }

  } catch (error) {
    // Ignore parse errors
  }
}

// Track last pong time for connection health
let lastPongTime: number = Date.now();

// Start heartbeat - respond to server pings (BULK API v1.0.12 requires pong response)
function startHeartbeat(): void {
  // The 'ws' library automatically responds to ping frames with pong
  // But we'll also track connection health
  lastPongTime = Date.now();
  
  // Monitor connection health every 45 seconds (server pings every 30s, timeout at 10s)
  heartbeatInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    
    const timeSinceLastActivity = Date.now() - lastPongTime;
    if (timeSinceLastActivity > 60000) {
      console.warn('⚠️ No ping/pong activity for 60s, connection may be stale');
    }
  }, 45000);
}

// Stop heartbeat
function stopHeartbeat(): void {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
}

// Schedule reconnection
function scheduleReconnect(): void {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }

  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(`❌ Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached. Giving up.`);
    return;
  }

  const delay = Math.min(5000 * Math.pow(2, reconnectAttempts), 60000);
  reconnectAttempts++;
  
  console.log(`🔄 Reconnecting in ${delay/1000}s... (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
  reconnectTimeout = setTimeout(connect, delay);
}

// Connect to WebSocket
function connect(): void {
  if (ws) {
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
      console.log('WebSocket already connected/connecting');
      return;
    }
    try {
      ws.terminate();
    } catch (e) {}
    ws = null;
  }

  console.log(`🔌 Connecting to BULK WebSocket: ${WS_URL}`);

  try {
    ws = new WebSocket(WS_URL, {
      handshakeTimeout: 10000,
      headers: {
        'User-Agent': 'BULK-Terminal/1.0',
      },
    });

    ws.on('open', () => {
      isConnected = true;
      reconnectAttempts = 0;
      console.log('✅ WebSocket connected to BULK Exchange');

      // BULK API v1.0.12 valid streams:
      // - trades (includes liquidations/ADL via "reason" field)
      // - ticker, candle, l2Snapshot, l2Delta, risk, frontendContext
      // - account.{wallet} (for per-wallet events)
      
      // All available BULK markets
      const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'GOLD-USD', 'XRP-USD'];
      
      // Try multiple subscription formats to find what works
      console.log('🧪 Testing WebSocket subscription formats...');
      
      // Start REST API polling as backup
      startTickerPolling();
      
      try {
        // Format 1: Original array format with symbol
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: symbols.map(symbol => ({ type: 'trades', symbol }))
        }));
        console.log('📡 Sent format 1: array with symbol field');
        
        // Send other formats with delays using setTimeout
        setTimeout(() => {
          // Format 2: Single object per symbol (Hyperliquid style)
          for (const symbol of symbols.slice(0, 1)) { // Just try BTC first
            ws?.send(JSON.stringify({
              method: 'subscribe',
              subscription: { type: 'trades', symbol }
            }));
          }
          console.log('📡 Sent format 2: single object with symbol');
        }, 500);
        
        setTimeout(() => {
          // Format 3: With coin instead of symbol
          for (const symbol of symbols.slice(0, 1)) {
            const coin = symbol.replace('-USD', '');
            ws?.send(JSON.stringify({
              method: 'subscribe',
              subscription: { type: 'trades', coin }
            }));
          }
          console.log('📡 Sent format 3: single object with coin');
        }, 1000);
        
        setTimeout(() => {
          // Format 4: Array with coin
          ws?.send(JSON.stringify({
            method: 'subscribe',
            subscription: symbols.map(s => ({ type: 'trades', coin: s.replace('-USD', '') }))
          }));
          console.log('📡 Sent format 4: array with coin field');
        }, 1500);
        
      } catch (e) {
        console.error('Failed to subscribe:', e);
      }

      startHeartbeat();
    });

    ws.on('message', (data: WebSocket.Data) => {
      processMessage(data);
    });

    // BULK API v1.0.12: Server sends ping every 30s, must respond with pong within 10s
    // The 'ws' library automatically sends pong in response to ping frames
    ws.on('ping', () => {
      lastPongTime = Date.now();
      // ws library auto-responds with pong, but we can also do it explicitly
      ws?.pong();
    });

    ws.on('pong', () => {
      lastPongTime = Date.now();
    });

    ws.on('close', (code, reason) => {
      isConnected = false;
      stopHeartbeat();
      console.log(`❌ WebSocket closed: ${code} - ${reason?.toString() || 'No reason'}`);
      scheduleReconnect();
    });

    ws.on('error', (error) => {
      console.error('WebSocket error:', error.message);
    });

  } catch (error) {
    console.error('Failed to create WebSocket:', error);
    scheduleReconnect();
  }
}

// Get connection stats
export function getWebSocketStats() {
  return {
    connected: isConnected,
    reconnectAttempts,
    ...stats,
  };
}

// Set to track which wallets we're subscribed to
const subscribedWallets = new Set<string>();

// Subscribe to a wallet's account channel to receive liquidation events
export function subscribeToWalletAccount(walletAddress: string): void {
  // TODO: BULK API changed - need to figure out correct format for account subscriptions
  // The "user" field error suggests account subscriptions need different format
  // Disabling for now to prevent WebSocket errors
  
  // if (!ws || ws.readyState !== WebSocket.OPEN) {
  //   console.log(`⚠️ Cannot subscribe to ${walletAddress.slice(0,8)}... - WebSocket not connected`);
  //   return;
  // }
  
  // if (subscribedWallets.has(walletAddress)) {
  //   return; // Already subscribed
  // }
  
  // subscribedWallets.add(walletAddress);
  // console.log(`📡 Subscribed to account channel: ${walletAddress.slice(0,8)}...`);
}

// Subscribe to all tracked wallets on connection
async function subscribeToTrackedWallets(): Promise<void> {
  try {
    const result = await query(`SELECT wallet_address FROM traders ORDER BY last_seen DESC LIMIT 100`);
    for (const row of result) {
      subscribeToWalletAccount(row.wallet_address);
    }
    console.log(`📡 Subscribed to ${result.length} tracked wallet accounts`);
  } catch (error) {
    console.error('Failed to subscribe to tracked wallets:', error);
  }
}

// Start WebSocket listener
export function startWebSocketListener(): void {
  console.log('🚀 Starting WebSocket listener...');
  
  // Start ticker snapshot collection immediately
  startTickerSnapshots();
  
  setTimeout(() => {
    connect();
  }, 3000);
}

// Stop WebSocket listener
export function stopWebSocketListener(): void {
  stopHeartbeat();
  stopTickerSnapshots();
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }
  if (ws) {
    try {
      ws.terminate();
    } catch (e) {}
    ws = null;
  }
  isConnected = false;
  console.log('🛑 WebSocket listener stopped');
}
