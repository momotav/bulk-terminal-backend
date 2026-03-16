import WebSocket from 'ws';
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';

const WS_URL = process.env.BULK_WS_URL || 'wss://exchange-ws1.bulk.trade';

let ws: WebSocket | null = null;
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
let heartbeatInterval: ReturnType<typeof setInterval> | null = null;
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
  lastTradeTime: null as Date | null,
  lastLiquidationTime: null as Date | null,
  lastAdlTime: null as Date | null,
};

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
  
  // Record all trades (minimum $1 to filter dust)
  if (value < 1) return;

  const walletAddress = trade.taker || trade.maker || null;
  
  try {
    // Insert trade
    await query(
      `INSERT INTO trades (wallet_address, symbol, side, size, price, value, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7/1000.0))`,
      [walletAddress, trade.symbol, trade.side, Math.abs(trade.size), trade.price, value, trade.time]
    );

    // Update trader stats if wallet known
    if (walletAddress) {
      await query(
        `INSERT INTO traders (wallet_address, total_trades, total_volume, last_seen)
         VALUES ($1, 1, $2, NOW())
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_trades = traders.total_trades + 1,
           total_volume = traders.total_volume + $2,
           last_seen = NOW()`,
        [walletAddress, value]
      );
      
      // Create notifications for users following this wallet
      await query(
        `INSERT INTO notifications (user_id, wallet_address, type, symbol, side, size, price, value)
         SELECT user_id, $1, 'trade', $2, $3, $4, $5, $6
         FROM watchlist WHERE wallet_address = $1`,
        [walletAddress, trade.symbol, trade.side, Math.abs(trade.size), trade.price, value]
      );
      
      // Fetch full wallet PnL from BULK API (async, don't wait)
      fetchAndStoreWalletData(walletAddress).catch(() => {});
    }

    stats.tradesReceived++;
    stats.lastTradeTime = new Date();

    console.log(`📈 Trade: ${trade.side.toUpperCase()} ${trade.symbol} | $${value.toFixed(2)}`);
  } catch (error) {
    console.error('Failed to record trade:', error);
  }
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
       VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7/1000.0))
       ON CONFLICT DO NOTHING`,
      [walletAddress, liq.symbol, liq.side, Math.abs(liq.size), liq.price, value, liq.time]
    );

    // Update trader stats if wallet known
    if (walletAddress) {
      await query(
        `INSERT INTO traders (wallet_address, total_liquidations, liquidation_value, last_seen)
         VALUES ($1, 1, $2, NOW())
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_liquidations = traders.total_liquidations + 1,
           liquidation_value = traders.liquidation_value + $2,
           last_seen = NOW()`,
        [walletAddress, value]
      );
      
      // Create notifications for users following this wallet
      await query(
        `INSERT INTO notifications (user_id, wallet_address, type, symbol, side, size, price, value)
         SELECT user_id, $1, 'liquidation', $2, $3, $4, $5, $6
         FROM watchlist WHERE wallet_address = $1`,
        [walletAddress, liq.symbol, liq.side, Math.abs(liq.size), liq.price, value]
      );
      
      // Auto-track liquidated wallets
      addWalletToTrack(walletAddress).catch(() => {});
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
       VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8/1000.0))
       ON CONFLICT DO NOTHING`,
      [walletAddress, counterparty, adl.symbol, adl.side, Math.abs(adl.size), adl.price, value, adl.time]
    );

    // Update trader stats if wallet known
    if (walletAddress) {
      await query(
        `INSERT INTO traders (wallet_address, total_adl, adl_value, last_seen)
         VALUES ($1, 1, $2, NOW())
         ON CONFLICT (wallet_address) DO UPDATE SET
           total_adl = COALESCE(traders.total_adl, 0) + 1,
           adl_value = COALESCE(traders.adl_value, 0) + $2,
           last_seen = NOW()`,
        [walletAddress, value]
      );
      
      // Create notifications for users following this wallet
      await query(
        `INSERT INTO notifications (user_id, wallet_address, type, symbol, side, size, price, value)
         SELECT user_id, $1, 'adl', $2, $3, $4, $5, $6
         FROM watchlist WHERE wallet_address = $1`,
        [walletAddress, adl.symbol, adl.side, Math.abs(adl.size), adl.price, value]
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
    
    // Log non-trade messages for debugging (limit to first 10)
    if (stats.tradesReceived < 10) {
      console.log(`📨 Raw message:`, JSON.stringify(message).slice(0, 500));
    }

    // Handle BULK format: { type: 'trades', data: { trades: [...] } }
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
          console.log(`🔥 LIQUIDATION detected in trade: ${side} ${symbol} | $${(price * size).toFixed(2)}`);
          recordLiquidation({
            symbol,
            price,
            size,
            side: side === 'buy' ? 'short' : 'long', // If liquidation buys, it's closing a short
            wallet: walletAddress,
            time,
          });
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
    if (message.type === 'trades' || message.e === 'trade') {
      const trades = message.data?.trades || message.trades || [message];
      for (const trade of trades) {
        recordTrade({
          symbol: trade.s || trade.symbol || message.symbol || 'UNKNOWN',
          price: parseFloat(trade.px || trade.p || trade.price),
          size: parseFloat(trade.sz || trade.q || trade.size || trade.qty),
          side: trade.side === true || trade.side === 'B' || trade.side === 'buy' ? 'buy' : 'sell',
          maker: trade.maker,
          taker: trade.taker,
          time: trade.time || trade.T || Date.now(),
        });
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

    // Handle subscription confirmations
    if (message.channel === 'subscriptionResponse') {
      console.log(`✅ Subscription confirmed:`, JSON.stringify(message.data?.subscription || message));
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

// Start heartbeat - just keep connection alive, no ping needed
function startHeartbeat(): void {
  // BULK API doesn't support ping method, connection stays alive automatically
  // We'll just monitor for disconnects via the close event
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

      // BULK expects subscription as an array, not object
      // Format: { "method": "subscribe", "subscription": [{"type": "trades", "coin": "BTC"}] }
      const symbols = ['BTC', 'ETH', 'SOL'];
      
      try {
        // Subscribe to all trades at once
        // BULK uses 'symbol' not 'coin', and format like 'BTC-USD'
        const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
        
        // Subscribe to trades
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: symbols.map(symbol => ({ type: 'trades', symbol }))
        }));
        
        console.log('📡 Sent subscription for trades: BTC-USD, ETH-USD, SOL-USD');
        
        // Also try subscribing to liquidations (if BULK supports it)
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: symbols.map(symbol => ({ type: 'liquidations', symbol }))
        }));
        
        console.log('📡 Sent subscription for liquidations: BTC-USD, ETH-USD, SOL-USD');
        
        // Alternative liquidation subscription formats BULK might use
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: [{ type: 'liquidations' }]
        }));
        
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: [{ type: 'liquidation' }]
        }));
        
        // Subscribe to ADL events
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: symbols.map(symbol => ({ type: 'adl', symbol }))
        }));
        
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: [{ type: 'adl' }]
        }));
        
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: [{ type: 'auto_deleverage' }]
        }));
        
        console.log('📡 Sent subscription for ADL events');
        
      } catch (e) {
        console.error('Failed to subscribe:', e);
      }

      startHeartbeat();
    });

    ws.on('message', (data: WebSocket.Data) => {
      processMessage(data);
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

// Start WebSocket listener
export function startWebSocketListener(): void {
  console.log('🚀 Starting WebSocket listener...');
  setTimeout(() => {
    connect();
  }, 3000);
}

// Stop WebSocket listener
export function stopWebSocketListener(): void {
  stopHeartbeat();
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
