import WebSocket from 'ws';
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';

const WS_URL = process.env.BULK_WS_URL || 'wss://exchange-wss1.northstarlabs.xyz';

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
  lastTradeTime: null as Date | null,
  lastLiquidationTime: null as Date | null,
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
    if (!account) return;
    
    // Calculate total notional - use notional if exists, otherwise size * price
    const totalNotional = account.positions.reduce((sum, p) => {
      const posNotional = p.notional || (Math.abs(p.size || 0) * (p.price || 0));
      return sum + Math.abs(posNotional);
    }, 0);
    
    const realizedPnl = account.margin.realizedPnl || 0;
    const unrealizedPnl = account.margin.unrealizedPnl || 0;
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
    
    console.log(`💰 Fetched ${walletAddress.slice(0, 8)}...: PnL=$${totalPnl.toFixed(2)} | Notional=$${totalNotional.toFixed(2)} | Positions=${account.positions.length}`);
  } catch (error) {
    // Silently fail - wallet might not exist on BULK
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

// Process incoming WebSocket message
function processMessage(data: WebSocket.Data): void {
  try {
    const message = JSON.parse(data.toString());
    
    // Log messages for debugging
    if (stats.tradesReceived < 5) {
      console.log(`📨 Raw message:`, JSON.stringify(message).slice(0, 500));
    }

    // Handle BULK format: { type: 'trades', data: { trades: [...] } }
    if (message.type === 'trades' && message.data?.trades) {
      const trades = message.data.trades;
      
      for (const trade of trades) {
        // Extract data from BULK format
        const symbol = trade.s || 'UNKNOWN';  // "ETH-USD"
        const price = parseFloat(trade.px);    // 2087.25
        const size = parseFloat(trade.sz);     // 0.024
        const time = trade.time || Date.now();
        const side = trade.side === true ? 'buy' : 'sell';
        const maker = trade.maker || null;
        const taker = trade.taker || null;
        
        // Use taker as the primary wallet (they initiated the trade)
        // Only record once per trade to avoid duplicates
        const walletAddress = taker || maker || null;
        
        console.log(`🔍 Trade: ${side} ${symbol} | price=${price} size=${size} | wallet=${walletAddress?.slice(0,8)}`);
        
        recordTrade({
          symbol,
          price,
          size,
          side,
          maker,
          taker: walletAddress,  // Use single wallet
          time,
        });
      }
      return;
    }

    // Handle Hyperliquid-style format (channel instead of type)
    if (message.channel === 'trades' && message.data) {
      const trades = Array.isArray(message.data) ? message.data : [message.data];
      
      for (const trade of trades) {
        if (trade.liquidation || trade.isLiquidation) {
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

    // Handle liquidation messages
    if (message.channel === 'liquidation' || message.type === 'liquidation') {
      const liq = message.data || message;
      recordLiquidation({
        symbol: liq.coin || liq.symbol || 'UNKNOWN',
        price: parseFloat(liq.px || liq.price),
        size: parseFloat(liq.sz || liq.size),
        side: liq.side || 'unknown',
        wallet: liq.wallet || liq.user,
        time: liq.time || Date.now(),
      });
      return;
    }

    // Handle subscription confirmations
    if (message.channel === 'subscriptionResponse') {
      console.log(`✅ Subscription confirmed:`, JSON.stringify(message.data?.subscription || message));
      return;
    }

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
        
        ws?.send(JSON.stringify({
          method: 'subscribe',
          subscription: symbols.map(symbol => ({ type: 'trades', symbol }))
        }));
        
        console.log('📡 Sent subscription for trades: BTC-USD, ETH-USD, SOL-USD');
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
