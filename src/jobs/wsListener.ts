import WebSocket from 'ws';
import { query } from '../db';
import { addWalletToTrack } from './dataCollector';

const WS_URL = process.env.BULK_WS_URL || 'wss://exchange-wss1.northstarlabs.xyz';

let ws: WebSocket | null = null;
let reconnectTimeout: NodeJS.Timeout | null = null;
let isConnected = false;

// Stats for logging
let stats = {
  tradesReceived: 0,
  liquidationsReceived: 0,
  lastTradeTime: null as Date | null,
  lastLiquidationTime: null as Date | null,
};

// Record a trade to database
async function recordTrade(trade: {
  symbol: string;
  price: number;
  size: number;
  side: boolean;
  maker?: string;
  taker?: string;
  reason?: string;
  time: number;
}): Promise<void> {
  const value = trade.price * trade.size;
  
  // Only record significant trades (> $1000)
  if (value < 1000) return;

  const side = trade.side ? 'buy' : 'sell';
  const walletAddress = trade.taker || trade.maker || null;
  
  try {
    // Insert trade
    await query(
      `INSERT INTO trades (wallet_address, symbol, side, size, price, value, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7/1000.0))`,
      [walletAddress, trade.symbol, side, trade.size, trade.price, value, trade.time]
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
    }

    stats.tradesReceived++;
    stats.lastTradeTime = new Date();

    if (stats.tradesReceived % 10 === 0) {
      console.log(`📊 Trades captured: ${stats.tradesReceived}`);
    }
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
  const value = liq.price * liq.size;
  const walletAddress = liq.wallet || null;

  try {
    // Insert liquidation
    await query(
      `INSERT INTO liquidations (wallet_address, symbol, side, size, price, value, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7/1000.0))`,
      [walletAddress, liq.symbol, liq.side, liq.size, liq.price, value, liq.time]
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
function processMessage(data: string): void {
  try {
    const message = JSON.parse(data);

    // Handle different message types
    switch (message.type) {
      case 'trades':
        if (message.data?.trades && Array.isArray(message.data.trades)) {
          for (const trade of message.data.trades) {
            // Check if this is a liquidation trade
            if (trade.reason === 'liquidation') {
              recordLiquidation({
                symbol: trade.s || message.data.symbol,
                price: trade.px,
                size: trade.sz,
                side: trade.side ? 'long' : 'short', // liquidated side
                wallet: trade.taker || trade.maker,
                time: trade.time,
              });
            } else {
              recordTrade({
                symbol: trade.s || message.data.symbol,
                price: trade.px,
                size: trade.sz,
                side: trade.side,
                maker: trade.maker,
                taker: trade.taker,
                reason: trade.reason,
                time: trade.time,
              });
            }
          }
        }
        break;

      case 'liquidation':
        if (message.data) {
          recordLiquidation({
            symbol: message.data.symbol,
            price: message.data.price,
            size: message.data.size,
            side: message.data.side,
            wallet: message.data.wallet || message.data.user,
            time: message.data.time || Date.now(),
          });
        }
        break;

      case 'fill':
        if (message.data) {
          const fill = message.data;
          recordTrade({
            symbol: fill.symbol,
            price: fill.price || fill.px,
            size: fill.size || fill.sz,
            side: fill.side === 'buy' || fill.side === true,
            maker: fill.maker,
            taker: fill.taker,
            time: fill.time || Date.now(),
          });
        }
        break;

      // Connection confirmations
      case 'subscribed':
        console.log(`✅ Subscribed to: ${message.channel || message.topic || 'channel'}`);
        break;

      case 'pong':
        // Heartbeat response
        break;

      default:
        // Log unknown message types for debugging (only first few)
        if (stats.tradesReceived < 5) {
          console.log(`📨 Message type: ${message.type}`);
        }
    }
  } catch (error) {
    // Ignore parse errors for binary/ping messages
  }
}

// Connect to WebSocket
function connect(): void {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    return;
  }

  console.log('🔌 Connecting to BULK WebSocket...');

  try {
    ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      isConnected = true;
      console.log('✅ WebSocket connected to BULK Exchange');

      // Subscribe to all trades for all symbols
      const subscriptions = [
        { type: 'trades', symbol: 'BTC-USD' },
        { type: 'trades', symbol: 'ETH-USD' },
        { type: 'trades', symbol: 'SOL-USD' },
      ];

      ws?.send(JSON.stringify({
        method: 'subscribe',
        subscription: subscriptions,
      }));

      console.log('📡 Subscribed to trade streams for BTC, ETH, SOL');

      // Start heartbeat
      startHeartbeat();
    };

    ws.onmessage = (event) => {
      processMessage(event.data.toString());
    };

    ws.onclose = () => {
      isConnected = false;
      console.log('❌ WebSocket disconnected');
      scheduleReconnect();
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

  } catch (error) {
    console.error('Failed to create WebSocket:', error);
    scheduleReconnect();
  }
}

// Heartbeat to keep connection alive
let heartbeatInterval: NodeJS.Timeout | null = null;

function startHeartbeat(): void {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }

  heartbeatInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ method: 'ping' }));
    }
  }, 25000); // Ping every 25 seconds
}

// Schedule reconnection
function scheduleReconnect(): void {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }

  console.log('🔄 Reconnecting in 5 seconds...');
  reconnectTimeout = setTimeout(connect, 5000);
}

// Get connection stats
export function getWebSocketStats() {
  return {
    connected: isConnected,
    ...stats,
  };
}

// Start WebSocket listener
export function startWebSocketListener(): void {
  console.log('🚀 Starting WebSocket listener...');
  connect();
}

// Stop WebSocket listener
export function stopWebSocketListener(): void {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }
  if (ws) {
    ws.close();
    ws = null;
  }
  isConnected = false;
  console.log('🛑 WebSocket listener stopped');
}
