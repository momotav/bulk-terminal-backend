// Service to interact with BULK Exchange API

const BULK_API_URL = process.env.BULK_API_URL || 'https://exchange-api.bulk.trade/api/v1';

export interface Ticker {
  symbol: string;
  lastPrice: number;
  markPrice: number;
  priceChangePercent: number;
  volume: number;
  quoteVolume: number;
  openInterest: number;
  fundingRate: number;
  timestamp: number;
}

export interface Position {
  symbol: string;
  size: number;
  price: number;
  notional: number;
  unrealizedPnl: number;
  realizedPnl: number;
  leverage: number;
  liquidationPrice: number;
}

export interface FullAccount {
  margin: {
    totalBalance: number;
    availableBalance: number;
    marginUsed: number;
    realizedPnl: number;
    unrealizedPnl: number;
  };
  positions: Position[];
  openOrders: unknown[];
}

interface AccountResponse {
  fullAccount?: FullAccount;
  orderHistory?: unknown;
  fills?: unknown[];
}

class BulkApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = BULK_API_URL;
  }

  // Fetch ticker for a symbol
  async getTicker(symbol: string): Promise<Ticker | null> {
    try {
      // Try the direct ticker endpoint first
      let res = await fetch(`${this.baseUrl}/ticker/${symbol}`);
      
      // If that fails, try without the symbol in path (some APIs use query params)
      if (!res.ok) {
        res = await fetch(`${this.baseUrl}/ticker?symbol=${symbol}`);
      }
      
      if (!res.ok) {
        console.log(`⚠️ Ticker endpoint failed for ${symbol}: ${res.status}`);
        return null;
      }
      
      const data: any = await res.json();
      console.log(`📊 Raw ticker data for ${symbol}:`, JSON.stringify(data).slice(0, 200));
      
      // Handle different response formats
      // Could be { symbol, markPrice, ... } or { data: { ... } } or array
      const ticker: any = data.data || data[0] || data;
      
      return {
        symbol: ticker.symbol || symbol,
        lastPrice: parseFloat(ticker.lastPrice || ticker.last || ticker.price || 0),
        markPrice: parseFloat(ticker.markPrice || ticker.mark || ticker.price || 0),
        priceChangePercent: parseFloat(ticker.priceChangePercent || ticker.change || 0),
        volume: parseFloat(ticker.volume || ticker.vol || 0),
        quoteVolume: parseFloat(ticker.quoteVolume || ticker.turnover || ticker.volume || 0),
        openInterest: parseFloat(ticker.openInterest || ticker.oi || 0),
        fundingRate: parseFloat(ticker.fundingRate || ticker.funding || 0),
        timestamp: ticker.timestamp || Date.now(),
      };
    } catch (error) {
      console.error(`Failed to fetch ticker for ${symbol}:`, error);
      return null;
    }
  }

  // Fetch all tickers
  async getAllTickers(): Promise<Ticker[]> {
    // First try bulk endpoint
    try {
      const res = await fetch(`${this.baseUrl}/tickers`);
      if (res.ok) {
        const data: any = await res.json();
        const tickersArray: any[] = data.data || data || [];
        if (Array.isArray(tickersArray) && tickersArray.length > 0) {
          console.log(`📊 Got ${tickersArray.length} tickers from bulk endpoint`);
          return tickersArray.map((t: any) => ({
            symbol: t.symbol,
            lastPrice: parseFloat(t.lastPrice || t.last || t.price || 0),
            markPrice: parseFloat(t.markPrice || t.mark || t.price || 0),
            priceChangePercent: parseFloat(t.priceChangePercent || t.change || 0),
            volume: parseFloat(t.volume || 0),
            quoteVolume: parseFloat(t.quoteVolume || t.turnover || t.volume || 0),
            openInterest: parseFloat(t.openInterest || t.oi || 0),
            fundingRate: parseFloat(t.fundingRate || t.funding || 0),
            timestamp: t.timestamp || Date.now(),
          }));
        }
      }
    } catch (e) {
      console.log('Bulk tickers endpoint not available, falling back to individual');
    }
    
    // Fall back to individual symbol requests
    const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
    const tickers = await Promise.all(
      symbols.map(symbol => this.getTicker(symbol))
    );
    return tickers.filter((t): t is Ticker => t !== null);
  }

  // Fetch account data for a wallet
  async getFullAccount(walletAddress: string): Promise<FullAccount | null> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'fullAccount', user: walletAddress }),
      });
      if (!res.ok) return null;
      const data = await res.json() as AccountResponse[];
      if (data && data[0] && data[0].fullAccount) {
        return data[0].fullAccount;
      }
      return null;
    } catch (error) {
      console.error(`Failed to fetch account for ${walletAddress}:`, error);
      return null;
    }
  }

  // Fetch exchange stats
  async getStats(): Promise<Record<string, unknown> | null> {
    try {
      const res = await fetch(`${this.baseUrl}/stats?period=1d`);
      if (!res.ok) return null;
      const data = await res.json() as Record<string, unknown>;
      return data;
    } catch (error) {
      console.error('Failed to fetch stats:', error);
      return null;
    }
  }

  // Fetch order history for a wallet
  async getOrderHistory(walletAddress: string): Promise<unknown[]> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'orderHistory', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json() as AccountResponse[];
      const results: unknown[] = [];
      for (const item of data) {
        if (item && item.orderHistory) {
          results.push(item.orderHistory);
        }
      }
      return results;
    } catch (error) {
      console.error(`Failed to fetch order history for ${walletAddress}:`, error);
      return [];
    }
  }

  // Fetch fills (executed trades) for a wallet
  async getFills(walletAddress: string): Promise<unknown[]> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'fills', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json() as AccountResponse[];
      const results: unknown[] = [];
      for (const item of data) {
        if (item && item.fills && Array.isArray(item.fills)) {
          results.push(...item.fills);
        }
      }
      return results;
    } catch (error) {
      console.error(`Failed to fetch fills for ${walletAddress}:`, error);
      return [];
    }
  }
}

export const bulkApi = new BulkApiService();
