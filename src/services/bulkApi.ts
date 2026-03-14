// Service to interact with BULK Exchange API

const BULK_API_URL = process.env.BULK_API_URL || 'https://exchange-api1.northstarlabs.xyz/api/v1';

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
  openOrders: any[];
}

class BulkApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = BULK_API_URL;
  }

  // Fetch ticker for a symbol
  async getTicker(symbol: string): Promise<Ticker | null> {
    try {
      const res = await fetch(`${this.baseUrl}/ticker/${symbol}`);
      if (!res.ok) return null;
      return await res.json();
    } catch (error) {
      console.error(`Failed to fetch ticker for ${symbol}:`, error);
      return null;
    }
  }

  // Fetch all tickers
  async getAllTickers(): Promise<Ticker[]> {
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
      const data = await res.json();
      return data[0]?.fullAccount || null;
    } catch (error) {
      console.error(`Failed to fetch account for ${walletAddress}:`, error);
      return null;
    }
  }

  // Fetch exchange stats
  async getStats(): Promise<any> {
    try {
      const res = await fetch(`${this.baseUrl}/stats?period=1d`);
      if (!res.ok) return null;
      return await res.json();
    } catch (error) {
      console.error('Failed to fetch stats:', error);
      return null;
    }
  }

  // Fetch order history for a wallet
  async getOrderHistory(walletAddress: string): Promise<any[]> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'orderHistory', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json();
      return data.map((d: any) => d.orderHistory).filter(Boolean);
    } catch (error) {
      console.error(`Failed to fetch order history for ${walletAddress}:`, error);
      return [];
    }
  }

  // Fetch fills (executed trades) for a wallet
  async getFills(walletAddress: string): Promise<any[]> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'fills', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json();
      return data.map((d: any) => d.fills).filter(Boolean).flat();
    } catch (error) {
      console.error(`Failed to fetch fills for ${walletAddress}:`, error);
      return [];
    }
  }
}

export const bulkApi = new BulkApiService();
