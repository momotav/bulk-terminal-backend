// Service to interact with BULK Exchange API

import { getActiveSymbols } from './markets';

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
  // v1.0.14 hierarchy fields. Optional on the type because BULK omits them
  // when not applicable (e.g. masters with no sub-accounts have no
  // `subAccounts` field at all rather than an empty array).
  kind?: 'MasterEOA' | 'SubAccount';
  parent?: string;                          // present on sub-accounts only
  subAccounts?: { pubkey: string; name?: string }[]; // present on masters with children
  multisigAccounts?: string[];              // multisigs this account is a member of
  authorizedAgentWallets?: string[];

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
  activityHistory?: ActivityEvent;
}

// Single row from BULK's `activityHistory` query. One physical event per row,
// wrapped in a single-key envelope by the upstream API.
//
// Possible activityType values (v1.0.14, may grow over time):
//   - "deposit"          : on-chain deposit landed (from = system program)
//   - "withdrawal"       : tokens left the protocol
//   - "transfer"         : protocol-native internal/external transfer
//   - "createSubAccount" : sub-account created under master
//   - "removeSubAccount" : sub-account removed (auto-sweeps balance to master)
//   - "renameSubAccount" : sub-account renamed (pubkey preserved)
//   - "multisigCreated"  : new multisig account created
//   - "proposalCreated", "proposalApproved", "proposalReadyForExecution",
//     "proposalExecuted", "proposalFailed", "proposalExpired",
//     "proposalCancelled", "proposalRejected" : multisig proposal lifecycle
//
// We model `activityType` as `string` to forward-compat unknown future events
// rather than coercing them into an enum that breaks on new values.
export interface ActivityEvent {
  activityType: string;
  status: string;             // "completed" | "failed" | etc.
  from?: string;              // source pubkey (system program "1111...111" for deposits)
  to?: string;                // destination pubkey
  symbol?: string;            // token symbol for transfers (e.g. "USDC")
  amount?: number;
  iso?: boolean;
  slot?: number;              // Solana slot number
  timestamp: number;          // nanoseconds (BULK convention)
  sequence?: number;
  // Multisig-specific fields likely appear here once that flow is active.
  // We pass them through untyped via a [k:string]:unknown index signature on
  // consumers when needed.
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
    
    // Fall back to individual symbol requests — use the live market list so
    // we don't miss newer coins (BNB, DOGE, FARTCOIN, SUI, ZEC). Falls back
    // internally to a hardcoded list if /exchangeInfo is unreachable.
    const symbols = await getActiveSymbols();
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

  // Fetch activity history (deposits, withdrawals, transfers, sub-account
  // events, multisig events) for a wallet.
  //
  // BULK returns these wrapped in single-key envelopes:
  //   [{ "activityHistory": {...event...} }, ...]
  // We unwrap to a flat ActivityEvent[] and trust BULK's ordering. If BULK
  // truncates / paginates we handle it at the caller; this method just hands
  // back what came down the wire.
  async getActivityHistory(walletAddress: string): Promise<ActivityEvent[]> {
    try {
      const res = await fetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'activityHistory', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json() as AccountResponse[];
      if (!Array.isArray(data)) return [];
      return data
        .map((row) => row.activityHistory)
        .filter((e): e is ActivityEvent => Boolean(e));
    } catch (error) {
      console.error(`Failed to fetch activity history for ${walletAddress}:`, error);
      return [];
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
