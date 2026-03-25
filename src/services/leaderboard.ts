import { query } from '../db';

export interface LeaderboardEntry {
  rank: number;
  wallet_address: string;
  value: number;
  trades?: number;
  positions?: number;
  change_24h?: number;
}

export type TimeFrame = '24h' | '7d' | '30d' | 'all';

// Wallets to exclude from leaderboards (e.g., Market Makers)
const EXCLUDED_WALLETS = [
  '7DHvrCZMMLZ2ovNfKaGpvJZXAQyydbTz6dM7w7qXtzX5', // BULK MM
];

// Simple in-memory cache
const cache: Map<string, { data: any; expiry: number }> = new Map();

function getCached<T>(key: string): T | null {
  const cached = cache.get(key);
  if (cached && cached.expiry > Date.now()) {
    return cached.data as T;
  }
  return null;
}

function setCache(key: string, data: any, ttlSeconds: number = 60): void {
  cache.set(key, { data, expiry: Date.now() + ttlSeconds * 1000 });
}

class LeaderboardService {
  // Get timeframe filter for SQL
  private getTimeFilter(timeframe: TimeFrame): string {
    switch (timeframe) {
      case '24h':
        return "AND timestamp > NOW() - INTERVAL '24 hours'";
      case '7d':
        return "AND timestamp > NOW() - INTERVAL '7 days'";
      case '30d':
        return "AND timestamp > NOW() - INTERVAL '30 days'";
      case 'all':
      default:
        return '';
    }
  }

  // Get excluded wallets filter
  private getExcludedFilter(column: string = 'wallet_address'): string {
    if (EXCLUDED_WALLETS.length === 0) return '';
    const placeholders = EXCLUDED_WALLETS.map(w => `'${w}'`).join(', ');
    return `AND ${column} NOT IN (${placeholders})`;
  }

  // Top Traders by PnL
  async getTopTradersByPnL(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `pnl_${timeframe}_${limit}`;
    const cached = getCached<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const timeFilter = this.getTimeFilter(timeframe);
    const excludeFilter = this.getExcludedFilter();
    
    try {
      // If timeframe is 'all', use aggregated data from traders table
      if (timeframe === 'all') {
        const rows = await query<{ wallet_address: string; value: number; trades: number }>(
          `SELECT wallet_address, total_pnl as value, total_trades as trades
           FROM traders
           WHERE total_pnl != 0 ${excludeFilter}
           ORDER BY total_pnl DESC
           LIMIT $1`,
          [limit]
        );
        
        const result = rows.map((row, index) => ({
          rank: index + 1,
          wallet_address: row.wallet_address,
          value: parseFloat(row.value as any) || 0,
          trades: row.trades,
        }));
        
        setCache(cacheKey, result, 30);
        return result;
      }
      
      // For time-based, use latest snapshot for each wallet
      const rows = await query<{ wallet_address: string; pnl: number }>(
        `SELECT DISTINCT ON (wallet_address)
          wallet_address,
          pnl + unrealized_pnl as pnl
         FROM trader_snapshots
         WHERE 1=1 ${timeFilter} ${excludeFilter}
         ORDER BY wallet_address, timestamp DESC`,
        []
      );
      
      // Sort by PnL and limit
      const sorted = rows
        .sort((a, b) => (parseFloat(b.pnl as any) || 0) - (parseFloat(a.pnl as any) || 0))
        .slice(0, limit);
      
      const result = sorted.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.pnl as any) || 0,
      }));
      
      setCache(cacheKey, result, 30);
      return result;
    } catch (e) {
      console.error('getTopTradersByPnL error:', e);
      return [];
    }
  }

  // Most Liquidated (Hall of Shame)
  async getMostLiquidated(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const timeFilter = this.getTimeFilter(timeframe);
    const excludeFilter = this.getExcludedFilter();
    
    if (timeframe === 'all') {
      const rows = await query<{ wallet_address: string; value: number; trades: number }>(
        `SELECT wallet_address, liquidation_value as value, total_liquidations as trades
         FROM traders
         WHERE liquidation_value > 0 ${excludeFilter}
         ORDER BY liquidation_value DESC
         LIMIT $1`,
        [limit]
      );
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.value as any) || 0,
        trades: row.trades,
      }));
    }
    
    const rows = await query<{ wallet_address: string; total_value: number; liq_count: number }>(
      `SELECT 
        wallet_address,
        SUM(value) as total_value,
        COUNT(*) as liq_count
       FROM liquidations
       WHERE wallet_address IS NOT NULL ${timeFilter} ${excludeFilter}
       GROUP BY wallet_address
       ORDER BY total_value DESC
       LIMIT $1`,
      [limit]
    );
    
    return rows.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.total_value as any) || 0,
      trades: parseInt(row.liq_count as any) || 0,
    }));
  }

  // Biggest Positions (Whale Watch) - fetch from BULK API directly with caching
  async getBiggestPositions(limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `whales_${limit}`;
    const cached = getCached<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    // Known active wallets to seed data
    const seedWallets = [
      '8cbNvb2Drc2m9CgosPKP8pWNWkbwbWCCQrqZ4h9MoFFN',
      '6q3BqzWLn7NZrDa2CNEH7mKsZbYHqHUKSnNfn46zGLn6',
      '9J8TUdEWrrcADK913r1Cs7DdqX63VdVU88imfDzT1ypt',
    ];
    
    try {
      // Get recent wallets from traders table
      let wallets: string[] = [];
      try {
        const dbWallets = await query<{ wallet_address: string }>(
          `SELECT wallet_address FROM traders 
           WHERE wallet_address IS NOT NULL ${excludeFilter}
           ORDER BY last_seen DESC 
           LIMIT 50`
        );
        wallets = dbWallets.map(w => w.wallet_address);
      } catch (e) {
        console.log('Using seed wallets only');
      }
      
      // Combine and dedupe, limit to 20 for speed
      const allWallets = [...new Set([...seedWallets, ...wallets])].slice(0, 20);
      
      // Fetch ALL wallets in PARALLEL (much faster!)
      const fetchWallet = async (wallet_address: string): Promise<LeaderboardEntry | null> => {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 2000);
          
          const response = await fetch('https://exchange-api.bulk.trade/api/v1/account', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: 'fullAccount', user: wallet_address }),
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          
          if (!response.ok) return null;
          
          const data = await response.json() as any[];
          if (!data || !data[0]?.fullAccount) return null;
          
          const account = data[0].fullAccount;
          if (!account.positions || account.positions.length === 0) return null;
          
          let totalNotional = 0;
          for (const pos of account.positions) {
            totalNotional += Math.abs(pos.notional || 0);
          }
          
          if (totalNotional > 0) {
            return {
              rank: 0,
              wallet_address,
              value: totalNotional,
              positions: account.positions.length
            };
          }
          return null;
        } catch (e) {
          return null;
        }
      };
      
      // Parallel fetch
      const results = await Promise.all(allWallets.map(fetchWallet));
      const validResults = results.filter((r): r is LeaderboardEntry => r !== null);
      
      // Sort by value and assign ranks
      validResults.sort((a, b) => b.value - a.value);
      const finalResults = validResults.slice(0, limit).map((r, i) => ({ ...r, rank: i + 1 }));
      
      // Cache for 60 seconds
      setCache(cacheKey, finalResults, 60);
      return finalResults;
      
    } catch (error) {
      console.error('getBiggestPositions error:', error);
      return [];
    }
  }

  async getMostActive(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `active_${timeframe}_${limit}`;
    const cached = getCached<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    try {
      // Use pre-aggregated traders table - FAST!
      const rows = await query<{ wallet_address: string; total_trades: number; total_volume: number }>(
        `SELECT wallet_address, total_trades, total_volume
         FROM traders
         WHERE total_trades > 0 ${excludeFilter}
         ORDER BY total_trades DESC
         LIMIT $1`,
        [limit]
      );
      
      const result = rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_volume as any) || 0,
        trades: parseInt(row.total_trades as any) || 0,
      }));
      
      setCache(cacheKey, result, 30);
      return result;
    } catch (e) {
      console.error('getMostActive error:', e);
      return [];
    }
  }

  // Top Volume Traders - use pre-aggregated traders table
  async getTopVolume(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `volume_${timeframe}_${limit}`;
    const cached = getCached<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    try {
      // Use pre-aggregated traders table - FAST!
      const rows = await query<{ wallet_address: string; total_volume: number; total_trades: number }>(
        `SELECT wallet_address, total_volume, total_trades
         FROM traders
         WHERE total_volume > 0 ${excludeFilter}
         ORDER BY total_volume DESC
         LIMIT $1`,
        [limit]
      );
      
      const result = rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_volume as any) || 0,
        trades: parseInt(row.total_trades as any) || 0,
      }));
      
      setCache(cacheKey, result, 30);
      return result;
    } catch (e) {
      console.error('getTopVolume error:', e);
      return [];
    }
  }

  // Get recent liquidations
  async getRecentLiquidations(limit: number = 50): Promise<any[]> {
    return query(
      `SELECT id, wallet_address, symbol, side, size, price, value, timestamp
       FROM liquidations
       ORDER BY timestamp DESC
       LIMIT $1`,
      [limit]
    );
  }

  // Get recent big trades
  async getRecentTrades(limit: number = 50, minValue: number = 100): Promise<any[]> {
    return query(
      `SELECT id, wallet_address, symbol, side, size, price, value, timestamp
       FROM trades
       ORDER BY timestamp DESC
       LIMIT $1`,
      [limit]
    );
  }
}

export const leaderboardService = new LeaderboardService();
