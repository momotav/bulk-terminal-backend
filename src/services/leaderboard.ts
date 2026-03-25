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
    const timeFilter = this.getTimeFilter(timeframe);
    const excludeFilter = this.getExcludedFilter();
    
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
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.value as any) || 0,
        trades: row.trades,
      }));
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
    
    return sorted.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.pnl as any) || 0,
    }));
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

  // Biggest Positions (Whale Watch) - fetch from BULK API directly
  async getBiggestPositions(limit: number = 50): Promise<LeaderboardEntry[]> {
    const excludeFilter = this.getExcludedFilter();
    
    // Known active wallets to seed data (can be expanded)
    const seedWallets = [
      '8cbNvb2Drc2m9CgosPKP8pWNWkbwbWCCQrqZ4h9MoFFN', // @momotavrrr - has SOL position
      '43FCw6GBmngMxPXSGXiAr1pQFyZ2D1BsAjYuim6W4pfE', // @quroolarc
      'BZSQTeUDnGX8CNNtgRPMQkL8GR1qLC95sJKwFLXG2kBV',
      '6q3BqzWLn7NZrDa2CNEH7mKsZbYHqHUKSnNfn46zGLn6', // Active trader from fills
      '9J8TUdEWrrcADK913r1Cs7DdqX63VdVU88imfDzT1ypt', // Liquidation counterparty
    ];
    
    try {
      // Get wallets from our traders table
      let wallets: string[] = [];
      try {
        const dbWallets = await query<{ wallet_address: string }>(
          `SELECT wallet_address FROM traders 
           WHERE wallet_address IS NOT NULL ${excludeFilter}
           ORDER BY last_seen DESC 
           LIMIT 100`
        );
        wallets = dbWallets.map(w => w.wallet_address);
      } catch (e) {
        console.log('No wallets in traders table, using seed wallets');
      }
      
      // Always include seed wallets
      const allWallets = [...new Set([...seedWallets, ...wallets])];
      
      // Fetch current positions from BULK API for each wallet using POST
      const results: LeaderboardEntry[] = [];
      
      for (const wallet_address of allWallets.slice(0, 30)) { // Limit API calls
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 3000);
          
          // Use POST request with { type: 'fullAccount', user: wallet }
          const response = await fetch('https://exchange-api.bulk.trade/api/v1/account', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: 'fullAccount', user: wallet_address }),
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          
          if (!response.ok) continue;
          
          const data = await response.json() as any[];
          if (!data || !data[0]?.fullAccount) continue;
          
          const account = data[0].fullAccount;
          if (!account.positions || account.positions.length === 0) continue;
          
          // Calculate total notional
          let totalNotional = 0;
          for (const pos of account.positions) {
            totalNotional += Math.abs(pos.notional || 0);
          }
          
          if (totalNotional > 0) {
            results.push({
              rank: 0,
              wallet_address,
              value: totalNotional,
              positions: account.positions.length
            });
          }
        } catch (e) {
          // Skip failed wallet
          console.log(`Failed to fetch wallet ${wallet_address.slice(0,8)}...`);
        }
      }
      
      // Sort by value and assign ranks
      results.sort((a, b) => b.value - a.value);
      return results.slice(0, limit).map((r, i) => ({ ...r, rank: i + 1 }));
      
    } catch (error) {
      console.error('getBiggestPositions error:', error);
      return [];
    }
  }

  // Most Active Traders
  async getMostActive(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const excludeFilter = this.getExcludedFilter();
    
    // For 'all' timeframe, use pre-aggregated data from traders table (FAST!)
    if (timeframe === 'all') {
      const rows = await query<{ wallet_address: string; total_trades: number; total_volume: number }>(
        `SELECT wallet_address, total_trades as trade_count, total_volume as total_value
         FROM traders
         WHERE total_trades > 0 ${excludeFilter}
         ORDER BY total_trades DESC
         LIMIT $1`,
        [limit]
      );
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_volume as any) || 0,
        trades: parseInt(row.total_trades as any) || 0,
      }));
    }
    
    // For time-based queries, use trades table with time filter
    const timeFilter = this.getTimeFilter(timeframe);
    
    const rows = await query<{ wallet_address: string; trade_count: number; total_value: number }>(
      `SELECT 
        COALESCE(wallet_address, 'unknown') as wallet_address,
        COUNT(*) as trade_count,
        SUM(value) as total_value
       FROM trades
       WHERE 1=1 ${timeFilter} ${excludeFilter}
       GROUP BY wallet_address
       ORDER BY trade_count DESC
       LIMIT $1`,
      [limit]
    );
    
    return rows.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.total_value as any) || 0,
      trades: parseInt(row.trade_count as any) || 0,
    }));
  }

  // Top Volume Traders
  async getTopVolume(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const timeFilter = this.getTimeFilter(timeframe);
    const excludeFilter = this.getExcludedFilter();
    
    // Always calculate from trades table for accuracy
    // The traders.total_volume may be stale or 0 from before fixes
    const timeCondition = timeframe === 'all' ? '' : timeFilter;
    
    const rows = await query<{ wallet_address: string; total_volume: number; trade_count: number }>(
      `SELECT 
        wallet_address,
        SUM(value) as total_volume,
        COUNT(*) as trade_count
       FROM trades
       WHERE wallet_address IS NOT NULL ${timeCondition} ${excludeFilter}
       GROUP BY wallet_address
       ORDER BY total_volume DESC
       LIMIT $1`,
      [limit]
    );
    
    return rows.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.total_volume as any) || 0,
      trades: parseInt(row.trade_count as any) || 0,
    }));
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
