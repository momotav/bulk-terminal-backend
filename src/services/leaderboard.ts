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

  // Top Traders by PnL
  async getTopTradersByPnL(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const timeFilter = this.getTimeFilter(timeframe);
    
    // If timeframe is 'all', use aggregated data from traders table
    if (timeframe === 'all') {
      const rows = await query<{ wallet_address: string; total_pnl: number; total_trades: number }>(
        `SELECT wallet_address, total_pnl as value, total_trades as trades
         FROM traders
         WHERE total_pnl != 0
         ORDER BY total_pnl DESC
         LIMIT $1`,
        [limit]
      );
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_pnl as any) || 0,
        trades: row.total_trades,
      }));
    }
    
    // For time-based, use latest snapshot for each wallet
    const rows = await query<{ wallet_address: string; pnl: number }>(
      `SELECT DISTINCT ON (wallet_address)
        wallet_address,
        pnl + unrealized_pnl as pnl
       FROM trader_snapshots
       WHERE 1=1 ${timeFilter}
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
    
    if (timeframe === 'all') {
      const rows = await query<{ wallet_address: string; liquidation_value: number; total_liquidations: number }>(
        `SELECT wallet_address, liquidation_value as value, total_liquidations as trades
         FROM traders
         WHERE liquidation_value > 0
         ORDER BY liquidation_value DESC
         LIMIT $1`,
        [limit]
      );
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.liquidation_value as any) || 0,
        trades: row.total_liquidations,
      }));
    }
    
    const rows = await query<{ wallet_address: string; total_value: number; liq_count: number }>(
      `SELECT 
        wallet_address,
        SUM(value) as total_value,
        COUNT(*) as liq_count
       FROM liquidations
       WHERE wallet_address IS NOT NULL ${timeFilter}
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

  // Biggest Positions (Whale Watch)
  async getBiggestPositions(limit: number = 50): Promise<LeaderboardEntry[]> {
    // Get latest snapshot for each wallet with notional > 0
    const rows = await query<{ wallet_address: string; value: number; positions: number }>(
      `SELECT DISTINCT ON (wallet_address)
        wallet_address,
        total_notional as value,
        positions_count as positions
       FROM trader_snapshots
       WHERE total_notional > 0
       ORDER BY wallet_address, timestamp DESC`,
      []
    );
    
    // Sort by value (notional) and limit
    const sorted = rows
      .sort((a, b) => (parseFloat(b.value as any) || 0) - (parseFloat(a.value as any) || 0))
      .slice(0, limit);
    
    return sorted.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.value as any) || 0,
      positions: row.positions,
    }));
  }

  // Most Active Traders
  async getMostActive(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const timeFilter = timeframe === 'all' ? '' : this.getTimeFilter(timeframe);
    
    const rows = await query<{ wallet_address: string; trade_count: number; total_value: number }>(
      `SELECT 
        COALESCE(wallet_address, 'unknown') as wallet_address,
        COUNT(*) as trade_count,
        SUM(value) as total_value
       FROM trades
       WHERE 1=1 ${timeFilter}
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
    
    if (timeframe === 'all') {
      const rows = await query<{ wallet_address: string; total_volume: number; total_trades: number }>(
        `SELECT wallet_address, total_volume as value, total_trades as trades
         FROM traders
         WHERE total_volume > 0
         ORDER BY total_volume DESC
         LIMIT $1`,
        [limit]
      );
      
      return rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_volume as any) || 0,
        trades: row.total_trades,
      }));
    }
    
    const rows = await query<{ wallet_address: string; total_volume: number; trade_count: number }>(
      `SELECT 
        wallet_address,
        SUM(value) as total_volume,
        COUNT(*) as trade_count
       FROM trades
       WHERE wallet_address IS NOT NULL ${timeFilter}
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
