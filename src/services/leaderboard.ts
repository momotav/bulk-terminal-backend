import { query } from '../db';
import { getCache, setCache } from './cache';

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
    const cacheKey = `leaderboard:pnl:${timeframe}:${limit}`;
    const cached = await getCache<LeaderboardEntry[]>(cacheKey);
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
        
        await setCache(cacheKey, result, 30);
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
      
      await setCache(cacheKey, result, 30);
      return result;
    } catch (e) {
      console.error('getTopTradersByPnL error:', e);
      return [];
    }
  }

  // Most Liquidated (Hall of Shame)
  async getMostLiquidated(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `leaderboard:liquidated:${timeframe}:${limit}`;
    const cached = await getCache<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;

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
      
      const result = rows.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.value as any) || 0,
        trades: row.trades,
      }));

      await setCache(cacheKey, result, 30);
      return result;
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
    
    const result = rows.map((row, index) => ({
      rank: index + 1,
      wallet_address: row.wallet_address,
      value: parseFloat(row.total_value as any) || 0,
      trades: parseInt(row.liq_count as any) || 0,
    }));

    await setCache(cacheKey, result, 30);
    return result;
  }

  // Biggest Positions (Whale Watch) - use database snapshots instead of API
  async getBiggestPositions(limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `leaderboard:whales:${limit}`;
    const cached = await getCache<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    try {
      // Get latest snapshot for each wallet with positions
      const rows = await query<{ 
        wallet_address: string; 
        total_notional: number; 
        positions_count: number;
      }>(
        `SELECT DISTINCT ON (wallet_address)
          wallet_address,
          total_notional,
          positions_count
         FROM trader_snapshots
         WHERE total_notional > 0 
           AND timestamp > NOW() - INTERVAL '24 hours'
           ${excludeFilter}
         ORDER BY wallet_address, timestamp DESC`
      );
      
      // Sort by notional and limit
      const sorted = rows
        .sort((a, b) => (parseFloat(b.total_notional as any) || 0) - (parseFloat(a.total_notional as any) || 0))
        .slice(0, limit);
      
      const result = sorted.map((row, index) => ({
        rank: index + 1,
        wallet_address: row.wallet_address,
        value: parseFloat(row.total_notional as any) || 0,
        positions: parseInt(row.positions_count as any) || 0,
      }));
      
      // Cache for 60 seconds
      await setCache(cacheKey, result, 60);
      
      return result;
    } catch (e) {
      console.error('getBiggestPositions error:', e);
      return [];
    }
  }

  /**
   * Volume / trade-count ranking over a window. 24h from the raw trades (kept 2
   * days, exact to the minute); 7d/30d from wallet_daily (whole UTC days, today
   * included). Both sides of every fill count, as in traders.total_volume.
   */
  private async windowed(timeframe: Exclude<TimeFrame, 'all'>, order: 'volume' | 'trades', limit: number): Promise<LeaderboardEntry[]> {
    const ex = this.getExcludedFilter('w');
    const by = order === 'volume' ? 'vol' : 'n';
    const rows = timeframe === '24h'
      ? await query<{ w: string; vol: string; n: string }>(
        `SELECT w, SUM(value) AS vol, COUNT(*) AS n FROM (
           SELECT p.w, t.value FROM trades t
           CROSS JOIN LATERAL (VALUES (t.wallet_address), (CASE WHEN t.counterparty IS DISTINCT FROM t.wallet_address THEN t.counterparty END)) AS p(w)
           WHERE t.timestamp > NOW() - INTERVAL '24 hours' AND p.w IS NOT NULL
         ) x WHERE 1=1 ${ex} GROUP BY w ORDER BY ${by} DESC LIMIT $1`, [limit])
      : await query<{ w: string; vol: string; n: string }>(
        `SELECT wallet_address AS w, SUM(volume) AS vol, SUM(trades) AS n FROM wallet_daily
         WHERE day > (NOW() AT TIME ZONE 'UTC')::date - $2::int AND trades > 0 ${this.getExcludedFilter('wallet_address')}
         GROUP BY wallet_address ORDER BY ${by} DESC LIMIT $1`, [limit, timeframe === '7d' ? 7 : 30]);
    return rows.map((r, i) => ({ rank: i + 1, wallet_address: r.w, value: parseFloat(r.vol) || 0, trades: parseInt(r.n) || 0 }));
  }

  // Most Active Traders (by trade count)
  async getMostActive(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `leaderboard:active:${timeframe}:${limit}`;
    const cached = await getCache<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    try {
      // Windowed tabs used to show all-time here (the timeframe was ignored).
      if (timeframe !== 'all') {
        const result = await this.windowed(timeframe, 'trades', limit);
        await setCache(cacheKey, result, 30);
        return result;
      }
      // All time: pre-aggregated traders table - FAST!
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
      
      await setCache(cacheKey, result, 30);
      return result;
    } catch (e) {
      console.error('getMostActive error:', e);
      return [];
    }
  }

  // Top Volume Traders - use pre-aggregated traders table
  async getTopVolume(timeframe: TimeFrame = 'all', limit: number = 50): Promise<LeaderboardEntry[]> {
    const cacheKey = `leaderboard:volume:${timeframe}:${limit}`;
    const cached = await getCache<LeaderboardEntry[]>(cacheKey);
    if (cached) return cached;
    
    const excludeFilter = this.getExcludedFilter();
    
    try {
      if (timeframe !== 'all') {
        const result = await this.windowed(timeframe, 'volume', limit);
        await setCache(cacheKey, result, 30);
        return result;
      }
      // All time: pre-aggregated traders table - FAST!
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
      
      await setCache(cacheKey, result, 30);
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
  async getRecentTrades(limit: number = 50, minValue: number = 0, symbol?: string): Promise<any[]> {
    // Size / market filters (live tape). The 7-day bound keeps a $100K filter on a
    // quiet market from walking the whole table.
    const where: string[] = [];
    const args: any[] = [limit];
    if (minValue > 0) { args.push(minValue); where.push(`value >= $${args.length}`); }
    if (symbol) { args.push(symbol); where.push(`symbol = $${args.length}`); }
    if (where.length) where.push(`timestamp > NOW() - INTERVAL '7 days'`);
    return query(
      `SELECT id, wallet_address, symbol, side, size, price, value, timestamp
       FROM trades
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY timestamp DESC
       LIMIT $1`,
      args
    );
  }

  // Get wallet's rank across all leaderboard types
  async getWalletRank(walletAddress: string): Promise<{
    wallet_address: string;
    found: boolean;
    rankings: {
      volume: { rank: number; total: number; value: number } | null;
      pnl: { rank: number; total: number; value: number } | null;
      trades: { rank: number; total: number; value: number } | null;
      liquidations: { rank: number; total: number; value: number } | null;
    };
    stats: {
      total_volume: number;
      total_trades: number;
      total_pnl: number;
      total_liquidations: number;
      liquidation_value: number;
    } | null;
  }> {
    const cacheKey = `leaderboard:wallet_rank:${walletAddress}`;
    const cached = await getCache<any>(cacheKey);
    if (cached) return cached;

    try {
      // First, check if wallet exists and get their stats
      const walletStats = await query<{
        wallet_address: string;
        total_volume: number;
        total_trades: number;
        total_pnl: number;
        total_liquidations: number;
        liquidation_value: number;
      }>(
        `SELECT wallet_address, total_volume, total_trades, total_pnl, total_liquidations, liquidation_value
         FROM traders
         WHERE wallet_address = $1`,
        [walletAddress]
      );

      if (walletStats.length === 0) {
        return {
          wallet_address: walletAddress,
          found: false,
          rankings: {
            volume: null,
            pnl: null,
            trades: null,
            liquidations: null,
          },
          stats: null,
        };
      }

      const stats = walletStats[0];

      // Get total count of traders for context
      const totalCountResult = await query<{ count: number }>(
        `SELECT COUNT(*) as count FROM traders WHERE total_volume > 0`
      );
      const totalTraders = parseInt(totalCountResult[0]?.count as any) || 0;

      // Get rank by volume
      const volumeRankResult = await query<{ rank: number }>(
        `SELECT COUNT(*) + 1 as rank 
         FROM traders 
         WHERE total_volume > $1`,
        [stats.total_volume]
      );
      const volumeRank = parseInt(volumeRankResult[0]?.rank as any) || 0;

      // Get rank by PnL
      const pnlRankResult = await query<{ rank: number }>(
        `SELECT COUNT(*) + 1 as rank 
         FROM traders 
         WHERE total_pnl > $1`,
        [stats.total_pnl]
      );
      const pnlRank = parseInt(pnlRankResult[0]?.rank as any) || 0;

      // Get rank by trades count
      const tradesRankResult = await query<{ rank: number }>(
        `SELECT COUNT(*) + 1 as rank 
         FROM traders 
         WHERE total_trades > $1`,
        [stats.total_trades]
      );
      const tradesRank = parseInt(tradesRankResult[0]?.rank as any) || 0;

      // Get rank by liquidation value
      const liqRankResult = await query<{ rank: number }>(
        `SELECT COUNT(*) + 1 as rank 
         FROM traders 
         WHERE liquidation_value > $1`,
        [stats.liquidation_value]
      );
      const liqRank = parseInt(liqRankResult[0]?.rank as any) || 0;

      // Count traders with liquidations for context
      const totalLiquidatedResult = await query<{ count: number }>(
        `SELECT COUNT(*) as count FROM traders WHERE liquidation_value > 0`
      );
      const totalLiquidated = parseInt(totalLiquidatedResult[0]?.count as any) || 0;

      const result = {
        wallet_address: walletAddress,
        found: true,
        rankings: {
          volume: {
            rank: volumeRank,
            total: totalTraders,
            value: parseFloat(stats.total_volume as any) || 0,
          },
          pnl: {
            rank: pnlRank,
            total: totalTraders,
            value: parseFloat(stats.total_pnl as any) || 0,
          },
          trades: {
            rank: tradesRank,
            total: totalTraders,
            value: parseInt(stats.total_trades as any) || 0,
          },
          liquidations: stats.liquidation_value > 0 ? {
            rank: liqRank,
            total: totalLiquidated,
            value: parseFloat(stats.liquidation_value as any) || 0,
          } : null,
        },
        stats: {
          total_volume: parseFloat(stats.total_volume as any) || 0,
          total_trades: parseInt(stats.total_trades as any) || 0,
          total_pnl: parseFloat(stats.total_pnl as any) || 0,
          total_liquidations: parseInt(stats.total_liquidations as any) || 0,
          liquidation_value: parseFloat(stats.liquidation_value as any) || 0,
        },
      };

      // Cache for 60 seconds
      await setCache(cacheKey, result, 60);

      return result;
    } catch (error) {
      console.error('getWalletRank error:', error);
      return {
        wallet_address: walletAddress,
        found: false,
        rankings: {
          volume: null,
          pnl: null,
          trades: null,
          liquidations: null,
        },
        stats: null,
      };
    }
  }
}

export const leaderboardService = new LeaderboardService();
