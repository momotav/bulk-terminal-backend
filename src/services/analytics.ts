import { query } from '../db';

export interface MarketStatsPoint {
  timestamp: Date;
  symbol: string;
  price: number;
  open_interest: number;
  volume_24h: number;
  funding_rate: number;
  long_ratio: number;
  short_ratio: number;
}

export interface LiquidationHeatmapPoint {
  price_bucket: number;
  long_liquidations: number;
  short_liquidations: number;
  total_value: number;
}

class AnalyticsService {
  // Get Open Interest history
  async getOpenInterestHistory(
    symbol: string, 
    hours: number = 168 // 7 days default
  ): Promise<{ timestamp: Date; value: number }[]> {
    const rows = await query<{ timestamp: Date; value: number }>(
      `SELECT timestamp, open_interest as value
       FROM market_stats
       WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
       ORDER BY timestamp ASC`,
      [symbol]
    );
    
    return rows.map(r => ({
      timestamp: r.timestamp,
      value: parseFloat(r.value as any) || 0,
    }));
  }

  // Get Funding Rate history
  async getFundingRateHistory(
    symbol: string, 
    hours: number = 168
  ): Promise<{ timestamp: Date; value: number }[]> {
    const rows = await query<{ timestamp: Date; value: number }>(
      `SELECT timestamp, funding_rate as value
       FROM market_stats
       WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
       ORDER BY timestamp ASC`,
      [symbol]
    );
    
    return rows.map(r => ({
      timestamp: r.timestamp,
      value: parseFloat(r.value as any) || 0,
    }));
  }

  // Get Volume history
  async getVolumeHistory(
    symbol: string, 
    hours: number = 168
  ): Promise<{ timestamp: Date; value: number }[]> {
    const rows = await query<{ timestamp: Date; value: number }>(
      `SELECT timestamp, volume_24h as value
       FROM market_stats
       WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
       ORDER BY timestamp ASC`,
      [symbol]
    );
    
    return rows.map(r => ({
      timestamp: r.timestamp,
      value: parseFloat(r.value as any) || 0,
    }));
  }

  // Get Long vs Short ratio history
  async getLongShortRatioHistory(
    symbol: string, 
    hours: number = 168
  ): Promise<{ timestamp: Date; long_ratio: number; short_ratio: number }[]> {
    const rows = await query<{ timestamp: Date; long_open_interest: number; short_open_interest: number }>(
      `SELECT timestamp, long_open_interest, short_open_interest
       FROM market_stats
       WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
       ORDER BY timestamp ASC`,
      [symbol]
    );
    
    return rows.map(r => {
      const longOI = parseFloat(r.long_open_interest as any) || 0;
      const shortOI = parseFloat(r.short_open_interest as any) || 0;
      const total = longOI + shortOI;
      
      return {
        timestamp: r.timestamp,
        long_ratio: total > 0 ? (longOI / total) * 100 : 50,
        short_ratio: total > 0 ? (shortOI / total) * 100 : 50,
      };
    });
  }

  // Get liquidation heatmap (price levels where liquidations happen)
  async getLiquidationHeatmap(
    symbol: string,
    bucketSize: number = 100, // $100 buckets
    hours: number = 168
  ): Promise<LiquidationHeatmapPoint[]> {
    const rows = await query<{ price_bucket: number; side: string; liq_count: number; total_value: number }>(
      `SELECT 
        FLOOR(price / $1) * $1 as price_bucket,
        side,
        COUNT(*) as liq_count,
        SUM(value) as total_value
       FROM liquidations
       WHERE symbol = $2 AND timestamp > NOW() - INTERVAL '${hours} hours'
       GROUP BY price_bucket, side
       ORDER BY price_bucket ASC`,
      [bucketSize, symbol]
    );
    
    // Combine long and short into single buckets
    const bucketMap = new Map<number, LiquidationHeatmapPoint>();
    
    for (const row of rows) {
      const bucket = parseFloat(row.price_bucket as any);
      const existing = bucketMap.get(bucket) || {
        price_bucket: bucket,
        long_liquidations: 0,
        short_liquidations: 0,
        total_value: 0,
      };
      
      if (row.side === 'long') {
        existing.long_liquidations = parseInt(row.liq_count as any) || 0;
      } else {
        existing.short_liquidations = parseInt(row.liq_count as any) || 0;
      }
      existing.total_value += parseFloat(row.total_value as any) || 0;
      
      bucketMap.set(bucket, existing);
    }
    
    return Array.from(bucketMap.values()).sort((a, b) => a.price_bucket - b.price_bucket);
  }

  // Get correlation matrix data
  async getCorrelationData(hours: number = 168): Promise<{
    symbols: string[];
    prices: Record<string, number[]>;
  }> {
    const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
    const prices: Record<string, number[]> = {};
    
    for (const symbol of symbols) {
      const rows = await query<{ price: number }>(
        `SELECT price
         FROM market_stats
         WHERE symbol = $1 AND timestamp > NOW() - INTERVAL '${hours} hours'
         ORDER BY timestamp ASC`,
        [symbol]
      );
      prices[symbol] = rows.map(r => parseFloat(r.price as any) || 0);
    }
    
    return { symbols, prices };
  }

  // Calculate Pearson correlation between two price arrays
  calculateCorrelation(x: number[], y: number[]): number {
    if (x.length !== y.length || x.length === 0) return 0;
    
    const n = x.length;
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((acc, xi, i) => acc + xi * y[i], 0);
    const sumX2 = x.reduce((acc, xi) => acc + xi * xi, 0);
    const sumY2 = y.reduce((acc, yi) => acc + yi * yi, 0);
    
    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
    
    return denominator === 0 ? 0 : numerator / denominator;
  }

  // Get full correlation matrix
  async getCorrelationMatrix(hours: number = 168): Promise<{
    symbols: string[];
    matrix: number[][];
  }> {
    const { symbols, prices } = await this.getCorrelationData(hours);
    const matrix: number[][] = [];
    
    for (let i = 0; i < symbols.length; i++) {
      const row: number[] = [];
      for (let j = 0; j < symbols.length; j++) {
        if (i === j) {
          row.push(1);
        } else {
          row.push(this.calculateCorrelation(prices[symbols[i]], prices[symbols[j]]));
        }
      }
      matrix.push(row);
    }
    
    return { symbols, matrix };
  }

  // Get exchange health metrics
  async getExchangeHealth(): Promise<{
    total_volume_24h: number;
    total_open_interest: number;
    total_traders: number;
    total_liquidations_24h: number;
    liquidation_value_24h: number;
  }> {
    // Get latest market stats for each symbol
    const marketStats = await query<{ volume_24h: number; open_interest: number }>(
      `SELECT DISTINCT ON (symbol) volume_24h, open_interest
       FROM market_stats
       ORDER BY symbol, timestamp DESC`
    );
    
    const totalVolume = marketStats.reduce((sum, s) => sum + (parseFloat(s.volume_24h as any) || 0), 0);
    const totalOI = marketStats.reduce((sum, s) => sum + (parseFloat(s.open_interest as any) || 0), 0);
    
    // Get trader count
    const [traderCount] = await query<{ count: number }>(
      'SELECT COUNT(*) as count FROM traders'
    );
    
    // Get 24h liquidation stats
    const [liqStats] = await query<{ count: number; value: number }>(
      `SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as value
       FROM liquidations
       WHERE timestamp > NOW() - INTERVAL '24 hours'`
    );
    
    return {
      total_volume_24h: totalVolume,
      total_open_interest: totalOI,
      total_traders: parseInt(traderCount?.count as any) || 0,
      total_liquidations_24h: parseInt(liqStats?.count as any) || 0,
      liquidation_value_24h: parseFloat(liqStats?.value as any) || 0,
    };
  }
}

export const analyticsService = new AnalyticsService();
