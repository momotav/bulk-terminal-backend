// Order flow: who is crossing the spread, and what it's doing to open interest.
//
//  • flow_hourly — taker buy / sell notional per market per hour, rolled up from
//    the trades table (which is pruned after 2 days) so the history is kept.
//    Every fill counts once, on the taker's side.
//  • Per market over a window: taker buys vs sells, and the price and open
//    interest change from the minute ticker snapshots. Price and OI together say
//    what kind of flow it was:
//      price ↑ OI ↑ new longs        price ↑ OI ↓ short covering
//      price ↓ OI ↑ new shorts       price ↓ OI ↓ longs closing

import { query } from '../db';

const OI_SIDE_FACTOR = 2; // ticker OI is one side

/** Re-aggregate the recent hours from trades into flow_hourly (idempotent). */
export async function rollupFlow(network: string, hours = 6): Promise<void> {
  await query(
    `INSERT INTO flow_hourly (hour, symbol, network, buy_usd, sell_usd, buys, sells)
     SELECT date_trunc('hour', timestamp), symbol, network,
            COALESCE(SUM(value) FILTER (WHERE side = 'buy'), 0), COALESCE(SUM(value) FILTER (WHERE side = 'sell'), 0),
            COUNT(*) FILTER (WHERE side = 'buy'), COUNT(*) FILTER (WHERE side = 'sell')
     FROM trades
     WHERE network = $1 AND timestamp >= date_trunc('hour', NOW() - make_interval(hours => $2))
     GROUP BY 1, 2, 3
     ON CONFLICT (symbol, network, hour) DO UPDATE SET
       buy_usd = EXCLUDED.buy_usd, sell_usd = EXCLUDED.sell_usd, buys = EXCLUDED.buys, sells = EXCLUDED.sells`,
    [network, hours],
  );
}

export type FlowKind = 'new_longs' | 'short_covering' | 'new_shorts' | 'longs_closing' | 'quiet';

/** The four price/OI quadrants; small moves on both are "quiet". */
export function flowKind(priceChg: number | null, oiChg: number | null): FlowKind | null {
  if (priceChg === null || oiChg === null) return null;
  if (Math.abs(priceChg) < 0.001 && Math.abs(oiChg) < 0.005) return 'quiet';
  if (priceChg >= 0) return oiChg >= 0 ? 'new_longs' : 'short_covering';
  return oiChg >= 0 ? 'new_shorts' : 'longs_closing';
}

export interface FlowRow {
  symbol: string; buyUsd: number; sellUsd: number; buys: number; sells: number;
  priceChg: number | null; oiChg: number | null; oiUsd: number | null; oiDeltaUsd: number | null; kind: FlowKind | null;
}

export async function flowSummary(network: string, hours: number): Promise<FlowRow[]> {
  const [flow, px] = await Promise.all([
    query(`SELECT symbol, SUM(buy_usd) AS b, SUM(sell_usd) AS s, SUM(buys) AS nb, SUM(sells) AS ns FROM flow_hourly
           WHERE network = $1 AND hour >= date_trunc('hour', NOW() - make_interval(hours => $2)) GROUP BY symbol`, [network, hours]),
    // First and last minute snapshot in the window, per market: read only the
    // 20 minutes at each end (the table holds a row per market per minute).
    query(`WITH a AS (SELECT DISTINCT ON (symbol) symbol, mark_price AS p0, open_interest_usd AS o0 FROM ticker_snapshots
                      WHERE network = $1 AND mark_price > 0 AND timestamp BETWEEN NOW() - make_interval(hours => $2) AND NOW() - make_interval(hours => $2) + INTERVAL '20 minutes'
                      ORDER BY symbol, timestamp ASC),
                z AS (SELECT DISTINCT ON (symbol) symbol, mark_price AS p1, open_interest_usd AS o1 FROM ticker_snapshots
                      WHERE network = $1 AND mark_price > 0 AND timestamp > NOW() - INTERVAL '20 minutes'
                      ORDER BY symbol, timestamp DESC)
           SELECT z.symbol, a.p0, z.p1, a.o0, z.o1 FROM z LEFT JOIN a ON a.symbol = z.symbol`, [network, hours]),
  ]);
  const P = new Map(px.map((r: any) => [r.symbol, r]));
  const symbols = new Set([...flow.map((r: any) => r.symbol), ...px.map((r: any) => r.symbol)]);
  const F = new Map(flow.map((r: any) => [r.symbol, r]));
  return Array.from(symbols).map((symbol) => {
    const f: any = F.get(symbol), p: any = P.get(symbol);
    const p0 = Number(p?.p0) || 0, p1 = Number(p?.p1) || 0, o0 = Number(p?.o0) || 0, o1 = Number(p?.o1) || 0;
    const priceChg = p0 > 0 && p1 > 0 ? p1 / p0 - 1 : null;
    const oiChg = o0 > 0 && o1 > 0 ? o1 / o0 - 1 : null;
    return {
      symbol, buyUsd: Number(f?.b ?? 0), sellUsd: Number(f?.s ?? 0), buys: Number(f?.nb ?? 0), sells: Number(f?.ns ?? 0),
      priceChg, oiChg, oiUsd: o1 > 0 ? o1 * OI_SIDE_FACTOR : null, oiDeltaUsd: o0 > 0 && o1 > 0 ? (o1 - o0) * OI_SIDE_FACTOR : null,
      kind: flowKind(priceChg, oiChg),
    };
  }).filter((r) => r.buyUsd + r.sellUsd > 0 || r.priceChg !== null)
    .sort((a, b) => b.buyUsd + b.sellUsd - (a.buyUsd + a.sellUsd));
}

/** One market's hourly flow, cumulative net (CVD), price and open interest. */
export async function flowSeries(network: string, symbol: string, days: number) {
  const [flow, px] = await Promise.all([
    query(`SELECT hour, buy_usd, sell_usd FROM flow_hourly WHERE network = $1 AND symbol = $2 AND hour > NOW() - make_interval(days => $3) ORDER BY hour`, [network, symbol, days]),
    query(`SELECT date_trunc('hour', timestamp) AS h, AVG(mark_price) AS p, AVG(open_interest_usd) AS o FROM ticker_snapshots
           WHERE network = $1 AND symbol = $2 AND timestamp > NOW() - make_interval(days => $3) AND mark_price > 0 GROUP BY 1 ORDER BY 1`, [network, symbol, days]),
  ]);
  const F = new Map(flow.map((r: any) => [new Date(r.hour).getTime(), r]));
  const hours = Array.from(new Set([...flow.map((r: any) => new Date(r.hour).getTime()), ...px.map((r: any) => new Date(r.h).getTime())])).sort((a, b) => a - b);
  const Px = new Map(px.map((r: any) => [new Date(r.h).getTime(), r]));
  let cvd = 0;
  return hours.map((t) => {
    const f: any = F.get(t), p: any = Px.get(t);
    const buy = Number(f?.buy_usd ?? 0), sell = Number(f?.sell_usd ?? 0);
    cvd += buy - sell;
    return { t, buy, sell, net: buy - sell, cvd, price: p ? Number(p.p) : null, oi: p ? Number(p.o) * OI_SIDE_FACTOR : null };
  });
}
