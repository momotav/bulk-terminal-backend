import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { bulkApi, FullAccount } from '../services/bulkApi';
import { requireAuth } from '../middleware/auth';
import { getCache, setCache } from '../services/cache';
import { resolveHierarchy } from '../services/accountResolver';

const router = Router();

// Net a FullAccount's PnL fields in-place semantics — returns a new object
// with `realizedPnl` and `unrealizedPnl` on both `margin` and each
// `position` overwritten to include fees+funding. BULK reports these
// values as GROSS (pure price math), with fees and funding as separate
// signed fields; downstream UI surfaces want the true economic PnL, so
// we collapse the breakdown here once at the API boundary rather than
// scattering the math across every consumer.
//
// We preserve the original gross-component fields (`fees`, `funding`)
// alongside so any consumer that wants to show a breakdown still can.
//
// Per-position semantics:
//   net realizedPnl   = realizedPnl + fees + funding
//   net unrealizedPnl = unrealizedPnl   (mark-to-market doesn't book fees)
//
// Wallet-aggregate (margin object) semantics:
//   margin.realizedPnl   gets the fees+funding adjustment (wallet-level)
//   margin.unrealizedPnl stays gross (mark-to-market)
//
// Logic mirrors wsListener.ts / dataCollector.ts so DB snapshots and
// HTTP responses stay in sync semantically.
function netFullAccount(account: FullAccount): FullAccount {
  const netPositions = account.positions.map((p) => ({
    ...p,
    // Roll per-position fees+funding into realized side. Leave fees/funding
    // fields intact so the frontend can show a breakdown if desired.
    realizedPnl: (p.realizedPnl || 0) + (p.fees || 0) + (p.funding || 0),
    // Unrealized stays gross — it's the mark-to-market component and
    // BULK doesn't accrue fees against unrealized.
    unrealizedPnl: p.unrealizedPnl || 0,
  }));

  const m = account.margin || ({} as FullAccount['margin']);
  const netMargin = {
    ...m,
    realizedPnl: (m.realizedPnl || 0) + (m.fees || 0) + (m.funding || 0),
    unrealizedPnl: m.unrealizedPnl || 0,
  };

  return {
    ...account,
    margin: netMargin,
    positions: netPositions,
  };
}

// Helper: maintain the `traders` table row for this wallet. Writes net
// PnL to `traders.total_pnl` and bumps `last_seen`. Fire-and-forget —
// nobody reads `total_pnl` back synchronously from the wallet response;
// it powers leaderboard fallback queries when BULK indexer data is
// unavailable. Sourced from BULK's `margin.*` aggregate fields.
//
// Replaces the older `storeWalletSnapshot` function which ALSO wrote to
// the `trader_snapshots` table on every page view + hourly cron. The
// snapshots table was sampled inconsistently (whoever happened to view
// the wallet, plus a 100-wallet hourly cron) so the resulting PnL
// history chart was a record of viewing patterns, not trading activity.
// We now derive the PnL chart from closed positions at query time
// (see deriveHistoryFromClosedPositions below) which is deterministic,
// dense, and accurate for any wallet — no DB writes required.
async function upsertTraderRow(walletAddress: string, account: FullAccount): Promise<void> {
  try {
    const realizedPnl = account.margin?.realizedPnl || 0;
    const unrealizedPnl = account.margin?.unrealizedPnl || 0;
    const fees = account.margin?.fees || 0;
    const funding = account.margin?.funding || 0;
    const totalPnl = realizedPnl + unrealizedPnl + fees + funding;

    void query(
      `INSERT INTO traders (wallet_address, total_pnl, last_seen)
       VALUES ($1, $2, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET
         total_pnl = $2,
         last_seen = NOW()`,
      [walletAddress, totalPnl]
    ).catch((err) => console.error('traders upsert failed:', err));
  } catch (error) {
    console.error(`Failed to upsert trader row for ${walletAddress.slice(0, 8)}:`, error);
  }
}

// Derive PnL history from BULK's closed-position log. Walks closed
// positions in chronological order, accumulating net realized PnL, then
// appends a synthetic "now" point with current unrealized added so the
// chart's right edge reflects live state.
//
// The shape of the returned array matches what the old snapshot-based
// path produced, so frontend rendering is shape-stable: each row has
// `{timestamp, pnl, unrealized_pnl, positions_count, total_notional}`.
// For closed-position rows we put cumulative-realized in `pnl` and 0
// in `unrealized_pnl`; for the "now" row we put current realized in
// `pnl` and current unrealized in `unrealized_pnl`. The chart plots
// the sum, so it shows realized-only across history with the live
// unrealized as a final-step extension.
//
// Caveats:
//  - BULK only exposes the last 5000 closed positions per wallet. For
//    heavy traders the earliest cumulative value won't be at zero (it
//    misses older closes). The chart shape is still right (deltas are
//    accurate) but the absolute baseline can be off. Most wallets have
//    fewer than 5000 lifetime trades so this is a long-tail edge case.
//  - Funding payments DON'T get their own time points — they're already
//    netted into each closed position's `realizedPnl` via netFullAccount.
//    Open-position funding accruals are folded into the "now" point's
//    unrealized via account.margin.funding. Good enough for visualizing
//    a trading curve.
function deriveHistoryFromClosedPositions(
  closedPositions: unknown[],
  account: FullAccount | null,
): Array<{ timestamp: string; pnl: number; unrealized_pnl: number; positions_count: number; total_notional: number }> {
  // BULK returns closed positions with ns-precision timestamps in
  // `closeTime`. We normalize at the boundary because bulkApi returns
  // `unknown[]` — the raw shape isn't guaranteed by TS — and we want
  // strong runtime guards in case BULK ever changes the field layout.
  const normalized: Array<{ closedAt: number; realizedPnl: number; fees: number; funding: number }> = [];
  for (const raw of closedPositions) {
    if (!raw || typeof raw !== 'object') continue;
    const p = raw as Record<string, unknown>;
    // Timestamps from BULK are ns; convert to ms. Some endpoints have
    // shipped responses with already-ms values during testing, so we
    // detect by magnitude: anything past year 3000 in ms is almost
    // certainly nanoseconds and needs to be divided down.
    const rawCloseTime = Number(p.closeTime) || 0;
    if (rawCloseTime === 0) continue;
    const closedAt = rawCloseTime > 1e15 ? rawCloseTime / 1_000_000 : rawCloseTime;
    normalized.push({
      closedAt,
      realizedPnl: Number(p.realizedPnl) || 0,
      fees: Number(p.fees) || 0,
      funding: Number(p.funding) || 0,
    });
  }
  const sorted = normalized.sort((a, b) => a.closedAt - b.closedAt);
  const history: Array<{ timestamp: string; pnl: number; unrealized_pnl: number; positions_count: number; total_notional: number }> = [];
  let cumulative = 0;
  for (const p of sorted) {
    // realizedPnl from the BULK endpoint is GROSS (per our earlier
    // diagnosis); the netting we do at the API boundary happens in
    // normalizeClosedPosition (which the wallet route uses for the
    // closed-positions endpoint). Here we're working with the raw
    // BULK response so we need to net manually: realized + fees + funding.
    const net = (p.realizedPnl || 0) + (p.fees || 0) + (p.funding || 0);
    cumulative += net;
    history.push({
      timestamp: new Date(p.closedAt).toISOString(),
      pnl: cumulative,
      unrealized_pnl: 0,
      positions_count: 0,
      total_notional: 0,
    });
  }
  // Append synthetic "now" row with current unrealized so chart's right
  // edge shows live state. Only emit when we have a live account — for
  // wallets without live data, the chart simply ends at the last close.
  if (account) {
    const realized = account.margin?.realizedPnl || 0;
    const fees = account.margin?.fees || 0;
    const funding = account.margin?.funding || 0;
    const unrealized = account.margin?.unrealizedPnl || 0;
    const totalNotional = account.positions.reduce((sum, p) => sum + Math.abs(p.notional || 0), 0);
    history.push({
      timestamp: new Date().toISOString(),
      pnl: realized + fees + funding,
      unrealized_pnl: unrealized,
      positions_count: account.positions.length,
      total_notional: totalNotional,
    });
  }
  return history;
}

// GET /wallet/:address - Get wallet info (live from BULK API + our data)
//
// Performance contract:
//   - 30s positive cache for fast repeat-views of the same wallet
//   - Stale-while-revalidate-style fallback: if BULK API fails on this
//     request, we serve the last-known-good cached payload (up to 5 min
//     stale) instead of erroring out
//   - Parallel fan-out: BULK getFullAccount, BULK getAllTickers, and our
//     trader/snapshots SELECT all run concurrently
//   - Snapshot writes are fire-and-forget (don't block the response)
//   - Failed responses are NOT cached — only successful payloads with a
//     non-null .live get the 30s TTL
router.get('/:address', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;

    // Two cache keys: a short-lived "fresh" key (30s) and a longer "stale"
    // key (5min). On BULK API failure we serve from stale rather than
    // returning an error, so the page never goes blank because BULK had
    // a 503 for two seconds.
    const freshKey = `wallet:profile:${address}`;
    const staleKey = `wallet:profile:stale:${address}`;

    const cached = await getCache<any>(freshKey);
    if (cached) {
      return res.json(cached);
    }

    // Parallel fan-out: all four data sources kick off at the same time.
    // Previously these were sequential, costing 4× round-trip latency.
    //
    // We fetch CLOSED POSITIONS (not the trader_snapshots DB table) for
    // the PnL history chart. The chart is now DERIVED — sort closes by
    // time, accumulate net realized PnL, plus a synthetic "now" point
    // with current unrealized added on. See deriveHistoryFromClosedPositions
    // below for the math.
    //
    // Why this is better than the old snapshot approach:
    //  - Snapshots only existed for the 100 most-viewed wallets via the
    //    hourly cron + on-page-view writes. New / quiet wallets had
    //    one data point (the visit), so their chart was empty.
    //  - The chart "history" was actually a record of WHO LOOKED AT THE
    //    WALLET WHEN, not when trading happened. Bumpy and irregular.
    //  - Sampling cadence was wildly inconsistent (every page view or
    //    every hour, sometimes both).
    // Deriving from closed positions gives every wallet a real, dense,
    // deterministic PnL curve from their first closed trade onward.
    const [account, tickers, trader, closedPositions] = await Promise.all([
      bulkApi.getFullAccount(address),
      bulkApi.getAllTickers(),
      queryOne(
        'SELECT * FROM traders WHERE wallet_address = $1',
        [address]
      ),
      bulkApi.getClosedPositions(address),
    ]);

    // BULK API call returned null. Two cases:
    //  (1) Wallet doesn't exist at all on BULK — legitimately empty
    //  (2) Transient BULK API error — we should serve stale data
    // We can't distinguish, so we look for stale cache first. If we have
    // any, serve it (with a header so the frontend knows). Otherwise we
    // return what we have but DO NOT cache.
    if (!account) {
      const stale = await getCache<any>(staleKey);
      if (stale) {
        // Serve last-known-good. Frontend behaves identically; this just
        // means BULK was temporarily unhappy and we're papering over it.
        res.setHeader('X-Bulkstats-Cache', 'stale');
        return res.json(stale);
      }
      // No stale either — wallet may genuinely not exist on BULK. Return
      // a valid response shape so the frontend renders the empty state
      // instead of erroring. Crucially, we do NOT cache this — next
      // request will retry BULK fresh.
      const markPrices: Record<string, number> = {};
      for (const ticker of tickers) {
        markPrices[ticker.symbol] = ticker.markPrice;
      }
      return res.json({
        address,
        live: null,
        markPrices,
        tracked: trader,
        history: deriveHistoryFromClosedPositions(closedPositions || [], null),
      });
    }

    // BULK responded successfully. Build the response.
    const markPrices: Record<string, number> = {};
    for (const ticker of tickers) {
      markPrices[ticker.symbol] = ticker.markPrice;
    }

    // Net the PnL fields at the API boundary so all consumers (the
    // wallet page KPIs, the positions list, the PnL chart fallback) see
    // true economic PnL without each surface having to do the math.
    // Frontend code can stay shape-stable; only the values change.
    const netted = netFullAccount(account);

    const result = {
      address,
      live: netted,
      markPrices,
      tracked: trader,
      history: deriveHistoryFromClosedPositions(closedPositions || [], account),
    };

    // Fire-and-forget: maintain the traders DB row (last_seen + total_pnl
    // fallback). We pass the ORIGINAL gross account because the helper
    // does its own netting from margin.* before writing. Previously this
    // also wrote a row to trader_snapshots on every page view; that path
    // is now obsolete (chart is derived from closed positions, not the
    // snapshots table).
    void upsertTraderRow(address, account);

    // Cache for both fresh (30s) and stale (5min) keys. The stale key is
    // only consulted when BULK fails on a future request.
    await Promise.all([
      setCache(freshKey, result, 30),
      setCache(staleKey, result, 300),
    ]);

    res.json(result);
  } catch (error: any) {
    console.error('GET /wallet/:address error:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch wallet data' });
  }
});

// POST /wallet/:address/track - Start tracking a wallet
//
// Bumps `last_seen` and records current PnL on the `traders` row. The
// old version of this endpoint also wrote to `trader_snapshots` (which
// is now obsolete — the PnL chart derives from BULK closed-positions
// at query time). We still need the traders upsert because that row
// is read by leaderboard fallback queries and the analytics views.
router.post('/:address/track', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;

    const account = await bulkApi.getFullAccount(address);
    if (account) {
      // Fire-and-forget — the request can return without waiting for the
      // DB write, same pattern as the main wallet route. If the wallet
      // doesn't exist on BULK we just skip the upsert silently.
      void upsertTraderRow(address, account);
    }

    res.json({ success: true, message: 'Wallet is now being tracked' });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to track wallet' });
  }
});

// GET /wallet/:address/trades - Get trade history
router.get('/:address/trades', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    
    const trades = await query(
      `SELECT * FROM trades 
       WHERE wallet_address = $1 
       ORDER BY timestamp DESC 
       LIMIT $2`,
      [address, limit]
    );
    
    res.json({ data: trades });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch trades' });
  }
});

// ============================================================================
// GET /wallet/:address/fills - Live fills from BULK API for chart overlay
//
// Used by the position chart modal to draw markers showing every entry the
// wallet made on a given market. We pull from BULK directly (not our DB)
// because users want the freshest data possible — fills that happened
// 30 seconds ago should show up.
//
// Filters by symbol when provided so we don't ship hundreds of unrelated
// fills to the frontend just to render markers for one market.
// Cached 60s — fills don't change retroactively, so a minute-stale set is
// fine even on a stream.
//
// Response shape (each fill):
//   {
//     timestamp: number (ms),
//     price: number,
//     size: number (positive),
//     isBuy: boolean,
//     symbol: string,
//     orderIdMaker?: string,
//     orderIdTaker?: string,
//     reasonCode?: string  // e.g. "trade", "liq", "adl"
//   }
// ============================================================================
router.get('/:address/fills', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const symbol = (req.query.symbol as string) || null;
    // Hard cap to keep payloads sane. 500 fills is plenty for a chart
    // overlay — beyond that, markers stack and become unreadable anyway.
    const limit = Math.min(parseInt(req.query.limit as string) || 500, 1000);

    const cacheKey = `wallet:fills:${address}:${symbol || 'all'}:${limit}`;
    const cached = await getCache<{ fills: unknown[] }>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // BULK's `fills` API returns the wallet's full fill history with no
    // server-side symbol filter. We slice it client-side here. The history
    // is sorted newest-first by BULK; we keep that ordering so the chart
    // sees the most recent fills if it has to truncate.
    const allFills = await bulkApi.getFills(address);

    // Each fill has the shape described in the JSDoc above. Type loosely
    // here because the BULK SDK returns `unknown[]` and we don't want to
    // hard-fail the route on a schema change — better to skip malformed
    // entries than 500 the whole response.
    //
    // Field-name and symbol-format tolerance:
    //   - Per BULK's v1.0.13 changelog, WS uses compact field names
    //     (`sym` instead of `symbol`). REST may follow the same convention
    //     so we accept either.
    //   - BULK's APIs have historically returned the bare coin ("BTC") in
    //     some payloads and the full pair ("BTC-USD") in others. We accept
    //     either by comparing both shapes.
    //
    // The first time we get a non-matching fill, we log its raw field set
    // so it's easy to spot if BULK changes the shape again — the missing
    // markers showed up because of exactly this kind of silent schema drift.
    // Normalize each fill to a stable shape so the frontend doesn't need
    // to know about BULK's quirks. Two important conversions:
    //   1. timestamp: BULK returns nanoseconds (e.g. 1.77e18). We divide
    //      by 1e6 to get milliseconds, which is what JS Date and our
    //      chart library expect.
    //   2. size: BULK uses `amount` for fill quantity (the position
    //      schema uses `size`). We expose it as `size` for consistency
    //      with our type definition.
    //   3. reasonCode: BULK returns a numeric code AND a string `reason`.
    //      We pass the string through as `reasonCode` since that's what
    //      the chart marker logic switches on ("trade" vs "liq" vs "adl").
    function normalizeFill(f: any): any {
      const tsRaw = Number(f.timestamp ?? f.time ?? 0);
      // Heuristic: BULK timestamps are nanoseconds (16+ digits when
      // looked at as ms). If the value is greater than ~year 5000 in
      // ms (~1e14), it's nanoseconds and needs scaling down by 1e6.
      // Anything else we trust as ms or seconds.
      const tsMs =
        tsRaw > 1e14 ? Math.floor(tsRaw / 1e6) :
        tsRaw > 1e11 ? tsRaw : // already ms
        tsRaw * 1000; // seconds → ms

      return {
        timestamp: tsMs,
        symbol: String(f.symbol ?? f.sym ?? ''),
        price: Number(f.price ?? f.px ?? 0),
        size: Number(f.amount ?? f.size ?? f.sz ?? 0),
        isBuy: Boolean(f.isBuy ?? false),
        // Prefer the string `reason` ("liquidation", "trade") over the
        // numeric `reasonCode` since the frontend marker logic compares
        // strings. Map to our standard tokens.
        //
        // v1.0.15 added reasonCode 3 = "liquidation_sweep" (partial-liq
        // cascade). Check for "sweep" BEFORE the generic "liq" check so
        // "liquidation_sweep" gets the more specific `liq_sweep` token
        // rather than the generic `liq`.
        reasonCode: ((): string => {
          const reason = String(f.reason ?? '').toLowerCase();
          if (reason.includes('sweep')) return 'liq_sweep';
          if (reason.includes('liq')) return 'liq';
          if (reason.includes('adl')) return 'adl';
          // Numeric fallback when string is missing — based on observed
          // BULK behavior: 0 = trade, 1 = liq, 2 = adl, 3 = liq_sweep (v1.0.15).
          const code = Number(f.reasonCode ?? 0);
          if (code === 1) return 'liq';
          if (code === 2) return 'adl';
          if (code === 3) return 'liq_sweep';
          return 'trade';
        })(),
        // Pass through identifiers in case the frontend needs them later.
        orderIdMaker: f.orderIdMaker,
        orderIdTaker: f.orderIdTaker,
        maker: f.maker,
        taker: f.taker,
        iso: Boolean(f.iso ?? false),
        counterpartyHint: f.counterpartyHint,
      };
    }

    const bareCoin = symbol ? symbol.replace(/-USD$/, '') : null;
    const filtered: any[] = [];
    let firstSampleLogged = false;
    for (const raw of allFills as any[]) {
      if (!raw || typeof raw !== 'object') continue;
      const f = normalizeFill(raw);

      if (symbol) {
        const matches = f.symbol === symbol || f.symbol === bareCoin;
        if (!matches) {
          if (!firstSampleLogged) {
            firstSampleLogged = true;
            console.log(
              `[fills filter] no match for "${symbol}" (or "${bareCoin}"). ` +
                `Raw symbol field: "${raw.symbol ?? raw.sym ?? '(missing)'}", ` +
                `keys: [${Object.keys(raw).join(', ')}]`
            );
          }
          continue;
        }
      }
      filtered.push(f);
      if (filtered.length >= limit) break;
    }

    const payload = { fills: filtered };
    await setCache(cacheKey, payload, 60);
    res.json(payload);
  } catch (error: any) {
    console.error('GET /wallet/:address/fills error:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch fills' });
  }
});

// ============================================================================
// GET /wallet/:address/closed-positions
//
// Returns the wallet's closed-position history from BULK. Each entry is one
// open→close lifecycle: a position that was opened, possibly added to /
// reduced over time, and then fully closed. BULK pre-computes realized PnL
// (net of fees and funding) and exposes both the open and close timestamps.
//
// This is what powers the wallet page's "Recent Trades" / "Closed Positions"
// list — far more useful than raw fills because each row is one decision
// the trader committed to (entered, exited, here's how it played out).
//
// Optional ?symbol filter for symbol-scoped views (e.g. position chart
// modal might want closed-positions for just BTC-USD).
//
// Caching: 60s. Closed positions are immutable (closed = closed) so we
// can cache aggressively without staleness concerns. Background data
// collectors might add new closed positions over time, but a 60s
// freshness window is fine for that.
// ============================================================================
router.get('/:address/closed-positions', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const symbol = (req.query.symbol as string) || null;
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);

    const cacheKey = `wallet:closed-positions:${address}:${symbol || 'all'}:${limit}`;
    const cached = await getCache<{ positions: unknown[] }>(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const allPositions = await bulkApi.getClosedPositions(address);

    // Normalize each position to a stable shape. Same defensive approach as
    // the /fills route: we don't know exactly what fields BULK uses, so we
    // try common alternatives and log mismatches. After the first real
    // response in production, we narrow this down to actual field names.
    function normalizePosition(p: any): any {
      // Real BULK response shape (verified from production logs):
      //   {
      //     owner, symbol, quantity, maxQuantity, totalVolume,
      //     avgOpenPrice, avgClosePrice, realizedPnl, fees, funding,
      //     openTime, closeTime, closeReason, iso
      //   }
      //
      // We keep alternate-name fallbacks below for forward compatibility
      // (BULK has changed field names before — see the /fills bug we hit
      // earlier). The real names appear first in each `??` chain.

      // Timestamps. BULK uses nanoseconds for fills; closed positions
      // appear to use ms (the live values 1777985752181 ≈ Nov 2026 in ms).
      // The toMs heuristic handles both safely.
      const openTs = Number(
        p.openTime ?? p.opened ?? p.openTimestamp ??
        p.openedAt ?? p.entryTime ?? p.startTime ?? p.openTs ?? 0
      );
      const closeTs = Number(
        p.closeTime ?? p.closed ?? p.closeTimestamp ??
        p.closedAt ?? p.exitTime ?? p.endTime ?? p.closeTs ?? 0
      );
      const toMs = (ns: number): number =>
        ns > 1e14 ? Math.floor(ns / 1e6) :
        ns > 1e11 ? ns :
        ns * 1000;

      // Size — BULK uses `quantity` for closed positions. Sometimes signed,
      // sometimes absolute; we always expose absolute + a side string.
      const rawSize = Number(
        p.quantity ?? p.size ?? p.amount ?? p.qty ??
        p.sz ?? p.signedSize ?? p.totalSize ?? 0
      );
      const side: 'long' | 'short' =
        p.side
          ? String(p.side).toLowerCase() === 'short' ? 'short' : 'long'
          : rawSize < 0 ? 'short' : 'long';

      // Prices — `avgOpenPrice` and `avgClosePrice` are the real BULK
      // names. Other names kept as fallbacks for forward compatibility.
      const openPrice = Number(
        p.avgOpenPrice ?? p.openPrice ?? p.open_price ??
        p.entryPrice ?? p.entry_price ?? p.entry ??
        p.avgEntryPrice ?? p.avgEntry ?? p.entryVwap ?? p.vwapEntry ??
        p.op ?? p.openVwap ?? p.price ?? 0
      );
      const closePrice = Number(
        p.avgClosePrice ?? p.closePrice ?? p.close_price ??
        p.exitPrice ?? p.exit_price ?? p.exit ??
        p.avgExit ?? p.exitVwap ?? p.vwapExit ??
        p.cp ?? p.closeVwap ?? 0
      );

      // Liquidation detection. BULK uses `closeReason` (string) on closed
      // positions, not `reason` or numeric `reasonCode`. Common values
      // observed: "liquidation", "trade", "adl". v1.0.15 added
      // "liquidation_sweep". Match flexibly so we don't miss variants
      // like "Liquidation" or "liquidated", and so sweep counts as a
      // liquidation-flavored close (it economically IS one).
      const closeReason = String(p.closeReason ?? p.reason ?? '').toLowerCase();
      const liquidated =
        Boolean(p.liquidated ?? false) ||
        closeReason.includes('liq') || // matches "liquidation" AND "liquidation_sweep"
        Number(p.reasonCode ?? 0) === 1 ||
        Number(p.reasonCode ?? 0) === 3;

      return {
        symbol: String(p.symbol ?? p.sym ?? p.c ?? ''),
        side,
        size: Math.abs(rawSize),
        openPrice,
        closePrice,
        openedAt: toMs(openTs),
        closedAt: toMs(closeTs),
        // NET realized PnL — BULK returns the gross price-PnL component
        // separately from fees/funding (e.g. realizedPnl=-266.94 +
        // fees=-192.19 + funding=0 = trueNet=-459.13). We expose ONE
        // `realizedPnl` field that's already net so downstream code stays
        // simple; the gross components are available separately below
        // for hover/tooltip breakdowns.
        realizedPnl:
          Number(p.realizedPnl ?? p.pnl ?? p.realized_pnl ?? p.realized ?? 0) +
          Number(p.fees ?? p.fee ?? 0) +
          Number(p.funding ?? 0),
        // Keep the original gross PnL so the UI can show "Gross $X · Fees $Y"
        // breakdowns without re-deriving anything.
        grossPnl: Number(p.realizedPnl ?? p.pnl ?? p.realized_pnl ?? p.realized ?? 0),
        fees: Number(p.fees ?? p.fee ?? 0),
        funding: Number(p.funding ?? 0),
        // Leverage isn't included on closed-position responses — BULK
        // only exposes it on the live position object. We'd have to
        // compute it from totalVolume / margin, which we don't have.
        // Pass through if BULK does include it; otherwise 0 (frontend
        // hides the badge when leverage=0).
        leverage: Number(p.leverage ?? p.lev ?? 0),
        notional: p.notional !== undefined ? Number(p.notional) : undefined,
        liquidated,
        // Pass through the close reason as a debug aid + so the frontend
        // can show "ADL" vs "LIQ" vs other distinctly if we want to
        // expand later. Empty string when missing.
        closeReason,
      };
    }

    const bareCoin = symbol ? symbol.replace(/-USD$/, '') : null;
    const filtered: any[] = [];
    let firstSampleLogged = false;
    for (const raw of allPositions as any[]) {
      if (!raw || typeof raw !== 'object') continue;
      const p = normalizePosition(raw);

      if (symbol) {
        const matches = p.symbol === symbol || p.symbol === bareCoin;
        if (!matches) {
          if (!firstSampleLogged) {
            firstSampleLogged = true;
            console.log(
              `[closed-positions filter] no match for "${symbol}". ` +
                `Raw symbol field: "${raw.symbol ?? raw.sym ?? '(missing)'}", ` +
                `keys: [${Object.keys(raw).join(', ')}]`
            );
          }
          continue;
        }
      }
      filtered.push(p);
      if (filtered.length >= limit) break;
    }

    // Sort newest-close first — that's how users expect a "recent" list to
    // be ordered, and BULK's response order isn't guaranteed.
    filtered.sort((a, b) => b.closedAt - a.closedAt);

    const payload = { positions: filtered };
    await setCache(cacheKey, payload, 60);
    res.json(payload);
  } catch (error: any) {
    console.error('GET /wallet/:address/closed-positions error:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch closed positions' });
  }
});

// GET /wallet/:address/liquidations - Risk event history (liquidations + ADL)
//
// Source: BULK POST /account type:"riskHistory" exclusively. The DB
// `liquidations` and `adl_events` tables are no longer consulted here
// — they remain populated by wsListener for analytics aggregations
// elsewhere in the app, but the wallet detail page is now BULK-only
// so users see the rich fields (marginPrior, marginAfter, reason) that
// only this endpoint carries.
//
// Tradeoff: BULK runs a 5000-event protocol-wide ring buffer and has
// been observed to reset across testnet redeploys, so a wallet that
// was liquidated weeks ago may return empty. Acceptable per the
// architectural decision in the v1.0.15 migration — single source of
// truth, richer data, fewer code paths.
//
// Query params:
//   ?limit=50           Max events to return (default 50, cap 500)
//   ?type=liquidation   Filter by event type ('liquidation' | 'adl' | 'all', default 'all')
//
// Response:
//   {
//     events: RiskEvent[],
//     source: 'bulk',
//     truncated: boolean,    // true when BULK returned >= 5000 events
//   }
router.get('/:address/liquidations', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 500);
    const typeFilter = String(req.query.type ?? 'all').toLowerCase();

    const raw = await bulkApi.getRiskHistory(address);

    // Convert ns timestamp → ms, derive position side from fill direction,
    // pre-compute margin delta + USD value so the frontend stays simple.
    //
    // Side derivation: `isBuy` is the side of the LIQUIDATING FILL, not the
    // position. A long position is closed by a forced sell, so:
    //   - isBuy: false (forced sell) → position was LONG
    //   - isBuy: true  (forced buy)  → position was SHORT
    const events = raw
      .filter((e) => {
        if (typeFilter === 'all') return true;
        return e.eventType === typeFilter;
      })
      .map((e) => {
        const tsMs = Math.floor(Number(e.timestamp) / 1e6); // ns → ms
        const size = Number(e.amount) || 0;
        const price = Number(e.price) || 0;
        const marginPrior = Number(e.marginPrior) || 0;
        const marginAfter = Number(e.marginAfter) || 0;
        return {
          eventType: e.eventType,
          symbol: String(e.symbol ?? ''),
          side: e.isBuy ? 'short' : 'long', // see comment above
          size,
          price,
          value: size * price,
          marginPrior,
          marginAfter,
          marginDelta: marginAfter - marginPrior, // negative = loss
          reason: String(e.reason ?? ''),
          iso: Boolean(e.iso),
          timestamp: tsMs,
          slot: Number(e.slot) || 0,
          sequence: Number(e.sequence) || 0,
        };
      })
      // Newest first. Tiebreak by sequence so events in the same slot keep
      // a deterministic order (BULK's intra-block sequence number).
      .sort((a, b) => {
        if (b.timestamp !== a.timestamp) return b.timestamp - a.timestamp;
        return b.sequence - a.sequence;
      });

    const trimmed = events.slice(0, limit);

    res.json({
      events: trimmed,
      source: 'bulk' as const,
      // BULK's ring buffer is 5000 events. If we got back 5000 from raw,
      // the wallet's history almost certainly extends further back than
      // BULK is keeping. UI can hint at this so users don't think they're
      // seeing the complete record.
      truncated: raw.length >= 5000,
    });
  } catch (error: any) {
    console.error('GET /wallet/:address/liquidations error:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch risk events' });
  }
});

// GET /wallet/:address/hierarchy - Account hierarchy (master/sub-account/multisig)
//
// Returns the hierarchy view PLUS a financial snapshot per account so the
// frontend can render a single table without N additional round-trips.
//
// Response shape:
//   {
//     address, kind, parent?, subAccounts, multisigAccounts,
//     summaries: {
//       [pubkey]: {
//         totalBalance, availableBalance, marginUsed, notional,
//         unrealizedPnl, realizedPnl, positionsCount
//       }
//     }
//   }
//
// The hierarchy itself is resolved through accountResolver (24h cache).
// Per-account summaries are short-lived (60s) since balances drift
// continuously with mark price.
router.get('/:address/hierarchy', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    if (!address || address.length < 32) {
      return res.status(400).json({ error: 'Invalid address' });
    }
    const hierarchy = await resolveHierarchy(address);

    // Build the list of pubkeys we need balance summaries for. For a master
    // it's the master itself + each sub-account; for a sub-account it's just
    // the sub-account (the frontend banner doesn't need the master's
    // balance). For Unknown/multisig accounts we still try to summarize the
    // address itself so the frontend has something to display.
    const pubkeys: string[] = [address];
    if (hierarchy.kind === 'MasterEOA') {
      for (const sa of hierarchy.subAccounts) pubkeys.push(sa.pubkey);
    }

    // Cache key for the per-pubkey financial snapshot. Short TTL because
    // balances move with mark price.
    const SUMMARY_TTL = 60;
    const summaries: Record<string, unknown> = {};
    await Promise.all(
      pubkeys.map(async (pk) => {
        const cacheKey = `wallet:summary:${pk}`;
        const cached = await getCache<unknown>(cacheKey);
        if (cached) {
          summaries[pk] = cached;
          return;
        }
        const acc = await bulkApi.getFullAccount(pk);
        if (!acc) return;
        // Net at the API boundary — same semantics as the main GET handler.
        // Hierarchy banner shows per-account "current PnL" and that's now
        // the user-facing net value, including the fees/funding components.
        const m = acc.margin;
        const netRealized = (m?.realizedPnl ?? 0) + (m?.fees ?? 0) + (m?.funding ?? 0);
        const netUnrealized =
          m?.unrealizedPnl ??
          acc.positions?.reduce((s, p) => s + (p.unrealizedPnl || 0), 0) ??
          0;
        const summary = {
          totalBalance: m?.totalBalance ?? 0,
          availableBalance: m?.availableBalance ?? 0,
          marginUsed: m?.marginUsed ?? 0,
          // notional/unrealized/realized may not be on the margin object on all
          // BULK API versions — fall back to summing positions when missing.
          notional:
            (m as any)?.notional ??
            acc.positions?.reduce((s, p) => s + (p.notional || 0), 0) ??
            0,
          unrealizedPnl: netUnrealized,
          realizedPnl: netRealized,
          positionsCount: acc.positions?.length ?? 0,
        };
        await setCache(cacheKey, summary, SUMMARY_TTL);
        summaries[pk] = summary;
      })
    );

    res.json({ ...hierarchy, summaries });
  } catch (error: any) {
    console.error('Hierarchy lookup failed:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch hierarchy' });
  }
});

// GET /wallet/:address/activity - Activity timeline (deposits, withdrawals,
// transfers, sub-account events, multisig events).
//
// Proxies BULK's `activityHistory` query, then enriches each event by
// resolving the `from` and `to` pubkeys through accountResolver. This means
// sub-accounts surface as e.g. "alice's farm" instead of opaque off-curve
// addresses, which is the whole point of building this on top of the
// resolver.
//
// Caching: 30-second TTL. Activity events are immutable once written, but
// new events arrive frequently for active wallets so we don't want to cache
// for too long. 30s is short enough that the timeline feels live without
// hammering BULK on every page load.
//
// The frontend can request `?limit=N` to cap response size; default 50.
router.get('/:address/activity', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    if (!address || address.length < 32) {
      return res.status(400).json({ error: 'Invalid address' });
    }
    const limit = Math.min(Math.max(parseInt(String(req.query.limit ?? '50')) || 50, 1), 200);

    // Cache key includes the limit so a request for 200 isn't served the
    // truncated 50-event response.
    const cacheKey = `wallet:activity:${address}:${limit}`;
    const cached = await getCache<unknown>(cacheKey);
    if (cached) return res.json(cached);

    const events = await bulkApi.getActivityHistory(address);
    const truncated = events.slice(0, limit);

    // Enrich with resolver labels. We only resolve unique pubkeys to avoid
    // hammering the resolver cache for repeated counterparties (e.g. a wallet
    // that received 50 transfers from the same source).
    const uniqueAddrs = new Set<string>();
    for (const e of truncated) {
      if (e.from) uniqueAddrs.add(e.from);
      if (e.to) uniqueAddrs.add(e.to);
    }
    // Skip the system program (Solana's all-1s pubkey) — it's not a real
    // account and would cost a wasted resolver lookup.
    uniqueAddrs.delete('11111111111111111111111111111111');

    // Resolve all unique pubkeys in parallel. The resolver caches per-pubkey
    // for 24h so this is cheap on repeat calls.
    const labelMap = new Map<string, string>();
    await Promise.all(
      Array.from(uniqueAddrs).map(async (pk) => {
        try {
          const h = await resolveHierarchy(pk);
          if (h.kind === 'SubAccount' && h.parent) {
            // Look up the master to find this sub-account's name.
            const master = await resolveHierarchy(h.parent);
            const ref = master.subAccounts.find((s: { pubkey: string; name?: string }) => s.pubkey === pk);
            const name = ref?.name ?? 'sub-account';
            labelMap.set(pk, `${name} (${shortAddr(h.parent)}'s sub-account)`);
          }
          // Masters and Unknown accounts use their pubkey as-is — the
          // frontend formats them via formatAddress() for display.
        } catch {
          // Resolver failure is non-fatal — the event still renders, just
          // without a friendly label.
        }
      })
    );

    const enriched = truncated.map((e) => ({
      ...e,
      fromLabel: e.from ? labelMap.get(e.from) : undefined,
      toLabel: e.to ? labelMap.get(e.to) : undefined,
    }));

    const response = { address, data: enriched, count: enriched.length };
    await setCache(cacheKey, response, 30);
    res.json(response);
  } catch (error: any) {
    console.error('Activity lookup failed:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch activity' });
  }
});

// Helper used inline above for sub-account label rendering.
function shortAddr(a: string): string {
  if (!a || a.length < 12) return a;
  return `${a.slice(0, 4)}…${a.slice(-4)}`;
}

// ============ WATCHLIST (requires auth) ============

// GET /wallet/watchlist - Get user's watchlist
router.get('/user/watchlist', requireAuth, async (req: Request, res: Response) => {
  try {
    const watchlist = await query(
      `SELECT w.wallet_address, w.nickname, w.created_at, t.total_pnl, t.total_volume
       FROM watchlist w
       LEFT JOIN traders t ON w.wallet_address = t.wallet_address
       WHERE w.user_id = $1
       ORDER BY w.created_at DESC`,
      [req.userId]
    );
    
    res.json({ data: watchlist });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch watchlist' });
  }
});

// POST /wallet/watchlist/:address - Add to watchlist
router.post('/watchlist/:address', requireAuth, async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const { nickname } = req.body;

    // Ensure the traders row exists so leaderboard fallback queries can
    // resolve this wallet later. Fire-and-forget — adding to watchlist
    // doesn't depend on this completing, and the BULK fetch can be slow.
    bulkApi.getFullAccount(address).then((account) => {
      if (account) void upsertTraderRow(address, account);
    }).catch(() => { /* swallow — non-blocking */ });

    await query(
      `INSERT INTO watchlist (user_id, wallet_address, nickname)
       VALUES ($1, $2, $3)
       ON CONFLICT (user_id, wallet_address) DO UPDATE SET nickname = $3`,
      [req.userId, address, nickname || null]
    );

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to add to watchlist' });
  }
});

// DELETE /wallet/watchlist/:address - Remove from watchlist
router.delete('/watchlist/:address', requireAuth, async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    await query(
      'DELETE FROM watchlist WHERE user_id = $1 AND wallet_address = $2',
      [req.userId, address]
    );
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to remove from watchlist' });
  }
});

// ============ NOTIFICATIONS ============

// GET /wallet/notifications - Get user's notifications
router.get('/user/notifications', requireAuth, async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const unreadOnly = req.query.unread === 'true';
    
    let sql = `
      SELECT n.*, w.nickname
      FROM notifications n
      LEFT JOIN watchlist w ON n.wallet_address = w.wallet_address AND w.user_id = n.user_id
      WHERE n.user_id = $1
    `;
    
    if (unreadOnly) {
      sql += ' AND n.read = false';
    }
    
    sql += ' ORDER BY n.created_at DESC LIMIT $2';
    
    const notifications = await query(sql, [req.userId, limit]);
    
    // Get unread count
    const [countResult] = await query<{ count: string }>(
      'SELECT COUNT(*) as count FROM notifications WHERE user_id = $1 AND read = false',
      [req.userId]
    );
    
    res.json({ 
      data: notifications,
      unread_count: parseInt(countResult?.count || '0')
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch notifications' });
  }
});

// POST /wallet/notifications/read - Mark notifications as read
router.post('/user/notifications/read', requireAuth, async (req: Request, res: Response) => {
  try {
    const { ids } = req.body; // Array of notification IDs, or empty for all
    
    if (ids && Array.isArray(ids) && ids.length > 0) {
      await query(
        'UPDATE notifications SET read = true WHERE user_id = $1 AND id = ANY($2)',
        [req.userId, ids]
      );
    } else {
      // Mark all as read
      await query(
        'UPDATE notifications SET read = true WHERE user_id = $1 AND read = false',
        [req.userId]
      );
    }
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to mark notifications as read' });
  }
});

// DELETE /wallet/notifications - Clear all notifications
router.delete('/user/notifications', requireAuth, async (req: Request, res: Response) => {
  try {
    await query(
      'DELETE FROM notifications WHERE user_id = $1',
      [req.userId]
    );
    
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to clear notifications' });
  }
});

export default router;
