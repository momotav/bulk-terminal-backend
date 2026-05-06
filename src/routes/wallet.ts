import { Router, Request, Response } from 'express';
import { query, queryOne } from '../db';
import { bulkApi, FullAccount } from '../services/bulkApi';
import { requireAuth } from '../middleware/auth';
import { getCache, setCache } from '../services/cache';
import { resolveHierarchy } from '../services/accountResolver';

const router = Router();

// Helper: store snapshot from an already-fetched FullAccount, avoiding
// a duplicate BULK API round-trip. The previous version of this function
// called bulkApi.getFullAccount() internally, which meant every wallet
// page view fetched the account twice — once in the GET handler, once
// here. That added 200-500ms per visit on no actual benefit.
async function storeWalletSnapshot(walletAddress: string, account: FullAccount): Promise<void> {
  try {
    // Calculate totals from positions
    let totalNotional = 0;
    let totalRealizedPnl = 0;
    let totalUnrealizedPnl = 0;

    for (const p of account.positions) {
      totalNotional += Math.abs(p.notional || 0);
      totalRealizedPnl += p.realizedPnl || 0;
      totalUnrealizedPnl += p.unrealizedPnl || 0;
    }

    // Use margin totals if available (more authoritative than per-position
    // sums because margin includes cross-account effects).
    const marginRealizedPnl = account.margin?.realizedPnl || 0;
    const marginUnrealizedPnl = account.margin?.unrealizedPnl || 0;

    const realizedPnl = marginRealizedPnl !== 0 ? marginRealizedPnl : totalRealizedPnl;
    const unrealizedPnl = marginUnrealizedPnl !== 0 ? marginUnrealizedPnl : totalUnrealizedPnl;
    const totalPnl = realizedPnl + unrealizedPnl;

    // Update trader with current PnL. Don't await — fire and forget. The
    // user is waiting on this endpoint and the trader update is purely
    // for our DB analytics; nobody is reading total_pnl back synchronously.
    void query(
      `INSERT INTO traders (wallet_address, total_pnl, last_seen)
       VALUES ($1, $2, NOW())
       ON CONFLICT (wallet_address) DO UPDATE SET
         total_pnl = $2,
         last_seen = NOW()`,
      [walletAddress, totalPnl]
    ).catch((err) => console.error('snapshot trader update failed:', err));

    // Store snapshot for history. Same fire-and-forget pattern. Skip if
    // the wallet has nothing to snapshot — saves snapshot table bloat.
    if (account.positions.length > 0 || totalPnl !== 0) {
      void query(
        `INSERT INTO trader_snapshots
         (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
         VALUES ($1, $2, $3, $4, $5)`,
        [walletAddress, realizedPnl, unrealizedPnl, account.positions.length, totalNotional]
      ).catch((err) => console.error('snapshot insert failed:', err));
    }
  } catch (error) {
    console.error(`Failed to store wallet snapshot for ${walletAddress.slice(0, 8)}:`, error);
  }
}

// Legacy function kept for the /track endpoint and the bulk hierarchy refresh.
// New callers should prefer storeWalletSnapshot (above) when they already have
// a FullAccount in hand.
async function fetchAndStoreWalletSnapshot(walletAddress: string): Promise<void> {
  const account = await bulkApi.getFullAccount(walletAddress);
  if (!account) return;
  await storeWalletSnapshot(walletAddress, account);
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
    const [account, tickers, trader, snapshots] = await Promise.all([
      bulkApi.getFullAccount(address),
      bulkApi.getAllTickers(),
      queryOne(
        'SELECT * FROM traders WHERE wallet_address = $1',
        [address]
      ),
      query(
        `SELECT timestamp, pnl, unrealized_pnl, positions_count, total_notional
         FROM trader_snapshots
         WHERE wallet_address = $1
         ORDER BY timestamp DESC
         LIMIT 168`,
        [address]
      ),
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
        history: snapshots.reverse(),
      });
    }

    // BULK responded successfully. Build the response.
    const markPrices: Record<string, number> = {};
    for (const ticker of tickers) {
      markPrices[ticker.symbol] = ticker.markPrice;
    }

    const result = {
      address,
      live: account,
      markPrices,
      tracked: trader,
      history: snapshots.reverse(),
    };

    // Fire-and-forget snapshot write. We already have the FullAccount, so
    // we pass it directly — no second BULK round-trip. Never blocks the
    // response.
    void storeWalletSnapshot(address, account);

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
router.post('/:address/track', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    
    // Actually fetch and store data
    await fetchAndStoreWalletSnapshot(address);
    
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
        reasonCode: ((): string => {
          const reason = String(f.reason ?? '').toLowerCase();
          if (reason.includes('liq')) return 'liq';
          if (reason.includes('adl')) return 'adl';
          // Numeric fallback when string is missing — based on observed
          // BULK behavior: 0 = trade, 1 = liq, 2 = adl.
          const code = Number(f.reasonCode ?? 0);
          if (code === 1) return 'liq';
          if (code === 2) return 'adl';
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
      // Timestamps — likely nanoseconds based on /fills precedent. Try
      // every plausible name; ts-in-ns gets normalized below.
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

      // Size: BULK has used many variants across endpoints. Signed sizes
      // are a strong convention (negative = short) so we honor that as
      // the side fallback when no explicit `side` field is present.
      const rawSize = Number(
        p.size ?? p.amount ?? p.qty ?? p.quantity ??
        p.sz ?? p.signedSize ?? p.totalSize ?? 0
      );
      const side: 'long' | 'short' =
        p.side
          ? String(p.side).toLowerCase() === 'short' ? 'short' : 'long'
          : rawSize < 0 ? 'short' : 'long';

      // Entry/close prices — try every plausible name. The wide set
      // includes compact (op/cp), camelCase, snake_case, vwap variants,
      // and "average" variants. After we see real BULK output in logs
      // we can narrow this down.
      const openPrice = Number(
        p.openPrice ?? p.open_price ??
        p.entryPrice ?? p.entry_price ?? p.entry ??
        p.avgEntryPrice ?? p.avgEntry ?? p.entryVwap ?? p.vwapEntry ??
        p.op ?? p.openVwap ?? p.price ?? 0
      );
      const closePrice = Number(
        p.closePrice ?? p.close_price ??
        p.exitPrice ?? p.exit_price ?? p.exit ??
        p.avgClosePrice ?? p.avgExit ?? p.exitVwap ?? p.vwapExit ??
        p.cp ?? p.closeVwap ?? 0
      );

      return {
        symbol: String(p.symbol ?? p.sym ?? p.c ?? ''),
        side,
        size: Math.abs(rawSize),
        openPrice,
        closePrice,
        openedAt: toMs(openTs),
        closedAt: toMs(closeTs),
        realizedPnl: Number(p.realizedPnl ?? p.pnl ?? p.realized_pnl ?? p.realized ?? 0),
        fees: Number(p.fees ?? p.fee ?? 0),
        funding: Number(p.funding ?? 0),
        leverage: Number(p.leverage ?? p.lev ?? 0),
        notional: p.notional !== undefined ? Number(p.notional) : undefined,
        liquidated:
          Boolean(p.liquidated ?? false) ||
          // BULK often signals forced exits via reasonCode rather than a
          // dedicated flag. Treat both presentations as "liquidated".
          String(p.reason ?? '').toLowerCase().includes('liq') ||
          Number(p.reasonCode ?? 0) === 1,
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

// GET /wallet/:address/liquidations - Get liquidation history
router.get('/:address/liquidations', async (req: Request, res: Response) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    
    const liquidations = await query(
      `SELECT * FROM liquidations 
       WHERE wallet_address = $1 
       ORDER BY timestamp DESC 
       LIMIT $2`,
      [address, limit]
    );
    
    res.json({ data: liquidations });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch liquidations' });
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
        const summary = {
          totalBalance: acc.margin?.totalBalance ?? 0,
          availableBalance: acc.margin?.availableBalance ?? 0,
          marginUsed: acc.margin?.marginUsed ?? 0,
          // notional/unrealized/realized may not be on the margin object on all
          // BULK API versions — fall back to summing positions when missing.
          notional:
            (acc.margin as any)?.notional ??
            acc.positions?.reduce((s, p) => s + (p.notional || 0), 0) ??
            0,
          unrealizedPnl:
            acc.margin.unrealizedPnl ??
            acc.positions?.reduce((s, p) => s + (p.unrealizedPnl || 0), 0) ??
            0,
          realizedPnl: acc.margin.realizedPnl ?? 0,
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
    
    // Start tracking if not already
    await fetchAndStoreWalletSnapshot(address);
    
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
