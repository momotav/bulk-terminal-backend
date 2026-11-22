// Live market data from BULK's WebSocket, held in memory so the exchange totals,
// the tickers endpoint and mark prices don't need an HTTP call per market.
//
// Two feeds (both on the indexer's one connection, see jobs/wsListener.ts):
//   • `ticker.<SYM>`     — the full per-market ticker (mark, OI, quoteVolume, high/low…)
//   • `frontendContext`  — every market in one message, every 2s (BULK's recommended
//                          source for dashboards): volume (base), oi (base), funding,
//                          lastPrice, priceChange(%).
// BULK's public limit is 5 HTTP req/s per IP and its docs say to prefer streams over
// polling; the per-market REST fan-out this replaces was the main source of our 429s.

/** Per-market OI from BULK is ONE side; longs + shorts = ×2 (see OI_SIDE_FACTOR). */
const OI_SIDE_FACTOR = 2;
const FRESH_MS = 30_000;

type Ticker = Record<string, unknown> & { symbol: string };
const tickers = new Map<string, { t: Ticker; ts: number }>();
let context: { ctx: Record<string, unknown>[]; ts: number } | null = null;

const num = (v: unknown) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

export function recordLiveTicker(symbol: string, t: Record<string, unknown>): void {
  if (!symbol) return;
  tickers.set(symbol, { t: { ...t, symbol }, ts: Date.now() });
}

export function recordFrontendContext(ctx: unknown): void {
  if (!Array.isArray(ctx) || ctx.length === 0) return;
  context = { ctx: ctx as Record<string, unknown>[], ts: Date.now() };
}

function freshContext(): Record<string, unknown>[] | null {
  return context && Date.now() - context.ts < FRESH_MS ? context.ctx : null;
}
function freshTicker(symbol: string): Ticker | null {
  const e = tickers.get(symbol);
  return e && Date.now() - e.ts < 5 * 60_000 ? e.t : null;
}

/** Price to value a market at: the ticker's mark when we have it, else last traded. */
function priceOf(symbol: string, last: number): number {
  const t = freshTicker(symbol);
  return num(t?.markPrice) || num(t?.lastPrice) || last;
}

/**
 * Exchange 24h volume (USD) and open interest (USD, both sides) across `symbols`,
 * from the live stream. null when the stream is stale or covers too few markets.
 */
export function liveExchangeTotals(symbols: string[]): { volume24h: number; openInterest: number; markets: number; ts: number } | null {
  const ctx = freshContext();
  if (!ctx) return null;
  const want = new Set(symbols);
  let vol = 0, oi = 0, n = 0;
  for (const c of ctx) {
    const s = String(c.symbol ?? '');
    if (want.size && !want.has(s)) continue;
    const last = num(c.lastPrice);
    const px = priceOf(s, last);
    // quoteVolume (USD) from the full ticker when fresh; else base volume × last price.
    const qv = num(freshTicker(s)?.quoteVolume);
    vol += qv > 0 ? qv : num(c.volume) * last;
    oi += num(c.oi) * px * OI_SIDE_FACTOR;
    n++;
  }
  // Too few markets would be a silent undercount: let the caller use another source.
  if (want.size && n < Math.ceil(want.size * 0.9)) return null;
  return { volume24h: vol, openInterest: oi, markets: n, ts: context!.ts };
}

/** Full tickers for `symbols` from the stream (ticker feed, topped up by the context feed). */
export function liveTickers(symbols: string[]): Ticker[] | null {
  const ctx = freshContext();
  const byCtx = new Map((ctx ?? []).map((c) => [String(c.symbol ?? ''), c]));
  const out: Ticker[] = [];
  for (const s of symbols) {
    const t = freshTicker(s);
    const c = byCtx.get(s);
    if (t) {
      // The context feed is newer (every 2s): take its live fields over the ticker's.
      out.push(c ? { ...t, lastPrice: num(c.lastPrice) || t.lastPrice, openInterest: c.oi ?? t.openInterest, fundingRate: c.funding ?? t.fundingRate,
        priceChange: c.priceChange ?? t.priceChange, priceChangePercent: c.priceChangePercent ?? t.priceChangePercent } : t);
    } else if (c) {
      const last = num(c.lastPrice);
      out.push({ symbol: s, lastPrice: last || null, markPrice: last || null, volume: num(c.volume), quoteVolume: num(c.volume) * last,
        openInterest: num(c.oi), fundingRate: num(c.funding), priceChange: c.priceChange ?? null, priceChangePercent: num(c.priceChangePercent) });
    }
  }
  if (out.length < Math.ceil(symbols.length * 0.9)) return null;
  return out;
}

/** Mark prices from the live ticker feed (fresh only). */
export function liveMarkPrices(): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [s] of tickers) {
    const t = freshTicker(s);
    const m = num(t?.markPrice);
    if (m > 0) out[s] = m;
  }
  return out;
}

export function liveMarketStatus() {
  return { contextAgeMs: context ? Date.now() - context.ts : null, contextMarkets: context?.ctx.length ?? 0, tickers: tickers.size };
}
