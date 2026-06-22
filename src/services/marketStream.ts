import { EventEmitter } from 'events';

// ---------------------------------------------------------------------------
// marketStream — process-wide pub/sub for live per-symbol price updates.
//
// The wsListener already holds the single upstream WebSocket to BULK and
// receives ticker (mark price) + trade (last price) messages. Instead of
// opening a second connection per SSE client, it publishes those prices onto
// this in-memory bus, and the /api/stream SSE route subscribes per symbol and
// fans them out to connected browsers.
//
// Event names ARE the (upper-cased) symbol, so each subscriber only receives
// its own symbol's events — no per-message filtering across all clients.
// ---------------------------------------------------------------------------

export type MarketUpdate = {
  symbol: string;
  price: number;          // last trade price OR mark price
  kind: 'trade' | 'mark';
  ts: number;             // ms epoch
};

const bus = new EventEmitter();
// Each open chart modal adds a listener; the default cap of 10 would warn.
// We manage cleanup explicitly on disconnect, so disable the cap.
bus.setMaxListeners(0);

export function publishMarketUpdate(u: MarketUpdate): void {
  const symbol = (u.symbol || '').toUpperCase();
  if (!symbol || symbol === 'UNKNOWN' || !(u.price > 0)) return;
  bus.emit(symbol, { ...u, symbol });
}

/** Subscribe to one symbol's price stream. Returns an unsubscribe fn. */
export function subscribeMarket(symbol: string, cb: (u: MarketUpdate) => void): () => void {
  const key = (symbol || '').toUpperCase();
  bus.on(key, cb);
  return () => bus.off(key, cb);
}
