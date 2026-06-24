// Request-scoped network context.
//
// Express handlers run synchronously when they accept a request, but
// the BULK fetch calls inside them are async. To avoid threading a
// `net` parameter through every helper function down to bulkFetch,
// we use Node's AsyncLocalStorage to attach the chosen network to the
// async execution context that started in the request handler.
//
// Flow:
//
//   1. requestNetworkMiddleware reads `?net=` from the request, parses
//      it to a valid NetworkId, and wraps `next()` inside
//      `networkContext.run(net, next)`.
//
//   2. Every downstream async operation (including bulkFetch) runs
//      "inside" that context. `getRequestNetwork()` returns the
//      stored network without needing it as a function argument.
//
//   3. If a function runs OUTSIDE of a request handler (e.g. a cron
//      job calling bulkFetch), `getRequestNetwork()` returns
//      'testnet' — the safe default.
//
// This is a standard Node pattern (similar to thread-local storage in
// Java/C++ or `contextvars` in Python). No magic, no globals, no
// per-function threading. The one caveat: every code path that creates
// a new async context (worker threads, setImmediate before await, etc.)
// preserves the storage automatically as long as the language runtime
// is used normally.

import { AsyncLocalStorage } from 'async_hooks';
import type { Request, Response, NextFunction } from 'express';
import { DEFAULT_NETWORK, parseNetworkId, type NetworkId } from './networks';

const networkContext = new AsyncLocalStorage<NetworkId>();

// Express middleware. Mount this BEFORE any BULK-touching route. It
// reads `?net=` once and stores it for the lifetime of the request.
//
// Usage in index.ts:
//   app.use(requestNetworkMiddleware);
//   app.use('/api/analytics', analyticsRoutes);
export function requestNetworkMiddleware(
  req: Request,
  _res: Response,
  next: NextFunction
): void {
  const net = parseNetworkId(req.query.net);
  networkContext.run(net, next);
}

// Returns the network chosen by the current request, or 'testnet' if
// called outside of any request context (e.g. from a cron job).
// Safe to call from anywhere; never throws.
export function getRequestNetwork(): NetworkId {
  return networkContext.getStore() ?? DEFAULT_NETWORK;
}

// Run a function with an explicit network bound to the async context, so
// getRequestNetwork()/bulkFetch/getActiveSymbols resolve to it. For
// background jobs (pollers/collectors) that have no HTTP request to read
// `?net=` from. Re-invoke per tick so timer callbacks always carry the store.
export function runWithNetwork<T>(net: NetworkId, fn: () => T): T {
  return networkContext.run(net, fn);
}
