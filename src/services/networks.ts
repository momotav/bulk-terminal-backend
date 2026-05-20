// Network configuration — defines the BULK networks BulkStats can read
// from. Currently two:
//
//   - "testnet" — the Paper Trading Testnet, the public-facing network
//     where the trading competition runs and where every user has been
//     active. This is the DEFAULT; if no network is specified, requests
//     route here. Same URLs the codebase has used since day one.
//
//   - "devnet" — the dev's internal environment for testing new
//     features (validator changes, new endpoints, etc.). Lives at the
//     `staging-*.bulk.trade` subdomains. Not where regular users want
//     to be looking.
//
// (Real "mainnet" doesn't exist yet — BULK is pre-launch. The Paper
// Trading Testnet is what people refer to as the production network
// today. When real mainnet ships, we add a third entry here.)
//
// The frontend sends `?net=<id>` on each request; the backend uses
// this to swap upstream BULK URLs before fetching. See
// `services/networkContext.ts` for the request-scoped routing
// mechanism.

export type NetworkId = 'testnet' | 'devnet';

export const DEFAULT_NETWORK: NetworkId = 'testnet';

interface NetworkConfig {
  // Trading REST API base. Callers pass full paths, so the base just
  // needs to match the protocol+host portion.
  apiBase: string;
  // Trading WebSocket URL (wss://)
  wsUrl: string;
  // Indexer base for leaderboard/wallet endpoints. Currently shared
  // across networks — BULK hasn't exposed a devnet indexer yet.
  indexerBase: string;
  // Explorer HTTP + WS bases. Both networks share the explorer at
  // 64.130.50.69 for now. When BULK exposes a separate explorer per
  // network, split these here.
  explorerHttp: string;
  explorerWs: string;
}

// Per-network URL map. The strings under `testnet` match what's been
// hardcoded across the codebase prior to this change — keep them
// identical so behavior is unchanged when `net=testnet` (or unset).
const NETWORKS: Record<NetworkId, NetworkConfig> = {
  testnet: {
    apiBase:      'https://exchange-api.bulk.trade/api/v1',
    wsUrl:        'wss://exchange-ws1.bulk.trade',
    indexerBase:  'https://indexer.bulk.trade/v1',
    explorerHttp: 'http://64.130.50.69:12003',
    explorerWs:   'ws://64.130.50.69:12004',
  },
  devnet: {
    apiBase:      'https://staging-api.bulk.trade/api/v1',
    wsUrl:        'wss://staging-ws.bulk.trade',
    // Devnet indexer not confirmed — fall back to testnet indexer.
    // If/when BULK exposes a `staging-indexer.bulk.trade` (or similar),
    // swap here.
    indexerBase:  'https://indexer.bulk.trade/v1',
    explorerHttp: 'http://64.130.50.69:12003',
    explorerWs:   'ws://64.130.50.69:12004',
  },
};

// Returns the config for the given network, falling back to the
// default (testnet) for invalid/missing values. Centralizes the
// validation so every caller doesn't repeat the same defensive check.
export function getNetworkConfig(net?: string | null): NetworkConfig {
  if (net === 'devnet') return NETWORKS.devnet;
  return NETWORKS.testnet;
}

// Coerces an arbitrary string into a valid NetworkId. Used in route
// handlers that read `req.query.net`.
export function parseNetworkId(value: unknown): NetworkId {
  if (typeof value === 'string' && value === 'devnet') return 'devnet';
  return DEFAULT_NETWORK;
}

// Rewrites a URL pointed at the testnet host to the target network's
// equivalent host. Path + query string preserved.
//
// Example:
//   resolveBulkUrl('https://exchange-api.bulk.trade/api/v1/klines?symbol=BTC', 'devnet')
//   → 'https://staging-api.bulk.trade/api/v1/klines?symbol=BTC'
//
// Used by `bulkFetch` so call sites don't need to know which host
// they're targeting — they keep using the testnet URL constants they
// always have, and the wrapper does the swap at request time.
//
// If the URL doesn't match any known testnet host, returns it
// unchanged. This makes the function safe to call on any URL
// (including third-party ones) without side effects.
export function resolveBulkUrl(url: string, net?: NetworkId): string {
  const target = net || DEFAULT_NETWORK;
  if (target === 'testnet') return url;
  // Only "devnet" remaining at this point.
  return url
    .replace(/^https:\/\/exchange-api\.bulk\.trade/, 'https://staging-api.bulk.trade')
    .replace(/^wss:\/\/exchange-ws1\.bulk\.trade/, 'wss://staging-ws.bulk.trade');
  // Note: indexer.bulk.trade NOT rewritten — devnet shares the
  // testnet indexer for now. Add the rewrite line when BULK ships a
  // devnet indexer.
}
