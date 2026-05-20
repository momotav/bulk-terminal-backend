// Network configuration — defines the BULK networks BulkStats can read
// from. Currently mainnet + staging. The frontend sends `?net=<id>` on
// requests; the backend uses this to swap upstream BULK URLs before
// fetching.
//
// Design notes:
//
// - "mainnet" is the default — same URLs we've used since day one. Code
//   paths without an explicit `net` parameter behave as if mainnet was
//   requested. Zero behavior change for existing routes.
//
// - "staging" maps each mainnet URL to its staging-host equivalent:
//     exchange-api.bulk.trade   → staging-api.bulk.trade
//     exchange-ws1.bulk.trade   → staging-ws.bulk.trade
//   Indexer doesn't have a known staging counterpart yet, so indexer
//   calls fall back to mainnet on staging mode. The dev can add a
//   staging-indexer subdomain later and we update one constant.
//
// - The explorer endpoints (64.130.50.69:12003/12004) are CURRENTLY
//   staging — see the conversation history with the dev. So on
//   "staging" we use them; on "mainnet" we... also use them for now,
//   because BULK hasn't exposed a mainnet explorer yet. When they do,
//   we add a MAINNET_EXPLORER_HTTP and the switching happens here.
//
// - This file is the single source of truth for network URLs. Don't
//   hardcode 'exchange-api.bulk.trade' elsewhere in the codebase —
//   route everything through `resolveBulkUrl()` so staging routing
//   actually takes effect.

export type NetworkId = 'mainnet' | 'staging';

export const DEFAULT_NETWORK: NetworkId = 'mainnet';

interface NetworkConfig {
  // Trading REST API base, ending without trailing slash. We accept the
  // `/api/v1` suffix or omit it — callers pass the full path anyway,
  // so the base just needs to match the protocol+host portion.
  apiBase: string;
  // Trading WebSocket URL (wss://)
  wsUrl: string;
  // Indexer base for leaderboard/wallet endpoints. May be the same
  // across networks if BULK runs a single indexer.
  indexerBase: string;
  // Explorer HTTP + WS bases. Both networks point at the same explorer
  // node for now (BULK only exposes one). When mainnet gets its own,
  // we split here.
  explorerHttp: string;
  explorerWs: string;
}

// Per-network URL map. The strings in MAINNET match what's been
// hardcoded across the codebase prior to this change; keep them
// identical so behavior is unchanged when `net=mainnet` (or unset).
const NETWORKS: Record<NetworkId, NetworkConfig> = {
  mainnet: {
    apiBase:      'https://exchange-api.bulk.trade/api/v1',
    wsUrl:        'wss://exchange-ws1.bulk.trade',
    indexerBase:  'https://indexer.bulk.trade/v1',
    explorerHttp: 'http://64.130.50.69:12003',
    explorerWs:   'ws://64.130.50.69:12004',
  },
  staging: {
    apiBase:      'https://staging-api.bulk.trade/api/v1',
    wsUrl:        'wss://staging-ws.bulk.trade',
    // Staging indexer not confirmed — fall back to mainnet indexer.
    // If/when BULK exposes staging-indexer.bulk.trade, swap here.
    indexerBase:  'https://indexer.bulk.trade/v1',
    explorerHttp: 'http://64.130.50.69:12003',
    explorerWs:   'ws://64.130.50.69:12004',
  },
};

// Returns the config for the given network, falling back to mainnet
// for invalid/missing values. Centralizes the validation so every
// caller doesn't repeat the same defensive check.
export function getNetworkConfig(net?: string | null): NetworkConfig {
  if (net === 'staging') return NETWORKS.staging;
  return NETWORKS.mainnet;
}

// Coerces an arbitrary string into a valid NetworkId. Used in route
// handlers that read `req.query.net`.
export function parseNetworkId(value: unknown): NetworkId {
  if (typeof value === 'string' && value === 'staging') return 'staging';
  return DEFAULT_NETWORK;
}

// Rewrites a URL pointed at the mainnet host to the target network's
// equivalent host. Path + query string preserved.
//
// Example:
//   resolveBulkUrl('https://exchange-api.bulk.trade/api/v1/klines?symbol=BTC', 'staging')
//   → 'https://staging-api.bulk.trade/api/v1/klines?symbol=BTC'
//
// Used by `bulkFetch` so call sites don't need to know which host
// they're targeting — they keep using the mainnet URL constants they
// always have, and the wrapper does the swap at request time.
//
// If the URL doesn't match any known mainnet host, returns it
// unchanged. This makes the function safe to call on any URL
// (including third-party ones) without side effects.
export function resolveBulkUrl(url: string, net?: NetworkId): string {
  const target = net || DEFAULT_NETWORK;
  if (target === 'mainnet') return url;
  // Only "staging" remaining at this point.
  return url
    .replace(/^https:\/\/exchange-api\.bulk\.trade/, 'https://staging-api.bulk.trade')
    .replace(/^wss:\/\/exchange-ws1\.bulk\.trade/, 'wss://staging-ws.bulk.trade');
  // Note: indexer.bulk.trade NOT rewritten — staging shares the
  // mainnet indexer for now. Add the rewrite line when BULK ships a
  // staging indexer.
}
