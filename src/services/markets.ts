/**
 * Shared helper that returns the list of markets BULK currently has listed.
 *
 * Before this existed, three places had hardcoded arrays like
 *   ['BTC-USD', 'ETH-USD', 'SOL-USD', 'GOLD-USD', 'XRP-USD']
 * which meant every time BULK listed a new coin (BNB, DOGE, SUI, FARTCOIN,
 * ZEC, ...) we silently stopped collecting its trades / liquidations / ADL
 * events and users never saw it on the charts.
 *
 * The function:
 *   1. Fetches `GET /exchangeInfo` from BULK (cached in memory for 5 minutes)
 *   2. Extracts the `symbol` of each market returned
 *   3. Falls back to a known-good hardcoded list if the fetch fails so the
 *      pipeline never comes to a complete halt during BULK outages.
 */

const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

// 5-minute cache. BULK adds/delists markets infrequently so this is plenty
// fresh, and it keeps us from hammering /exchangeInfo on every reconnect.
const CACHE_TTL_MS = 5 * 60 * 1000;

// Known-good fallback used when /exchangeInfo is unreachable. Keep at least
// the coins we've always supported here so the site never fully breaks.
const FALLBACK_SYMBOLS: readonly string[] = [
  'BTC-USD',
  'ETH-USD',
  'SOL-USD',
  'GOLD-USD',
  'XRP-USD',
  'BNB-USD',
  'DOGE-USD',
  'FARTCOIN-USD',
  'SUI-USD',
  'ZEC-USD',
];

let cache: { symbols: string[]; expiresAt: number } | null = null;
let inflight: Promise<string[]> | null = null;

interface ExchangeInfoMarket {
  symbol?: unknown;
  status?: unknown;
}

async function fetchFromBulk(): Promise<string[]> {
  const res = await fetch(`${BULK_API_BASE}/exchangeInfo`);
  if (!res.ok) {
    throw new Error(`BULK /exchangeInfo returned ${res.status}`);
  }
  const raw: unknown = await res.json();
  if (!Array.isArray(raw)) {
    throw new Error('Unexpected /exchangeInfo response shape');
  }

  const symbols: string[] = [];
  for (const item of raw as ExchangeInfoMarket[]) {
    if (item && typeof item.symbol === 'string' && item.symbol.length > 0) {
      // Optionally skip markets that aren't TRADING. For analytics we usually
      // still want historical data so we accept any status here, but guarding
      // against weird/internal entries with an explicit symbol check above is
      // enough.
      symbols.push(item.symbol);
    }
  }
  if (symbols.length === 0) {
    throw new Error('No markets returned by /exchangeInfo');
  }
  return symbols;
}

/**
 * Return the list of BULK market symbols (e.g. ["BTC-USD", "ETH-USD", ...]).
 *
 * Result is cached in memory for 5 minutes. Multiple concurrent callers while
 * a refresh is in flight will share the same promise instead of firing off
 * duplicate requests.
 *
 * On any error fetching from BULK, the function returns the fallback list
 * so the rest of the app can keep making progress.
 */
export async function getActiveSymbols(forceRefresh = false): Promise<string[]> {
  const now = Date.now();

  if (!forceRefresh && cache && cache.expiresAt > now) {
    return cache.symbols;
  }

  // De-dupe concurrent refreshes
  if (inflight) return inflight;

  inflight = (async () => {
    try {
      const symbols = await fetchFromBulk();
      cache = { symbols, expiresAt: Date.now() + CACHE_TTL_MS };
      console.log(`📋 Market list refreshed: ${symbols.length} symbols — ${symbols.join(', ')}`);
      return symbols;
    } catch (err) {
      console.error('⚠️  Failed to fetch /exchangeInfo, using fallback symbol list:', err);
      // Populate cache with the fallback so we don't retry on every single call;
      // short TTL so we try BULK again soon.
      cache = {
        symbols: [...FALLBACK_SYMBOLS],
        expiresAt: Date.now() + 30 * 1000, // 30s retry window after failure
      };
      return cache.symbols;
    } finally {
      inflight = null;
    }
  })();

  return inflight;
}

/**
 * Synchronous accessor — returns the last cached list or the fallback. Use
 * only in code paths that MUST NOT await (e.g. inside a hot WebSocket handler
 * reading allowed symbols). Production callers should prefer getActiveSymbols().
 */
export function getActiveSymbolsSync(): string[] {
  return cache?.symbols ?? [...FALLBACK_SYMBOLS];
}
