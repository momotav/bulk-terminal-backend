// Service to interact with BULK Exchange API

import { getActiveSymbols } from './markets';
import { bulkFetch } from './bulkAuth';

const BULK_API_URL = process.env.BULK_API_URL || 'https://exchange-api.bulk.trade/api/v1';

export interface Ticker {
  symbol: string;
  lastPrice: number;
  markPrice: number;
  priceChangePercent: number;
  volume: number;
  quoteVolume: number;
  openInterest: number;
  fundingRate: number;
  timestamp: number;
}

export interface Position {
  symbol: string;
  size: number;
  price: number;
  notional: number;
  // PnL convention (BULK v1.0.x): realizedPnl + unrealizedPnl are GROSS —
  // they're pure price math (close - open) × size, with no fees or funding
  // netted. To compute the true economic PnL of a position you must add
  // the `fees` (already negative-signed when paid) and `funding` fields:
  //
  //   netPnl = realizedPnl + unrealizedPnl + fees + funding
  //
  // Confirmed live 2026-05-21 against `Cpc7KD5C…`: closed-position rows
  // had realizedPnl=-266.94 matching (close-open)*size exactly, fees=-192.19
  // returned separately, and margin.realizedPnl summed gross across all
  // closed positions. We net at every display surface.
  unrealizedPnl: number;
  realizedPnl: number;
  fees: number;        // negative when paid by trader; zero before first fill
  funding: number;     // signed (negative when paid, positive when received)
  leverage: number;
  liquidationPrice: number;
}

// Raw risk-event row as returned by BULK's POST /account type:"riskHistory"
// endpoint (v1.0.15). Wrapper shape on the wire is [{ riskHistory: {...} }, ...]
// — we unwrap to RawRiskEvent in getRiskHistory(). Field names verified
// against a live production sample on 2026-05-21.
export interface RawRiskEvent {
  owner: string;
  symbol: string;
  isBuy: boolean;       // direction of the forced fill, NOT the position side
  amount: number;       // size in base units (unscaled)
  price: number;        // fill price
  eventType: 'liquidation' | 'adl';
  marginPrior: number;  // margin balance snapshot before the event
  marginAfter: number;  // margin balance snapshot after the event
  reason: string;       // human-readable, e.g. "liquidation due to equity X < maintenance margin: Y"
  iso: boolean;         // true for isolated-margin per-instrument events
  slot: number;         // Solana slot number
  timestamp: number;    // NANOSECONDS since epoch — divide by 1e6 for ms
  sequence: number;     // per-block sequence; useful as tiebreaker on identical timestamps
}

export interface FullAccount {
  // v1.0.14 hierarchy fields. Optional on the type because BULK omits them
  // when not applicable (e.g. masters with no sub-accounts have no
  // `subAccounts` field at all rather than an empty array).
  kind?: 'MasterEOA' | 'SubAccount';
  parent?: string;                          // present on sub-accounts only
  /**
   * Display name. Present on sub-account snapshots only — BULK v1.0.15
   * removed `name` from the master's subAccounts[] array, so to render
   * "gamble" instead of a pubkey you must fetch the sub's own snapshot
   * and read this field. `null` for masters and unnamed subs.
   */
  name?: string | null;
  subAccounts?: { pubkey: string; name?: string }[]; // v1.0.15: only {pubkey} now
  multisigAccounts?: string[];              // multisigs this account is a member of
  authorizedAgentWallets?: string[];

  margin: {
    totalBalance: number;
    availableBalance: number;
    marginUsed: number;
    // Aggregate wallet PnL across all positions. Same gross convention as
    // per-position fields — must net `fees + funding` to display true PnL.
    // See `Position` doc above for the full rationale.
    realizedPnl: number;
    unrealizedPnl: number;
    fees: number;       // wallet-wide total, negative when net paid
    funding: number;    // wallet-wide total, signed
    notional?: number;  // wallet-wide gross notional; BULK includes this on margin too
  };
  positions: Position[];
  openOrders: unknown[];
}

interface AccountResponse {
  fullAccount?: FullAccount;
  orderHistory?: unknown;
  fills?: unknown[];
  activityHistory?: ActivityEvent;
}

// Single row from BULK's `activityHistory` query. One physical event per row,
// wrapped in a single-key envelope by the upstream API.
//
// Possible activityType values (v1.0.14, may grow over time):
//   - "deposit"          : on-chain deposit landed (from = system program)
//   - "withdrawal"       : tokens left the protocol
//   - "transfer"         : protocol-native internal/external transfer
//   - "createSubAccount" : sub-account created under master
//   - "removeSubAccount" : sub-account removed (auto-sweeps balance to master)
//   - "renameSubAccount" : sub-account renamed (pubkey preserved)
//   - "multisigCreated"  : new multisig account created
//   - "proposalCreated", "proposalApproved", "proposalReadyForExecution",
//     "proposalExecuted", "proposalFailed", "proposalExpired",
//     "proposalCancelled", "proposalRejected" : multisig proposal lifecycle
//
// We model `activityType` as `string` to forward-compat unknown future events
// rather than coercing them into an enum that breaks on new values.
export interface ActivityEvent {
  activityType: string;
  status: string;             // "completed" | "failed" | etc.
  from?: string;              // source pubkey (system program "1111...111" for deposits)
  to?: string;                // destination pubkey
  symbol?: string;            // token symbol for transfers (e.g. "USDC")
  amount?: number;
  iso?: boolean;
  slot?: number;              // Solana slot number
  timestamp: number;          // nanoseconds (BULK convention)
  sequence?: number;
  // Multisig-specific fields likely appear here once that flow is active.
  // We pass them through untyped via a [k:string]:unknown index signature on
  // consumers when needed.
}

class BulkApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = BULK_API_URL;
  }

  // Fetch ticker for a symbol
  async getTicker(symbol: string): Promise<Ticker | null> {
    try {
      // Try the direct ticker endpoint first
      let res = await bulkFetch(`${this.baseUrl}/ticker/${symbol}`);
      
      // If that fails, try without the symbol in path (some APIs use query params)
      if (!res.ok) {
        res = await bulkFetch(`${this.baseUrl}/ticker?symbol=${symbol}`);
      }
      
      if (!res.ok) {
        console.log(`⚠️ Ticker endpoint failed for ${symbol}: ${res.status}`);
        return null;
      }
      
      const data: any = await res.json();
      console.log(`📊 Raw ticker data for ${symbol}:`, JSON.stringify(data).slice(0, 200));
      
      // Handle different response formats
      // Could be { symbol, markPrice, ... } or { data: { ... } } or array
      const ticker: any = data.data || data[0] || data;
      
      return {
        symbol: ticker.symbol || symbol,
        lastPrice: parseFloat(ticker.lastPrice || ticker.last || ticker.price || 0),
        markPrice: parseFloat(ticker.markPrice || ticker.mark || ticker.price || 0),
        priceChangePercent: parseFloat(ticker.priceChangePercent || ticker.change || 0),
        volume: parseFloat(ticker.volume || ticker.vol || 0),
        quoteVolume: parseFloat(ticker.quoteVolume || ticker.turnover || ticker.volume || 0),
        openInterest: parseFloat(ticker.openInterest || ticker.oi || 0),
        fundingRate: parseFloat(ticker.fundingRate || ticker.funding || 0),
        timestamp: ticker.timestamp || Date.now(),
      };
    } catch (error) {
      console.error(`Failed to fetch ticker for ${symbol}:`, error);
      return null;
    }
  }

  // Fetch all tickers
  async getAllTickers(): Promise<Ticker[]> {
    // First try bulk endpoint
    try {
      const res = await bulkFetch(`${this.baseUrl}/tickers`);
      if (res.ok) {
        const data: any = await res.json();
        const tickersArray: any[] = data.data || data || [];
        if (Array.isArray(tickersArray) && tickersArray.length > 0) {
          console.log(`📊 Got ${tickersArray.length} tickers from bulk endpoint`);
          return tickersArray.map((t: any) => ({
            symbol: t.symbol,
            lastPrice: parseFloat(t.lastPrice || t.last || t.price || 0),
            markPrice: parseFloat(t.markPrice || t.mark || t.price || 0),
            priceChangePercent: parseFloat(t.priceChangePercent || t.change || 0),
            volume: parseFloat(t.volume || 0),
            quoteVolume: parseFloat(t.quoteVolume || t.turnover || t.volume || 0),
            openInterest: parseFloat(t.openInterest || t.oi || 0),
            fundingRate: parseFloat(t.fundingRate || t.funding || 0),
            timestamp: t.timestamp || Date.now(),
          }));
        }
      }
    } catch (e) {
      console.log('Bulk tickers endpoint not available, falling back to individual');
    }
    
    // Fall back to individual symbol requests — use the live market list so
    // we don't miss newer coins (BNB, DOGE, FARTCOIN, SUI, ZEC). Falls back
    // internally to a hardcoded list if /exchangeInfo is unreachable.
    const symbols = await getActiveSymbols();
    const tickers = await Promise.all(
      symbols.map(symbol => this.getTicker(symbol))
    );
    return tickers.filter((t): t is Ticker => t !== null);
  }

  // Fetch account data for a wallet
  async getFullAccount(walletAddress: string): Promise<FullAccount | null> {
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'fullAccount', user: walletAddress }),
      });
      if (!res.ok) return null;
      const data = await res.json() as AccountResponse[];
      if (data && data[0] && data[0].fullAccount) {
        return data[0].fullAccount;
      }
      return null;
    } catch (error) {
      console.error(`Failed to fetch account for ${walletAddress}:`, error);
      return null;
    }
  }

  // Fetch activity history (deposits, withdrawals, transfers, sub-account
  // events, multisig events) for a wallet.
  //
  // BULK returns these wrapped in single-key envelopes:
  //   [{ "activityHistory": {...event...} }, ...]
  // We unwrap to a flat ActivityEvent[] and trust BULK's ordering. If BULK
  // truncates / paginates we handle it at the caller; this method just hands
  // back what came down the wire.
  async getActivityHistory(walletAddress: string): Promise<ActivityEvent[]> {
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'activityHistory', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json() as AccountResponse[];
      if (!Array.isArray(data)) return [];
      return data
        .map((row) => row.activityHistory)
        .filter((e): e is ActivityEvent => Boolean(e));
    } catch (error) {
      console.error(`Failed to fetch activity history for ${walletAddress}:`, error);
      return [];
    }
  }

  // Fetch exchange stats
  async getStats(): Promise<Record<string, unknown> | null> {
    try {
      const res = await bulkFetch(`${this.baseUrl}/stats?period=1d`);
      if (!res.ok) return null;
      const data = await res.json() as Record<string, unknown>;
      return data;
    } catch (error) {
      console.error('Failed to fetch stats:', error);
      return null;
    }
  }

  // Fetch order history for a wallet
  async getOrderHistory(walletAddress: string): Promise<unknown[]> {
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'orderHistory', user: walletAddress }),
      });
      if (!res.ok) return [];
      const data = await res.json() as AccountResponse[];
      const results: unknown[] = [];
      for (const item of data) {
        if (item && item.orderHistory) {
          results.push(item.orderHistory);
        }
      }
      return results;
    } catch (error) {
      console.error(`Failed to fetch order history for ${walletAddress}:`, error);
      return [];
    }
  }

  // Fetch fills (executed trades) for a wallet.
  //
  // Heavy wallets (high-volume traders) can have thousands of fills, and
  // BULK occasionally takes 5-10s to return the full set. We add an
  // explicit timeout so a slow fills response doesn't bottleneck the
  // wallet page or chart modal.
  //
  // On any error we still return [] so callers don't have to special-case
  // failures, but we log loudly so Railway logs show the actual reason
  // when fills appear missing on the frontend.
  async getFills(walletAddress: string): Promise<unknown[]> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'fills', user: walletAddress }),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        console.warn(
          `[bulkApi.getFills] BULK returned ${res.status} for ${walletAddress.slice(0, 8)}…`
        );
        return [];
      }
      const data = await res.json() as unknown[];
      const results: unknown[] = [];

      // Real BULK fills response shape (verified from production logs):
      //   [
      //     { "fills": { ...one fill object... } },   ← .fills is an OBJECT
      //     { "fills": { ...one fill object... } },     not an array
      //     ...
      //   ]
      // i.e. each array element wraps exactly one fill under the "fills"
      // key. Earlier we assumed `.fills` was an array per item — that
      // assumption silently dropped every fill.
      //
      // We also defensively handle two alternate shapes seen in older
      // BULK versions:
      //   - Wrapped-array:  [{ fills: [fill, fill, ...] }]
      //   - Flat:           [fill, fill, ...]
      // Detection is by inspecting each item's `.fills` value.
      if (Array.isArray(data)) {
        for (const raw of data) {
          if (!raw || typeof raw !== 'object') continue;
          const item = raw as { fills?: unknown };
          const f = item.fills;
          if (Array.isArray(f)) {
            // wrapped-array shape
            results.push(...f);
          } else if (f && typeof f === 'object') {
            // single-object shape (current production behavior)
            results.push(f);
          } else if (!('fills' in item)) {
            // flat shape — the element itself is the fill
            results.push(raw);
          }
        }
      }
      // If extraction found nothing, dump the raw BULK response so we
      // can see whether it's truly empty or whether the response shape
      // doesn't match `[{ fills: [...] }]` (e.g. it might be a flat
      // array, or use a different wrapper key). Without this, an empty
      // result is indistinguishable from a parse failure.
      if (results.length === 0) {
        try {
          const preview = JSON.stringify(data).slice(0, 500);
          console.warn(
            `[bulkApi.getFills] EMPTY for ${walletAddress.slice(0, 8)}… — ` +
              `data type: ${Array.isArray(data) ? `array(${data.length})` : typeof data}, ` +
              `raw preview: ${preview}`
          );
        } catch {
          /* serialization failure shouldn't crash the route */
        }
      }
      // Log the count + first symbol so Railway logs make it obvious
      // whether the issue is "no fills" vs "wrong symbol shape" vs
      // "BULK timed out".
      const sample = results[0] as { symbol?: string } | undefined;
      console.log(
        `[bulkApi.getFills] ${walletAddress.slice(0, 8)}… → ${results.length} fills` +
          (sample?.symbol ? ` (sample symbol: "${sample.symbol}")` : '')
      );
      return results;
    } catch (error: any) {
      clearTimeout(timer);
      const reason =
        error?.name === 'AbortError'
          ? 'timed out after 8s'
          : error?.message || 'unknown error';
      console.error(
        `[bulkApi.getFills] failed for ${walletAddress.slice(0, 8)}…: ${reason}`
      );
      return [];
    }
  }

  // Fetch closed-position history for a wallet.
  //
  // Per BULK docs, `positions` returns the last 5000 closed positions —
  // each one is a full open→close lifecycle with `realizedPnl`, `fees`,
  // and `funding` as separate fields. realizedPnl is GROSS price-only
  // PnL; downstream code nets `realizedPnl + fees + funding` to display
  // true economic PnL. See `normalizeClosedPosition` in wallet.ts for
  // the canonical netting site. Convention verified live 2026-05-21.
  //
  // Same shape-tolerance and logging strategy as getFills since BULK has
  // a habit of wrapping responses inconsistently (single-object .X vs
  // array .X vs flat).
  async getClosedPositions(walletAddress: string): Promise<unknown[]> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'positions', user: walletAddress }),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        console.warn(
          `[bulkApi.getClosedPositions] BULK returned ${res.status} for ${walletAddress.slice(0, 8)}…`
        );
        return [];
      }
      const data = await res.json() as unknown[];
      const results: unknown[] = [];

      // Try the same three shapes we've seen on /fills. Likely BULK uses
      // [{ position: {...} }] or [{ positions: [...] }] or flat
      // [position, ...]. Once we see a real response in the logs, we can
      // narrow this down.
      const wrapperKeys = ['position', 'positions', 'closedPosition'];
      if (Array.isArray(data)) {
        for (const raw of data) {
          if (!raw || typeof raw !== 'object') continue;
          const obj = raw as Record<string, unknown>;
          let extracted = false;
          for (const k of wrapperKeys) {
            if (k in obj) {
              const v = obj[k];
              if (Array.isArray(v)) {
                results.push(...v);
              } else if (v && typeof v === 'object') {
                results.push(v);
              }
              extracted = true;
              break;
            }
          }
          if (!extracted) {
            // Flat shape — the element itself is the position
            results.push(raw);
          }
        }
      }
      if (results.length === 0) {
        try {
          const preview = JSON.stringify(data).slice(0, 500);
          console.warn(
            `[bulkApi.getClosedPositions] EMPTY for ${walletAddress.slice(0, 8)}… — ` +
              `data type: ${Array.isArray(data) ? `array(${data.length})` : typeof data}, ` +
              `raw preview: ${preview}`
          );
        } catch { /* ignore */ }
      }
      const sample = results[0] as { symbol?: string } | undefined;
      console.log(
        `[bulkApi.getClosedPositions] ${walletAddress.slice(0, 8)}… → ${results.length} positions` +
          (sample?.symbol ? ` (sample symbol: "${sample.symbol}")` : '')
      );
      // One-time dump of the first raw position's full shape so we can see
      // which field names BULK actually uses for entry/close/size. Only
      // logged when results > 0 (no point dumping when EMPTY already
      // dumped). We log keys + the full JSON of the first item, capped to
      // 800 chars so Railway logs stay readable.
      if (results.length > 0) {
        try {
          const first = results[0] as Record<string, unknown>;
          const keys = Object.keys(first).join(', ');
          const preview = JSON.stringify(first).slice(0, 800);
          console.log(
            `[bulkApi.getClosedPositions] sample keys: [${keys}]\n` +
              `  full sample: ${preview}`
          );
        } catch {
          /* serialization failure shouldn't crash the route */
        }
      }
      return results;
    } catch (error: any) {
      clearTimeout(timer);
      const reason =
        error?.name === 'AbortError'
          ? 'timed out after 8s'
          : error?.message || 'unknown error';
      console.error(
        `[bulkApi.getClosedPositions] failed for ${walletAddress.slice(0, 8)}…: ${reason}`
      );
      return [];
    }
  }

  // Fetch the last 5000 protocol-wide risk events (liquidation + ADL) for a
  // wallet via POST /account type:"riskHistory" (BULK v1.0.15).
  //
  // Response shape on the wire (verified live 2026-05-21):
  //   [
  //     { "riskHistory": { owner, symbol, isBuy, amount, price, eventType,
  //                        marginPrior, marginAfter, reason, iso, slot,
  //                        timestamp, sequence } },
  //     ...
  //   ]
  //
  // IMPORTANT: BULK's riskHistory is a 5000-event ring buffer at the
  // protocol level, not a per-wallet log. Heavy traders eventually fall
  // off as the ring rotates. BULK has also been observed to reset this
  // state across testnet resets / redeploys (our DB has wallets with 500+
  // historical liqs that come back empty from this endpoint).
  //
  // We accept this tradeoff in exchange for the rich fields BULK provides
  // (marginPrior / marginAfter / reason) that our DB doesn't capture from
  // the WS stream. The wallet detail page reads exclusively from this
  // endpoint; DB liquidations remain for analytics aggregations only.
  //
  // Same defensive shape-detection as getFills / getClosedPositions so a
  // future BULK rewrap doesn't silently break the panel.
  async getRiskHistory(walletAddress: string): Promise<RawRiskEvent[]> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await bulkFetch(`${this.baseUrl}/account`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'riskHistory', user: walletAddress }),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        console.warn(
          `[bulkApi.getRiskHistory] BULK returned ${res.status} for ${walletAddress.slice(0, 8)}…`
        );
        return [];
      }
      const data = await res.json() as unknown[];
      const results: RawRiskEvent[] = [];

      // Production shape: [{ riskHistory: {...one event...} }, ...]
      // Defensive: also handle flat [event, ...] and wrapped-array
      // [{ riskHistory: [event, ...] }] in case BULK changes its mind.
      if (Array.isArray(data)) {
        for (const raw of data) {
          if (!raw || typeof raw !== 'object') continue;
          const obj = raw as Record<string, unknown>;
          if ('riskHistory' in obj) {
            const v = obj.riskHistory;
            if (Array.isArray(v)) {
              for (const ev of v) {
                if (ev && typeof ev === 'object') results.push(ev as RawRiskEvent);
              }
            } else if (v && typeof v === 'object') {
              results.push(v as RawRiskEvent);
            }
          } else if ('eventType' in obj) {
            // Flat shape — the element itself is the event
            results.push(obj as unknown as RawRiskEvent);
          }
        }
      }

      // Empty result is plausible (wallet has no liquidations, OR BULK reset)
      // — both are valid states. Log raw preview only when we got something
      // back but extracted nothing, which signals a shape mismatch.
      if (results.length === 0 && Array.isArray(data) && data.length > 0) {
        try {
          const preview = JSON.stringify(data).slice(0, 500);
          console.warn(
            `[bulkApi.getRiskHistory] SHAPE MISMATCH for ${walletAddress.slice(0, 8)}… — ` +
              `raw preview: ${preview}`
          );
        } catch { /* ignore */ }
      }

      console.log(
        `[bulkApi.getRiskHistory] ${walletAddress.slice(0, 8)}… → ${results.length} events`
      );
      return results;
    } catch (error: any) {
      clearTimeout(timer);
      const reason =
        error?.name === 'AbortError'
          ? 'timed out after 8s'
          : error?.message || 'unknown error';
      console.error(
        `[bulkApi.getRiskHistory] failed for ${walletAddress.slice(0, 8)}…: ${reason}`
      );
      return [];
    }
  }
}

export const bulkApi = new BulkApiService();
