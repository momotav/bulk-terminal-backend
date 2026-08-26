// Shared authentication header + fetch wrapper for BULK upstream calls.
//
// BULK has issued us a private API key (Mainnet sprint era). The key
// must be attached to every request we send to BULK-owned endpoints as
// the `x-bulk-api-key` HTTP header. To avoid scattering header logic
// across 25+ fetch call sites, we centralize:
//
//   bulkHeaders()           — returns the auth header object
//   bulkFetch(url, init)    — drop-in replacement for `fetch()` that merges in auth
//
// Every BULK-bound fetch in this codebase should use `bulkFetch`. Direct
// `fetch()` to a BULK domain is a bug; the request will succeed for now
// (BULK might not enforce yet) but will start failing once enforcement
// is on — and we'd have no central place to fix it.
//
// The key is read from env once at module load. If `BULK_API_KEY` is
// not set, `bulkHeaders()` returns `{}` and `bulkFetch` behaves like
// plain `fetch`. This keeps local development working without a key,
// and means a misconfigured production deploy fails loud (401 from
// BULK) rather than silently sending an empty key.
//
// `bulkFetch` also accepts an optional `{ net }` parameter to route
// the request at devnet instead of testnet. The URL passed in is
// always a testnet URL — `resolveBulkUrl` rewrites the host based on
// the target network. This means existing call sites don't need to
// change at all to support devnet; routes just need to thread the
// `net` value down from the request to the call site.

import { resolveBulkUrl, type NetworkId } from './networks';
import { getRequestNetwork } from './networkContext';

const BULK_API_KEY = process.env.BULK_API_KEY;

if (!BULK_API_KEY) {
  console.warn(
    '⚠️  BULK_API_KEY not set — BULK upstream calls will be unauthenticated. ' +
      'Set BULK_API_KEY in Railway env vars for production.'
  );
}

// Returns the auth header object. Use this in places that build their
// own headers and merge with other values (e.g. POSTs with Content-Type).
export function bulkHeaders(): Record<string, string> {
  if (!BULK_API_KEY) return {};
  return { 'x-bulk-api-key': BULK_API_KEY };
}

// Wrapper around fetch() that automatically attaches the auth header.
// Use this for any GET/POST to a BULK endpoint instead of raw fetch().
// All existing fetch options pass through unchanged.
//
// The optional `opts.net` argument controls which BULK network to
// target. When unset (or 'testnet'), the URL is used as-is. When
// 'devnet', the URL's host is rewritten to the devnet equivalent
// via resolveBulkUrl. The same auth key is sent regardless — BULK
// has confirmed devnet accepts the testnet key.
export interface BulkFetchOpts {
  net?: NetworkId;
}

// BULK executor upgrades (API v1.0.18) pause transaction admission and return
// `503 { error: { code: "UPGRADE_IN_PROGRESS" } }`, optionally with a
// `Retry-After` header. The request was NOT processed, so it's safe to retry.
// We ride out the blip with a bounded backoff instead of surfacing an error.
const MAX_UPGRADE_RETRIES = 3;
const MAX_RETRY_DELAY_MS = 5000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Parse a `Retry-After` header (delta-seconds or HTTP-date) into ms, or null.
function parseRetryAfterMs(res: Response): number | null {
  const h = res.headers.get('retry-after');
  if (!h) return null;
  const secs = Number(h);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(h);
  if (!Number.isNaN(at)) return Math.max(0, at - Date.now());
  return null;
}

export async function bulkFetch(
  url: string | URL,
  init?: RequestInit,
  opts?: BulkFetchOpts
): Promise<Response> {
  const callerHeaders = init?.headers || {};
  const mergedHeaders = {
    ...bulkHeaders(),
    ...normalizeHeaders(callerHeaders),
  };

  // Pick network in priority order:
  //   1. Explicit opts.net (caller knows exactly what they want)
  //   2. Request-scoped context (set by requestNetworkMiddleware)
  //   3. testnet (default, for cron jobs and contexts without a request)
  const net = opts?.net ?? getRequestNetwork();
  const targetUrl = resolveBulkUrl(typeof url === 'string' ? url : url.toString(), net);

  // Retry loop scoped to the documented `UPGRADE_IN_PROGRESS` 503 only. Any
  // other status — including a 503 that isn't an upgrade — is returned to the
  // caller untouched, so this can't mask real errors or double-run non-upgrade
  // requests. Only string/JSON bodies are used here, so re-sending `init` is
  // safe (no consumed streams).
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(targetUrl, { ...init, headers: mergedHeaders });
    if (res.status !== 503 || attempt >= MAX_UPGRADE_RETRIES) return res;

    let isUpgrade = false;
    try {
      isUpgrade = (await res.clone().text()).includes('UPGRADE_IN_PROGRESS');
    } catch {
      // If the body can't be read, treat it as a normal 503 and hand it back.
    }
    if (!isUpgrade) return res;

    const delay = Math.min(parseRetryAfterMs(res) ?? 300 * 2 ** attempt, MAX_RETRY_DELAY_MS);
    console.warn(`BULK upgrade in progress — retry ${attempt + 1}/${MAX_UPGRADE_RETRIES} in ${delay}ms`);
    await sleep(delay);
  }
}

// fetch() accepts headers in three formats: plain object, Headers instance,
// or array of [name, value] tuples. Convert all three to a plain object
// for easy merging.
//
// Param typed as `any` because TypeScript's `HeadersInit` type isn't
// always available depending on tsconfig lib settings, and the Node /
// DOM type definitions for fetch headers have shifted across versions.
// Internal helper — we control all callers — so type safety here is
// less important than build portability.
function normalizeHeaders(h: any): Record<string, string> {
  if (!h) return {};
  if (typeof Headers !== 'undefined' && h instanceof Headers) {
    const obj: Record<string, string> = {};
    h.forEach((value: string, key: string) => {
      obj[key] = value;
    });
    return obj;
  }
  if (Array.isArray(h)) {
    const obj: Record<string, string> = {};
    for (const [k, v] of h) obj[k] = String(v);
    return obj;
  }
  // Plain object — coerce all values to strings since fetch sometimes
  // accepts arrays of strings (e.g. `Cookie: ['a=1', 'b=2']`) and we
  // need to flatten down to a single header line.
  const obj: Record<string, string> = {};
  for (const k of Object.keys(h)) {
    const v = h[k];
    obj[k] = Array.isArray(v) ? v.join(', ') : String(v);
  }
  return obj;
}
