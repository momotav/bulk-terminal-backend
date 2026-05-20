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
// Implementation detail: we merge our headers with whatever the caller
// passed in. Caller-provided headers take precedence — except that if
// a caller explicitly tries to set `x-bulk-api-key`, we let them (their
// override wins). This matters for edge cases like proxying a request
// where the auth header came from somewhere else.
export async function bulkFetch(
  url: string | URL,
  init?: RequestInit
): Promise<Response> {
  const callerHeaders = init?.headers || {};
  const mergedHeaders = {
    ...bulkHeaders(),
    // Normalize caller's headers to an object regardless of whether they
    // passed a plain object, Headers instance, or array of tuples.
    ...normalizeHeaders(callerHeaders),
  };

  return fetch(url, {
    ...init,
    headers: mergedHeaders,
  });
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
