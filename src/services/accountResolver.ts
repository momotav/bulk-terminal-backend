/**
 * Account Resolver
 * ---------------------------------------------------------------------------
 * Single source of truth for "what kind of account is this address?"
 *
 * Calls BULK's POST /account endpoint with type=fullAccount and extracts the
 * v1.0.14 hierarchy fields (kind, parent, subAccounts, multisigAccounts).
 * Caches results aggressively because account hierarchy changes rarely:
 *   - kind transitions (master → sub-account) are impossible by protocol
 *   - new sub-accounts are created via explicit user action; not a high-volume event
 *   - multisig membership is similarly low-frequency
 *
 * The cache TTL is 24h. We deliberately don't expose any "force refresh" path
 * to callers — invalidation happens via the WS account stream when we see a
 * createSubAccount / removeSubAccount / renameSubAccount event for a known
 * address. This keeps the read path dead simple.
 *
 * Used by:
 *   - GET /api/wallet/:address/hierarchy   (frontend wallet profile)
 *   - Leaderboard aggregation              (group by master)
 *   - Liquidations feed labeling           ("alice's farm" instead of pubkey)
 *   - Featured trades labeling             (same)
 */

import { bulkApi } from './bulkApi';
import { getCache, setCache, deleteCache } from './cache';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type AccountKind = 'MasterEOA' | 'SubAccount' | 'Unknown';

export interface SubAccountRef {
  pubkey: string;
  name?: string;
}

/**
 * Lightweight hierarchy view for any address. Intentionally smaller than the
 * full BULK fullAccount payload — we only persist what callers need to make
 * routing/labeling decisions.
 */
export interface AccountHierarchy {
  address: string;
  kind: AccountKind;
  parent?: string;             // master pubkey for sub-accounts
  subAccounts: SubAccountRef[]; // empty for non-masters or masters with no children
  multisigAccounts: string[];   // multisig pubkeys this account is a member of
  /** Wall-clock millis when this view was resolved. Useful for debugging stale data. */
  resolvedAt: number;
}

const HIERARCHY_TTL_SECONDS = 24 * 60 * 60; // 24h
const NEGATIVE_TTL_SECONDS = 60 * 60;       // 1h for "not found / errored"

const cacheKey = (address: string): string => `hierarchy:${address}`;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Resolve a single address to its hierarchy view.
 *
 * Returns Unknown kind if BULK doesn't recognize the address (e.g. a wallet
 * that has never deposited). Negative results are cached too, with a shorter
 * TTL so a wallet that just signed up doesn't stay invisible for 24h.
 */
export async function resolveHierarchy(address: string): Promise<AccountHierarchy> {
  const cached = await getCache<AccountHierarchy>(cacheKey(address));
  if (cached) return cached;

  const fullAccount = await bulkApi.getFullAccount(address);

  const result: AccountHierarchy = fullAccount
    ? {
        address,
        kind: fullAccount.kind ?? 'Unknown',
        parent: fullAccount.parent,
        subAccounts: fullAccount.subAccounts ?? [],
        multisigAccounts: fullAccount.multisigAccounts ?? [],
        resolvedAt: Date.now(),
      }
    : {
        address,
        kind: 'Unknown',
        subAccounts: [],
        multisigAccounts: [],
        resolvedAt: Date.now(),
      };

  // Negative results (Unknown kind) get a shorter TTL so newly-registered
  // wallets become visible relatively quickly.
  const ttl = result.kind === 'Unknown' ? NEGATIVE_TTL_SECONDS : HIERARCHY_TTL_SECONDS;
  await setCache(cacheKey(address), result, ttl);

  return result;
}

/**
 * Resolve many addresses in parallel. Useful for leaderboard / liquidation
 * batches where we want to label every visible row.
 *
 * Deduplicates requests internally so callers don't have to.
 */
export async function resolveHierarchyBatch(
  addresses: string[]
): Promise<Map<string, AccountHierarchy>> {
  const unique = Array.from(new Set(addresses));
  const results = await Promise.all(unique.map((a) => resolveHierarchy(a)));
  const map = new Map<string, AccountHierarchy>();
  for (const r of results) map.set(r.address, r);
  return map;
}

/**
 * Roll an address up to its master.
 *
 * - For MasterEOA → returns the address itself
 * - For SubAccount → returns the parent master pubkey
 * - For Unknown    → returns the address itself (best-effort fallback)
 *
 * Used by leaderboards to deduplicate sub-accounts under a single master row.
 */
export async function masterOf(address: string): Promise<string> {
  const h = await resolveHierarchy(address);
  if (h.kind === 'SubAccount' && h.parent) return h.parent;
  return address;
}

/**
 * Build a display label for an address. For masters returns a shortened
 * pubkey; for sub-accounts returns the human-friendly name.
 *
 * Examples:
 *   masterOf("8cbN...oFFN")          → "8cbN…oFFN"
 *   subAccount with name "farm"      → "farm (8cbN…oFFN's sub-account)"
 *   unknown                          → "unknown_addr…"
 */
export async function displayLabel(address: string): Promise<string> {
  const h = await resolveHierarchy(address);
  if (h.kind === 'SubAccount' && h.parent) {
    // Look up master to get a name for the sub-account. Master's hierarchy
    // contains the named children list.
    const master = await resolveHierarchy(h.parent);
    const ref = master.subAccounts.find((s) => s.pubkey === address);
    const subName = ref?.name ?? 'sub-account';
    return `${subName} (${shortAddr(h.parent)}'s sub-account)`;
  }
  return shortAddr(address);
}

// ---------------------------------------------------------------------------
// Cache invalidation hooks
// ---------------------------------------------------------------------------

/**
 * Force a cache eviction for a single address. Call this from the WS listener
 * when we see a createSubAccount / removeSubAccount / renameSubAccount event
 * affecting this address (or its master). Next resolve() will re-fetch from
 * BULK with fresh data.
 *
 * No-throw: cache failures shouldn't break the trade-handling hot path.
 */
export async function invalidateHierarchy(address: string): Promise<void> {
  try {
    await deleteCache(cacheKey(address));
  } catch (err) {
    // Logged but never re-thrown — the next resolve() will hit BULK if cache
    // is still in a weird state, which is acceptable.
    console.warn(`[accountResolver] invalidate failed for ${address}:`, err);
  }
}

/**
 * Invalidate both an account and its parent (or all its children). Used when
 * a sub-account event fires and we want both views (master + child) to
 * refresh on next read.
 */
export async function invalidateHierarchyTree(address: string): Promise<void> {
  const h = await resolveHierarchy(address); // hits cache, cheap
  await invalidateHierarchy(address);
  if (h.parent) {
    await invalidateHierarchy(h.parent);
  }
  for (const sub of h.subAccounts) {
    await invalidateHierarchy(sub.pubkey);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function shortAddr(a: string): string {
  if (!a || a.length < 12) return a;
  return `${a.slice(0, 4)}…${a.slice(-4)}`;
}
