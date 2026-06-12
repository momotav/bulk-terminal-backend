// ----------------------------------------------------------------------------
// Cache warmer
//
// Problem: SWR caching (services/cache.ts) makes every WARM request fast,
// but somebody still has to be the first request that populates a cold
// cache — right after a deploy, a Redis flush, or for a key that nobody
// has hit yet. That somebody used to be a real visitor eating a 10-20s
// page load.
//
// Fix: the backend warms its own hot endpoints by fetching them against
// localhost. Runs once shortly after boot, then on an interval. The
// interval (10 min) is well inside every SWR hard TTL (≥24 min), so
// entries never disappear entirely; SWR's stale-serving handles freshness
// in between. Net effect: real users only ever see cache-read latency.
//
// Self-fetch (rather than calling compute functions directly) is
// deliberate: the rebuild logic lives inside route handlers, and going
// through HTTP exercises exactly the same code path users hit — caching
// envelope included — with zero refactoring of the handlers.
// ----------------------------------------------------------------------------

// Hot paths and the hour-windows the frontend actually requests.
// Keep in sync with the analytics page's fetches (see Network tab):
// 24h default views, 168h (7d) revenue, 720h (30d) users, 17520h (ALL).
const WARM_PATHS: string[] = [
  // Volume / trades / liquidations / ADL charts — default + all-time
  '/api/analytics/volume-chart-api?hours=24',
  '/api/analytics/volume-chart-api?hours=17520',
  '/api/analytics/trades-chart?hours=24',
  '/api/analytics/trades-chart?hours=17520',
  '/api/analytics/liquidations-chart?hours=24',
  '/api/analytics/liquidations-chart?hours=17520',
  '/api/analytics/adl-chart?hours=24',
  '/api/analytics/adl-chart?hours=17520',
  // OI / funding combined charts — all range pills
  '/api/analytics/oi-chart?hours=24',
  '/api/analytics/oi-chart?hours=168',
  '/api/analytics/oi-chart?hours=720',
  '/api/analytics/funding-chart?hours=24',
  '/api/analytics/funding-chart?hours=168',
  '/api/analytics/funding-chart?hours=720',
  // Misc analytics page widgets
  '/api/analytics/protocol-revenue-chart?hours=168',
  '/api/analytics/cumulative-new-users?hours=720',
  '/api/analytics/daily-active-users?hours=720',
  '/api/analytics/unique-traders-by-coin?hours=720',
  '/api/analytics/stats',
];

const WARM_INTERVAL_MS = 10 * 60 * 1000; // 10 min — inside every hard TTL
const BOOT_DELAY_MS = 15 * 1000; // let DB/Redis/WS connections settle first

let timer: NodeJS.Timeout | null = null;

async function warmOnce(baseUrl: string): Promise<void> {
  const started = Date.now();
  let ok = 0;
  let failed = 0;
  // Sequential with small gaps rather than Promise.all — warming is
  // background work and shouldn't compete with real users for DB
  // connections or BULK rate limits.
  for (const path of WARM_PATHS) {
    try {
      const res = await fetch(`${baseUrl}${path}`, {
        signal: AbortSignal.timeout(30_000),
      });
      if (res.ok) ok++;
      else failed++;
      // Drain the body so the socket is released cleanly.
      await res.arrayBuffer().catch(() => undefined);
    } catch {
      failed++;
    }
  }
  console.log(
    `🔥 Cache warm pass: ${ok}/${WARM_PATHS.length} ok` +
      (failed > 0 ? `, ${failed} failed` : '') +
      ` in ${((Date.now() - started) / 1000).toFixed(1)}s`,
  );
}

export function startCacheWarmer(port: number | string): void {
  const baseUrl = `http://127.0.0.1:${port}`;
  // First pass shortly after boot — this is the one that saves the first
  // real visitor after a deploy.
  setTimeout(() => {
    void warmOnce(baseUrl);
  }, BOOT_DELAY_MS);
  timer = setInterval(() => {
    void warmOnce(baseUrl);
  }, WARM_INTERVAL_MS);
  // Don't let the interval keep the process alive on shutdown.
  timer.unref?.();
  console.log(
    `🔥 Cache warmer scheduled (${WARM_PATHS.length} paths, every ${WARM_INTERVAL_MS / 60000} min)`,
  );
}
