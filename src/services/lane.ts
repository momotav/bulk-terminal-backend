// Priority lanes for BULK calls.
//
// BULK rate-limits us per IP, and our own background work (cron collectors, the
// leaderboard sweep, the cache warmer, stale-cache refreshes) shares that budget with
// people waiting on a page. Measured 2026-11-08: 428 BULK 429s in 10 minutes, and a
// cold wallet page took 6.5s mostly in retries. So every call carries a lane:
//
//   interactive  a person is waiting: an HTTP request (set by laneMiddleware)
//   background   nobody is: anything without a request, the warmer, SWR refreshes
//
// Background calls are spaced out, and all of them stand down for a while after BULK
// answers 429, so the budget left over goes to the person waiting.

import { AsyncLocalStorage } from 'async_hooks';
import type { Request, Response, NextFunction } from 'express';

export type Lane = 'interactive' | 'background';

const laneContext = new AsyncLocalStorage<Lane>();

/** Header the cache warmer sends, so its self-requests run in the background lane. */
export const WARM_HEADER = 'x-bulkstats-warm';

export function laneMiddleware(req: Request, _res: Response, next: NextFunction): void {
  laneContext.run(req.headers[WARM_HEADER] ? 'background' : 'interactive', next);
}

export function runInLane<T>(lane: Lane, fn: () => T): T {
  return laneContext.run(lane, fn);
}

/** No request on the stack means nobody is waiting: cron jobs default to background. */
export function currentLane(): Lane {
  return laneContext.getStore() ?? 'background';
}

const BACKGROUND_SPACING_MS = 150;
const COOLDOWN_START_MS = 10_000;
const COOLDOWN_MAX_MS = 60_000;

let nextBackgroundAt = 0;
let cooldownUntil = 0;
let cooldownMs = COOLDOWN_START_MS;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Holds a background call until its turn: spaced out, and never during a cooldown. */
export async function waitForTurn(): Promise<void> {
  if (currentLane() !== 'background') return;
  const now = Date.now();
  const start = Math.max(now, nextBackgroundAt, cooldownUntil);
  nextBackgroundAt = start + BACKGROUND_SPACING_MS;
  if (start > now) await sleep(start - now);
}

const counts = { interactive: { calls: 0, limited: 0 }, background: { calls: 0, limited: 0 } };

/** Record a finished call. A 429 starts a cooldown, longer each time one lands inside it. */
export function noteResult(status: number): void {
  const lane = currentLane();
  counts[lane].calls += 1;
  if (status !== 429) return;
  counts[lane].limited += 1;
  const now = Date.now();
  cooldownMs = now < cooldownUntil ? Math.min(COOLDOWN_MAX_MS, cooldownMs * 2) : COOLDOWN_START_MS;
  cooldownUntil = now + cooldownMs;
}

// One line every five minutes, so whether the budget is holding is visible in the logs.
setInterval(() => {
  const { interactive: i, background: b } = counts;
  console.log(`🚦 BULK calls 5m: interactive ${i.calls} (429 ${i.limited}) · background ${b.calls} (429 ${b.limited})`);
  i.calls = i.limited = b.calls = b.limited = 0;
}, 5 * 60_000).unref?.();
