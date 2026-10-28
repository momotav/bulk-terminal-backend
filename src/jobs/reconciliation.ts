// Reconciliation: continuously check that what we STORED matches what BULK
// SAYS, and shout when it doesn't.
//
// Why this exists. Every data bug this project has hit shared one shape: an
// ingestion path broke, the site kept rendering, the numbers were quietly
// wrong, and a human noticed weeks later.
//
//   • ADL stopped recording when BULK renamed a reason token  → found weeks later
//   • liquidations stopped when `reason` became `reason_code` → found by the founder
//   • the taker-fee series lagged a day (MAX of a negative running total)
//   • wallet snapshots were deleted after 25h, so PnL history was ~2 days
//
// `/health` said "websocket: connected ✓" through all of it. Connected is not
// the same as correct. These checks compare our aggregates against BULK's own
// authoritative counters, so divergence is caught in minutes by the system
// rather than in weeks by a user.
//
// Results land in the `health_checks` table and, when ALERT_WEBHOOK (a Telegram
// sendMessage URL or any POST endpoint) is set, are pushed out on transition
// into failure and on recovery — not every run, so a persistent problem does
// not become noise.
import { query } from '../db';
import { bulkApi } from '../services/bulkApi';
import { bulkFetch } from '../services/bulkAuth';
import { getWebSocketStats } from './wsListener';

const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1';

const RUN_EVERY_MS = 15 * 60_000;
const FIRST_RUN_DELAY_MS = 90_000; // let the WS connect and caches warm
const ALERT_WEBHOOK = process.env.ALERT_WEBHOOK || '';

type Status = 'ok' | 'warn' | 'fail';
interface CheckResult {
  name: string;
  status: Status;
  detail: string;
  /** Our value and BULK's, when the check is a comparison. */
  ours?: number;
  theirs?: number;
}

const lastStatus = new Map<string, Status>();

async function alert(text: string): Promise<void> {
  if (!ALERT_WEBHOOK) return;
  try {
    const isTelegram = ALERT_WEBHOOK.includes('api.telegram.org');
    await fetch(ALERT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(
        isTelegram
          ? { chat_id: process.env.ALERT_CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }
          : { text },
      ),
    });
  } catch (e) {
    console.error('[reconciliation] alert failed:', (e as Error).message);
  }
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/** Trades still arriving? A silent tape means the WS is up but useless. */
async function checkTradeFlow(): Promise<CheckResult> {
  const rows = await query<{ age: string | null }>(
    `SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))::text AS age FROM trades`);
  const age = Number(rows[0]?.age ?? NaN);
  if (!Number.isFinite(age)) return { name: 'trade_flow', status: 'fail', detail: 'no trades in the table at all' };
  if (age > 1800) return { name: 'trade_flow', status: 'fail', detail: `last trade ${Math.round(age / 60)} min ago`, ours: age };
  if (age > 600) return { name: 'trade_flow', status: 'warn', detail: `last trade ${Math.round(age / 60)} min ago`, ours: age };
  return { name: 'trade_flow', status: 'ok', detail: `last trade ${Math.round(age)}s ago`, ours: age };
}

/**
 * Liquidations arriving while the exchange is clearly active? This is the
 * check that would have caught the Oct 21 reason_code rename in hours instead
 * of three days. A quiet market genuinely produces none, so it only fails when
 * the tape is busy and liquidations are nonetheless absent for a long stretch.
 */
async function checkLiquidationFlow(): Promise<CheckResult> {
  const rows = await query<{ liq_age: string | null; trades_6h: string }>(
    `SELECT (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) FROM liquidations)::text AS liq_age,
            (SELECT count(*) FROM trades WHERE timestamp > NOW() - INTERVAL '6 hours')::text AS trades_6h`);
  const age = Number(rows[0]?.liq_age ?? NaN);
  const trades6h = Number(rows[0]?.trades_6h ?? 0);
  const hours = Number.isFinite(age) ? age / 3600 : Infinity;
  // Only meaningful when the exchange is busy; 2k+ trades in 6h is a live market.
  if (trades6h < 2000) return { name: 'liquidation_flow', status: 'ok', detail: `market quiet (${trades6h} trades/6h), last liq ${Number.isFinite(hours) ? hours.toFixed(1) + 'h' : 'never'} ago` };
  if (hours > 12) return { name: 'liquidation_flow', status: 'fail', detail: `no liquidation in ${hours.toFixed(1)}h while ${trades6h} trades flowed in 6h — ingestion likely broken`, ours: hours };
  if (hours > 6) return { name: 'liquidation_flow', status: 'warn', detail: `no liquidation in ${hours.toFixed(1)}h (${trades6h} trades/6h)`, ours: hours };
  return { name: 'liquidation_flow', status: 'ok', detail: `last liquidation ${hours.toFixed(1)}h ago`, ours: hours };
}

/**
 * Our stored protocol revenue vs BULK's own /feeState counter. Catches both a
 * stalled fee collector and any arithmetic drift in how we bucket the totals.
 */
async function checkRevenue(): Promise<CheckResult> {
  // Read BULK's counter the same way the collector does, including the
  // snake_case fallback — BULK renamed these fields once already and the
  // collector silently wrote zeros for nine days before anyone noticed.
  let theirs = NaN;
  try {
    const res = await bulkFetch(`${BULK_API_BASE}/feeState`);
    if (res.ok) {
      const fs = (await res.json()) as Record<string, number>;
      theirs = Number(fs.totalProtocolSettlement ?? fs.total_protocol_settlement ?? NaN);
    }
  } catch { /* leave NaN → reported as warn below */ }
  const rows = await query<{ v: string | null }>(
    `SELECT MAX(total_protocol_settlement)::text AS v FROM fee_snapshots`);
  const ours = Number(rows[0]?.v ?? NaN);
  if (!Number.isFinite(theirs) || !Number.isFinite(ours)) {
    return { name: 'revenue_vs_bulk', status: 'warn', detail: 'could not read one side', ours, theirs };
  }
  const drift = Math.abs(ours - theirs) / Math.max(theirs, 1);
  const d = `ours $${Math.round(ours).toLocaleString('en-US')} vs BULK $${Math.round(theirs).toLocaleString('en-US')} (${(drift * 100).toFixed(2)}%)`;
  if (drift > 0.02) return { name: 'revenue_vs_bulk', status: 'fail', detail: d, ours, theirs };
  if (drift > 0.005) return { name: 'revenue_vs_bulk', status: 'warn', detail: d, ours, theirs };
  return { name: 'revenue_vs_bulk', status: 'ok', detail: d, ours, theirs };
}

/**
 * Fee identity: |taker| must equal maker + protocol on each day. This is
 * exactly the inconsistency that exposed the taker series lagging a bucket.
 */
async function checkFeeIdentity(): Promise<CheckResult> {
  const rows = await query<{ bad: string }>(
    `WITH d AS (
       SELECT date_trunc('day', timestamp) AS day,
              (array_agg(total_protocol_settlement ORDER BY timestamp DESC))[1] AS p,
              (array_agg(total_maker_fees        ORDER BY timestamp DESC))[1] AS m,
              (array_agg(total_taker_fees        ORDER BY timestamp DESC))[1] AS t
       FROM fee_snapshots WHERE timestamp > NOW() - INTERVAL '7 days' GROUP BY day
     ), x AS (
       SELECT day, abs(t) - (m + p) AS gap, abs(t) AS tk FROM d
     )
     SELECT count(*)::text AS bad FROM x WHERE tk > 0 AND abs(gap) / tk > 0.02`);
  const bad = Number(rows[0]?.bad ?? 0);
  if (bad > 0) return { name: 'fee_identity', status: 'fail', detail: `${bad} of the last 7 days fail |taker| = maker + protocol`, ours: bad };
  return { name: 'fee_identity', status: 'ok', detail: 'identity holds for the last 7 days' };
}

/**
 * Sample a few wallets we hold liquidations for and compare our row counts to
 * BULK's per-account riskHistory ledger. Catches silent ingestion drift that
 * the flow checks can't see because events are still arriving, just not all.
 */
async function checkLiquidationAccuracy(): Promise<CheckResult> {
  const sample = await query<{ wallet_address: string; n: string }>(
    `SELECT wallet_address, count(*)::text AS n FROM liquidations
     WHERE timestamp > NOW() - INTERVAL '30 days'
     GROUP BY wallet_address ORDER BY count(*) DESC LIMIT 5`);
  if (sample.length === 0) return { name: 'liquidation_accuracy', status: 'warn', detail: 'no liquidations to sample' };
  let ours = 0, theirs = 0, checked = 0;
  for (const r of sample) {
    try {
      const ev = await bulkApi.getRiskHistory(r.wallet_address);
      theirs += ev.filter((e) => e.eventType === 'liquidation').length;
      ours += Number(r.n);
      checked++;
    } catch { /* throttled — skip this wallet */ }
    await new Promise((res) => setTimeout(res, 400));
  }
  if (checked === 0) return { name: 'liquidation_accuracy', status: 'warn', detail: 'BULK throttled every sample' };
  const drift = theirs > 0 ? (ours - theirs) / theirs : 0;
  const d = `${checked} wallets: ours ${ours} vs BULK ${theirs} (${(drift * 100).toFixed(1)}%)`;
  // Our rows cover only what we've collected; BULK's ledger is lifetime, so a
  // mild shortfall is expected. A large one, or ANY surplus, is a real problem.
  if (drift > 0.02) return { name: 'liquidation_accuracy', status: 'fail', detail: `${d} — we have MORE than BULK, likely phantom rows`, ours, theirs };
  if (drift < -0.15) return { name: 'liquidation_accuracy', status: 'fail', detail: `${d} — large shortfall`, ours, theirs };
  if (drift < -0.05) return { name: 'liquidation_accuracy', status: 'warn', detail: d, ours, theirs };
  return { name: 'liquidation_accuracy', status: 'ok', detail: d, ours, theirs };
}

/**
 * Schema drift: BULK renamed `reason` to `reason_code` without notice and we
 * lost three days of liquidations. The WS listener now counts prints carrying
 * a reason token it does not recognise; any such print means BULK changed the
 * feed and our classification is already wrong.
 */
async function checkSchemaDrift(): Promise<CheckResult> {
  const s = getWebSocketStats() as { unknownReasonCodes?: number; tradesReceived?: number; connected?: boolean };
  if (!s.connected) return { name: 'schema_drift', status: 'fail', detail: 'websocket not connected' };
  const unknown = Number(s.unknownReasonCodes ?? 0);
  if (unknown > 0) return { name: 'schema_drift', status: 'fail', detail: `${unknown} trade print(s) carried an unrecognised reason token — BULK likely changed the feed`, ours: unknown };
  return { name: 'schema_drift', status: 'ok', detail: `no unknown reason tokens in ${s.tradesReceived ?? 0} prints` };
}

// ---------------------------------------------------------------------------

async function runOnce(): Promise<void> {
  const checks = [checkTradeFlow, checkLiquidationFlow, checkRevenue, checkFeeIdentity, checkLiquidationAccuracy, checkSchemaDrift];
  const results: CheckResult[] = [];
  for (const c of checks) {
    try {
      results.push(await c());
    } catch (e) {
      results.push({ name: c.name.replace(/^check/, '').toLowerCase(), status: 'warn', detail: `check threw: ${(e as Error).message}` });
    }
  }

  for (const r of results) {
    await query(
      `INSERT INTO health_checks (name, status, detail, ours, theirs) VALUES ($1,$2,$3,$4,$5)`,
      [r.name, r.status, r.detail, Number.isFinite(r.ours as number) ? r.ours : null, Number.isFinite(r.theirs as number) ? r.theirs : null],
    ).catch(() => {});
    // Alert only on TRANSITION, so a standing problem doesn't spam.
    const prev = lastStatus.get(r.name);
    if (prev !== r.status) {
      if (r.status === 'fail') await alert(`🔴 <b>${r.name}</b>\n${r.detail}`);
      else if (prev === 'fail') await alert(`🟢 <b>${r.name}</b> recovered\n${r.detail}`);
      lastStatus.set(r.name, r.status);
    }
  }

  const bad = results.filter((r) => r.status !== 'ok');
  if (bad.length === 0) console.log(`✅ reconciliation: ${results.length}/${results.length} checks ok`);
  else console.warn(`⚠️ reconciliation: ${bad.map((b) => `${b.name}=${b.status} (${b.detail})`).join(' | ')}`);

  await query(`DELETE FROM health_checks WHERE checked_at < NOW() - INTERVAL '30 days'`).catch(() => {});
}

export function startReconciliation(): void {
  setTimeout(() => { void runOnce(); }, FIRST_RUN_DELAY_MS);
  setInterval(() => { void runOnce(); }, RUN_EVERY_MS);
  console.log(`🔎 Reconciliation scheduled (every ${RUN_EVERY_MS / 60000} min, alerts ${ALERT_WEBHOOK ? 'ON' : 'OFF — set ALERT_WEBHOOK'})`);
}

/** Latest result per check, for GET /health/detailed. */
export async function latestHealth(): Promise<unknown[]> {
  return query(
    `SELECT DISTINCT ON (name) name, status, detail, ours, theirs, checked_at
     FROM health_checks ORDER BY name, checked_at DESC`);
}
