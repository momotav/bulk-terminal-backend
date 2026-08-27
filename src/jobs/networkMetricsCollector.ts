// Network metrics collector.
//
// Every 60 seconds, snapshots the live throughput reading from the in-process
// explorer WS buffer (getThroughput) into the `network_metrics` table. This is
// what turns the instantaneous TPS/APS/block-time numbers into the historical
// series the analytics Network page charts.
//
// IMPORTANT: there is no backfill. History accumulates forward from the moment
// this job first runs after deploy, so it's wired to start as early as possible
// (right after the explorer listener) and takes one sample immediately.
//
// A row is written every minute regardless of network health so gaps are
// visible; the read side (routes/explorer.ts /network-history) filters to
// samples with >= 2 blocks so a momentary WS reconnect can't dent the averages.

import cron from 'node-cron';
import { getThroughput, getRecentBlocks } from '../services/bulkExplorer';
import { sampleActionBreakdown } from '../services/networkBreakdown';
import { query } from '../db';

const NETWORK = 'testnet'; // the network the explorer WS is connected to

// Nearest-rank percentile of a pre-sorted ascending array.
function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.round((sorted.length - 1) * p)));
  return sorted[i];
}

// Snapshot the instantaneous throughput reading (TPS/APS/block time).
async function snapshotThroughput(): Promise<void> {
  try {
    const t = getThroughput();
    if (!t) return;
    // Don't record a snapshot when the explorer feed is disconnected or observed
    // no blocks this window. Writing zeros would poison the historical
    // percentiles/heatmap with fake "0 TPS / 0ms" rows (e.g. when BULK's explorer
    // node is down). A genuine gap in the series is honest; fake zeros are not.
    if (t.status === 'disconnected' || (t.sampleCount ?? 0) === 0) return;
    await query(
      `INSERT INTO network_metrics
         (network, tps, aps, block_time_ms, latest_round, sample_blocks, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        NETWORK,
        Number.isFinite(t.tps) ? t.tps : 0,
        Number.isFinite(t.aps) ? t.aps : 0,
        t.blockTimeMs,        // null when the window has < 2 blocks
        t.latestRound,        // null until the first block arrives
        t.sampleCount ?? 0,
        t.status ?? null,
      ]
    );
  } catch (err) {
    console.error('❌ network metrics snapshot failed:', (err as Error).message);
  }
}

// Sample recent blocks for the per-type action/transaction split and store one
// row per action code. Heavier than the throughput snapshot (it fetches block
// detail over HTTP), so it's capped to a modest sample and fully guarded.
async function snapshotBreakdown(): Promise<void> {
  try {
    const b = await sampleActionBreakdown(30);
    if (b.opsSampled === 0) return; // nothing to record this minute
    const codes = new Set([...Object.keys(b.opsByCode), ...Object.keys(b.txByCode)]);
    for (const code of codes) {
      await query(
        `INSERT INTO network_action_metrics (network, action_code, op_count, tx_count)
         VALUES ($1, $2, $3, $4)`,
        [NETWORK, code, b.opsByCode[code] ?? 0, b.txByCode[code] ?? 0]
      );
    }
  } catch (err) {
    console.error('❌ network breakdown snapshot failed:', (err as Error).message);
  }
}

// Sample the recent-blocks buffer for per-block detail the minute-averaged
// throughput can't express: the block-time distribution (percentiles), empty
// vs non-empty counts, and block-size stats. In-memory read (no HTTP), cheap.
async function snapshotBlockMetrics(): Promise<void> {
  try {
    const blocks = getRecentBlocks(1000);
    if (blocks.length < 5) return;
    // Ascending by round so consecutive-block time deltas are well-defined.
    const asc = [...blocks].sort((a, b) => a.round - b.round);
    const times: number[] = [];
    for (let i = 1; i < asc.length; i++) {
      const dtMs = (asc[i].timestampNs - asc[i - 1].timestampNs) / 1e6;
      if (dtMs > 0 && dtMs < 60_000) times.push(dtMs); // drop gaps / bad clocks
    }
    times.sort((a, b) => a - b);

    let empty = 0, maxTx = 0, maxAct = 0, sumTx = 0, sumAct = 0;
    for (const b of asc) {
      if (b.txCount === 0) empty += 1;
      if (b.txCount > maxTx) maxTx = b.txCount;
      if (b.actionCount > maxAct) maxAct = b.actionCount;
      sumTx += b.txCount;
      sumAct += b.actionCount;
    }
    const n = asc.length;

    await query(
      `INSERT INTO network_block_metrics
         (network, blocks_seen, empty_blocks, bt_p50, bt_p95, bt_p99, bt_min, bt_max,
          max_tx, max_actions, avg_tx, avg_actions)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        NETWORK, n, empty,
        pct(times, 0.5), pct(times, 0.95), pct(times, 0.99),
        times[0] ?? 0, times[times.length - 1] ?? 0,
        maxTx, maxAct, sumTx / n, sumAct / n,
      ]
    );
  } catch (err) {
    console.error('❌ network block-metrics snapshot failed:', (err as Error).message);
  }
}

export function startNetworkMetricsCollector(): void {
  console.log('📈 Starting network metrics collector (60s snapshots)...');
  cron.schedule('* * * * *', () => {
    void snapshotThroughput();
    void snapshotBreakdown();
    void snapshotBlockMetrics();
  });
  // Land the first samples immediately rather than waiting a full minute.
  void snapshotThroughput();
  void snapshotBreakdown();
  void snapshotBlockMetrics();
}
