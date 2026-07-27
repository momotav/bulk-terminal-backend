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
import { getThroughput } from '../services/bulkExplorer';
import { sampleActionBreakdown } from '../services/networkBreakdown';
import { query } from '../db';

const NETWORK = 'testnet'; // the network the explorer WS is connected to

// Snapshot the instantaneous throughput reading (TPS/APS/block time).
async function snapshotThroughput(): Promise<void> {
  try {
    const t = getThroughput();
    if (!t) return;
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

export function startNetworkMetricsCollector(): void {
  console.log('📈 Starting network metrics collector (60s snapshots)...');
  cron.schedule('* * * * *', () => {
    void snapshotThroughput();
    void snapshotBreakdown();
  });
  // Land the first samples immediately rather than waiting a full minute.
  void snapshotThroughput();
  void snapshotBreakdown();
}
