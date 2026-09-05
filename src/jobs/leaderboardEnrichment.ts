// Leaderboard PnL enrichment.
//
// Our `traders` table accurately tracks per-wallet total_volume / total_trades
// from the indexed trade feed, but total_pnl is only written when a wallet page
// is viewed — too sparse to rank a PnL leaderboard. BULK's own indexer
// leaderboard is disabled, so we build the PnL ranking ourselves: for the top
// traders (by the volume WE collected), fetch each account's live PnL from
// BULK's public POST /account and write it back to traders.total_pnl. The
// /leaderboard/pnl route then ranks by that.
//
// PnL convention matches the wallet page (upsertTraderRow): net =
// realizedPnl + unrealizedPnl + fees + funding (fees/funding are signed,
// negative when paid), i.e. all-time net PnL.
//
// Outbound-safety: bounded to TOP_N wallets and spaced SPACING_MS apart, so
// this stays a trickle (~1 req/s) against BULK — nowhere near a rate limit.

import { query } from '../db';
import { bulkApi } from '../services/bulkApi';
import { isSystemWallet } from '../services/systemWallets';

const REFRESH_MS = 5 * 60_000; // re-rank every 5 minutes
const TOP_N = 300;             // enrich the top-N traders by collected volume
const SPACING_MS = 150;        // gap between BULK account calls (throttle)
const FIRST_RUN_DELAY_MS = 30_000; // let the server settle after boot

let running = false;

async function enrichOnce(): Promise<void> {
  if (running) return; // never overlap cycles
  running = true;
  try {
    const rows = await query<{ wallet_address: string }>(
      `SELECT wallet_address FROM traders
       WHERE total_volume > 0
       ORDER BY total_volume DESC
       LIMIT $1`,
      [TOP_N]
    );

    let updated = 0;
    for (const { wallet_address } of rows) {
      if (isSystemWallet(wallet_address)) continue;
      try {
        const acc = await bulkApi.getFullAccount(wallet_address);
        const m = acc?.margin;
        if (m) {
          const netPnl =
            (m.realizedPnl || 0) + (m.unrealizedPnl || 0) + (m.fees || 0) + (m.funding || 0);
          await query(`UPDATE traders SET total_pnl = $1 WHERE wallet_address = $2`, [
            netPnl,
            wallet_address,
          ]);

          // Snapshot open-position notional so "Whale Watch" (biggest positions)
          // ranks from live account data. Same fetch we already made for PnL.
          const positions = acc?.positions ?? [];
          const totalNotional = positions.reduce(
            (s, p) => s + Math.abs(Number((p as { notional?: number }).notional) || 0),
            0
          );
          await query(
            `INSERT INTO trader_snapshots (wallet_address, pnl, unrealized_pnl, positions_count, total_notional)
             VALUES ($1, $2, $3, $4, $5)`,
            [wallet_address, netPnl, m.unrealizedPnl || 0, positions.length, totalNotional]
          );
          updated++;
        }
      } catch {
        // One bad/absent account must not abort the sweep.
      }
      await new Promise((r) => setTimeout(r, SPACING_MS));
    }

    // Bound the snapshot table: the whales query only looks back 24h.
    await query(`DELETE FROM trader_snapshots WHERE timestamp < NOW() - INTERVAL '25 hours'`).catch(
      () => {}
    );

    console.log(`💹 Leaderboard PnL enrichment: updated ${updated}/${rows.length} traders`);
  } catch (e) {
    console.error('Leaderboard PnL enrichment failed:', (e as Error).message);
  } finally {
    running = false;
  }
}

export function startLeaderboardEnrichment(): void {
  setTimeout(enrichOnce, FIRST_RUN_DELAY_MS);
  setInterval(enrichOnce, REFRESH_MS);
  console.log(`💹 Leaderboard PnL enrichment scheduled (top ${TOP_N} by volume, every ${REFRESH_MS / 60000}m)`);
}
