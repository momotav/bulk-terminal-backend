// Remove forced-event rows wrongly attributed to BULK protocol accounts
// (liquidation engine, risk vault). These accounts sit on the counterparty
// side of real liquidations, so ingesting them makes protocol infrastructure
// look like the most-liquidated traders on the exchange.
//
// Idempotent and narrowly scoped: only touches rows whose wallet_address is in
// SYSTEM_WALLET_ADDRESSES. Run with --dry-run first to see what would go.
//
//   docker compose exec -T app node dist/scripts/cleanupSystemWallets.js --dry-run
//   docker compose exec -T app node dist/scripts/cleanupSystemWallets.js
import dotenv from 'dotenv';
dotenv.config();
import { query } from '../db';
import { SYSTEM_WALLET_ADDRESSES } from '../services/systemWallets';

const dry = process.argv.includes('--dry-run');

async function main() {
  const addrs = SYSTEM_WALLET_ADDRESSES;
  console.log(`${dry ? 'DRY RUN — ' : ''}checking ${addrs.length} system wallet(s)`);

  const liq = await query<{ wallet_address: string; n: string; v: string }>(
    `SELECT wallet_address, count(*)::text AS n, COALESCE(SUM(value),0)::text AS v
     FROM liquidations WHERE wallet_address = ANY($1::varchar[]) GROUP BY wallet_address`, [addrs]);
  const adl = await query<{ wallet_address: string; n: string; v: string }>(
    `SELECT wallet_address, count(*)::text AS n, COALESCE(SUM(value),0)::text AS v
     FROM adl_events WHERE wallet_address = ANY($1::varchar[]) GROUP BY wallet_address`, [addrs]);

  for (const r of liq) console.log(`  liquidations  ${r.wallet_address.slice(0, 8)}…  ${r.n} rows  $${Number(r.v).toLocaleString('en-US')}`);
  for (const r of adl) console.log(`  adl_events    ${r.wallet_address.slice(0, 8)}…  ${r.n} rows  $${Number(r.v).toLocaleString('en-US')}`);
  if (liq.length === 0 && adl.length === 0) { console.log('  nothing to clean'); process.exit(0); }

  if (dry) { console.log('\ndry run — nothing deleted. Re-run without --dry-run to apply.'); process.exit(0); }

  const dl = await query(`DELETE FROM liquidations WHERE wallet_address = ANY($1::varchar[])`, [addrs]);
  const da = await query(`DELETE FROM adl_events WHERE wallet_address = ANY($1::varchar[])`, [addrs]);
  // Zero the derived counters so leaderboards and the wallet page agree with
  // the now-empty event tables.
  await query(
    `UPDATE traders SET total_liquidations = 0, liquidation_value = 0, total_adl = 0, adl_value = 0
     WHERE wallet_address = ANY($1::varchar[])`, [addrs]);
  console.log(`\ndeleted: ${(dl as unknown as unknown[]).length ?? 0} liquidation row-sets, counters reset`);
  console.log('done');
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
