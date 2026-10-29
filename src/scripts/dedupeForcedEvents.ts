// Remove duplicate rows from the forced-event tables.
//
// A backfill pass re-inserted events it should have recognised as already
// stored: its value match compared BULK's full-precision numbers against
// columns that had rounded them, so nothing ever matched. One wallet went
// from 71 liquidation rows to 141.
//
// A duplicate is the same (wallet, symbol, timestamp, size, price). We keep
// one row per group — preferring one that carries BULK's (slot, sequence)
// identity, since that's the most useful — and delete the rest, then rebuild
// the per-trader counters from what actually remains.
//
//   docker compose exec -T app node dist/scripts/dedupeForcedEvents.js --dry-run
//   docker compose exec -T app node dist/scripts/dedupeForcedEvents.js
import dotenv from 'dotenv';
dotenv.config();
import { query } from '../db';

const dry = process.argv.includes('--dry-run');

const TABLES: { table: string; walletCol: string }[] = [
  { table: 'liquidations', walletCol: 'wallet_address' },
  { table: 'adl_events', walletCol: 'wallet_address' },
  { table: 'vault_events', walletCol: 'vault_address' },
];

async function main() {
  console.log(`${dry ? 'DRY RUN — ' : ''}de-duplicating forced-event tables`);
  for (const { table, walletCol } of TABLES) {
    const dupes = await query<{ n: string }>(
      `SELECT COALESCE(SUM(c - 1), 0)::text AS n FROM (
         SELECT count(*) AS c FROM ${table}
         GROUP BY ${walletCol}, symbol, timestamp, size, price
         HAVING count(*) > 1
       ) g`);
    const n = parseInt(dupes[0]?.n || '0', 10);
    const total = await query<{ n: string }>(`SELECT count(*)::text AS n FROM ${table}`);
    console.log(`  ${table}: ${Number(total[0].n).toLocaleString('en-US')} rows, ${n.toLocaleString('en-US')} duplicate(s) to remove`);
    if (dry || n === 0) continue;

    await query(
      `DELETE FROM ${table} a USING (
         SELECT ${walletCol} AS w, symbol, timestamp, size, price,
                (array_agg(id ORDER BY (slot IS NULL), id))[1] AS keep_id
         FROM ${table}
         GROUP BY ${walletCol}, symbol, timestamp, size, price
         HAVING count(*) > 1
       ) d
       WHERE a.${walletCol} = d.w AND a.symbol = d.symbol AND a.timestamp = d.timestamp
         AND a.size = d.size AND a.price = d.price AND a.id <> d.keep_id`);
    const after = await query<{ n: string }>(`SELECT count(*)::text AS n FROM ${table}`);
    console.log(`    → ${Number(after[0].n).toLocaleString('en-US')} rows remain`);
  }

  if (dry) { console.log('\ndry run — nothing deleted.'); process.exit(0); }

  // Counters were incremented per insert, so they counted the duplicates too.
  // Rebuild them from the surviving rows rather than trying to subtract.
  console.log('rebuilding trader counters from surviving rows…');
  await query(`
    UPDATE traders t SET total_liquidations = COALESCE(s.c, 0), liquidation_value = COALESCE(s.v, 0)
    FROM (SELECT wallet_address, count(*) AS c, SUM(value) AS v FROM liquidations GROUP BY wallet_address) s
    WHERE t.wallet_address = s.wallet_address`);
  await query(`
    UPDATE traders t SET total_adl = COALESCE(s.c, 0), adl_value = COALESCE(s.v, 0)
    FROM (SELECT wallet_address, count(*) AS c, SUM(value) AS v FROM adl_events GROUP BY wallet_address) s
    WHERE t.wallet_address = s.wallet_address`);
  // Wallets with no rows left must read zero, not keep a stale count.
  await query(`UPDATE traders SET total_liquidations = 0, liquidation_value = 0
               WHERE wallet_address NOT IN (SELECT DISTINCT wallet_address FROM liquidations)
                 AND COALESCE(total_liquidations, 0) <> 0`);
  await query(`UPDATE traders SET total_adl = 0, adl_value = 0
               WHERE wallet_address NOT IN (SELECT DISTINCT wallet_address FROM adl_events)
                 AND COALESCE(total_adl, 0) <> 0`);
  console.log('done');
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
