// Network action/transaction breakdown sampler.
//
// BULK's block events (and our throughput buffer) only carry totals
// (txCount / actionCount) — NOT the per-type split. The type of each action
// lives in the FULL block detail: transactions[].actions[] is a list of short
// string codes ("l" = limit order, "cx" = cancel, "px" = price update, …).
//
// To surface "Operations by Type" / "Transactions by Type" we sample the most
// recent blocks over HTTP, walk their transactions, and tally:
//   - opsByCode:  every action code seen (Operations by Type)
//   - txByCode:   each transaction bucketed by its PRIMARY (first) action
//
// This is a SAMPLE of recent activity, not an exhaustive count — at ~150
// blocks/sec we can't fetch detail for every block, so we take the newest N
// with transactions. That's plenty for a representative type distribution
// (which is all the donut / composition charts show). Raw codes are returned;
// the frontend maps them to human labels + categories so meaning stays in one
// place (lib/explorerActions).

const EXPLORER_HTTP =
  process.env.BULK_EXPLORER_HTTP_URL || 'http://64.130.50.69:12003';

export interface ActionBreakdown {
  opsByCode: Record<string, number>;
  txByCode: Record<string, number>;
  blocksSampled: number;
  txSampled: number;
  opsSampled: number;
  sampledAt: number; // ms epoch
}

async function fetchJson(url: string, timeoutMs = 5000): Promise<any> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: controller.signal });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// Sample the newest `maxBlocks` blocks that contain transactions and tally the
// action/transaction type codes. Never throws — returns empty tallies if the
// explorer is unreachable so callers can degrade gracefully.
export async function sampleActionBreakdown(maxBlocks = 30): Promise<ActionBreakdown> {
  const empty: ActionBreakdown = {
    opsByCode: {}, txByCode: {}, blocksSampled: 0, txSampled: 0, opsSampled: 0, sampledAt: Date.now(),
  };

  // Pull a generous recent window, then keep the newest N with txs.
  const list = await fetchJson(`${EXPLORER_HTTP}/blocks?limit=600`);
  const blocks: any[] = Array.isArray(list?.blocks) ? list.blocks : [];
  const withTx = blocks
    .filter((b) => (b?.txCount ?? 0) > 0 && typeof b?.blockhash === 'string')
    .slice(0, maxBlocks);
  if (withTx.length === 0) return empty;

  const opsByCode: Record<string, number> = {};
  const txByCode: Record<string, number> = {};
  let blocksSampled = 0;
  let txSampled = 0;
  let opsSampled = 0;

  // Fetch block details with limited concurrency so we don't hammer the node.
  const CONCURRENCY = 6;
  for (let i = 0; i < withTx.length; i += CONCURRENCY) {
    const batch = withTx.slice(i, i + CONCURRENCY);
    const details = await Promise.all(
      batch.map((b) => fetchJson(`${EXPLORER_HTTP}/block/${b.blockhash}`))
    );
    for (const d of details) {
      const txs: any[] = Array.isArray(d?.transactions) ? d.transactions : [];
      if (!d) continue;
      blocksSampled += 1;
      for (const tx of txs) {
        const actions: any[] = Array.isArray(tx?.actions) ? tx.actions : [];
        if (actions.length === 0) continue;
        txSampled += 1;
        // Primary action = the first code; buckets the whole tx.
        const primary = typeof actions[0] === 'string' ? actions[0] : 'obj';
        txByCode[primary] = (txByCode[primary] ?? 0) + 1;
        for (const a of actions) {
          const code = typeof a === 'string' ? a : 'obj';
          opsByCode[code] = (opsByCode[code] ?? 0) + 1;
          opsSampled += 1;
        }
      }
    }
  }

  return { opsByCode, txByCode, blocksSampled, txSampled, opsSampled, sampledAt: Date.now() };
}
