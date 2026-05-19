// Explorer routes — read-only views into BULK's chain via the explorer
// node. Routes:
//
//   GET /explorer/throughput          — TPS/APS/round stats (dashboard)
//   GET /explorer/blocks?limit=50     — last N blocks from in-mem buffer
//   GET /explorer/block/:blockhash    — block detail, proxied + cached
//   GET /explorer/tx/:txhash          — tx detail, proxied + cached
//
// The blocks list endpoint reads our local ring buffer (filled by the
// WS listener). Block and tx detail endpoints proxy BULK's HTTP API
// with caching — both block and tx data is immutable once committed,
// so we cache for 5 minutes which is enough to absorb any traffic
// burst without making the same call to BULK twice.

import { Router, Request, Response } from 'express';
import { getThroughput, getRecentBlocks } from '../services/bulkExplorer';
import { getCache, setCache } from '../services/cache';
import { bulkFetch } from '../services/bulkAuth';

const router = Router();

const BULK_EXPLORER_HTTP =
  process.env.BULK_EXPLORER_HTTP_URL || 'http://64.130.50.69:12003';

// Validates hex hash params. BULK's block hashes are 64-char hex;
// tx hashes are 1-16 char hex (per their error message: "expected
// 1-16 char hex"). We use a permissive 1-64 range to cover both.
const HEX_HASH = /^[a-f0-9]{1,64}$/i;

router.get('/throughput', (_req: Request, res: Response) => {
  try {
    res.json(getThroughput());
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to read throughput' });
  }
});

// Recent blocks for the /explorer page block list.
router.get('/blocks', (req: Request, res: Response) => {
  try {
    const limit = Math.max(1, Math.min(parseInt(req.query.limit as string) || 50, 200));
    const blocks = getRecentBlocks(limit);
    res.json({ blocks, limit });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to read recent blocks' });
  }
});

// Block detail. Proxies to BULK's /block/:hash with 5min cache.
//
// Note: BULK returns 4xx with `{"error": "..."}` for invalid/missing
// hashes — we forward those statuses so the frontend can show
// "Block not found" cleanly rather than a generic 500.
router.get('/block/:blockhash', async (req: Request, res: Response) => {
  const { blockhash } = req.params;
  if (!HEX_HASH.test(blockhash)) {
    return res.status(400).json({ error: 'invalid block hash format' });
  }

  const cacheKey = `explorer:block:${blockhash}`;
  const cached = await getCache(cacheKey);
  if (cached) {
    res.setHeader('X-Bulkstats-Cache', 'fresh');
    return res.json(cached);
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    const upstream = await bulkFetch(`${BULK_EXPLORER_HTTP}/block/${blockhash}`, {
      signal: controller.signal,
    });
    clearTimeout(timer);

    const body = await upstream.json();
    if (!upstream.ok) {
      return res.status(upstream.status).json(body);
    }

    // Cache for 5 minutes. Blocks are immutable once committed so
    // there's no risk of caching stale data.
    await setCache(cacheKey, body, 300);
    res.setHeader('X-Bulkstats-Cache', 'miss');
    return res.json(body);
  } catch (error: any) {
    console.error('explorer /block proxy failed:', error.message);
    return res.status(502).json({ error: 'failed to reach BULK explorer' });
  }
});

// Transaction detail. Same pattern as block detail.
router.get('/tx/:txhash', async (req: Request, res: Response) => {
  const { txhash } = req.params;
  if (!HEX_HASH.test(txhash)) {
    return res.status(400).json({ error: 'invalid tx hash format' });
  }

  const cacheKey = `explorer:tx:${txhash}`;
  const cached = await getCache(cacheKey);
  if (cached) {
    res.setHeader('X-Bulkstats-Cache', 'fresh');
    return res.json(cached);
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    const upstream = await bulkFetch(`${BULK_EXPLORER_HTTP}/tx/${txhash}`, {
      signal: controller.signal,
    });
    clearTimeout(timer);

    const body = await upstream.json();
    if (!upstream.ok) {
      return res.status(upstream.status).json(body);
    }

    await setCache(cacheKey, body, 300);
    res.setHeader('X-Bulkstats-Cache', 'miss');
    return res.json(body);
  } catch (error: any) {
    console.error('explorer /tx proxy failed:', error.message);
    return res.status(502).json({ error: 'failed to reach BULK explorer' });
  }
});

export default router;
