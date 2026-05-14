// Explorer routes — read-only views into BULK's chain via the explorer
// node. Currently we expose just the throughput summary the dashboard
// needs; more endpoints (latest blocks list, block detail, tx detail)
// can be added here when we build out the explorer UI.

import { Router, Request, Response } from 'express';
import { getThroughput } from '../services/bulkExplorer';

const router = Router();

// GET /explorer/throughput
//
// Returns the live TPS / APS / latest-block snapshot tracked by our
// in-process explorer WS listener. Reads happen against an in-memory
// rolling buffer so this is essentially free; no caching layer needed.
// Frontend can poll every few seconds without putting any load on us
// or on BULK.
router.get('/throughput', (_req: Request, res: Response) => {
  try {
    res.json(getThroughput());
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to read throughput' });
  }
});

export default router;
