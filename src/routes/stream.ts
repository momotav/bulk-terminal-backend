import { Router, Request, Response } from 'express';
import { subscribeMarket, type MarketUpdate } from '../services/marketStream';

// ---------------------------------------------------------------------------
// /api/stream — Server-Sent Events for live market data.
//
// GET /api/stream/market/:symbol opens an SSE stream of live mark/last prices
// for one symbol. PositionChartModal opens an EventSource here while mounted
// and updates the last candle + mark line imperatively, then closes it on
// unmount. Each client subscribes to the in-process marketStream bus, which is
// fed by the single upstream BULK WebSocket in wsListener — so N viewers on the
// same symbol cost one upstream subscription, not N.
//
// SSE (not WebSocket) because this is one-way (server → client), runs over
// plain HTTP (no extra socket server), and EventSource reconnects on its own.
// ---------------------------------------------------------------------------

const router = Router();

router.get('/market/:symbol', (req: Request, res: Response) => {
  const symbol = String(req.params.symbol || '').toUpperCase();
  if (!symbol) {
    res.status(400).json({ error: 'symbol required' });
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    // Disable proxy buffering (nginx / Railway edge) so frames flush live.
    'X-Accel-Buffering': 'no',
  });
  // Tell the client to wait 3s before reconnecting if the stream drops, and
  // flush an initial comment so proxies open the pipe immediately.
  res.write('retry: 3000\n\n');
  res.write(`: connected ${symbol}\n\n`);

  const onUpdate = (u: MarketUpdate) => {
    res.write(`data: ${JSON.stringify({ price: u.price, kind: u.kind, ts: u.ts })}\n\n`);
  };
  const unsubscribe = subscribeMarket(symbol, onUpdate);

  // Heartbeat comment keeps the connection alive through idle stretches and
  // past intermediary idle-timeouts.
  const heartbeat = setInterval(() => {
    res.write(': ping\n\n');
  }, 15000);

  let closed = false;
  const cleanup = () => {
    if (closed) return;
    closed = true;
    clearInterval(heartbeat);
    unsubscribe();
    try { res.end(); } catch { /* already closed */ }
  };
  req.on('close', cleanup);
  req.on('error', cleanup);
  res.on('error', cleanup);
});

export default router;
