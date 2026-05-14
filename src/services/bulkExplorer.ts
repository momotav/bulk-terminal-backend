// BULK Explorer client.
//
// Maintains a persistent WebSocket connection to BULK's explorer node
// and tracks recent block activity for throughput metrics (TPS, APS),
// latest block info, and a derived "network status" indicator.
//
// Design notes:
//
// - One WS per backend instance, NOT per HTTP request. The frontend
//   polls our `/api/explorer/throughput` route every few seconds; that
//   route reads the in-memory state this service maintains. The cost
//   to BULK is one persistent connection per backend deploy, regardless
//   of how many BulkStats users are online.
//
// - We subscribe with `compact: true` to minimize payload size. We only
//   need round / txCount / actionCount / timestampNs / blockhash —
//   compact mode trims everything else.
//
// - Rolling window: we keep the last ~60 seconds of block events in a
//   bounded ring buffer. TPS = sum(txCount) / window_seconds. Same for
//   APS. Window is shorter than you might expect because BULK produces
//   blocks every ~20ms (50 blocks/sec at full speed) and we want the
//   stat to react quickly without being noisy.
//
// - Reconnect logic: exponential backoff capped at 30s. WS errors don't
//   crash the process; they queue a reconnect attempt. If we never
//   connect, the throughput endpoint just returns nulls and the
//   frontend renders the tile as "--".
//
// - Address is configurable via BULK_EXPLORER_WS_URL env var. Default
//   is the IP-based test address BULK gave us; production will likely
//   move to a domain (`explorer-api.bulk.trade` or similar).

import WebSocket from 'ws';

const WS_URL =
  process.env.BULK_EXPLORER_WS_URL || 'ws://64.130.50.69:12004';

// Rolling window of recent block events. Each entry captures just
// enough to compute throughput and surface "latest block" info.
interface BlockSample {
  round: number;
  txCount: number;
  actionCount: number;
  timestampNs: number;   // BULK's nanosecond timestamp
  receivedAt: number;    // Date.now() for window math (ms)
  blockhash?: string;
}

// How far back the throughput window looks. 60 seconds gives a stable
// rolling average without being too laggy for live feeling.
const WINDOW_MS = 60_000;

// Hard cap on stored samples. At BULK's max throughput (~50 blocks/sec)
// 60s = 3000 samples. We round up generously.
const MAX_SAMPLES = 5000;

const samples: BlockSample[] = [];

// Latest snapshot from the stream. Updated on every block event,
// independent of the rolling window (so even if window pruning is
// behind, latest is fresh).
let latestBlock: BlockSample | null = null;

// Last time we received ANY WS message — used for stale detection.
let lastMessageAt = 0;

// Connection state tracking. Exposed for the throughput endpoint so
// it can label the status accurately (connected / connecting / down).
let ws: WebSocket | null = null;
let connectionState: 'idle' | 'connecting' | 'open' | 'closed' = 'idle';
let reconnectAttempts = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleReconnect() {
  if (reconnectTimer) return;
  // Exponential backoff capped at 30s. Random jitter (±10%) to avoid
  // thundering-herd if multiple backend instances restart together.
  const baseDelay = Math.min(30_000, 1000 * Math.pow(2, reconnectAttempts));
  const jitter = baseDelay * 0.1 * (Math.random() * 2 - 1);
  const delay = Math.max(500, baseDelay + jitter);
  reconnectAttempts += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function pruneOldSamples() {
  const cutoff = Date.now() - WINDOW_MS;
  while (samples.length > 0 && samples[0].receivedAt < cutoff) {
    samples.shift();
  }
  // Hard cap as a defensive belt — should never trigger in practice
  // since pruning by time is more aggressive, but protects against
  // pathological cases (clock jumps, very long disconnect+drain).
  while (samples.length > MAX_SAMPLES) {
    samples.shift();
  }
}

function handleBlockEvent(data: any) {
  if (!data || typeof data.round !== 'number') return;
  const sample: BlockSample = {
    round: data.round,
    txCount: data.txCount || 0,
    actionCount: data.actionCount || 0,
    timestampNs: data.timestampNs || 0,
    receivedAt: Date.now(),
    blockhash: typeof data.blockhash === 'string' ? data.blockhash : undefined,
  };
  samples.push(sample);
  latestBlock = sample;
  pruneOldSamples();
}

function connect() {
  if (ws) {
    try { ws.close(); } catch { /* noop */ }
    ws = null;
  }
  connectionState = 'connecting';

  try {
    ws = new WebSocket(WS_URL);
  } catch (err) {
    console.error('Explorer WS construct failed:', err);
    connectionState = 'closed';
    scheduleReconnect();
    return;
  }

  ws.on('open', () => {
    console.log(`📡 Explorer WS connected: ${WS_URL}`);
    connectionState = 'open';
    reconnectAttempts = 0;
    lastMessageAt = Date.now();

    // Subscribe to block events in compact mode. The docs say this
    // must arrive within ~25ms of connect — we send immediately on
    // open, well within that window.
    try {
      ws!.send(JSON.stringify({ types: ['block'], compact: true }));
    } catch (err) {
      console.error('Explorer WS subscribe send failed:', err);
    }
  });

  ws.on('message', (raw) => {
    lastMessageAt = Date.now();
    let parsed: any;
    try {
      parsed = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Backpressure event = consumer (us) lagged. We log but don't
    // act — the stream catches up on its own; the dropped events
    // would have been pruned by the rolling window anyway.
    if (parsed.eventType === 'backpressure') {
      console.warn(
        `⚠️  Explorer WS backpressure: ${parsed.data?.droppedEvents} events dropped`
      );
      return;
    }

    if (parsed.eventType === 'block' && parsed.data) {
      handleBlockEvent(parsed.data);
    }
  });

  ws.on('error', (err) => {
    console.error('Explorer WS error:', err.message);
  });

  ws.on('close', (code, reason) => {
    console.warn(
      `📡 Explorer WS closed (code=${code} reason=${reason.toString()}). Reconnecting...`
    );
    connectionState = 'closed';
    ws = null;
    scheduleReconnect();
  });
}

// Start the connection. Called once during backend boot.
export function startExplorerListener() {
  if (connectionState !== 'idle' && connectionState !== 'closed') return;
  console.log(`📡 Starting BULK explorer listener (${WS_URL})`);
  connect();
}

// Derived throughput stats. Computed on demand so callers always get
// fresh numbers; the underlying samples array is updated by the WS
// handler in real time.
export function getThroughput() {
  pruneOldSamples();

  // If we have < 2 samples we can't compute a rate. Return zeros
  // explicitly so the frontend can distinguish "loading" (samples=0)
  // from "actually zero throughput" (samples>=2, sums=0).
  if (samples.length < 2) {
    return {
      tps: 0,
      aps: 0,
      sampleCount: samples.length,
      windowSeconds: WINDOW_MS / 1000,
      latestRound: latestBlock?.round ?? null,
      latestBlockhash: latestBlock?.blockhash ?? null,
      latestTimestampNs: latestBlock?.timestampNs ?? null,
      blockTimeMs: null as number | null,
      status: deriveStatus(),
    };
  }

  // Total tx/action across the window, divided by the actual elapsed
  // window time (first sample to last sample). Using actual elapsed
  // rather than WINDOW_MS gives a more accurate rate during startup
  // when we don't have a full 60s of samples yet.
  let totalTx = 0;
  let totalActions = 0;
  for (const s of samples) {
    totalTx += s.txCount;
    totalActions += s.actionCount;
  }

  const elapsedSec = (samples[samples.length - 1].receivedAt - samples[0].receivedAt) / 1000;
  // Guard against divide-by-zero if all samples arrive within 1ms
  // (shouldn't happen but cheap to protect against).
  const safeSec = Math.max(elapsedSec, 0.001);

  const tps = totalTx / safeSec;
  const aps = totalActions / safeSec;

  // Average block time = elapsed / (sampleCount - 1) intervals.
  const blockTimeMs = (samples[samples.length - 1].receivedAt - samples[0].receivedAt) /
    Math.max(1, samples.length - 1);

  return {
    tps,
    aps,
    sampleCount: samples.length,
    windowSeconds: WINDOW_MS / 1000,
    latestRound: latestBlock?.round ?? null,
    latestBlockhash: latestBlock?.blockhash ?? null,
    latestTimestampNs: latestBlock?.timestampNs ?? null,
    blockTimeMs,
    status: deriveStatus(),
  };
}

// Connection / freshness status for the frontend. Distinguishes between
// "stream is alive" vs "we haven't heard from it in a while" vs "no
// connection at all." Helps the frontend show an honest signal instead
// of stale numbers as if they were live.
function deriveStatus(): 'live' | 'stale' | 'disconnected' {
  if (connectionState !== 'open') return 'disconnected';
  // If WS is "open" but no messages in 10s on a chain that produces
  // blocks every 20ms, something's wrong upstream.
  if (Date.now() - lastMessageAt > 10_000) return 'stale';
  return 'live';
}
