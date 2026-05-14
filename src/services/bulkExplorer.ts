// BULK Explorer client.
//
// Maintains a persistent WebSocket connection to BULK's explorer node
// and tracks recent block activity for two consumers:
//
//   1. Throughput metrics (TPS, APS) for the dashboard tiles
//   2. Recent-blocks list for the /explorer page
//
// Both share the same underlying ring buffer of block events. The
// throughput consumer reads samples over a rolling 60-second window;
// the explorer-list consumer reads the most-recent N blocks.
//
// Design notes:
//
// - One WS per backend instance, NOT per HTTP request. Frontend polls
//   our routes; routes read from this in-memory state. One persistent
//   connection to BULK regardless of how many users are online.
//
// - We subscribe in FULL mode (not compact) because the /explorer page
//   needs txHashes and they're stripped in compact mode. The bandwidth
//   tradeoff is fine — BULK's stream is local enough.
//
// - Ring buffer cap: 1000 blocks. At BULK's ~150 blocks/sec that's
//   roughly 7 seconds of history live, which is enough for the
//   explorer's "last 50" view. Older blocks have to be fetched via
//   HTTP `/block/:hash` by walking previousRoundHash.
//
// - Reconnect logic: exponential backoff capped at 30s. WS errors
//   don't crash the process; they queue a reconnect attempt. Buffer
//   survives across reconnects (we don't clear it on disconnect; new
//   blocks accumulate when we reconnect, old ones age out naturally).
//
// - Address configurable via BULK_EXPLORER_WS_URL env var.

import WebSocket from 'ws';

const WS_URL =
  process.env.BULK_EXPLORER_WS_URL || 'ws://64.130.50.69:12004';

// Full block event payload as received from BULK in non-compact mode.
// We type only the fields we use; BULK may add more, those pass through.
export interface BlockEvent {
  round: number;
  txCount: number;
  actionCount: number;
  timestampNs: number;
  blockhash: string;
  previousRoundHash?: string | null;
  txHashes?: string[];
  txHashXor?: string;
  nextRound?: number;
}

// What we keep per block in the ring buffer. Adds `receivedAt` (our
// clock) for window math without depending on BULK's nanosecond
// timestamps (which can drift, be wrong on reset, etc.).
interface BlockSample extends BlockEvent {
  receivedAt: number;  // Date.now() in ms
}

// How far back the throughput window looks. 60 seconds gives a stable
// rolling average without being too laggy for live feeling.
const WINDOW_MS = 60_000;

// Ring buffer cap. ~7 seconds at full throughput, plenty for explorer
// "recent blocks" view. We sort descending (newest first) when reading
// for the list endpoint; ascending order is preserved during inserts.
const MAX_BUFFER_SIZE = 1000;

const buffer: BlockSample[] = [];

// Latest snapshot. Updated on every block event.
let latestBlock: BlockSample | null = null;

let lastMessageAt = 0;

let ws: WebSocket | null = null;
let connectionState: 'idle' | 'connecting' | 'open' | 'closed' = 'idle';
let reconnectAttempts = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleReconnect() {
  if (reconnectTimer) return;
  const baseDelay = Math.min(30_000, 1000 * Math.pow(2, reconnectAttempts));
  const jitter = baseDelay * 0.1 * (Math.random() * 2 - 1);
  const delay = Math.max(500, baseDelay + jitter);
  reconnectAttempts += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function pruneBuffer() {
  // Hard cap by size. Time-based pruning is implicit (oldest go first
  // when we exceed the cap). The throughput window does its own
  // time-based filter at compute time.
  while (buffer.length > MAX_BUFFER_SIZE) {
    buffer.shift();
  }
}

function handleBlockEvent(data: any) {
  if (!data || typeof data.round !== 'number') return;
  const sample: BlockSample = {
    round: data.round,
    txCount: data.txCount || 0,
    actionCount: data.actionCount || 0,
    timestampNs: data.timestampNs || 0,
    blockhash: typeof data.blockhash === 'string' ? data.blockhash : '',
    previousRoundHash: data.previousRoundHash ?? null,
    txHashes: Array.isArray(data.txHashes) ? data.txHashes : [],
    txHashXor: typeof data.txHashXor === 'string' ? data.txHashXor : undefined,
    nextRound: typeof data.nextRound === 'number' ? data.nextRound : undefined,
    receivedAt: Date.now(),
  };
  buffer.push(sample);
  latestBlock = sample;
  pruneBuffer();
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

    // Subscribe in FULL mode (compact: false). We need txHashes for
    // the /explorer page's block list, and they're omitted in compact.
    // Bandwidth difference is negligible at our scale.
    try {
      ws!.send(JSON.stringify({ types: ['block'], compact: false }));
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

export function startExplorerListener() {
  if (connectionState !== 'idle' && connectionState !== 'closed') return;
  console.log(`📡 Starting BULK explorer listener (${WS_URL})`);
  connect();
}

// Derived throughput stats (TPS/APS over the rolling window). Filters
// the buffer to the window at compute time so old blocks don't skew
// the rate even though they remain in the ring buffer for the
// explorer-list consumer.
export function getThroughput() {
  const cutoff = Date.now() - WINDOW_MS;
  const windowSamples = buffer.filter(s => s.receivedAt >= cutoff);

  if (windowSamples.length < 2) {
    return {
      tps: 0,
      aps: 0,
      sampleCount: windowSamples.length,
      windowSeconds: WINDOW_MS / 1000,
      latestRound: latestBlock?.round ?? null,
      latestBlockhash: latestBlock?.blockhash ?? null,
      latestTimestampNs: latestBlock?.timestampNs ?? null,
      blockTimeMs: null as number | null,
      status: deriveStatus(),
    };
  }

  let totalTx = 0;
  let totalActions = 0;
  for (const s of windowSamples) {
    totalTx += s.txCount;
    totalActions += s.actionCount;
  }

  const elapsedSec =
    (windowSamples[windowSamples.length - 1].receivedAt - windowSamples[0].receivedAt) / 1000;
  const safeSec = Math.max(elapsedSec, 0.001);

  const tps = totalTx / safeSec;
  const aps = totalActions / safeSec;

  const blockTimeMs =
    (windowSamples[windowSamples.length - 1].receivedAt - windowSamples[0].receivedAt) /
    Math.max(1, windowSamples.length - 1);

  return {
    tps,
    aps,
    sampleCount: windowSamples.length,
    windowSeconds: WINDOW_MS / 1000,
    latestRound: latestBlock?.round ?? null,
    latestBlockhash: latestBlock?.blockhash ?? null,
    latestTimestampNs: latestBlock?.timestampNs ?? null,
    blockTimeMs,
    status: deriveStatus(),
  };
}

// Returns the most-recent N blocks for the /explorer page list view.
// Sorted newest-first so the UI can render directly without sorting.
// Limited to the buffer cap (1000) — for older blocks the frontend
// has to use the HTTP per-block endpoint via previousRoundHash walking.
export function getRecentBlocks(limit: number = 50): BlockSample[] {
  const n = Math.max(1, Math.min(limit, MAX_BUFFER_SIZE));
  // Slice the tail (newest) and reverse for descending-by-round order.
  // We assume blocks arrive in round order; reset edge cases get
  // tolerated since rounds are still monotonic within a session.
  return buffer.slice(-n).reverse();
}

function deriveStatus(): 'live' | 'stale' | 'disconnected' {
  if (connectionState !== 'open') return 'disconnected';
  if (Date.now() - lastMessageAt > 10_000) return 'stale';
  return 'live';
}
