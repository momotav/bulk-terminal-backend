// Devnet ticker-snapshot poller.
//
// The main WebSocket listener (jobs/wsListener.ts) is a singleton that
// collects the TESTNET network only. Rather than refactor that live
// pipeline to run twice, this is a small, isolated REST poller dedicated
// to DEVNET — it never touches the WS listener's state.
//
// Every minute it:
//   1. Resolves the devnet market list (staging /exchangeInfo)
//   2. Fetches /ticker/{symbol} for each (host auto-rewritten to staging)
//   3. Inserts a ticker_snapshots row tagged `network = 'devnet'`
//
// This powers the devnet versions of the Open Interest, Funding Rate,
// Volatility, and Fair-vs-Mark Spread charts — the ticker_snapshots-backed
// ones. Trades/volume/liquidations/ADL for devnet are a separate slice
// (they need the WS or a trades REST endpoint).
//
// All BULK calls run inside `networkContext.run('devnet', ...)` so
// getActiveSymbols() and bulkFetch() resolve to staging, and every insert
// sets network='devnet' explicitly (belt-and-suspenders with the column
// default).

import { query } from '../db';
import { bulkFetch } from '../services/bulkAuth';
import { getActiveSymbols } from '../services/markets';
import { runWithNetwork } from '../services/networkContext';
import { publishMarketUpdate } from '../services/marketStream';

const BULK_API_BASE = 'https://exchange-api.bulk.trade/api/v1'; // rewritten → staging by bulkFetch
const POLL_INTERVAL_MS = 60_000;

interface TickerData {
  openInterest?: string | number;
  markPrice?: string | number;
  lastPrice?: string | number;
  fundingRate?: string | number;
  regime?: number;
  regimeVol?: number;
  fairBookPx?: number;
}

let pollTimer: ReturnType<typeof setInterval> | null = null;

async function pollOnce(): Promise<void> {
  // Everything inside resolves to the devnet (staging) host.
  await runWithNetwork('devnet', async () => {
    let symbols: string[];
    try {
      symbols = await getActiveSymbols();
    } catch (err) {
      console.error('[devnet-poller] failed to resolve symbols:', err);
      return;
    }

    let ok = 0;
    for (const symbol of symbols) {
      try {
        const res = await bulkFetch(`${BULK_API_BASE}/ticker/${symbol}`);
        if (!res.ok) continue;
        const ticker = (await res.json()) as TickerData;

        const openInterestCoins = parseFloat(String(ticker.openInterest || 0));
        const markPrice = parseFloat(String(ticker.markPrice || ticker.lastPrice || 0));
        const openInterestUsd = openInterestCoins * markPrice;
        const fundingRate = parseFloat(String(ticker.fundingRate || 0));
        const regime = ticker.regime ?? null;
        const regimeVol = ticker.regimeVol ?? null;
        const fairBookPx = ticker.fairBookPx ?? null;

        await query(
          `INSERT INTO ticker_snapshots
             (symbol, open_interest_coins, open_interest_usd, funding_rate, mark_price, regime, regime_vol, fair_book_px, network, timestamp)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'devnet', NOW())`,
          [symbol, openInterestCoins, openInterestUsd, fundingRate, markPrice, regime, regimeVol, fairBookPx],
        );
        ok++;

        if (markPrice > 0) {
          publishMarketUpdate({ symbol, price: markPrice, kind: 'mark', ts: Date.now() });
        }
      } catch {
        // skip this symbol this tick
      }
    }
    if (ok > 0) {
      console.log(`📊 [devnet-poller] wrote ${ok}/${symbols.length} ticker snapshots`);
    }
  });
}

export function startDevnetPoller(): void {
  if (pollTimer) return;
  console.log('🛰️  Starting devnet ticker poller (60s)…');
  // Kick off shortly after boot, then on an interval.
  setTimeout(() => void pollOnce(), 5_000);
  pollTimer = setInterval(() => void pollOnce(), POLL_INTERVAL_MS);
}

export function stopDevnetPoller(): void {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}
