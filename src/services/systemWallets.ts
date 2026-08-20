// Known BULK exchange system wallets.
//
// These are operational accounts run by the BULK protocol itself —
// liquidation engine, insurance fund, market-maker bots, treasury,
// etc. They show up in BULK's indexer and APIs alongside real user
// wallets, often with massive volume/PnL numbers that would skew
// any "top traders" or "whale" view.
//
// We filter them out of every ranked/leaderboard surface so users
// see actual traders, not protocol infrastructure. The wallets are
// still directly inspectable — visiting /whales/<system-address>
// shows their full data with a "Bulk System Account" badge so
// curious users can investigate.
//
// Stored as a Set for O(1) membership checks. Addresses are
// case-sensitive — Solana base58 is case-sensitive so we don't
// normalize.
const SYSTEM_WALLETS = new Set<string>([
  '9J8TUdEWrrcADK913r1Cs7DdqX63VdVU88imfDzT1ypt',
  // Liquidation engine — the protocol account that takes over every liquidated
  // position. It appears as the counterparty (taker) on one side of every
  // liquidation fill, so if recorded it becomes a phantom "liquidated party" on
  // every liquidation. Excluded from ingestion and from ranked liquidation views
  // so the real liquidated user is what surfaces.
  '5rXNKZnrV88vPbwWKDkUCsYUvNDCChY2Gzdj4MJDqvaa',
]);

export function isSystemWallet(address: string | null | undefined): boolean {
  if (!address) return false;
  return SYSTEM_WALLETS.has(address);
}

// The system-wallet addresses as a plain array, for excluding them directly in
// SQL (e.g. `WHERE wallet_address <> ALL($1::varchar[])`) when read-time JS
// filtering isn't practical — such as ranked queries where system rows would
// otherwise dominate the top before we ever see the real rows.
export const SYSTEM_WALLET_ADDRESSES: string[] = Array.from(SYSTEM_WALLETS);

// Returns the input list with any system wallets removed. Useful for
// filtering leaderboard rows, live activity entries, etc.
//
// The picker function extracts the wallet address from each item, so
// this works for any item shape (BULK indexer rows, our DB rows,
// trade events with a `wallet_address` field, ...).
export function filterOutSystemWallets<T>(items: T[], pick: (item: T) => string | null | undefined): T[] {
  return items.filter(item => !isSystemWallet(pick(item)));
}
