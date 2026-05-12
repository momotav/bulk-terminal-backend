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
]);

export function isSystemWallet(address: string | null | undefined): boolean {
  if (!address) return false;
  return SYSTEM_WALLETS.has(address);
}

// Returns the input list with any system wallets removed. Useful for
// filtering leaderboard rows, live activity entries, etc.
//
// The picker function extracts the wallet address from each item, so
// this works for any item shape (BULK indexer rows, our DB rows,
// trade events with a `wallet_address` field, ...).
export function filterOutSystemWallets<T>(items: T[], pick: (item: T) => string | null | undefined): T[] {
  return items.filter(item => !isSystemWallet(pick(item)));
}
