# Changelog

## [1.1.2] - 2025-10-21
- Added a dedicated diagnostics hotkey grouping RPC tests and Ultra order checks alongside the existing launcher.
- Introduced the `test-ultra` CLI command so operators can dry-run (or submit with `--submit`) Ultra orders end-to-end, including optional wallet and slippage overrides.
- Extended Jupiter Lend with `lend overview`, improved `lend borrow close <wallet> *` fan-out behaviour, and automatic transaction submission for Earn deposits/withdrawals unless `--no-send` is set.
- Earn flows now auto-create missing ATAs, taper SOL spending with configurable buffers, and skip wallets below the 0.005 SOL safety threshold while printing actionable guidance.
- Lend position queries now surface wallet-by-wallet counts and token balances to make post-deposit verification easier.

## [1.1.1] - 2025-10-21
- Earn deposit/withdraw commands understand `*` wildcards for wallets, base assets, and share tokens, defaulting to spendable balances while preserving rent/gas reserves.
- Launcher lend prompts accept blank inputs to trigger wildcard behaviour without typing explicit paths or amounts.

## [1.1.0] - 2025-10-21
- Swaps now default to Ultra execution (`https://api.jup.ag/ultra/91233f8d-d064-48c7-a97a-87b5d4d8a511`, override via `JUPITER_ULTRA_API_KEY`) with detailed order/execution logging and RPC confirmation; Lite API remains available via `JUPITER_SWAP_ENGINE=lite`.
- Switched token discovery to Jupiter Tokens API v2 and price lookups to Price API v3; added `tokens --refresh` to force catalog updates.
- Introduced experimental Jupiter Lend (Earn/Borrow) CLI commands and launcher menu (deposit/withdraw/mint/redeem/open/repay/close) with full API response logging.
- Added environment knobs for Ultra and Lend endpoints plus README guidance covering swap engine changes.
- Improved SOL aggregation to cascade balances from the tail wallet toward the target, preserving donor reserves.

## [1.0.2] - 2025-10-18
- Refactored startup resource checks to eliminate duplicate logic and added clarifying comments.
- Ensured `crew_1.json` always bypasses the wallet guard, regardless of balance.
- Applied cyan styling to the full banner and version headers, now showing “made by zayd / cold”.
- Bumped project version to v1.0.2 and expanded the running patch log.

## [1.0.1] - 2025-10-18
- Added automatic creation of `keypairs/` and `rpc_endpoints.txt` with startup announcements.
- Initialised the patch log and introduced author/version banner text.
- Added coloured header output to distinguish status lines.
