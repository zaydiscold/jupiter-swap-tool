# Changelog

## [1.3.1] - 2025-10-27

### Added
- **QUIET_MODE**: New environment variable to suppress verbose console output
  - Hides RPC retry messages, ultra payload JSON dumps, mint addresses, balance calculations
  - Shows only critical info: swap confirmations, errors, completions
  - Set `QUIET_MODE=1` or `JUPITER_QUIET_MODE=1` to enable
- **Infinite Loop Support**: Flows can now run indefinitely without manual prompts
  - Add `--infinite` or `--loop` flag to any flow command
  - Add `--loop-cooldown <ms>` to set delay between loops (default: 60s)
  - Graceful shutdown: Ctrl+C finishes current loop before exiting
  - Shows loop counter: "Loop #5 completed, starting Loop #6..."
  - Works with: icarus, zenith, aurora, titan, odyssey, sovereign, nova, arpeggio, horizon, echo
- **Flow Throttling Environment Variables**:
  - `FLOW_WALLET_DELAY_MS` - Delay between processing wallets (default: 500ms)
  - `FLOW_LOOP_COOLDOWN_MS` - Delay between flow loops (default: 60000ms)
  - `RPC_RETRY_BASE_DELAY_MS` - Base RPC retry delay (default: 1000ms)
  - `MAX_CONCURRENT_WALLETS` - Max concurrent wallet operations (default: 5)

### Fixed
- **Fatal 429 Crashes**: Flows no longer crash on RPC rate limit errors
  - Catches all 429/rate limit errors during flow execution
  - In infinite mode: logs warning, adds 10s recovery delay, continues to next loop
  - In manual mode: displays error and exits gracefully
- **Token Discovery Retry Logic**: Added exponential backoff to `getAllParsedTokenAccounts()`
  - Retries 3 times with increasing delays (500ms, 1000ms, 1500ms)
  - Prevents sweep commands from showing "No token balances" during RPC rate limits
- **Perps Command Output**: Improved user-friendly formatting
  - Open: Shows clear position metrics (entry price, liquidation, leverage) instead of raw JSON
  - Positions: Displays detailed position cards with PnL, liquidation distance, close instructions
  - Close: Shows realized PnL and close fees in readable format
  - Verbose JSON hidden behind `DEBUG_PERPS=1` or `VERBOSE=1` flags
- **Perps Price Check Optimization**: Check USD value before swapping to avoid wasting gas
  - Fetches SOL price before swap to verify $10 minimum for new positions
  - Skips wallets that don't meet minimum without wasting gas on swap

### Improved
- **Flow Wallet Delays**: All flow execution now uses configurable wallet delays
  - Reduces RPC rate limiting by spacing out wallet operations
  - Customizable via `FLOW_WALLET_DELAY_MS` environment variable
  - Applied to all flow types (icarus, titan, etc.)

### Usage Examples
```bash
# Run Icarus infinitely with quiet mode (recommended for unattended operation)
QUIET_MODE=1 node cli_trader.js icarus --infinite

# Run with custom delays to avoid rate limits
QUIET_MODE=1 FLOW_WALLET_DELAY_MS=2000 node cli_trader.js titan --infinite

# Run with shorter loop cooldown
node cli_trader.js icarus --infinite --loop-cooldown 30000

# Check perps positions with clean output
node cli_trader.js perps positions crew_19.json

# View verbose perps data for debugging
VERBOSE=1 node cli_trader.js perps positions crew_19.json
```

## [1.3.0] - 2025-10-27

### Added
- **Jupiter Perps Trading**: Complete USDC collateral flow for opening perpetual positions
  - Intelligent SOL → USDC → SOL swap pipeline with 0.01 SOL gas reserve
  - Automatic $10 minimum collateral validation for new positions
  - Proper leverage calculation: positionSize = collateral × leverage
  - Supports batch trading with "all wallets" option
- **Wallet Transfer Fast Mode**: Skip balance loading for instant transfers
  - Option [1]: Fast mode (default) - skip RPC balance queries, instant transfer setup
  - Option [2]: Detailed mode - show all wallet balances before transfer
  - Eliminates RPC rate limiting delays when user knows wallet numbers

### Fixed
- **Ultra API Integration**: Corrected parameter names and transaction signing
  - Fixed `createUltraOrder` parameter mapping (amountLamports, userPublicKey)
  - Fixed transaction signing with `VersionedTransaction.deserialize/sign`
  - Fixed API response property access (swapTransaction, outAmount)
  - Fixed collateral mint requirement for perps positions (must match market mint)
- **Perps API Payload**: All field types now match API requirements
  - Leverage, side, maxSlippageBps are strings (not numbers)
  - Proper decimal handling for SOL (9) and USDC (6)

### Improved
- **Error Handling**: Better feedback when swaps fail or collateral is insufficient
- **User Experience**: Clearer console output showing swap progression and position sizing

## [1.2.3] - 2025-10-26

### Changed
- **Jupiter API Endpoints**: All APIs now use lite version URLs for free tier access (Price v3, Tokens v2, Lend, Perps)
- **Buckshot Mode**: Now spreads across ALL swappable tokens in catalog (not limited to 8). Each wallet swaps to ONE unique random token for maximum transaction size
- **Flow Optimizations**: Removed ATA rent pre-reservation from buckshot and sweep-to-btc-eth flows - maximizes available SOL per transaction
- **Nova Flow**: Reduced minimum to 0.01 SOL (from 0.02 SOL) for more accessible supernova trading
- **Titan Flow**: Increased minimum hold time to 1 minute (from 30 seconds) for whale-sized positions
- **Close Token Accounts**: Dust rescue threshold changed to 0.002 SOL worth (from $0.05) - better aligned with transaction economics

### Fixed
- **Price API v3 Compatibility**: Updated response format handling (`usdPrice` field, direct response without `data` wrapper) - fixes sweep-to-SOL showing "No token balances" when tokens exist

### Removed
- **Dead Code Cleanup**: Removed Nexus and Nebula flows (not exposed in CLI menu)

## [1.2.2] - 2025-10-26

### Fixed
- **ATA Creation Optimization**: Removed excessive RPC calls during gas estimation. ATA checks now happen only during swap execution, preventing rate limiting issues when processing multiple wallets.
- **Sweep-to-BTC-ETH Flow**: Each wallet now sweeps to a single randomly selected token instead of spreading across all targets. Excludes USDT/USDC while including all swappable tokens (SPYX, NVDAX, CRCLX, JUPSOL, etc.).

### Pending
- Jupiter Lend withdraw dust handling for amounts below protocol threshold

## [1.2.1] - 2025-10-23
- Bumped project version to v1.2.1 across `VERSION`, `package.json`, and documentation.
- Refreshed docs (root operator guide, in-repo manual, command reference, CLAUDE brief) to highlight the numbered wallet registry, smarter Ultra retries, and current environment knobs.
- Noted the smarter Ultra ↔ Lite fallbacks in patch notes and changelog for operator visibility.
- Initial multi-wallet support
- Jupiter Ultra API integration
- Prewritten flow system

## [1.2.0] - 2025-10-22
- Added a filesystem-backed wallet registry manifest that auto-numbers keypairs, records master/master-master relationships, and exposes the hierarchy through new CLI commands (`wallet list|info|sync|groups|transfer`).
- Implemented number-aware wallet resolution across the CLI (including Lend/Earn, aggregation, and transfers) plus dedicated commands for hierarchical aggregation (`aggregate-hierarchical`, `aggregate-masters`).
- Converted shared wallet helpers to ESM, eliminating runtime `require` errors and ensuring registry sync happens automatically when wallets are listed.
- Reduced default execution delays (60 ms base, 25 ms Ultra wallet spacing, 18 ms Ultra execute gap) and raised campaign concurrency to 16; token-account cleanup now closes 12 ATA accounts per transaction by default.
- Ultra swaps now retry transient API failures a few times (configurable via `MAX_ULTRA_FALLBACK_RETRIES`) before falling back to Lite, and RPC rotation treats rate limits with shorter cooldowns so the CLI cycles endpoints instead of looping on `api.mainnet-beta.solana.com`.
- Disabled Jupiter Lend borrow tooling with clear "coming soon" messaging while keeping the Earn implementation aligned with the `/lend/v1/earn` API (share-token mapping, ATA pre-creation, highest-yield auto-selection).
- Simplified the token catalog to rely on the `swappable` tag (plus `default-sweep`/`secondary-terminal` where relevant) so every non-SOL asset participates in all flows without per-flow tag maintenance.
- Added targeted unit tests for the wallet registry so numbering, group assignment, and identifier resolution stay covered by `npm test`.

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
