# Changelog

## [1.3.2.6] - 2025-10-27

### Added
- **Total SOL Balance Display**: Wallet balance check now shows total SOL across all wallets
  - Added accumulation of SOL from all wallets during balance check
  - Displays prominent summary: "💰 Total SOL balance across all wallets: X.XXXXXX SOL"
  - Shows at end of balances output for easy fee/loss tracking
  - Helps monitor overall portfolio value

### Improved
- **RPC Logging Spam Reduction**: Silenced repetitive "no healthy alternatives" messages during rate limits
  - Only logs when RPC endpoint actually changes (successful rotation)
  - Suppresses spam when same endpoint retried during rate limits
  - Allows silent retry with exponential backoff during rate limit periods
  - Reduces console noise while maintaining connection rotation logic
  - Combined with global error handlers for seamless rate limit recovery

## [1.3.2.5] - 2025-10-27

### Fixed (CRITICAL - NUCLEAR OPTION)
- **Process-Level Error Handlers**: Added global unhandledRejection and uncaughtException handlers
  - Catches ANY error that escapes all try/catch blocks (including deep @solana/web3.js errors)
  - Detects rate limit errors (429, "too many requests", "rate limit") at process level
  - Rate limit errors: Log warning and continue (process never crashes)
  - Non-rate limit errors: Log error and continue (with counter safety)
  - Safety limit: Exits after 50 unhandled errors to prevent infinite loops
  - This is the FINAL safety net - guarantees process continuity
  - Fixes: Unhandled promise rejections from web3.js RPC client causing "status 1" exits

## [1.3.2.4] - 2025-10-27

### Fixed (CRITICAL)
- **Rate Limit Crash Prevention**: Added multiple layers of safety to prevent 429 errors from crashing the process
  - PRIMARY SAFETY NET: Raw string check for "429", "too many requests", "rate limit" BEFORE error classification
  - Executes immediately when outer catch block triggers, rotating RPC with 2s delay
  - SECONDARY CHECK: Classified rate limit errors handled with 1s delay
  - EXPLICIT CONTINUE: All error paths now explicitly continue to next wallet instead of relying on fall-through
  - Guarantees process never crashes on rate limits - always logs, rotates, and continues
  - Fixes: "Error: 429 Too Many Requests" causing "Node.js exited with status 1"

## [1.3.2.3] - 2025-10-27

### Improved
- **ATA Creation Retry Logic**: ensureAta() now retries transaction confirmation timeouts up to 3 times
  - Implements exponential backoff: 2s, 5s, 10s delays between retries
  - Detects confirmation timeout errors: "was not confirmed in" or "timeout"
  - Shows user-friendly retry messages: "ATA creation timeout (attempt X/3), retrying in Xs..."
  - Prevents transaction timeout errors from immediately failing swaps
  - After 3 attempts, throws error for doSwapAcross to handle gracefully
  - Reduces "skipping: cannot create ATA" errors during network congestion

## [1.3.2.2] - 2025-10-27

### Fixed
- **RPC Rate Limiting Crash**: ensureAta() now gracefully handles 429 rate limit errors instead of crashing
  - Wraps all RPC calls with `runWithRpcRetry()` for automatic retry with backoff
  - Detects rate limit errors and falls back without creating ATA instead of crashing
  - Silent fallback for rate limits (expected behavior during heavy load)
  - Continues flow execution instead of terminating with error
  - Prevents "Error: 429 Too Many Requests" from terminating the entire trading session

## [1.3.2.1] - 2025-10-27

### Fixed (CRITICAL)
- **Token-2022 ATA Creation Bug**: GOOGLx, CRCLx, NVDAx, and other Token-2022 mints were incorrectly marked as "spl" program in token_catalog.json
  - Caused "incorrect program id for instruction" errors when creating ATAs
  - Fixed catalog entries: GOOGLx, CRCLx, NVDAx now correctly marked as "token-2022"
  - Added on-chain mint owner verification in `ensureAta()` function
  - Now automatically detects and uses correct token program (SPL or Token-2022) regardless of catalog data
  - Falls back gracefully if RPC verification fails during rate limiting

## [1.3.2] - 2025-10-27

### Added
- **Session Volume Tracking**: All trading flows now track and display total SOL volume traded
  - Running volume total displayed every 10 hops (when closing token accounts)
  - Final session summary shows total SOL traded across all swaps
  - Format: `📊 Total session volume: X.XXXXXX SOL traded across all swaps`
  - Tracks absolute value of SOL balance deltas to measure actual trading activity
  - Applied to all flows: Arpeggio, Horizon, Echo, Icarus, Zenith, Aurora, Titan, Odyssey, Sovereign, Nova

- **True Random Flow Behavior**: Crypto-grade randomization for unpredictable trading patterns
  - Each cycle now generates completely unique token sequences (no repeats)
  - Three layers of entropy: millisecond timestamp + Math.random() + process high-res timer
  - Running the same flow 1 second apart produces different results
  - Applies to ALL flows universally (deterministic and random flows alike)
  - Example: Running Icarus 3 times = 3 completely different token sequences
  - Prevents pattern detection and ensures untraceable trading behavior

### Changed
- **RNG Seed Generation**: Upgraded from deterministic cycle-based to true random entropy
  - Old: `flow:wallet:cycle1:flow` (predictable, same cycle = same tokens)
  - New: `flow:wallet:cycle1:t1730000000000:r123456789:pXXXXX` (unique every time)
  - Makes trading patterns impossible to predict or replicate

## [1.3.1] - 2025-10-27

### Fixed (CRITICAL)
- **Perps Position Opening Bug**: The `perps open` command was only calling the API but never signing/submitting the transaction to the blockchain
  - Swaps would execute successfully (visible on-chain)
  - But the perps position transaction was never submitted
  - Result: No actual position opened, funds just swapped back and forth
  - Now properly deserializes, signs, and submits the transaction returned by the API
  - Waits for confirmation before showing success message
  - Positions will now actually appear on Jupiter Perps interface

### Added
- **Interactive Flow Configuration**: Flows now prompt for settings when started via CLI
  - "Enable quiet mode?" - Default: Yes (less console spam)
  - "Run flow infinitely?" - Default: Yes (no manual Y/N prompts)
  - "Loop cooldown in seconds?" - Default: 60s (delay between loops)
  - Displays configuration summary before starting
  - Prompts only appear when not using command-line flags (backwards compatible)
  - Applied to all flows: icarus, zenith, aurora, titan, odyssey, sovereign, nova, arpeggio, horizon, echo
- **QUIET_MODE**: New environment variable to suppress verbose console output
  - Hides RPC retry messages, ultra payload JSON dumps, mint addresses, balance calculations
  - Shows only critical info: swap confirmations, errors, completions
  - Set `QUIET_MODE=1` or `JUPITER_QUIET_MODE=1` to enable
  - Can also be enabled via interactive prompt when starting flows
- **Infinite Loop Support**: Flows can now run indefinitely without manual prompts
  - Add `--infinite` or `--loop` flag to any flow command (or enable via interactive prompt)
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
- **Quiet Mode Interference**: Removed broken logMuted override that was breaking error handling in sweep and other commands
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
# Interactive mode (EASIEST - prompts for all options with smart defaults)
node cli_trader.js icarus
# Prompts: Enable quiet mode? (Y/n)
# Prompts: Run infinitely? (Y/n)
# Prompts: Loop cooldown? (default: 60)
# Just press Enter 3 times to accept defaults!

# Command-line flag mode (bypass prompts)
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
