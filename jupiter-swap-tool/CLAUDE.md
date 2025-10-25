# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Jupiter Swap Tool CLI (v1.2.0) is a Node.js command-line tool for automated Solana trading through Jupiter's swap infrastructure. The tool manages multiple wallets, executes swap strategies (buckshot, long-circle, prewritten flows), and integrates with Jupiter Perps and Lend APIs.

## Core Architecture

### Single-File Design
The entire application lives in `cli_trader.js` (~15,000 lines). This is intentional - the codebase prioritizes deployment simplicity and inline visibility over module separation. All swap logic, wallet management, Jupiter API integration, RPC handling, and automation flows are co-located.

### Key Subsystems (within cli_trader.js)

**Token Management System** (lines ~1700-2400)
- `token_catalog.json`: Central registry of tradeable tokens with tags for flow selection
- Tag system: `"swappable"` (all tradeable tokens), `"long-circle"` (core liquidity tokens), `"terminal"` (SOL), `"default-sweep"`, `"secondary-terminal"` (USDC/wBTC/cbBTC/wETH)
- Dynamic token selection via `buildDynamicLongCircleSegments()` and tag-based filtering
- Token catalog can be refreshed from Jupiter Tokens API v2 with `tokens --refresh`

**Prewritten Flow System** (lines ~8740-9200)
- `PREWRITTEN_FLOW_PLAN_MAP`: Defines 9 automated trading strategies
- 6 deterministic flows: Arpeggio, Horizon, Echo (hardcoded token sequences)
- 3 whale flows: Titan, Odyssey, Sovereign (randomized tokens, $5+ minimum swaps)
- Flow structure:
  - `cycleTemplate`: Array of swap steps with `fromMint`, `toMint`, `amount`, `randomization`, `delayAfterMs`
  - `swapCountRange`: Min/max swaps to execute
  - `waitBoundsMs`: Inter-step delay ranges
  - `forceSolReturnEvery`: Safety mechanism to return to SOL every N swaps

**Whale Flow Optimizations** (recently added)
- Per-step delays: 30s-10min when holding non-SOL tokens, 1-3s when returning to SOL
- Swap counter: Forces complete SOL return every 4-7 swaps in infinite loops
- Dust prevention: All Token→Token swaps use `amount: "all"` to prevent balance fragmentation
- Slippage optimization: `restrictIntermediateTokens: "true"` forces high-liquidity routing

**Jupiter API Integration** (lines ~10740-11500)
- Dual engine support: Ultra API (paid tier, default) and Lite API (legacy fallback)
- Ultra: `/ultra/v1/order` → `/ultra/v1/execute` (transaction handled by Jupiter)
- Lite: `/swap/v1/quote` → `/swap/v1/swap` (local transaction signing)
- Functions: `createUltraOrder()`, `fetchLegacyQuote()`, `executeUltraTransaction()`
- Automatic fallback: 404/401/403 from Ultra → switch to Lite API

**Wallet Guard System** (lines ~2500-3000)
- Tracks wallet SOL balances and disables wallets with <0.01 SOL from swap flows
- `balances` command refreshes guard state
- `force-reset-wallets` clears guard without balance check
- Prevents transaction failures due to insufficient gas

**RPC Rotation** (lines ~2212-2350)
- Loads endpoints from `rpc_endpoints.txt` (one per line or comma-separated)
- `nextRpcEndpoint()`: Round-robin with unhealthy endpoint tracking
- `markRpcEndpointUnhealthy()`: Temporarily sidelines failing RPCs
- Health checks with `test-rpcs` command

**Flow Execution Engine** (lines ~9440-10050)
- `runPrewrittenFlowPlan()`: Main orchestration function
- Resolves randomization, applies per-step delays, executes swaps, handles forced SOL returns
- Deterministic RNG from wallet seed for reproducible randomization
- Session state tracking for multi-step random token selection

## Running the CLI

### Development Commands
```bash
# Run CLI directly
node cli_trader.js <command> [args]

# Syntax validation
node --check cli_trader.js

# Launch interactive mode (macOS)
./run_cli_trader.command
```

### Common Commands
```bash
# Wallet management
node cli_trader.js generate 5 crew          # Generate 5 wallets (crew_1.json, crew_2.json, ...)
node cli_trader.js list                      # List all wallets
node cli_trader.js balances                  # Check balances + refresh wallet guard

# Swaps
node cli_trader.js swap SOL USDC 0.1        # Swap 0.1 SOL → USDC
node cli_trader.js swap-all SOL USDC        # Swap all SOL → USDC

# Flows (strategies)
node cli_trader.js flow run icarus          # Run Icarus flow (high-tempo random swaps)
node cli_trader.js flow run titan           # Run Titan whale flow ($5+ minimum)

# Automation modes
node cli_trader.js buckshot                  # Equal SOL split into all long-circle tokens
node cli_trader.js long-circle              # Extended multi-hop route through catalog

# Utilities
node cli_trader.js test-ultra               # Test Jupiter Ultra API integration
node cli_trader.js test-rpcs all            # Probe all RPC endpoints
node cli_trader.js tokens --refresh         # Refresh token catalog from API
```

## Token Catalog System

**File**: `token_catalog.json`

**Structure**:
```json
{
  "symbol": "USDC",
  "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "decimals": 6,
  "program": "spl",
  "tags": ["swappable", "default-sweep", "secondary-terminal", "long-circle"]
}
```

**Tag Usage**:
- Add `"swappable"` to make token available in random flows (Icarus, Zenith, Aurora, Titan, Odyssey, Sovereign)
- Add `"long-circle"` to include in buckshot/long-circle modes (requires deep liquidity)
- Tag changes apply immediately without code modifications

**Dynamic Token Selection**:
- `buildDynamicLongCircleSegments()` generates segments from `"long-circle"` tagged tokens
- Random flows use `poolTags: ["swappable"]` in `randomization` blocks
- Avoid hardcoding mints in flow definitions - use tags instead

## Flow Modification Patterns

### Adding a New Flow
1. Add entry to `PREWRITTEN_FLOW_PLAN_MAP` (line ~8740):
```javascript
[
  "flowkey",
  {
    key: "flowkey",
    label: "Flow Name",
    description: "Strategy description",
    startMint: SOL_MINT,
    cycleTemplate: [
      {
        fromMint: SOL_MINT,
        toMint: RANDOM_MINT_PLACEHOLDER,  // For random token selection
        amount: { mode: "range", min: 0.1, max: 0.3 },
        delayAfterMs: { min: 30_000, max: 600_000 },  // Optional per-step delay
        randomization: {
          mode: "sol-to-random",
          poolTags: ["swappable"],
          excludeMints: [SOL_MINT],
        },
      },
      {
        fromMint: RANDOM_MINT_PLACEHOLDER,
        toMint: SOL_MINT,
        amount: "all",  // Always use "all" for non-SOL → X to prevent dust
        delayAfterMs: { min: 1_000, max: 3_000 },
      },
    ],
    swapCountRange: { min: 10, max: 50 },
    minimumCycles: 2,
    requireTerminalSolHop: true,
    waitBoundsMs: { min: 60_000, max: 180_000 },
    defaultDurationMs: 30 * 60 * 1000,
    forceSolReturnEvery: { min: 4, max: 7 },  // Safety for infinite loops
  },
]
```

2. Register command in `VALID_COMMANDS` if needed
3. Test with: `node cli_trader.js flow run flowkey --loops 1`

### Amount Modes
- `"all"`: Entire token balance (required for Token→Token/Token→SOL to prevent dust)
- `"random"`: Random percentage of balance (only use for SOL→Token)
- `{ mode: "range", min: 0.1, max: 0.3 }`: Random SOL amount in range
- `null`: Uses session default from launcher

### Randomization Modes
- `"sol-to-random"`: SOL → random token from pool
- `"random-to-random"`: Random token → different random token
- `"session-to-random"`: Use session-tracked token → new random token
- `"session-to-sol"`: Use session-tracked token → SOL

## Environment Variables

### Essential
```bash
RPC_URL=https://your-rpc.com                          # Solana RPC endpoint
JUPITER_SWAP_ENGINE=ultra                             # ultra (default) or lite
JUPITER_ULTRA_API_KEY=your-key                        # Ultra API authentication
```

### Tuning
```bash
SLIPPAGE_BPS=20                                       # Default 0.20% slippage
MAX_SLIPPAGE_RETRIES=5                                # Retries on slippage errors
GAS_RESERVE_LAMPORTS=1000000                          # 0.001 SOL gas reserve
MIN_SOL_PER_SWAP_LAMPORTS=10000000                    # 0.01 SOL minimum for swaps
JUPITER_SOL_BUFFER_LAMPORTS=2000000                   # Extra SOL buffer for wrapping
```

### Debugging
```bash
JUPITER_SWAP_TOOL_TIMING=1                            # Emit planning vs execution timings
PERPS_DRY_RUN=1                                       # Print perps payloads without submitting
NO_COLOR=1                                            # Disable colored output
```

## Jupiter API Parameters

### Quote/Order Requests
Always include:
- `inputMint`, `outputMint`: Token mint addresses
- `amount`: Lamports as string
- `slippageBps`: Basis points (default 20 = 0.20%)
- `restrictIntermediateTokens: "true"`: Forces high-liquidity routing (reduces slippage)
- `swapMode: "ExactIn"`: Specify input amount exactly

### Ultra API Authentication
- Header: `x-api-key: <JUPITER_ULTRA_API_KEY>`
- Base URL: `https://api.jup.ag/ultra/v1` (authenticated) or `https://lite-api.jup.ag/ultra/v1` (free tier)

## Testing

### Syntax Check
```bash
node --check cli_trader.js
```

### Integration Tests
```bash
# Test Jupiter Ultra API
node cli_trader.js test-ultra --wallet crew_1.json

# Test RPC endpoints
node cli_trader.js test-rpcs all

# Dry-run a flow
PERPS_DRY_RUN=1 node cli_trader.js flow run titan --loops 1
```

### Smoke Test Pattern
```bash
# 1. Generate test wallet
node cli_trader.js generate 1 test

# 2. Fund with devnet airdrop (devnet only)
node cli_trader.js airdrop test_1.json 1000000000

# 3. Check balances
node cli_trader.js balances

# 4. Test small swap
node cli_trader.js swap SOL USDC 0.001
```

## Critical Implementation Rules

### Dust Prevention (MANDATORY)
- All Token→Token swaps MUST use `amount: "all"`
- All Token→SOL swaps MUST use `amount: "all"`
- Only SOL→Token swaps may use `"random"` or range amounts
- This prevents balance fragmentation across wallets

### Slippage Optimization
- Always set `restrictIntermediateTokens: "true"` in Jupiter API calls
- This applies to both Ultra orders (line ~10994) and Lite quotes (line ~10900)

### Wallet Safety
- Check wallet guard status before executing flows
- Reserve at least `GAS_RESERVE_LAMPORTS` for transaction fees
- SOL swaps automatically subtract `JUPITER_SOL_BUFFER_LAMPORTS` for wrap costs

### Flow Safety (Infinite Loops)
- Always include `forceSolReturnEvery: { min: 4, max: 7 }` in whale flows
- This forces periodic SOL returns to prevent stuck positions
- Implementation is in flow execution engine (lines ~9970-10010)

### Tag System Maintenance
- When adding new tokens to `token_catalog.json`, include appropriate tags
- Use `"swappable"` for general inclusion in random flows
- Use `"long-circle"` only for deep-liquidity tokens (USDC, POPCAT, PUMP, etc.)
- Avoid creating flow-specific tags - they create maintenance burden

## Common Debugging Scenarios

### "Insufficient lamports" Error
- Wallet SOL balance too low for swap + gas
- CLI automatically retries with reduced amounts (up to `JUPITER_SOL_MAX_RETRIES`)
- Check `GAS_RESERVE_LAMPORTS` and `JUPITER_SOL_BUFFER_LAMPORTS` settings

### "Slippage tolerance exceeded"
- Price moved between quote and execution
- Automatically retries up to `MAX_SLIPPAGE_RETRIES` times
- Consider increasing `SLIPPAGE_BPS` or checking pool liquidity

### Flow Completes Too Quickly
- `swapCountRange` determines number of swaps
- `waitBoundsMs` controls inter-step delays
- Per-step `delayAfterMs` overrides global wait bounds

### Tokens Not Appearing in Random Flows
- Check token has `"swappable"` tag in `token_catalog.json`
- Verify flow uses `poolTags: ["swappable"]` in randomization block
- Run `node cli_trader.js tokens` to confirm catalog loaded correctly

## Project Structure

```
jupiter-swap-tool/
├── cli_trader.js              # Main application (15,000+ lines)
├── token_catalog.json         # Token registry with tags
├── run_cli_trader.command     # macOS launcher script
├── package.json               # Node.js dependencies
├── README.md                  # User documentation
├── ARCHITECTURE_PLAN.md       # Multi-chain roadmap
├── perps_config.sample.json   # Perpetuals configuration template
├── perps_idl.json            # Jupiter Perps program IDL
└── keypairs/                  # Wallet storage (gitignored)
    ├── crew_1.json
    ├── crew_2.json
    └── ...
```

## Future Architecture (ARCHITECTURE_PLAN.md)

The codebase is planned for modularization:
- Phase 1: Extract chain adapter interface
- Phase 2: Move Solana logic to `chains/solana/` modules
- Phase 3: Add Hyperliquid, Aster, BNB Chain adapters
- Phase 4: Unified `multi_trader.js` launcher
- Phase 5: Cross-chain concurrent automation

For now, maintain compatibility with the single-file architecture. When making changes:
1. Keep functions self-contained
2. Use clear section comments (`/* --- Section Name --- */`)
3. Prioritize readability over abstraction
4. Document environment variables inline where used
