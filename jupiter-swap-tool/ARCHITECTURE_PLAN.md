# Multi-Chain Jupiter Swap Roadmap

## Repository Layout
- Keep the existing `jupiter swap tool/` repo as the master workspace.
- Create new top-level directories for modular chain adapters:
  - `chains/solana/`
  - `chains/aster/`
  - `chains/hyperliquid/`
  - `chains/common/` (shared utilities: logging, scheduling, plan validation, volume tracking, error taxonomy).
- Move current Solana-specific logic from `cli_trader.js` into `chains/solana/` once the new structure is stable.
- Maintain a single launcher at the repo root (e.g., `multi_trader.js`) responsible for dispatching to the selected chain adapter.

## Implementation Phases

### Phase 1 — Scaffolding & Shared Interfaces
1. Define a `ChainAdapter` interface (TypeScript-style pseudo-definition for clarity):
   ```ts
   interface ChainAdapter {
     id: string;               // e.g., "solana", "aster", "hyperliquid"
     description: string;      // human-readable label
     init(config: AdapterConfig): Promise<void>;
     listWallets(): Promise<WalletSummary[]>;
     showBalances(options?: BalanceOptions): Promise<void>;
     executeSwap(plan: SwapPlan): Promise<SwapResult[]>;
     runAutomationLoop(params: AutomationParams): Promise<void>;
     aggregate?(options?: AggregateOptions): Promise<void>;
     shutdown?(): Promise<void>;
   }
   ```
2. Build `chains/common/` with:
   - `logger.js`: colorized logging, error throttling, structured summaries.
   - `scheduler.js`: reusable async loop with jitter, cooldown management, queue persistence.
   - `volumeTracker.js`: SOL/BNB/ETH notionals tracking per wallet + global totals.
   - `planTypes.js`: shared data types (`SwapPlan`, `AutomationParams`, error enums).
   - `tokenCatalog.js`: loader that reads chain-specific token lists from `token_catalogs/<chain>.json`.
3. Establish `token_catalogs/` with separate JSON files:
   - `solana.json`
   - `aster.json`
   - `hyperliquid.json`
   Each file lists mints/contracts, decimals, tags, and metadata relevant to that chain.

### Phase 2 — Fresh Chain Adapters
1. **Hyperliquid (EVM)**
   - Clone the provided Hyperliquid API repo into `chains/hyperliquid/sdk/`.
   - Implement wallet utilities (private key management, address derivation, signer wrappers) in `chains/hyperliquid/wallet.js`.
   - Build swap logic (`swap.js`) translating abstract `SwapPlan` steps into Hyperliquid API calls (orders/liquidity pools).
   - Implement `index.js` exporting the `HyperliquidAdapter` with full adherence to `ChainAdapter`.
   - Include robust error mapping (rate limits, auth failures) into the shared error taxonomy.
2. **Aster / BNB chain**
   - Clone the Aster API repo into `chains/aster/sdk/`.
   - Mirror the wallet/swap scaffolding described above, but using BNB/EVM tooling (BIP-44, RPC endpoints, gas estimation).
   - Implement `AsterAdapter` with automation-friendly methods.
3. Both adapters should:
   - Support wallet generation/import (JSON keystores or mnemonic-based) stored under `wallets/<chain>/`.
   - Provide balance queries using their RPCs.
   - Include “24h automation” loops leveraging `scheduler.js`.

### Phase 3 — Solana Migration
1. Carve existing Solana code into `chains/solana/`:
   - `wallet.js`: wraps `@solana/web3.js` Keypair utilities.
   - `swap.js`: Jupiter interaction logic.
   - `automation.js`: port the long-run loop, reusing shared scheduler & volume tracker.
2. Replace `cli_trader.js` with a thin compatibility layer that proxies Solana commands to the new adapter while the multi-chain launcher matures.
3. Update token metadata loading to use `token_catalogs/solana.json`.

### Phase 4 — Unified Launcher & CLI
1. Create `multi_trader.js` at repo root:
   - Parses `--chain=<id>` argument (default `solana`).
   - Loads the corresponding adapter (`import { HyperliquidAdapter } from './chains/hyperliquid';` etc.).
   - Presents shared CLI commands (`balances`, `swap`, `run-loop`, `tokens`, `config`).
   - Delegates execution to adapter methods.
2. Provide a configuration file per chain (`config/solana.json`, `config/aster.json`, `config/hyperliquid.json`) and a loader that merges env vars with defaults.
3. Ensure the launcher prints the ASCII banner once, and each adapter can output chain-specific info beneath it.

### Phase 5 — Cross-Chain Automation Enhancements
1. Extend the shared scheduler to support running multiple adapters concurrently (if desired) via separate queues.
2. Implement the “interrupt & resume” user flow (custom token injection, wait for manual sell) in the shared automation controller, but ensure adapter-specific swap logic handles the actual trades.
3. Add observability hooks (structured logs, optional JSON output) so you can monitor each chain’s automation from external dashboards.

## Development Workflow
- Start in the existing project repo. Create the new directories inside it (`chains/`, `token_catalogs/`, etc.). No need for a separate master folder unless you want a clean history.
- For each chain:
  1. Pull the official API repo into the respective `sdk/` subfolder.
  2. Wrap the SDK with thin service modules (`wallet.js`, `swap.js`, `balances.js`).
  3. Hook the service modules into the adapter implementation (`index.js`).
- Use feature branches per chain (`feature/hyperliquid-adapter`, `feature/aster-adapter`, etc.) for clarity.
- Write lightweight smoke scripts in each adapter (`scripts/dev-swap.js`) to test wallet actions independently before integrating with the launcher.

## Testing Strategy
- Shared unit tests for scheduler, volume tracker, and token catalogs (run with `npm test` once proper tests are set up).
- Chain-specific integration tests:
  - Solana: reuse existing Jupiter simulation harness.
  - Hyperliquid/Aster: build mock responses or sandbox credentials if available.
- CLI smoke tests: run `node multi_trader.js --chain=<id> balances` against dev/test endpoints.

## Milestones & Validation
1. **M1:** Shared scaffolding + Hyperliquid adapter prototype that lists wallets, prints balances.
2. **M2:** Aster adapter parity with Hyperliquid (swaps + automation loop demo).
3. **M3:** Solana code migrated into adapter; legacy CLI proxies to new structure.
4. **M4:** Unified launcher operational with all three chains selectable.
5. **M5:** Interrupt/resume automation feature available for each adapter.

## Execution Pipeline Modernisation (In Progress)
The Solana-only CLI is currently rate-limited almost entirely by RPC calls; however, many flows still interleave planning, logging, and networking step-by-step. We are prioritising a refactor that front-loads deterministic work and opens the door for parallel CPU-bound orchestration.

### Jupiter Ultra Swap Migration
- Swaps now default to the Ultra order/execute flow (`https://api.jup.ag/ultra/<key>`). Provide `JUPITER_ULTRA_API_KEY` (defaults to `91233f8d-d064-48c7-a97a-87b5d4d8a511`) to target your own key-scoped endpoint; set `JUPITER_SWAP_ENGINE=lite` to fall back to the legacy quote/swap flow. Ultra prints the client order id, expected out amount, execution signature, and any error payloads for observability.
- Holding, shield, and router endpoints are available for future guardrails (token warnings, transfer pre-checks, etc.)—integrate as the UI requirements solidify.
- Keep the order/execute helpers side-effect free so future adapters or services (automation bots) can reuse them without the CLI wrapper.

### Targeted Command Set
- **Buckshot mode** — recently converted to pre-plan per wallet before firing swaps. Remaining work includes shared planning helpers and parallel preflight validation.
- **Long circle (`long-circle`)** — heavy segment randomisation and gas estimation can be performed ahead of RPC usage, allowing for staged execution batches per wallet.
- **BTC/ETH sweep (`sweep-to-btc-eth`)** — wallet scanning, allocation weighting, and reserve maths can be completed in a single pass prior to sending swaps.

### Roadmap
1. **Telemetry & Guard Rails**
   - Add lightweight timers around planning vs execution to confirm gains.
   - Surface counters in verbose mode so operators can confirm batching behaviour.
2. **Shared Planning Engine**
   - Extract planning primitives (token allocation, segment shuffle, reserve maths) into pure functions under `lib/planning/`.
   - Introduce an execution context object that carries pre-computed steps into the RPC layer.
3. **Buckshot Batch Enhancements**
   - Reuse wallet plans for rotation prompts.
   - Parallelise catalog lookups and reserve calculations with `Promise.all` where safe.
4. **Long Circle Refactor**
   - Precompute per-wallet segment lists, gas estimates, and amount strategies before any RPC calls.
   - Batch print summaries and allow optional dry-run output for review before execution.
5. **Sweep-to-btc-eth Optimisation**
   - Separate token discovery from swap execution so wallet scans hit RPC once.
   - Support deterministic vs random allocation strategies via pre-built weight maps.
6. **Worker Thread Exploration**
   - Investigate using Node worker threads for CPU-heavy shuffles and amount randomisation if profiling shows contention.
7. **Roll-Out & Testing**
   - Introduce feature flags (env-driven) to toggle new planners.
   - Backfill unit tests for planning helpers and add smoke tests for each mode.

### Notes
- Planning layers must remain side-effect free so that replays (dry runs or retries) do not diverge.
- Keep RPC mutations behind a single executor interface to ensure future multi-chain adapters can reuse the pipeline.
- Document each migration in `PATCH_NOTES.txt` and the README “Workstreams” section for operator visibility.

## Jupiter Lend Integration (Design)
We are preparing a dedicated Lend subsystem so the CLI can supply liquidity (“Earn”) and manage collateralised borrowing (“Borrow”) via Jupiter’s APIs.

### Scoping
- **Earn flows:** deposit, withdraw, mint shares, redeem shares for supported pools. Requires integration with `/lend/earn/*` endpoints plus token metadata and health checks. The CLI’s wildcard inputs (`*`) should auto-fan-out across wallets, filter to approved base/share tokens, and compute spendable amounts (native SOL keeps a rent/fee reserve and handles ATA creation costs).
- **Borrow flows:** open position (deposit collateral + borrow), adjust collateral, repay, close. Depends on forthcoming `/lend/borrow/*` endpoints (TBD); design must allow us to plug them in once public.
- **Shared services:** token discovery (Tokens API v2) and price feeds (Price API v3) to evaluate LTV, health factor, and earnings projections.

### Architecture
1. **Service Layer**
   - `services/lend/earnClient.js`: wraps REST calls for deposit/withdraw/mint/redeem plus tokens/positions/earnings. Accepts custom base URL via `JUPITER_LEND_API_BASE`.
   - `services/lend/positionClient.js`: placeholder for borrow-oriented endpoints; should share HTTP transport with earn client.
   - `services/pricing/priceClient.js`: migrates existing price lookups to `https://api.jup.ag/price/v3/price`.
   - `services/tokens/tokenDirectory.js`: switches catalog fetching to Tokens API v2 (search/tag/category/recent) with caching + fallback to existing token catalog.
2. **Planning Layer**
   - `planning/lend/earnPlanner.js`: pre-computes deposit/withdraw batches per wallet (taking ATA requirements, rate info, and min-withdraw windows into account).
   - `planning/lend/borrowPlanner.js`: computes collateral requirements, projected LTV, and liquidation thresholds before hitting RPC.
3. **CLI Commands**
   - `lend earn tokens` — list supported pools (from `/lend/earn/tokens`).
   - `lend earn deposit <mint> <amount>` / `lend earn withdraw <mint> <amount>` etc.
   - `lend borrow open <collateralMint> <borrowMint> <amount>` and supporting adjust/repay/close commands (wired once borrow endpoints ship).
   - All commands must display risk disclosures (smart contract risk, market risk, withdrawal throttles, liquidation risk) upfront and require confirmation.
4. **Configuration**
   - New env vars: `JUPITER_LEND_API_BASE`, `JUPITER_LEND_DEFAULT_PAIR`, `JUPITER_LEND_CONFIRM=1` for non-interactive confirmation, optional `JUPITER_PRICE_API_BASE`, `JUPITER_TOKENS_API_BASE`.
   - Extend launcher hotkey menu with a “Lend” subsection, mirroring the advanced menu.
5. **Telemetry & Safety**
   - Reuse timing helpers to profile planning vs execution.
   - Log API request IDs for support.
   - Detect and respect dynamic withdrawal limits exposed by the API; retries should obey cooldowns.
6. **Testing & Staging**
   - Mock clients around fetch so unit tests can replay API fixtures without live network.
   - Provide integration script (`scripts/dev-lend.js`) for smoke tests against devnet/mainnet with feature flags.

### Outstanding Questions
- Confirm final REST paths (`/lend/earn/...`) and authentication (if any) once documentation solidifies.
- Clarify borrow endpoint shape before implementing borrowing planner.
- Determine whether share minting requires additional ATA handling beyond standard SPL logic.

## Notes
- Keep secrets isolated per chain (env files, `.gitignore` updates for `wallets/<chain>/*`).
- Document chain-specific quirks in `docs/<chain>.md` (e.g., gas tuning for BNB, Hyperliquid order types).
- Consider introducing TypeScript once the adapters stabilize—will make the shared interfaces safer.
- **Launcher cache considerations:** the CLI launcher now caches wallet guard summaries and wallet counts at startup using the environment flags `JUPITER_SWAP_TOOL_SKIP_INIT=1` and `JUPITER_SWAP_TOOL_NO_BANNER=1`. Use the refresh hotkey (or clear the cache in `run_cli_trader.command`) if behaviour seems stale, and roll back this change if future debugging requires the original eager RPC checks.
