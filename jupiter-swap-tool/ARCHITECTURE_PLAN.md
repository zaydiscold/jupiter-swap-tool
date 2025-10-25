# Multi-Chain Jupiter Swap Roadmap

This plan tracks the evolution of the Jupiter Swap Tool from the existing Solana-focused CLI (`cli_trader.js`) toward a modular, multi-chain automation platform. The current v1.2.1 codebase already separates several subsystems—wallet helpers, campaign planning, and the perps client—so the roadmap below builds on what ships today.

## Current implementation snapshot (v1.2.1)
- **CLI façade (`cli_trader.js`)** – houses command parsing, swap execution, Jupiter Lend integration, perps helpers, automation presets, and diagnostics (`test-rpcs`, `test-ultra`, etc.).
- **Shared wallet helpers (`shared/wallet_helpers.js`)** – filesystem-backed keypair discovery, ATA creation, and wrapped SOL utilities reused by both the CLI and perps client.
- **Campaign planner (`chains/solana/campaigns_runtime.js`)** – deterministic scheduler powering `campaign …` presets, wallet-seeded RNG, budget truncation, and token-tag filtering.
- **Perps module (`perps/`)** – REST client, cache helpers, and wrappers that expose Jupiter Perps accounts, markets, and transaction builders.
- **Token catalog (`token_catalog.json`)** – hydrated at startup (from the bundled cache or Jupiter Tokens API v2) and used by automation flows, lend wildcard expansion, and random mint selection.

These components already expose reusable primitives (e.g. `instantiateCampaignForWallets`, `ensureAtaForMint`) that future chain adapters can consume. The remaining work tracks moving Solana-specific orchestration into a dedicated adapter while standing up sibling adapters for other chains.

## Phase 1 — Adapter interface & shared services
1. Define a `ChainAdapter` contract (TypeScript-style pseudo-definition used for guidance):
   ```ts
   interface ChainAdapter {
     id: string;                    // e.g. "solana", "aster", "hyperliquid"
     description: string;           // human-readable label
     init(config: AdapterConfig): Promise<void>;
     listWallets(): Promise<WalletSummary[]>;
     showBalances(options?: BalanceOptions): Promise<void>;
     executeSwap(plan: SwapPlan): Promise<SwapResult[]>;
     runAutomationLoop(params: AutomationParams): Promise<void>;
     aggregate?(options?: AggregateOptions): Promise<void>;
     shutdown?(): Promise<void>;
   }
   ```
2. Build a shared services layer under `chains/common/` with:
   - `logger.js` for structured/colourised logging plus suppression timers (mirroring the existing `paint` helpers).
   - `scheduler.js` implementing async loops with jitter and cooldown controls, suitable for campaign presets or keeper daemons.
   - `planTypes.js` housing common data types used by adapters (`SwapPlan`, `AutomationParams`, error enums, etc.).
   - `tokenCatalog.js` wrapper that reads chain-specific catalogs from `token_catalogs/<chain>.json` with API refresh support.
3. Promote RNG helpers (`createDeterministicRng`, `walletSeededRng`) and reserve maths into reusable modules so adapters can share budgeting logic.

## Phase 2 — Solana adapter extraction
1. Carve Solana-specific logic out of `cli_trader.js` into `chains/solana/` modules:
   - `wallet.js`: wraps `shared/wallet_helpers.js`, adds guard-state handling, and surfaces swap session seeds.
   - `swap.js`: isolates Jupiter Ultra/Lite order execution plus SOL reserve maths.
   - `automation.js`: ports long-circle, sweep, and buckshot planners so they can run via the adapter without the CLI shell.
   - `perpsAdapter.js`: re-exports the existing perps helpers with adapter-friendly ergonomics.
2. Replace direct imports in `cli_trader.js` with adapter calls while keeping backwards-compatible commands.
3. Introduce a minimal `multi_trader.js` launcher that selects the Solana adapter by default but already honours a `--chain=solana` flag.

## Phase 3 — New chain adapters
1. **Hyperliquid (EVM):**
   - Vendor the official SDK into `chains/hyperliquid/sdk/`.
   - Implement `wallet.js` (private key handling, signer wrappers), `swap.js` (order translation), and `automation.js` (campaign-compatible loop) mirroring the Solana adapter surface.
   - Map Hyperliquid-specific error codes into the shared error taxonomy for consistent CLI reporting.
2. **Aster / BNB Chain:**
   - Mirror the Hyperliquid structure using BNB RPC tooling (BIP-44 derivation, gas estimation, transaction confirmation helpers).
   - Ensure adapters respect the shared planning primitives and token catalog loader.
3. Add `wallets/<chain>/` directories and make the launcher derive defaults per chain (e.g., RPC endpoints, wrap buffers, gas reserves).

## Phase 4 — Unified launcher & UX polish
1. Promote `multi_trader.js` to the default entry point:
   - Parse `--chain=<id>` and `--profile=<name>` arguments.
   - Load chain-specific config from `config/<chain>.json` (merging env vars and per-user overrides).
   - Expose shared commands (`balances`, `swap`, `run-loop`, `tokens`, `config`, `test-rpcs`, `test-ultra`) and delegate to the chosen adapter.
2. Ensure the ASCII banner prints once, followed by adapter-specific context (active RPC, catalog source, automation status).
3. Persist launcher state (wallet guard summaries, session seeds) per chain to avoid cross-chain contamination.

## Phase 5 — Cross-chain automation enhancements
1. Extend the shared scheduler to run multiple adapters concurrently so operators can orchestrate Solana and Hyperliquid strategies side by side.
2. Introduce an “interrupt & resume” workflow (pause automation, inject manual swaps, resume) using adapter-agnostic checkpoints.
3. Add structured telemetry (JSON logs, timing metrics) toggled via env vars (`JUPITER_SWAP_TOOL_TIMING`, adapter-specific flags).
4. Evaluate worker threads or lightweight workers for CPU-heavy planning (e.g., mint randomisation, amount shuffling) if profiling shows contention on multi-chain runs.

## Testing strategy
- **Unit tests:** scheduling utilities, token catalog loaders, and deterministic RNG functions (run via `npm test`).
- **Integration tests:**
  - Solana: reuse Jupiter simulation harness plus `test-ultra --submit` for live smoke tests.
  - Hyperliquid/Aster: build fixtures or sandbox credentials; provide mockable transport layers for CI.
- **CLI smoke tests:** `node multi_trader.js --chain=<id> balances` against devnet/testnet endpoints per chain.

## Documentation & observability commitments
- Update `README.md`, `PATCH_NOTES.txt`, and `changelogs/CHANGELOG.md` with every adapter milestone (summaries plus new env defaults).
- Keep per-chain quirks documented in `docs/<chain>.md` (e.g., Hyperliquid authentication, BNB gas tuning).
- Maintain sample configs (`perps_config.sample.json`, future `adapter_config.sample.json`) illustrating keeper-only modes, automation budgets, and staging endpoints.
- Preserve dry-run modes (`--dry-run`, `PERPS_DRY_RUN`, `--no-send`) across adapters so operators can audit payloads safely.

## Outstanding questions
- Confirm final REST paths and authentication expectations for future chains before hard-coding endpoints.
- Determine whether adapter orchestration warrants a TypeScript migration once interfaces stabilise.
- Decide how much of the legacy launcher UI should be retained vs. replaced by the unified `multi_trader.js` experience.
