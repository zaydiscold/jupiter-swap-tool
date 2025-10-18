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

## Notes
- Keep secrets isolated per chain (env files, `.gitignore` updates for `wallets/<chain>/*`).
- Document chain-specific quirks in `docs/<chain>.md` (e.g., gas tuning for BNB, Hyperliquid order types).
- Consider introducing TypeScript once the adapters stabilize—will make the shared interfaces safer.
- **Launcher cache considerations:** the CLI launcher now caches wallet guard summaries and wallet counts at startup using the environment flags `JUPITER_SWAP_TOOL_SKIP_INIT=1` and `JUPITER_SWAP_TOOL_NO_BANNER=1`. Use the refresh hotkey (or clear the cache in `run_cli_trader.command`) if behaviour seems stale, and roll back this change if future debugging requires the original eager RPC checks.
