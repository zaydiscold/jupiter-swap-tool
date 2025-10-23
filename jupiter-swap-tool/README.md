# Jupiter Swap Tool CLI

Node-based operations console for orchestrating batches of Solana wallets, swapping through Jupiter, experimenting with Jupiter Lend, and exercising the beta perpetuals API. The repo ships with a single entry point (`cli_trader.js`) plus helper modules in `perps/`, `shared/`, and `chains/` that back specialised flows like campaign scheduling and keeper-style order handling.

## Getting started
1. Install Node.js 18 or newer and the Anchor CLI dependencies required by Jupiter Perps (for example `cargo install --git https://github.com/coral-xyz/anchor anchor-cli`).
2. Clone this repository and run `npm install` inside this directory to pull JavaScript dependencies.
3. Launch the CLI with `node cli_trader.js` (or run the provided `run_cli_trader.command` on macOS).
4. Supply an RPC URL when prompted (press **Enter** to accept the default mainnet endpoint).
5. Use the launcher hotkeys to generate wallets, import existing keypairs, configure funding, and run swap, lend, perps, or automation routines. The `keypairs/` folder is created automatically and is already ignored by git.
6. Run `node cli_trader.js --help` to print the latest command summary or `node cli_trader.js --version` to confirm the installed build.

**Launcher hint:** the interactive menu mirrors the commands listed below. Hotkeys are grouped by category (wallet tools, funding, swaps, automation presets, diagnostics). Press `0` to exit, or type the command names directly for scripting.

## Repository layout snapshot
- `cli_trader.js` – primary CLI with command parsing, swap execution, Jupiter Lend helpers, perps integration, and automation flows.
- `perps/` – thin client around Jupiter's Perps REST endpoints plus caching helpers used by the CLI.
- `shared/wallet_helpers.js` – filesystem backed wallet discovery, ATA creation utilities, and wrapped-SOL helpers reused by the CLI and perps module.
- `chains/solana/campaigns_runtime.js` – deterministic planning and scheduling logic for long-running campaign presets (`campaign …`).
- `perps_config.sample.json` – annotated template for configuring keeper-style accounts and market preferences.

## Environment variables
All knobs are optional; unset variables fall back to the defaults below. Grouped tables match the major subsystems surfaced by the CLI.

### Core connectivity
| Variable | Description | Default |
| --- | --- | --- |
| `RPC_URL` | RPC endpoint used at startup (also exposed in the launcher prompt). | `https://api.mainnet-beta.solana.com` |
| `RPC_LIST_FILE` | Path to newline/CSV-separated RPC endpoints used for rotation & failover. | `./rpc_endpoints.txt` next to the CLI |
| `RPC_HEALTH_URL` / `RPC_HEALTH_ENDPOINT` | Override the first health probe instead of sampling from the rotation list. | *(unset)* |
| `RPC_HEALTH_INDEX` | 1-based index into the rotation list used for the first probe. | *(unset)* |
| `JUP_HTTP_TIMEOUT_MS` | HTTP timeout applied to all Jupiter REST calls. | `15000` |
| `BALANCE_RPC_DELAY_MS` | Delay between wallet balance RPC calls when scanning wallets. | `250` |
| `NO_COLOR` | Disable colourised console output when set to `1`. | `0` |
| `NODE_NO_WARNINGS` | Suppress Node.js deprecation noise (set automatically by the launcher). | `1` when launched via script |

### Swap engine & automation guardrails
| Variable | Description | Default |
| --- | --- | --- |
| `SWAP_AMOUNT_MODE` | Session default for swap amount (`all` or `random`). | `all` |
| `SLIPPAGE_BPS` | Base slippage tolerance applied to swaps. | `20` |
| `MAX_SLIPPAGE_RETRIES` | Quote refresh attempts when Jupiter reports a slippage failure. | `5` |
| `JUPITER_SWAP_ENGINE` | Swap backend selector (`ultra` or `lite`). | `ultra` |
| `JUPITER_SWAP_API_BASE` | Base URL for the legacy Lite quote/swap endpoints. | `https://lite-api.jup.ag` |
| `JUPITER_ULTRA_API_KEY` | Jupiter Ultra API key used for `/order` + `/execute`. | `91233f8d-d064-48c7-a97a-87b5d4d8a511` |
| `JUPITER_ULTRA_API_BASE` | Override Ultra base URL (`…/ultra/<key>` by default, `…/ultra/v1` when no key is set). | Derived from `JUPITER_ULTRA_API_KEY` |
| `JUPITER_SOL_BUFFER_LAMPORTS` | Lamports left behind when spending SOL to cover wrap rent & route fees. | `2_000_000` |
| `JUPITER_SOL_RETRY_DELTA_LAMPORTS` | Lamports trimmed after each "insufficient lamports" response. | `200_000` |
| `JUPITER_SOL_MAX_RETRIES` | Maximum retries before abandoning a wallet after lamport failures. | `3` |
| `MIN_SOL_PER_SWAP_LAMPORTS` | Minimum SOL balance required to attempt SOL-based swaps. | `10_000_000` (≈0.01 SOL) |
| `ESTIMATED_GAS_PER_SWAP_LAMPORTS` | Gas estimate used by planners and wrap helpers. | `5_000` |
| `ESTIMATED_ATA_CREATION_LAMPORTS` | Rent+fee estimate for creating ATAs during planning. | `2_000_000` |
| `GENERAL_SIMULATION_RETRY_LIMIT` | Simulation retries before giving up on an instruction bundle. | `3` |
| `GENERAL_SIMULATION_REDUCTION_BPS` | Percentage trimmed from the amount after each failed simulation. | `2500` (25 %) |
| `PASSIVE_STEP_DELAY_MS` | Base delay between passive automation swaps. | `1500` |
| `PASSIVE_STEP_DELAY_JITTER_MS` | Random jitter added to passive automation delays. | `800` |
| `JUPITER_SWAP_TOOL_TIMING` | Set to `1` to print planning vs execution timing for heavy flows. | `0` |
| `JUPITER_SWAP_TOOL_SESSION_SEED` / `JUPITER_SWAP_TOOL_RANDOM_SEED` | Override the deterministic RNG namespace used for automation randomness. | `"jupiter-swap-tool-session"` |

### Wallet funding & reserves
| Variable | Description | Default |
| --- | --- | --- |
| `PRINT_SECRET_KEYS` | Emit base58 secrets when generating wallets. | `0` |
| `GAS_RESERVE_LAMPORTS` | Minimum lamports reserved per wallet before redistribution. | `1_000_000` |
| `MIN_TRANSFER_LAMPORTS` | Smallest lamport transfer allowed during redistribution/aggregation. | `50_000` |
| `LEND_SOL_WRAP_BUFFER_LAMPORTS` | Buffer left as wSOL when lend flows wrap SOL. | `2_200_000` |
| `MIN_LEND_SOL_DEPOSIT_LAMPORTS` | SOL headroom required before attempting lend deposits. | `5_000_000` |
| `LEND_SOL_BASE_PERCENT` | Initial percentage of spendable SOL targeted for lend deposits. | `7000` (70 %) |
| `LEND_SOL_RETRY_DECREMENT_PERCENT` | Percentage trimmed from the lend deposit target on retries. | `1500` (15 %) |
| `LEND_SOL_MIN_PERCENT` | Floor percentage of spendable SOL considered for lend deposits. | `2000` (20 %) |

### Jupiter Lend & market data
| Variable | Description | Default |
| --- | --- | --- |
| `JUPITER_LEND_API_BASE` | Base URL for Lend Earn endpoints. | `https://lite-api.jup.ag/lend/v1/earn` |
| `JUPITER_LEND_BORROW_API_BASE` | Base URL for Lend Borrow endpoints. | `https://lite-api.jup.ag/lend/v1/borrow` |
| `JUPITER_PRICE_API_BASE` | Base URL for Jupiter Price API v3 queries. | `https://api.jup.ag/price/v3` |
| `JUPITER_TOKENS_API_BASE` | Base URL for Jupiter Tokens API v2 searches. | `https://api.jup.ag/tokens/v2` |

### Perpetuals (beta)
| Variable | Description | Default |
| --- | --- | --- |
| `PERPS_CONFIG_PATH` | Path to the perps configuration JSON consumed by CLI helpers. | `./perps_config.json` |
| `PERPS_MARKET_CACHE_PATH` | Location for cached market metadata. | `perps/market_cache.json` |
| `PERPS_COMPUTE_UNIT_LIMIT` | Compute budget override applied to perps transactions. | `1_200_000` |
| `PERPS_COMPUTE_UNIT_PRICE_MICROLAMPORTS` | Compute price override for perps transactions. | `10_000` |
| `PERPS_KEEPER_ONLY` | When `1`, skip direct order placement and only service keeper tasks. | `0` |
| `PERPS_DRY_RUN` | When `1`, print perps payloads without sending transactions. | `0` |
| `JUPITER_PERPS_API_BASE` | Base URL for the Jupiter Perps REST gateway. | `https://lite-api.jup.ag/perps/v1` |

### Diagnostics & UX
| Variable | Description | Default |
| --- | --- | --- |
| `JUPITER_SWAP_TOOL_VERBOSE_ERRORS` | Emit expanded stack traces for caught errors. | `0` |
| `JUPITER_SWAP_TOOL_ERROR_SUPPRESSION_MS` | Coalesce repeated error logs within the configured window. | `0` |
| `JUPITER_SWAP_TOOL_LAUNCHER_GUARD_MAX_AGE_MS` | Expire cached wallet guard snapshots after this many milliseconds. | `0` (refresh every run) |
| `JUPITER_SWAP_TOOL_SKIP_INIT` | Skip initial RPC guard refresh during launcher startup. | `0` |
| `JUPITER_SWAP_TOOL_NO_BANNER` / `JUPITER_NO_BANNER` | Suppress the ASCII art banner at startup. | `0` |

## CLI quick reference (v1.1.2)
Commands can be invoked directly via `node cli_trader.js <command …>` or through the interactive launcher hotkeys. Amount arguments accept decimals (auto-converted to base units) unless the `--raw` flag is present.

### Wallet & key management
- `generate <count> [prefix]` – create numbered keypairs (e.g. `prefix_1.json`).
- `import-wallet --secret <secret> [--prefix name] [--path path] [--force]` – import base58, JSON, or mnemonic wallets (default path `m/44'/501'/0'/0'`).
- `list` – enumerate wallet filenames, public keys, and creation timestamps.
- `wallet wrap <wallet> [amount|all] [--raw]` – wrap SOL into wSOL while respecting fee buffers.
- `wallet unwrap <wallet> [amount|all] [--raw]` – close the wSOL ATA (auto re-wraps leftovers above the configured buffer).

### Balances, funding, and redistribution
- `balances [mint[:symbol] …]` – show SOL plus SPL balances; always prints the default sweep list even when zero. Running this command refreshes the automatic wallet guard (wallets under 0.01 SOL are paused until refilled).
- `wallet-guard-status [--summary] [--refresh]` – inspect or refresh the guard cache; `--summary` emits a terse line for scripting.
- `force-reset-wallets` – temporarily re-enable every wallet until the next `balances` run recomputes guard status.
- `fund-all <fromWallet> <lamportsEach>` – pay the same lamport amount from one wallet to every other wallet.
- `redistribute <anchorWallet>` – even out SOL balances across all wallets while preserving gas reserves and skipping dust transfers.
- `fund <from> <to> <lamports>` / `send <from> <to> <lamports>` – direct SOL transfers between two wallets.
- `aggregate <targetWallet>` – sweep SOL back into a single wallet (respecting reserves and dust thresholds).
- `airdrop <wallet> <lamports>` / `airdrop-all <lamports>` – devnet-only SOL faucets for one or all wallets.

### Token discovery & swapping
- `tokens [--verbose] [--refresh]` – print the current token catalog; `--refresh` pulls from Jupiter Tokens API v2 before printing.
- `swap <inputMint> <outputMint> [amount|all|random]` – execute swaps across every active wallet.
- `swap-all <inputMint> <outputMint>` – shortcut for swapping the entire balance from one mint into another.
- `swap-sol-to <mint> [amount|all|random]` – convenience SOL → token route.
- `buckshot` – split spendable SOL evenly across the long-circle rotation, then prompt for new target mints to recycle positions.
- `sol-usdc-popcat` – deterministic SOL→USDC→POPCAT lap across all wallets.
- `target-loop [startMint]` – interactive loop that repeatedly swaps into operator-specified mints, ideal for guided rotations.
- `reclaim-sol` / `close-token-accounts` – close empty SPL & Token-2022 ATAs to recover rent (skips accounts with withheld fees).

### Automation presets & campaigns
- `long-circle [extra]` – run the multi-hop long circle route; `extra` appends the secondary randomised SOL-out segment when available.
- `crew1-cycle` – three lap SOL↔USDC↔meme cycles focused on `crew_1.json`, with minute-scale waits between legs.
- `sweep-defaults` – convert the default sweep list back into SOL.
- `sweep-all` – enumerate every token balance and sweep everything into SOL.
- `sweep-to-btc-eth` – sweep non-SOL holdings into SOL, then spread the result across wBTC, cbBTC, and wETH using per-wallet weights.
- `arpeggio`, `icarus`, `zenith`, `aurora` – run the prewritten flow presets (≈15 min/≈15 min randomised, ≈60 min, and ≈6 hr cadences respectively). Icarus/Zenith/Aurora draw random catalog tokens each hop.
- `campaign <meme-carousel|scatter-then-converge|btc-eth-circuit|icarus|zenith|aurora> <30m|1h|2h|6h> [--batch <1|2|all>] [--dry-run]` – instantiate deterministic campaign schedules built by `chains/solana/campaigns_runtime.js`. `--batch` restricts execution to alternating wallet sets; `--dry-run` prints plans without firing swaps.

### Jupiter Lend (Earn & Borrow)
- `lend overview` – aggregate Earn balances, borrow positions, and realised earnings for every discovered wallet.
- `lend earn tokens` – list supported Earn pools from Jupiter.
- `lend earn positions` / `lend earn earnings` – inspect active deposits and cumulative rewards per wallet.
- `lend earn deposit <wallet|*> <mint|*> <amount|*> [--no-send]` – deposit into Earn pools, auto-creating ATAs, wrapping SOL, and tapering amounts when SOL is scarce. Use `*` to fan across eligible wallets/mints and `--no-send` to dry run.
- `lend earn withdraw|mint|redeem …` – mirror deposit behaviour for withdrawals, share minting, and share redemption.
- `lend borrow open <wallet> <collateralMint> <borrowMint> <amount> [options]` – open a borrow position, printing health metrics before submission.
- `lend borrow adjust|repay|close …` – manage borrow positions; passing `*` for the amount or IDs fans across detected exposures.

### Jupiter Perps (beta)
- `perps markets [--group <group>]` – list available markets for the requested group (defaults to `mainnet-beta`).
- `perps positions <wallet|pubkey|*> [--market <symbol>] [--all]` – fetch open positions and margin health for specific wallets or every configured wallet.
- `perps open <wallet> <market> <long|short> <size> [price] [--leverage <x>] [--reduce-only] [--tag <label>] […]` – submit new orders; computes keeper parameters, enforces config-defined margins, and prints payloads before dispatch.
- `perps close <wallet> [market] [positionId] [size] [--close-all] [--reduce-only] [--dry-run]` – reduce or fully close exposures; dry runs print payloads without broadcasting.

### Diagnostics & health checks
- `test-rpcs [all|index|match|url] [--swap --confirm] [--loops N] [--delay MS] [--amount SOL]` – measure RPC latency/version/health; optionally execute a SOL↔USDC stress loop once `--confirm` is supplied.
- `test-ultra [inputMint] [outputMint] [amount] [--wallet name] [--submit] [--slippage-bps N]` – dry run Jupiter Ultra order/execute; `--submit` signs with the selected wallet and broadcasts via Ultra before confirming on the active RPC.
- `balances` and `tokens` (above) double as quick sanity checks for wallet health and token metadata coverage.

## Campaign timing presets
Campaigns share the same duration keys but differ in swap counts, token pools, and checkpoint cadences. The planner seeds each wallet using `walletSeededRng` so reruns are reproducible per pubkey while still randomising mint order and wait windows.

| Campaign | Swap range (30 m / 1 h / 2 h / 6 h) | Notes |
| --- | --- | --- |
| `meme-carousel` | 20–60 / 60–120 / 140–260 / 300–600 | Meme-heavy rotation that periodically checkpoints into SOL. |
| `scatter-then-converge` | 20–60 / 60–120 / 140–260 / 300–600 | Fans out into tagged tokens before regrouping into SOL. |
| `btc-eth-circuit` | 15–45 / 40–100 / 120–220 / 260–520 | Focused on BTC/ETH/wrapped variants; preloads holdings metadata for sizing. |
| `icarus` | 24–64 / 60–140 / 140–320 / 360–720 | Fast-paced random rotations drawn from the long-circle pool. |
| `zenith` | 18–42 / 48–108 / 110–240 / 280–560 | Medium cadence mix of long-circle and secondary-pool tags. |
| `aurora` | 12–32 / 36–80 / 90–180 / 220–420 | Long-form randomised orbits favouring secondary pools. |

## Known mint cache (excerpt)
The CLI pre-bundles metadata for high-traffic routes so swaps can proceed even when the RPC throttles metadata fetches:
- Native SOL (`So11111111111111111111111111111111111111112`)
- USDC (`EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`)
- POPCAT (`7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr`)
- PUMP (`pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn`)
- PENGU, FART, USELESS, WIF, PFP, wBTC, cbBTC, wETH, and other long-circle staples (see `token_catalog.json`).

## Safety reminders
- **Perpetuals risk:** margin trading introduces leverage and liquidation risk. Always ensure the configured wallet has adequate USDC collateral before submitting perps orders.
- **Automation guard:** wallets under 0.01 SOL are automatically removed from swap flows until topped up; rerun `balances` once funded.
- **API stability:** Jupiter Lend and Perps endpoints are still evolving. Commands log request/response payloads so you can inspect failures and share request IDs with support.

## Troubleshooting tips
- Run `test-rpcs --swap --confirm` to benchmark RPC endpoints under load.
- Run `test-ultra --submit` with a small amount to confirm Ultra credentials end-to-end.
- Use `wallet-guard-status --summary --refresh` to script guard resets in automation.
- For verbose stack traces during debugging, export `JUPITER_SWAP_TOOL_VERBOSE_ERRORS=1`.

Refer to `PATCH_NOTES.txt` and `ARCHITECTURE_PLAN.md` for ongoing roadmap updates and refactor milestones.
