# Jupiter Swap Tool CLI — Operator Manual (v1.2.1)

This document covers day-to-day usage of `cli_trader.js`, the launcher, and the surrounding tooling. For a higher-level project overview see the root [`README.md`](../README.md).

---

## 1. Overview

| Feature | Status | Notes |
| --- | --- | --- |
| Wallet registry with numbering / hierarchy | ✅ | Every keypair is auto-numbered; commands accept filenames *and* `#` references. |
| Ultra-first swaps with smart fallback | ✅ | Retries Ultra transient errors before falling back to Lite, then auto-returns to Ultra after success. |
| Jupiter Lend (Earn) integration | ✅ (beta) | Deposits, withdrawals, mint/redeem, positions & overview. Borrow remains “coming soon”. |
| Prewritten flows | ✅ | Buckshot, Long Circle, meme rotations, whale scripts. |
| Perps tooling | ✅ (beta) | Requires Anchor dependencies; see `docs/cli-commands.txt`. |
| Launcher (`run_cli_trader.command`) | ✅ | macOS wrapper with menus, hotkeys, and background guard refresh. |

---

## 2. Quickstart

```bash
cd jupiter-swap-project/jupiter-swap-tool/jupiter-swap-tool
npm install                 # install JS deps
node --check cli_trader.js  # optional syntax sanity check
```

Choose one:
- **macOS launcher**: double-click `run_cli_trader.command`.
- **Direct CLI**: `node cli_trader.js hotkeys` to browse commands, or invoke verbs directly (e.g. `node cli_trader.js buckshot`).

First-run checklist:
1. Pick an RPC (prompted in the launcher or via `RPC_URL` env). Add extras to `rpc_endpoints.txt` if you want round-robin rotation.
2. Generate wallets (`w → 2` in the launcher or `node cli_trader.js generate 5 crew`).
3. Fund wallets → run `balances` (refreshes the guard) → run the flow of your choice.

---

## 3. Wallet Registry & Hotkeys

Every wallet in `keypairs/` is assigned:
- `number`: sequential, starting at 1 (displayed as `#1`, `#2`, …).
- `role`: `master-master` (always wallet #1), `master` (every 5th wallet), or `slave`.
- `group`: cohorts of 5 wallets (1–5, 6–10, …).

**Numeric selectors work everywhere**:
```
node cli_trader.js aggregate 1
node cli_trader.js fund --from 6 --to 2 0.25
node cli_trader.js lend earn deposit #2 jlUSDC
```

### Launcher hotkeys (excerpt)

| Menu | Key(s) | Description |
| --- | --- | --- |
| Launcher | `w` / `1` | Wallet tools (balances, generate, import, list). |
| Launcher | `d` / `3` | Redistribute SOL evenly across wallets. |
| Launcher | `a` / `4` | Aggregate SOL back into wallet `#1`. |
| Launcher | `s` / `8` | Sweep all SPL balances back to SOL. |
| Launcher | `v` / `9` | Advanced trade tools (flows, buckshot, lend menu). |
| Wallet menu | `5` | `wallet list` — numbers, roles, SOL balance, guard status. |
| Wallet menu | `transfer` | Prompts for `from`, `to`, and amount (numbers or filenames). |
| Advanced | `1` | Target loop (interactive swaps). |
| Advanced | `7` | Titan whale flow. |
| Lend | `2` | Earn deposit (accepts wallet number and JL symbols). |

Hotkeys are generated dynamically; run `node cli_trader.js hotkeys --all` to see every context.

---

## 4. Swap Engine & Flow Commands

### Ultra / Lite behaviour
- Ultra is used by default (`JUPITER_SWAP_ENGINE=ultra`).
- On **order/execute errors** (401, 404, “missing payload”, “failed to get quotes”, etc.) we retry Ultra up to `MAX_ULTRA_FALLBACK_RETRIES` (default `3`) with exponential backoff based on `ULTRA_RETRY_BACKOFF_BASE_MS` (default `250 ms`).
- After a successful Lite swap we flip the engine back to Ultra automatically.
- RPC endpoints that rate-limit or auth-fail are placed in a cooldown set (`RPC_RATE_LIMIT_COOLDOWN_MS` default `20 s`, `RPC_GENERAL_COOLDOWN_MS` default `5 min`). Rotation skips unhealthy URLs until the timer expires.

### Core verbs

| Command | Summary |
| --- | --- |
| `swap <inMint> <outMint> [amount|all|random]` | One swap per active wallet; logs RPC, amount, outAmount, balance deltas. |
| `swap-all <inMint> <outMint>` | Shorthand for `swap … all`. |
| `swap-sol-to <mint>` | Convenience SOL→mint wrapper. |
| `buckshot` | Evenly spreads SOL across the long-circle catalog, keeps interactive prompts for rotations. |
| `long-circle [extra|primary-only]` | Extended multi-hop route across curated `swappable` tokens with deterministic RNG per wallet. |
| `sweep-to-btc-eth` | Sweep to SOL, then split into wBTC / cbBTC / wETH (weights derived per wallet). |
| `sol-usdc-popcat` | Two-hop chain used by memes (SOL→USDC→POPCAT). |
| `reclaim-sol` | Closes zero-balance ATAs in batches of 12 instructions per transaction before falling back to single closes. |

Every swap-compatible command honours:
- Wallet guard (disabled below 0.005 SOL).
- Wallet skip registry (avoids repeating failing tokens).
- Numeric selectors.

---

## 5. Funding & Aggregation

| Command | Description |
| --- | --- |
| `fund-all <from> <lamportsEach>` | Send the same amount from a source wallet to every other wallet. |
| `redistribute <anchorWallet>` | Even out spendable SOL across wallets (keeps reserves). |
| `aggregate <#|file>` | Cascade SOL backwards toward the target wallet. |
| `aggregate-hierarchical` | Slaves → group master within each cohort. |
| `aggregate-masters` | Each group master (6, 11, 16, …) → master-master (#1). |
| `wallet transfer <from> <to> <amount>` | Manual SOL transfer (supports `all`, decimals, numbers). |

All funding helpers honour `GAS_RESERVE_LAMPORTS`, `MIN_TRANSFER_LAMPORTS`, and guard bypass (`crew_1.json` always enabled).

---

## 6. Jupiter Lend (Earn Beta)

| Command | Notes |
| --- | --- |
| `lend earn tokens --refresh` | List vaults (APR, supply, share mint). Cache refresh optional. |
| `lend earn deposit <wallets> <mint|*|jlSymbol> <amount|*>` | Deposits SOL/USDC/USDT/wBTC/cbBTC/wETH or JL tokens. `*` picks highest-yield asset per wallet. |
| `lend earn withdraw` / `mint` / `redeem` | Mirrors the API semantics. Withdraw/redeem auto-create destination ATAs and unwrap wSOL if needed. |
| `lend earn positions [wallets|*]` | Positions per wallet. |
| `lend earn earnings [wallets|*]` | Reported earnings per position. |
| `lend overview` | Aggregate positions/earnings for every discovered wallet. |

Borrow endpoints are disabled server-side; we log “coming soon” to avoid confusion.

---

## 7. Diagnostics & Utilities

| Command | Description |
| --- | --- |
| `tokens --refresh` | Refresh token catalog from Jupiter Tokens API v2 before printing. |
| `wallet-guard-status [--refresh] [--summary]` | Inspect guard state; `--refresh` triggers a balance sweep. |
| `test-rpcs [all|index|match|url]` | Probe RPC endpoints with health checks. |
| `test-ultra [inputMint] [outputMint] [amount] [--wallet <name>] [--submit]` | Dry-run or execute an Ultra order. |
| `hotkeys [--all|context...]` | Print launcher/test/advanced/lend hotkeys without opening the UI. |

Logs always include:
- Current RPC endpoint.
- Jupiter request IDs on success/failure.
- Retry reason (rate limit, RPC auth, simulation failure, etc.).

---

## 8. Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `RPC_URL` | Prompt | Default RPC endpoint. |
| `RPC_LIST_FILE` | `rpc_endpoints.txt` | Additional RPCs to rotate through. |
| `MAX_ULTRA_FALLBACK_RETRIES` | `3` | Ultra order/execute retries before falling back to Lite. |
| `ULTRA_RETRY_BACKOFF_BASE_MS` | `250` | Base delay for Ultra retry backoff. |
| `RPC_RATE_LIMIT_COOLDOWN_MS` | `20000` | Cooldown applied after an RPC 429. |
| `RPC_GENERAL_COOLDOWN_MS` | `300000` | Cooldown applied after other RPC failures. |
| `JUPITER_SOL_BUFFER_LAMPORTS` | `2_000_000` | SOL kept aside after SOL spends. |
| `JUPITER_SOL_MAX_RETRIES` | `3` | SOL reduction retries on “insufficient funds”. |
| `SLIPPAGE_BPS` | `20` | Base slippage for all swaps. |
| `MAX_SLIPPAGE_RETRIES` | `5` | Quote retries on slippage failure. |
| `MIN_SOL_PER_SWAP_LAMPORTS` | `10_000_000` | Minimum SOL to attempt SOL-based swap. |
| `PRINT_SECRET_KEYS` | `0` | Set to `1` to dump base58 keys on wallet generation. |
| `NO_COLOR` | `0` | Disable ANSI colours (useful in CI). |

See `docs/cli-commands.txt` for per-command flags and additional environment controls (perps, lend, flows).

---

## 9. Troubleshooting

| Symptom | Suggested Action |
| --- | --- |
| `Ultra API returned incomplete response; falling back to Lite API.` | Check `MAX_ULTRA_FALLBACK_RETRIES`/`ULTRA_RETRY_BACKOFF_BASE_MS`, ensure Ultra key is valid. Logs show each retry. |
| Endless `rate-limited during sendRawTransaction` on a single RPC | Add more endpoints to `rpc_endpoints.txt`; cooldowns already prevent hammering but more diversity helps. |
| Wallet skipped due to low SOL | Top up the wallet, run `balances` (refresh guard) or `wallet-guard-status --refresh`. |
| Lend withdraw fails | Ensure destination ATA exists; CLI creates it automatically but logs warn if rent is missing. |
| Perps command errors | Confirm Anchor CLI & dependencies are installed; see `docs/cli-commands.txt` for perps config. |

Use `npm test` before pushing changes—unit tests cover wallet registry, campaign planners, and ATA helpers.

---

## 10. Additional Resources

- [`docs/cli-commands.txt`](../docs/cli-commands.txt) — exhaustive command list, flags, and examples.
- [`docs/lend-api-reference.md`] (if present) — Jupiter’s official docs (cross-check earn endpoints).
- [`CLAUDE.md`](../CLAUDE.md) — AI assistant instructions (keep updated when behaviours change).
- [`ARCHITECTURE_PLAN.md`](../ARCHITECTURE_PLAN.md) — roadmap (module extraction, cross-chain adapters).

Happy trading! Keep logs, guard states, and manifest files under version control (where appropriate) so you can recover quickly if endpoints misbehave.
