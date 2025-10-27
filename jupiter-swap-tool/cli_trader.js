#!/usr/bin/env node

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";
import bs58 from "bs58";
import readline from "readline";
import * as bip39 from "bip39";
import { derivePath } from "ed25519-hd-key";
import * as walletRegistry from "./shared/wallet_registry.js";
import {
  Connection,
  Keypair,
  PublicKey,
  VersionedTransaction,
  Transaction,
  SystemProgram,
  ComputeBudgetProgram,
  TransactionMessage,
} from "@solana/web3.js";
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
  getAssociatedTokenAddress,
  createAssociatedTokenAccountInstruction,
  getMint,
  MintLayout,
  createSyncNativeInstruction,
  closeAccount,
  createCloseAccountInstruction,
  createBurnInstruction,
  NATIVE_MINT,
} from "@solana/spl-token";
import {
  buildIncreaseRequestInstruction,
  buildDecreaseRequestInstruction,
  simulatePerpsInstructions,
  fetchPoolAccount,
  fetchCustodyAccounts,
  fetchPositionsForOwners,
  buildComputeBudgetInstructions,
  preparePreviewTransaction,
  resolveCustodyIdentifier,
  extractSideLabel,
  convertDbpsToHourlyRate,
  KNOWN_CUSTODIES,
  getPerpsProgram,
} from "./perps.js";
import {
  instantiateCampaignForWallets,
  executeTimedPlansAcrossWallets,
  registerHooks as registerCampaignHooks,
  truncatePlanToBudget,
  resolveScheduledLogicalStep,
  CAMPAIGNS,
  RANDOM_MINT_PLACEHOLDER,
  resolveRandomizedStep,
} from "./chains/solana/campaigns_runtime.js";
import {
  listWallets as sharedListWallets,
  ensureAtaForMint as sharedEnsureAtaForMint,
  ensureWrappedSolBalance as sharedEnsureWrappedSolBalance,
  loadKeypairFromFile as sharedLoadKeypairFromFile,
  configureWalletHelpers,
} from "./shared/wallet_helpers.js";

// --------------------------------------------------
// Jupiter Swap Tool CLI — maintained by @coldcooks (zayd)
// version 1.3.1
// --------------------------------------------------

const TOOL_VERSION = "1.3.1";
const GENERAL_USAGE_MESSAGE = `Commands: tokens [--verbose|--refresh] | lend earn ... | lend overview (borrow coming soon) | perps <markets|positions|open|close> [...options] | wallet <wrap|unwrap|list|info|sync|groups|transfer|fund|redistribute|aggregate> [...] | list | generate <n> [prefix] | import-wallet --secret <secret> [--prefix name] [--path path] [--force] | balances [tokenMint[:symbol] ...] | fund-all <from> <lamportsEach> | redistribute <wallet> | fund <from> <to> <lamports> | send <from> <to> <lamports> | aggregate <wallet> | aggregate-hierarchical | aggregate-masters | airdrop <wallet> <lamports> | airdrop-all <lamports> | campaign <meme-carousel|scatter-then-converge|btc-eth-circuit|icarus|zenith|aurora> <30m|1h|2h|6h> [--batch <1|2|all>] [--dry-run] | swap <inputMint> <outputMint> [amount|all|random] | swap-all <inputMint> <outputMint> | swap-sol-to <mint> [amount|all|random] | buckshot | wallet-guard-status [--summary|--refresh] | test-rpcs [all|index|match|url] | test-ultra [inputMint] [outputMint] [amount] [--wallet name] [--submit] | sol-usdc-popcat | long-circle | interval-cycle | crew1-cycle | arpeggio | horizon | echo | icarus | zenith | aurora | titan | odyssey | sovereign | nova | sweep-defaults | sweep-all | sweep-to-btc-eth | reclaim-sol | target-loop [startMint] | force-reset-wallets
See docs/cli-commands.txt for a detailed command reference.`;

function printGeneralUsage() {
  console.log(GENERAL_USAGE_MESSAGE);
}

function normalizeHotkeyEntry(entry) {
  const normalized = { ...entry };
  normalized.action = typeof entry.action === "string" ? entry.action : "";
  normalized.keys = Array.isArray(entry.keys)
    ? Object.freeze(entry.keys.map((key) => String(key).trim()).filter((key) => key.length > 0))
    : Object.freeze([]);
  normalized.displayKeys = Array.isArray(entry.displayKeys)
    ? Object.freeze(entry.displayKeys.map((key) => String(key).trim()).filter((key) => key.length > 0))
    : undefined;
  if (typeof entry.description !== "string" || entry.description.trim().length === 0) {
    normalized.description = "";
  } else {
    normalized.description = entry.description.trim();
  }
  normalized.inline = entry.inline === false ? false : true;
  normalized.hidden = entry.hidden === true;
  return Object.freeze(normalized);
}

function buildHotkeyMap(definitions) {
  const contexts = new Map();
  for (const [context, config] of definitions) {
    const entries = Array.isArray(config.entries)
      ? config.entries.map((entry) => normalizeHotkeyEntry(entry))
      : [];
    contexts.set(
      context,
      Object.freeze({
        id: context,
        label:
          typeof config.label === "string" && config.label.trim().length > 0
            ? config.label.trim()
            : context,
        entries: Object.freeze(entries),
      })
    );
  }
  return contexts;
}

const HOTKEY_MAP = buildHotkeyMap([
  [
    "launcher",
    {
      label: "Launcher hotkeys",
      entries: [
        {
          action: "wallet-tools",
          keys: ["w", "1"],
          description: "Open wallet tools (balances / generate / import / list)",
        },
        {
          action: "force-reset-guard",
          keys: ["g", "2", "forcereset", "force-reset", "reset"],
          description: "Force reset wallet guard (enable all wallets until next balance refresh)",
        },
        {
          action: "redistribute",
          keys: ["d", "3", "redistribute"],
          description: "Redistribute SOL from the crew wallet across all others (supports wallet numbers)",
        },
        {
          action: "aggregate",
          keys: ["a", "4", "aggregate"],
          description: "Aggregate SOL back into the crew wallet (e.g. 'aggregate 1')",
        },
        {
          action: "reclaim-sol",
          keys: ["c", "5", "reclaim", "close", "close-token-accounts", "reclaimsol"],
          description: "Reclaim SOL by closing empty token accounts",
        },
        {
          action: "swap-sol-usdc",
          keys: ["u", "6", "sol2usdc", "swap-sol-usdc"],
          description: "Swap SOL → USDC using the launcher's default amount mode",
        },
        {
          action: "buckshot",
          keys: ["b", "7", "buckshot"],
          description: "Start buckshot mode (spread + interactive token rotation)",
        },
        {
          action: "sweep-all",
          keys: ["s", "8", "sweep-all", "sweep", "sweepall"],
          description: "Sweep all token balances back to SOL",
        },
        {
          action: "test-menu",
          keys: ["t", "test", "tests"],
          description: "Open test utilities (RPC diagnostics / Ultra swap check)",
        },
        {
          action: "perps-menu",
          keys: ["p", "perps"],
          description: "Open Jupiter Perps trading menu",
        },
        {
          action: "advanced-menu",
          keys: ["v", "9", "advanced"],
          description: "Open advanced trade tools",
        },
        {
          action: "quit",
          keys: ["q", "0", "quit", "exit"],
          description: "Quit the launcher",
        },
      ],
    },
  ],
  [
    "wallet-menu",
    {
      label: "Wallet tools menu",
      entries: [
        { action: "show-balances", keys: ["1"], description: "Show balances" },
        { action: "generate-wallets", keys: ["2"], description: "Generate wallets" },
        {
          action: "import-secret",
          keys: ["3"],
          description: "Import secret key / JSON",
        },
        {
          action: "import-mnemonic",
          keys: ["4"],
          description: "Import mnemonic phrase",
        },
        {
          action: "list-wallets",
          keys: ["5"],
          description: "List wallet addresses (includes registry numbers)",
        },
        {
          action: "force-reset-guard",
          keys: ["6"],
          description: "Force reset wallet guard",
        },
        {
          action: "unwrap-wsol",
          keys: ["7"],
          description: "Unwrap wSOL → SOL",
        },
        {
          action: "wallet-fund",
          keys: ["8"],
          description: "Fund wallet (interactive)",
        },
        {
          action: "wallet-redistribute",
          keys: ["9"],
          description: "Redistribute SOL across wallets",
        },
        {
          action: "wallet-aggregate",
          keys: ["0"],
          description: "Aggregate SOL toward target wallet",
        },
        { action: "back", keys: ["b", "back"], description: "Back to launcher" },
      ],
    },
  ],
  [
    "rpc-tests-menu",
    {
      label: "RPC endpoint diagnostics menu",
      entries: [
        { action: "test-all", keys: ["1"], description: "Test all endpoints" },
        {
          action: "test-index",
          keys: ["2"],
          description: "Test by index (1-based)",
        },
        {
          action: "test-match",
          keys: ["3"],
          description: "Test by substring match",
        },
        {
          action: "test-url",
          keys: ["4"],
          description: "Test a custom URL",
        },
        {
          action: "swap-stress",
          keys: ["5"],
          description: "Swap stress test (requires confirmation)",
        },
        { action: "back", keys: ["b", "back"], description: "Back to test utilities" },
      ],
    },
  ],
  [
    "test-menu",
    {
      label: "Test utilities menu",
      entries: [
        {
          action: "rpc-tests",
          keys: ["1"],
          description: "RPC endpoint diagnostics",
        },
        {
          action: "ultra-swap-check",
          keys: ["2"],
          description: "Ultra API swap check",
        },
        { action: "back", keys: ["b", "back"], description: "Back to previous menu" },
      ],
    },
  ],
  [
    "lend-menu",
    {
      label: "Jupiter Lend menu",
      entries: [
        {
          action: "earn-tokens",
          keys: ["1"],
          description: "List earn tokens (refresh)",
        },
        {
          action: "earn-deposit",
          keys: ["2"],
          description: "Earn deposit",
        },
        {
          action: "earn-withdraw",
          keys: ["3"],
          description: "Earn withdraw",
        },
        {
          action: "earn-positions",
          keys: ["4"],
          description: "Earn positions",
        },
        {
          action: "earn-earnings",
          keys: ["5"],
          description: "Earn earnings",
        },
        {
          action: "overview",
          keys: ["6"],
          description: "Overview (earn all wallets)",
        },
        { action: "back", keys: ["b", "back"], description: "Back to advanced tools" },
      ],
    },
  ],
  [
    "flows-menu",
    {
      label: "Trading flows menu",
      entries: [
        {
          action: "arpeggio-flow",
          keys: ["1"],
          description: "Arpeggio (15min fast rotation)",
        },
        {
          action: "horizon-flow",
          keys: ["2"],
          description: "Horizon (60min mid-duration)",
        },
        {
          action: "echo-flow",
          keys: ["3"],
          description: "Echo (6hr extended loop)",
        },
        {
          action: "icarus-flow",
          keys: ["4"],
          description: "Icarus (high-tempo random meme)",
        },
        {
          action: "zenith-flow",
          keys: ["5"],
          description: "Zenith (mid-tempo random pools)",
        },
        {
          action: "aurora-flow",
          keys: ["6"],
          description: "Aurora (slow steady random)",
        },
        {
          action: "titan-flow",
          keys: ["7", "t"],
          description: "Titan (whale Icarus — 0.02 SOL min, 1min-10m holds)",
        },
        {
          action: "odyssey-flow",
          keys: ["8", "o"],
          description: "Odyssey (whale Zenith — 0.02 SOL min, 30s-10m holds)",
        },
        {
          action: "sovereign-flow",
          keys: ["9", "s"],
          description: "Sovereign (whale Aurora — 0.02 SOL min, 30s-10m holds)",
        },
        {
          action: "nova-flow",
          keys: ["0", "n"],
          description: "Nova (supernova Icarus — 0.01 SOL min, 30s-10m holds)",
        },
        { action: "back", keys: ["b", "back"], description: "Back to advanced tools" },
      ],
    },
  ],
  [
    "advanced-menu",
    {
      label: "Advanced trade tools menu",
      entries: [
        {
          action: "target-loop",
          keys: ["1"],
          description: "Target loop (paste mint, flatten with SOL, exit when done)",
        },
        {
          action: "long-circle",
          keys: ["2"],
          description: "Long circle swap",
        },
        {
          action: "test-menu",
          keys: ["3"],
          description: "Test utilities (RPC / Ultra)",
        },
        {
          action: "crew-cycle",
          keys: ["4"],
          description: "Interval cycle (all wallets)",
        },
        {
          action: "btc-eth-sweep",
          keys: ["5"],
          description: "Sweep balances into wBTC / cbBTC / wETH",
        },
        {
          action: "sol-usdc-popcat",
          keys: ["6"],
          description: "SOL → USDC → POPCAT lap",
        },
        {
          action: "lend-menu",
          keys: ["7"],
          description: "Jupiter Lend (earn — borrow coming soon)",
        },
        {
          action: "flows-menu",
          keys: ["8", "flows"],
          description: "Trading flows (Arpeggio, Horizon, Echo, Icarus, Zenith, Aurora, Titan, Odyssey, Sovereign, Nova)",
        },
        { action: "back", keys: ["b", "back"], description: "Back to launcher" },
      ],
    },
  ],
  [
    "target-loop",
    {
      label: "Target loop commands",
      entries: [
        {
          action: "rotate",
          displayKeys: ["<mint>"],
          description: "Paste a mint address to rotate holdings",
        },
        {
          action: "flatten-to-sol",
          keys: ["sol", "base"],
          description: "Swap current holdings back to SOL",
        },
        {
          action: "show-catalog",
          keys: ["list", "catalog", "tokens"],
          description: "Print the token catalog",
        },
        {
          action: "show-help",
          keys: ["help", "?"],
          description: "Reprint this help",
        },
        {
          action: "exit",
          keys: ["exit", "quit", "q"],
          description: "Exit target loop mode",
        },
      ],
    },
  ],
  [
    "buckshot-rotation",
    {
      label: "Buckshot rotation commands",
      entries: [
        {
          action: "rotate",
          displayKeys: ["<mint>"],
          description: "Paste a mint address to rotate held tokens",
        },
        {
          action: "exit",
          keys: ["exit", "quit", "q"],
          displayKeys: ["<enter>", "exit", "quit", "q"],
          description: "Exit buckshot rotation mode",
        },
      ],
    },
  ],
  [
    "perps-menu",
    {
      label: "Jupiter Perps trading menu",
      entries: [
        {
          action: "perps-view-markets",
          keys: ["1", "markets"],
          description: "View available markets (SOL-PERP, ETH-PERP, BTC-PERP, etc.)",
        },
        {
          action: "perps-view-positions",
          keys: ["2", "positions"],
          description: "View open positions (all wallets)",
        },
        {
          action: "perps-open-long",
          keys: ["3", "long"],
          description: "Open long position",
        },
        {
          action: "perps-open-short",
          keys: ["4", "short"],
          description: "Open short position",
        },
        {
          action: "perps-close-position",
          keys: ["5", "close"],
          description: "Close specific position",
        },
        {
          action: "perps-close-all",
          keys: ["6", "closeall"],
          description: "Close all positions (emergency exit)",
        },
        { action: "back", keys: ["b", "back"], description: "Back to launcher" },
      ],
    },
  ],
]);

const DEFAULT_HOTKEY_CONTEXTS = Object.freeze([
  "launcher",
  "wallet-menu",
  "advanced-menu",
  "test-menu",
  "rpc-tests-menu",
  "lend-menu",
  "flows-menu",
  "perps-menu",
  "target-loop",
  "buckshot-rotation",
]);

function findHotkeyContext(context) {
  if (!context) return null;
  return HOTKEY_MAP.get(context) || null;
}

function findHotkeyEntry(context, action) {
  const ctx = findHotkeyContext(context);
  if (!ctx || !ctx.entries) return null;
  if (!action) return null;
  return ctx.entries.find((entry) => entry.action === action) || null;
}

function collectHotkeyDisplayKeys(entry) {
  if (!entry) return [];
  const seen = new Set();
  const display = [];
  const addKey = (key) => {
    const trimmed = typeof key === "string" ? key.trim() : "";
    if (!trimmed) return;
    const lower = trimmed.toLowerCase();
    if (seen.has(lower)) return;
    seen.add(lower);
    display.push(trimmed);
  };
  if (Array.isArray(entry.displayKeys)) {
    for (const key of entry.displayKeys) addKey(key);
  }
  if (Array.isArray(entry.keys)) {
    for (const key of entry.keys) addKey(key);
  }
  return display;
}

function formatHotkeyToken(token) {
  if (typeof token !== "string" || token.length === 0) return "";
  if (token.startsWith("<") && token.endsWith(">")) {
    return token;
  }
  return `'${token}'`;
}

function formatHotkeyKeysFromEntry(entry, { joiner = " / " } = {}) {
  const keys = collectHotkeyDisplayKeys(entry);
  if (keys.length === 0) return "";
  return keys.map((key) => formatHotkeyToken(key)).join(joiner);
}

function formatHotkeyKeys(context, action, options = {}) {
  const entry = findHotkeyEntry(context, action);
  if (!entry) return "";
  return formatHotkeyKeysFromEntry(entry, options);
}

function formatHotkeyPrimaryKey(context, action) {
  const entry = findHotkeyEntry(context, action);
  if (!entry) return "";
  const keys = collectHotkeyDisplayKeys(entry);
  if (keys.length === 0) return "";
  return formatHotkeyToken(keys[0]);
}

function isHotkeyMatch(context, action, value) {
  if (!value) return false;
  const entry = findHotkeyEntry(context, action);
  if (!entry || !Array.isArray(entry.keys) || entry.keys.length === 0) return false;
  const normalizedValue = String(value).trim().toLowerCase();
  if (!normalizedValue) return false;
  for (const key of entry.keys) {
    const normalizedKey = String(key).trim().toLowerCase();
    if (normalizedKey && normalizedKey === normalizedValue) {
      return true;
    }
  }
  return false;
}

function buildHotkeyInlineSummary(context) {
  const ctx = findHotkeyContext(context);
  if (!ctx) return "";
  const parts = [];
  for (const entry of ctx.entries) {
    if (entry.hidden) continue;
    if (entry.inline === false) continue;
    const keysLabel = formatHotkeyKeysFromEntry(entry);
    if (!keysLabel) continue;
    const description = entry.description || "";
    if (!description) continue;
    parts.push(`${keysLabel} → ${description}`);
  }
  return parts.join("; ");
}

function buildHotkeyLines(context, { indent = "", includeTitle = true } = {}) {
  const ctx = findHotkeyContext(context);
  if (!ctx) return [];
  const visibleEntries = ctx.entries.filter((entry) => !entry.hidden);
  const keyLabels = visibleEntries.map((entry) => formatHotkeyKeysFromEntry(entry));
  const keyWidth = keyLabels.reduce((width, label) => Math.max(width, label.length), 0);
  const lines = [];
  const prefix = indent ?? "";
  if (includeTitle && ctx.label) {
    lines.push(`${prefix}${ctx.label}:`);
  }
  const entryIndent = `${prefix}${includeTitle && ctx.label ? "  " : ""}`;
  for (let i = 0; i < visibleEntries.length; i += 1) {
    const entry = visibleEntries[i];
    const label = keyLabels[i];
    const paddedLabel = label.padEnd(keyWidth, " ");
    lines.push(`${entryIndent}${paddedLabel}  ${entry.description}`.trimEnd());
  }
  return lines;
}

function listHotkeyContexts() {
  return Array.from(HOTKEY_MAP.keys()).sort();
}

function renderHotkeyTable(contexts, { indent = "", includeTitle = true } = {}) {
  const lines = [];
  let first = true;
  for (const context of contexts) {
    const block = buildHotkeyLines(context, { indent, includeTitle });
    if (!block || block.length === 0) continue;
    if (!first) {
      lines.push("");
    }
    lines.push(...block);
    if (context === "wallet-menu") {
      const ctx = findHotkeyContext(context);
      const noteIndent = `${indent}${includeTitle && ctx?.label ? "  " : ""}`;
      lines.push(`${noteIndent}Tip: wallet commands accept registry numbers, for example:`);
      lines.push(`${noteIndent}  • aggregate 1 (or aggregate #1)`);
      lines.push(`${noteIndent}  • fund --from 6 --to 2`);
      lines.push(`${noteIndent}  • lend earn deposit #2 jlUSDC`);
    }
    first = false;
  }
  return lines;
}

function parseHotkeyIndentOption(rawIndent) {
  if (rawIndent === undefined || rawIndent === null) return "";
  const text = String(rawIndent);
  if (/^\d+$/.test(text)) {
    const count = Math.min(32, Math.max(0, parseInt(text, 10)));
    if (count <= 0) return "";
    return " ".repeat(count);
  }
  return text;
}

function normalizeHotkeyContextList(rawContexts) {
  const normalized = [];
  const seen = new Set();
  for (const context of rawContexts) {
    const trimmed = typeof context === "string" ? context.trim() : "";
    if (!trimmed) continue;
    const lowered = trimmed.toLowerCase();
    if (!HOTKEY_MAP.has(lowered)) {
      throw new Error(
        `Unknown hotkey context '${trimmed}'. Use --list to show available contexts.`
      );
    }
    if (seen.has(lowered)) continue;
    seen.add(lowered);
    normalized.push(lowered);
  }
  return normalized;
}

function handleHotkeysCommand(rawArgs = []) {
  const { options, rest } = parseCliOptions(rawArgs);
  if (options.list) {
    for (const context of listHotkeyContexts()) {
      console.log(context);
    }
    return;
  }

  const indent = parseHotkeyIndentOption(options.indent);
  const includeTitle = options["no-title"] ? false : true;

  let contexts = [];
  if (options.context) {
    contexts.push(options.context);
  }
  if (Array.isArray(rest) && rest.length > 0) {
    contexts.push(...rest);
  }

  if (options.all) {
    contexts = Array.from(DEFAULT_HOTKEY_CONTEXTS);
  }

  let normalizedContexts;
  if (contexts.length === 0) {
    normalizedContexts = Array.from(DEFAULT_HOTKEY_CONTEXTS);
  } else {
    normalizedContexts = normalizeHotkeyContextList(contexts);
    if (normalizedContexts.length === 0) {
      normalizedContexts = Array.from(DEFAULT_HOTKEY_CONTEXTS);
    }
  }

  const lines = renderHotkeyTable(normalizedContexts, { indent, includeTitle });
  for (const line of lines) {
    console.log(line);
  }
}

// ---------------- Config ----------------
// All of the CLI's tunable parameters live in this block so the rest of the
// code has a single source of truth. Most values can be overridden via
// environment variables; RPC endpoints can also be provided via a file next
// to the script.
const SCRIPT_FILE_PATH = fileURLToPath(import.meta.url);
const SCRIPT_DIR = path.dirname(SCRIPT_FILE_PATH);

// Normalise a filesystem path for equality comparisons that tolerate symlinks.
const toComparablePath = (rawPath) => {
  if (!rawPath) return null;
  const valueAsString =
    typeof rawPath === "string" ? rawPath : String(rawPath ?? "");
  const normalizedInput = path.resolve(
    valueAsString.startsWith("file://") ? fileURLToPath(valueAsString) : valueAsString
  );
  try {
    return path.normalize(fs.realpathSync(normalizedInput));
  } catch (err) {
    if (err?.code === "ENOENT" || err?.code === "EACCES" || err?.code === "EPERM") {
      return path.normalize(normalizedInput);
    }
    return path.normalize(normalizedInput);
  }
};
const SCRIPT_COMPARABLE_PATH =
  toComparablePath(SCRIPT_FILE_PATH) ?? path.normalize(SCRIPT_FILE_PATH);

const stripTrailingSlashes = (value) => {
  if (typeof value !== "string") return "";
  return value.replace(/\/+$/, "");
};

const normalizeApiBase = (raw, fallback) => {
  const candidate =
    typeof raw === "string" && raw.trim().length > 0 ? raw.trim() : fallback;
  if (!candidate) return "";
  return stripTrailingSlashes(candidate);
};

const getEnvInteger = (name, fallback, { min, max } = {}) => {
  const raw = process.env?.[name];
  if (raw === undefined || raw === null || raw === "") {
    return fallback;
  }
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  let value = parsed;
  if (typeof min === "number" && value < min) {
    value = min;
  }
  if (typeof max === "number" && value > max) {
    value = max;
  }
  return value;
};

const IS_MAIN_EXECUTION = (() => {
  const entry = process?.argv?.[1];
  if (!entry) return false;
  const entryComparablePath = toComparablePath(entry);
  if (!entryComparablePath || !SCRIPT_COMPARABLE_PATH) {
    return false;
  }
  return entryComparablePath === SCRIPT_COMPARABLE_PATH;
})();

const KEYPAIR_DIR = "./keypairs";
const loadKeypairFromFile = sharedLoadKeypairFromFile;
const DEFAULT_RPC_URL = "https://mainnet.helius-rpc.com/?api-key=98a9fb2e-26c6-4420-b0bf-a38ece2eb907";
const PERPS_COMPUTE_BUDGET = Object.freeze({
  unitLimit: getEnvInteger("PERPS_COMPUTE_UNIT_LIMIT", 1_400_000, { min: 1 }),
  priceMicrolamports: getEnvInteger(
    "PERPS_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
    100_000,
    { min: 0 }
  ),
});
const PERPS_MARKET_CACHE_PATH = path.resolve(
  SCRIPT_DIR,
  process.env.PERPS_MARKET_CACHE_PATH || "perps/market_cache.json"
);
const RPC_LIST_FILE = path.resolve(
  SCRIPT_DIR,
  process.env.RPC_LIST_FILE || "rpc_endpoints.txt"
);
let RPC_ENDPOINTS_FILE_USED = null;
const UNHEALTHY_RPC_ENDPOINTS = new Map(); // endpoint -> unhealthyUntil timestamp
const DEFAULT_ULTRA_API_KEY = "91233f8d-d064-48c7-a97a-87b5d4d8a511";
const JUPITER_SWAP_ENGINE = (process.env.JUPITER_SWAP_ENGINE || "ultra").toLowerCase();
const JUPITER_SWAP_CONFIG = (() => {
  const defaultBase = "https://lite-api.jup.ag";
  const base =
    normalizeApiBase(process.env.JUPITER_SWAP_API_BASE, defaultBase) ||
    defaultBase;
  return Object.freeze({
    base,
    quoteUrl: `${base}/swap/v1/quote`,
    swapUrl: `${base}/swap/v1/swap`,
  });
})();
const JUPITER_SWAP_API_BASE = JUPITER_SWAP_CONFIG.base;
const JUPITER_SWAP_QUOTE_URL = JUPITER_SWAP_CONFIG.quoteUrl;
const JUPITER_SWAP_URL = JUPITER_SWAP_CONFIG.swapUrl;
const JUP_HTTP_TIMEOUT_MS = getEnvInteger("JUP_HTTP_TIMEOUT_MS", 15_000, {
  min: 1_000,
});
const JUPITER_ULTRA_CONFIG = (() => {
  const apiKey = process.env.JUPITER_ULTRA_API_KEY || DEFAULT_ULTRA_API_KEY;
  // Official Ultra API: send key as header, NOT in URL path
  const defaultBase = apiKey
    ? "https://api.jup.ag/ultra/v1"  // Paid tier with x-api-key header
    : "https://lite-api.jup.ag/ultra/v1";  // Free tier
  const base =
    normalizeApiBase(process.env.JUPITER_ULTRA_API_BASE, defaultBase) ||
    defaultBase;
  return Object.freeze({
    apiKey,
    base,
    includeUltraKeyHeader: Boolean(apiKey),
  });
})();
const JUPITER_ULTRA_API_KEY = JUPITER_ULTRA_CONFIG.apiKey;
const JUPITER_ULTRA_API_BASE = JUPITER_ULTRA_CONFIG.base;
const SHOULD_SEND_ULTRA_HEADER = JUPITER_ULTRA_CONFIG.includeUltraKeyHeader;

function normalizeSwapEngineMode(mode) {
  return mode === "ultra" ? "ultra" : "lite";
}

function formatSwapEngineLabel(mode) {
  const normalized = normalizeSwapEngineMode(mode);
  const tag = normalized === "ultra" ? "[ULTRA]" : "[LITE]";
  if (normalized === "ultra") {
    const detail = JUPITER_ULTRA_API_KEY
      ? "Ultra API (authenticated)"
      : "Ultra API (no key)";
    return `${detail} ${tag}`.trim();
  }
  return `Legacy Lite API ${tag}`.trim();
}

const SOL_MINT = "So11111111111111111111111111111111111111112";
const RAW_SWAP_AMOUNT_MODE = (process.env.SWAP_AMOUNT_MODE || "all").toLowerCase();
const DEFAULT_SWAP_AMOUNT_MODE = RAW_SWAP_AMOUNT_MODE === "random" ? "random" : "all";
const SLIPPAGE_BPS = process.env.SLIPPAGE_BPS ? Math.max(1, parseInt(process.env.SLIPPAGE_BPS, 10) || 1) : 20;
const DELAY_BETWEEN_CALLS_MS = 60;
const ULTRA_WALLET_DELAY_MS = process.env.ULTRA_WALLET_DELAY_MS
  ? Math.max(0, parseInt(process.env.ULTRA_WALLET_DELAY_MS, 10) || 0)
  : 25;
const ULTRA_EXECUTE_DELAY_MS = process.env.ULTRA_EXECUTE_DELAY_MS
  ? Math.max(0, parseInt(process.env.ULTRA_EXECUTE_DELAY_MS, 10) || 0)
  : 18;
const BALANCE_RPC_DELAY_MS = process.env.BALANCE_RPC_DELAY_MS
  ? Math.max(0, parseInt(process.env.BALANCE_RPC_DELAY_MS, 10) || 0)
  : 150;
const PRINT_SECRET_KEYS = process.env.PRINT_SECRET_KEYS === "1";
const PASSIVE_STEP_DELAY_MS = process.env.PASSIVE_STEP_DELAY_MS
  ? Math.max(0, parseInt(process.env.PASSIVE_STEP_DELAY_MS, 10) || 0)
  : 240;
const PASSIVE_STEP_JITTER_MS = process.env.PASSIVE_STEP_DELAY_JITTER_MS
  ? Math.max(0, parseInt(process.env.PASSIVE_STEP_DELAY_JITTER_MS, 10) || 0)
  : 90;
const GENERAL_SIMULATION_RETRY_LIMIT = process.env.GENERAL_SIMULATION_RETRY_LIMIT
  ? Math.max(0, parseInt(process.env.GENERAL_SIMULATION_RETRY_LIMIT, 10) || 0)
  : 3;
const GENERAL_SIMULATION_REDUCTION_BPS = process.env.GENERAL_SIMULATION_REDUCTION_BPS
  ? Math.min(9999, Math.max(1, parseInt(process.env.GENERAL_SIMULATION_REDUCTION_BPS, 10) || 2500))
  : 2500; // default reduce by 25%
const MIN_SOL_PER_SWAP_LAMPORTS = process.env.MIN_SOL_PER_SWAP_LAMPORTS
  ? BigInt(process.env.MIN_SOL_PER_SWAP_LAMPORTS)
  : BigInt(10_000_000); // default ~0.01 SOL to keep room for rent/fees
const MIN_LEND_SOL_DEPOSIT_LAMPORTS = process.env.MIN_LEND_SOL_DEPOSIT_LAMPORTS
  ? BigInt(process.env.MIN_LEND_SOL_DEPOSIT_LAMPORTS)
  : BigInt(5_000_000); // ~0.005 SOL minimum deposit to avoid dust / rent issues
const LEND_SOL_WRAP_BUFFER_LAMPORTS = process.env.LEND_SOL_WRAP_BUFFER_LAMPORTS
  ? BigInt(process.env.LEND_SOL_WRAP_BUFFER_LAMPORTS)
  : BigInt(2_200_000); // ~0.0022 SOL to cover wrap + ATA rent/fees
// Minimum token balance to attempt swap (in USD equivalent) - skip dust
const MIN_SWAP_VALUE_USD = process.env.MIN_SWAP_VALUE_USD
  ? parseFloat(process.env.MIN_SWAP_VALUE_USD)
  : 0.01; // $0.01 minimum to avoid wasting gas on dust
// Dust rescue threshold - only attempt to rescue dust worth more than this
const DUST_RESCUE_THRESHOLD_USD = process.env.DUST_RESCUE_THRESHOLD_USD
  ? parseFloat(process.env.DUST_RESCUE_THRESHOLD_USD)
  : 0.05; // $0.05 minimum - below this, just burn and close
const SOL_DUST_AUTOCLOSE_THRESHOLD_USD = process.env.SOL_DUST_AUTOCLOSE_THRESHOLD_USD
  ? parseFloat(process.env.SOL_DUST_AUTOCLOSE_THRESHOLD_USD)
  : 0.05; // Only unwrap SOL dust worth less than ~5 cents
const GAS_RESERVE_LAMPORTS = process.env.GAS_RESERVE_LAMPORTS
  ? BigInt(process.env.GAS_RESERVE_LAMPORTS)
  : BigInt(1_000_000); // default reserve ~0.001 SOL
const JUPITER_SOL_BUFFER_LAMPORTS = process.env.JUPITER_SOL_BUFFER_LAMPORTS
  ? BigInt(process.env.JUPITER_SOL_BUFFER_LAMPORTS)
  : BigInt(2_000_000); // extra buffer for wrap + routing side-adds
const JUPITER_SOL_RETRY_DELTA_LAMPORTS = process.env.JUPITER_SOL_RETRY_DELTA_LAMPORTS
  ? BigInt(process.env.JUPITER_SOL_RETRY_DELTA_LAMPORTS)
  : BigInt(200_000);
const JUPITER_SOL_MAX_RETRIES = process.env.JUPITER_SOL_MAX_RETRIES
  ? Math.max(0, parseInt(process.env.JUPITER_SOL_MAX_RETRIES, 10) || 0)
  : 3;
const MAX_ULTRA_FALLBACK_RETRIES = process.env.MAX_ULTRA_FALLBACK_RETRIES
  ? Math.max(0, parseInt(process.env.MAX_ULTRA_FALLBACK_RETRIES, 10) || 0)
  : 3;
const ULTRA_RETRY_BACKOFF_BASE_MS = process.env.ULTRA_RETRY_BACKOFF_BASE_MS
  ? Math.max(0, parseInt(process.env.ULTRA_RETRY_BACKOFF_BASE_MS, 10) || 0)
  : 250;
const RPC_RATE_LIMIT_COOLDOWN_MS = process.env.RPC_RATE_LIMIT_COOLDOWN_MS
  ? Math.max(1000, parseInt(process.env.RPC_RATE_LIMIT_COOLDOWN_MS, 10) || 0)
  : 20_000;
const RPC_GENERAL_COOLDOWN_MS = process.env.RPC_GENERAL_COOLDOWN_MS
  ? Math.max(1000, parseInt(process.env.RPC_GENERAL_COOLDOWN_MS, 10) || 0)
  : 5 * 60 * 1000;

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;

const DEFAULT_RNG = Math.random;

function normaliseRng(rng) {
  return typeof rng === "function" ? rng : DEFAULT_RNG;
}

function randomFloat(rng = DEFAULT_RNG) {
  const generator = normaliseRng(rng);
  let value = generator();
  if (!Number.isFinite(value)) value = 0;
  if (value >= 1 || value <= -1) {
    value = value % 1;
  }
  if (value < 0) value += 1;
  if (value >= 1) value = 0;
  return value;
}

function randomIntInclusive(minValue, maxValue, rng = DEFAULT_RNG) {
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return 0;
  }
  const lower = Math.ceil(Math.min(minValue, maxValue));
  const upper = Math.floor(Math.max(minValue, maxValue));
  if (upper <= lower) return lower;
  const span = upper - lower + 1;
  const pick = Math.floor(randomFloat(rng) * span);
  return lower + Math.min(span - 1, pick);
}

const EMPTY_RANDOM_MINT_OPTIONS = Object.freeze({
  includeTags: [],
  excludeTags: [],
  excludeMints: [],
  excludeSymbols: [],
  allowSol: false,
  matchAnyTags: false,
});

function normaliseTagList(tags) {
  if (!tags) return [];
  let list;
  if (tags instanceof Set) {
    list = Array.from(tags);
  } else if (Array.isArray(tags)) {
    list = tags;
  } else {
    list = [tags];
  }
  return list
    .map((tag) =>
      typeof tag === "string" ? tag.trim().toLowerCase() : null
    )
    .filter((tag) => tag && tag.length > 0);
}

function normaliseSymbolList(symbols) {
  if (!symbols) return [];
  const list = Array.isArray(symbols) ? symbols : [symbols];
  return list
    .map((symbol) =>
      typeof symbol === "string" ? symbol.trim().toUpperCase() : ""
    )
    .filter((symbol) => symbol.length > 0);
}

function normaliseMintValue(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  try {
    return new PublicKey(trimmed).toBase58();
  } catch (_) {
    return trimmed;
  }
}

function normaliseMintList(values) {
  if (!values) return [];
  const list = Array.isArray(values) ? values : [values];
  const result = [];
  for (const value of list) {
    const normalised = normaliseMintValue(
      typeof value === "string" ? value : String(value ?? "")
    );
    if (normalised) result.push(normalised);
  }
  return result;
}

function normaliseRandomMintOptions(raw) {
  if (!raw || typeof raw !== "object") {
    return { ...EMPTY_RANDOM_MINT_OPTIONS };
  }
  const includeTags = normaliseTagList(
    raw.includeTags ?? raw.tags ?? raw.withTags
  );
  const excludeTags = normaliseTagList(raw.excludeTags ?? raw.withoutTags);
  const excludeMints = normaliseMintList(raw.excludeMints ?? raw.exclude);
  const excludeSymbols = normaliseSymbolList(raw.excludeSymbols);
  const allowSol = raw.allowSol === true;
  const matchAnyTags =
    raw.matchAnyTags === true ||
    raw.matchAny === true ||
    raw.anyTag === true;
  return {
    includeTags,
    excludeTags,
    excludeMints,
    excludeSymbols,
    allowSol,
    matchAnyTags,
  };
}

function combineRandomMintOptions(...sources) {
  const combined = {
    includeTags: new Set(),
    excludeTags: new Set(),
    excludeMints: new Set(),
    excludeSymbols: new Set(),
    allowSol: false,
    matchAnyTags: false,
  };
  for (const source of sources) {
    if (!source || typeof source !== "object") continue;
    const normalised = normaliseRandomMintOptions(source);
    for (const tag of normalised.includeTags) combined.includeTags.add(tag);
    for (const tag of normalised.excludeTags) combined.excludeTags.add(tag);
    for (const mint of normalised.excludeMints)
      combined.excludeMints.add(mint);
    for (const symbol of normalised.excludeSymbols)
      combined.excludeSymbols.add(symbol);
    if (normalised.allowSol) combined.allowSol = true;
    if (normalised.matchAnyTags) combined.matchAnyTags = true;
  }
  return {
    includeTags: Array.from(combined.includeTags),
    excludeTags: Array.from(combined.excludeTags),
    excludeMints: Array.from(combined.excludeMints),
    excludeSymbols: Array.from(combined.excludeSymbols),
    allowSol: combined.allowSol,
    matchAnyTags: combined.matchAnyTags,
  };
}

function parseRandomMintRequest(raw) {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (trimmed.length === 0) return null;
    if (trimmed.toLowerCase() !== "random") return null;
    return {
      placeholder: raw,
      ...EMPTY_RANDOM_MINT_OPTIONS,
    };
  }
  if (typeof raw === "object") {
    const mode =
      typeof raw.mode === "string" ? raw.mode.trim().toLowerCase() : null;
    const mintField =
      typeof raw.mint === "string" ? raw.mint.trim().toLowerCase() : null;
    const randomFlag =
      raw.random === true ||
      mode === "random" ||
      mintField === "random";
    if (!randomFlag) return null;
    const options = normaliseRandomMintOptions(raw);
    return {
      placeholder: raw,
      ...options,
    };
  }
  return null;
}

function resolveRandomMintRequest(request, context = {}) {
  const baseOptions = combineRandomMintOptions(
    EMPTY_RANDOM_MINT_OPTIONS,
    context.baseOptions || EMPTY_RANDOM_MINT_OPTIONS,
    request || EMPTY_RANDOM_MINT_OPTIONS
  );
  const additionalExclusions = normaliseMintList(
    context.additionalExclusions || []
  );
  const excludeMints = new Set([
    ...baseOptions.excludeMints,
    ...additionalExclusions,
  ]);
  const entry = pickRandomCatalogMint({
    includeTags: baseOptions.includeTags,
    matchAnyTags: baseOptions.matchAnyTags,
    excludeTags: baseOptions.excludeTags,
    excludeMints: Array.from(excludeMints),
    excludeSymbols: baseOptions.excludeSymbols,
    allowSol: baseOptions.allowSol,
    rng: context.rng,
  });
  return {
    entry,
    options: baseOptions,
  };
}

const clamp = (value, min, max) => {
  if (Number.isNaN(value)) return min;
  if (!Number.isFinite(value)) return max;
  if (value < min) return min;
  if (value > max) return max;
  return value;
};
const minutesToMs = (minutes) => {
  const numeric = Number(minutes);
  if (!Number.isFinite(numeric)) return 0;
  return Math.round(numeric * MINUTE_MS);
};
const segmentWindow = (minMinutes, maxMinutes) => {
  const minMs = Math.max(0, minutesToMs(minMinutes));
  const maxMs = Math.max(minMs, minutesToMs(maxMinutes));
  return { minMs, maxMs };
};
const freezeLegs = (legs) =>
  Object.freeze(
    legs.map((leg) =>
      Object.freeze({
        ...leg,
        segmentWaitsMs: Object.freeze({ ...leg.segmentWaitsMs }),
      })
    )
  );

const freezeRuntimeProfile = (profile) => {
  if (!profile || typeof profile !== "object") {
    return Object.freeze({
      swapCountRange: Object.freeze({ min: 0, max: 0 }),
    });
  }
  const swapRange = Object.freeze({
    min: Math.max(0, Math.floor(profile.swapCountRange?.min ?? 0)),
    max: Math.max(
      Math.max(0, Math.floor(profile.swapCountRange?.min ?? 0)),
      Math.floor(profile.swapCountRange?.max ?? profile.swapCountRange?.min ?? 0)
    ),
  });
  const targetDurationMs = (() => {
    if (Number.isFinite(profile.targetDurationMs)) {
      return Math.max(0, Math.floor(profile.targetDurationMs));
    }
    if (Number.isFinite(profile.targetMinutes)) {
      return Math.max(0, Math.floor(minutesToMs(profile.targetMinutes)));
    }
    return undefined;
  })();
  const minimumSwapCount = Math.max(
    0,
    Math.floor(
      profile.minimumSwapCount ??
        profile.swapCountRange?.min ??
        0
    )
  );
  return Object.freeze({
    ...profile,
    targetDurationMs,
    swapCountRange: swapRange,
    minimumSwapCount,
  });
};

export const PREWRITTEN_FLOW_DEFINITIONS = Object.freeze({
  arpeggio: Object.freeze({
    key: "arpeggio",
    label: "Arpeggio",
    description: "Fast rotation flow for short, rhythmic bursts.",
    defaultLoops: 1,
    defaultDurationMs: 15 * MINUTE_MS,
    runtimeProfile: freezeRuntimeProfile({
      label: "≈15 minute coverage",
      targetMinutes: 15,
      swapCountRange: { min: 10, max: 100 },
      minimumSwapCount: 10,
    }),
    legs: freezeLegs([
      {
        key: "warmup",
        label: "Warmup rotation",
        segmentWaitsMs: segmentWindow(2, 3),
      },
      {
        key: "build",
        label: "Momentum build",
        segmentWaitsMs: segmentWindow(3, 4),
      },
      {
        key: "peak",
        label: "Peak sweep",
        segmentWaitsMs: segmentWindow(4, 5),
      },
      {
        key: "cooldown",
        label: "Cooldown recycle",
        segmentWaitsMs: segmentWindow(3, 4),
      },
    ]),
  }),
  horizon: Object.freeze({
    key: "horizon",
    label: "Horizon",
    description: "Mid-duration rotation intended for hourly cadences.",
    defaultLoops: 1,
    defaultDurationMs: 60 * MINUTE_MS,
    runtimeProfile: freezeRuntimeProfile({
      label: "≈60 minute coverage",
      targetMinutes: 60,
      swapCountRange: { min: 50, max: 300 },
      minimumSwapCount: 50,
    }),
    legs: freezeLegs([
      {
        key: "warmup",
        label: "Warmup block",
        segmentWaitsMs: segmentWindow(9, 12),
      },
      {
        key: "build",
        label: "Expansion push",
        segmentWaitsMs: segmentWindow(11, 15),
      },
      {
        key: "sustain",
        label: "Sustain rotation",
        segmentWaitsMs: segmentWindow(13, 18),
      },
      {
        key: "rebalance",
        label: "Rebalance sweep",
        segmentWaitsMs: segmentWindow(11, 16),
      },
      {
        key: "cooldown",
        label: "Cooldown wrap",
        segmentWaitsMs: segmentWindow(9, 12),
      },
    ]),
  }),
  echo: Object.freeze({
    key: "echo",
    label: "Echo",
    description: "Extended loop suitable for multi-hour background runs.",
    defaultLoops: 1,
    defaultDurationMs: 6 * HOUR_MS,
    runtimeProfile: freezeRuntimeProfile({
      label: "≈6 hour coverage",
      targetMinutes: 6 * 60,
      swapCountRange: { min: 250, max: 750 },
      minimumSwapCount: 250,
    }),
    legs: freezeLegs([
      {
        key: "dawn",
        label: "Dawn accumulation",
        segmentWaitsMs: segmentWindow(38, 55),
      },
      {
        key: "climb",
        label: "Morning climb",
        segmentWaitsMs: segmentWindow(48, 72),
      },
      {
        key: "crest",
        label: "Midday crest",
        segmentWaitsMs: segmentWindow(58, 88),
      },
      {
        key: "drift",
        label: "Afternoon drift",
        segmentWaitsMs: segmentWindow(53, 85),
      },
      {
        key: "fade",
        label: "Evening fade",
        segmentWaitsMs: segmentWindow(45, 70),
      },
      {
        key: "twilight",
        label: "Twilight reset",
        segmentWaitsMs: segmentWindow(38, 55),
      },
    ]),
  }),
  icarus: Object.freeze({
    key: "icarus",
    label: "Icarus",
    description:
      "Fast rotation that mirrors Arpeggio's pacing while sampling random catalog tokens each hop.",
    defaultLoops: 1,
    defaultDurationMs: 15 * MINUTE_MS,
    legs: freezeLegs([
      {
        key: "ignite",
        label: "Ignition cadence",
        segmentWaitsMs: segmentWindow(2, 4),
      },
      {
        key: "soar",
        label: "Ascent shuffle",
        segmentWaitsMs: segmentWindow(3, 5),
      },
      {
        key: "apex",
        label: "Apex recycle",
        segmentWaitsMs: segmentWindow(4, 6),
      },
      {
        key: "glide",
        label: "Glide cooldown",
        segmentWaitsMs: segmentWindow(2, 4),
      },
    ]),
  }),
  zenith: Object.freeze({
    key: "zenith",
    label: "Zenith",
    description:
      "Hourly cadence companion to Horizon that rotates through randomised catalog picks.",
    defaultLoops: 1,
    defaultDurationMs: 60 * MINUTE_MS,
    legs: freezeLegs([
      {
        key: "glow",
        label: "Glow block",
        segmentWaitsMs: segmentWindow(8, 12),
      },
      {
        key: "rise",
        label: "Rise push",
        segmentWaitsMs: segmentWindow(10, 14),
      },
      {
        key: "halo",
        label: "Halo sustain",
        segmentWaitsMs: segmentWindow(12, 18),
      },
      {
        key: "rebalance",
        label: "Rebalance sweep",
        segmentWaitsMs: segmentWindow(10, 16),
      },
      {
        key: "anchor",
        label: "Anchor wrap",
        segmentWaitsMs: segmentWindow(8, 12),
      },
    ]),
  }),
  aurora: Object.freeze({
    key: "aurora",
    label: "Aurora",
    description:
      "Echo's long-form schedule with dynamic mint sampling for each rotation leg.",
    defaultLoops: 1,
    defaultDurationMs: 6 * HOUR_MS,
    legs: freezeLegs([
      {
        key: "spark",
        label: "Spark accumulation",
        segmentWaitsMs: segmentWindow(35, 55),
      },
      {
        key: "arc",
        label: "Arc climb",
        segmentWaitsMs: segmentWindow(45, 75),
      },
      {
        key: "zenith",
        label: "Zenith crest",
        segmentWaitsMs: segmentWindow(60, 90),
      },
      {
        key: "drift",
        label: "Drift glide",
        segmentWaitsMs: segmentWindow(55, 95),
      },
      {
        key: "veil",
        label: "Veil fade",
        segmentWaitsMs: segmentWindow(45, 75),
      },
      {
        key: "reset",
        label: "Reset twilight",
        segmentWaitsMs: segmentWindow(35, 55),
      },
    ]),
  }),
  "titan": Object.freeze({
    key: "titan",
    label: "Titan",
    description:
      "High-value flow with 0.02 SOL minimum swaps and 30s-10min delays for powerful whale-sized positions.",
    defaultLoops: 1,
    defaultDurationMs: 30 * MINUTE_MS,
    minSwapSol: 0.02,
    legs: freezeLegs([
      {
        key: "ignite",
        label: "Ignition cadence",
        segmentWaitsMs: segmentWindow(30, 120), // 30s to 2min
      },
      {
        key: "soar",
        label: "Ascent shuffle",
        segmentWaitsMs: segmentWindow(60, 180), // 1min to 3min
      },
      {
        key: "apex",
        label: "Apex recycle",
        segmentWaitsMs: segmentWindow(120, 300), // 2min to 5min
      },
      {
        key: "glide",
        label: "Glide cooldown",
        segmentWaitsMs: segmentWindow(180, 600), // 3min to 10min
      },
    ]),
  }),
  "odyssey": Object.freeze({
    key: "odyssey",
    label: "Odyssey",
    description:
      "High-value flow with 0.02 SOL minimum swaps and 30s-10min delays for epic trading journeys.",
    defaultLoops: 1,
    defaultDurationMs: 90 * MINUTE_MS,
    minSwapSol: 0.02,
    legs: freezeLegs([
      {
        key: "glow",
        label: "Glow block",
        segmentWaitsMs: segmentWindow(60, 180), // 1min to 3min
      },
      {
        key: "rise",
        label: "Rise push",
        segmentWaitsMs: segmentWindow(120, 300), // 2min to 5min
      },
      {
        key: "halo",
        label: "Halo sustain",
        segmentWaitsMs: segmentWindow(180, 420), // 3min to 7min
      },
      {
        key: "rebalance",
        label: "Rebalance sweep",
        segmentWaitsMs: segmentWindow(240, 480), // 4min to 8min
      },
      {
        key: "anchor",
        label: "Anchor wrap",
        segmentWaitsMs: segmentWindow(300, 600), // 5min to 10min
      },
    ]),
  }),
  "sovereign": Object.freeze({
    key: "sovereign",
    label: "Sovereign",
    description:
      "High-value flow with 0.02 SOL minimum swaps and 30s-10min delays for commanding long-term positions.",
    defaultLoops: 1,
    defaultDurationMs: 8 * HOUR_MS,
    minSwapSol: 0.02,
    legs: freezeLegs([
      {
        key: "spark",
        label: "Spark accumulation",
        segmentWaitsMs: segmentWindow(120, 360), // 2min to 6min
      },
      {
        key: "arc",
        label: "Arc climb",
        segmentWaitsMs: segmentWindow(180, 480), // 3min to 8min
      },
      {
        key: "zenith",
        label: "Zenith crest",
        segmentWaitsMs: segmentWindow(240, 600), // 4min to 10min
      },
      {
        key: "drift",
        label: "Drift glide",
        segmentWaitsMs: segmentWindow(180, 540), // 3min to 9min
      },
      {
        key: "veil",
        label: "Veil fade",
        segmentWaitsMs: segmentWindow(120, 420), // 2min to 7min
      },
      {
        key: "reset",
        label: "Reset twilight",
        segmentWaitsMs: segmentWindow(60, 300), // 1min to 5min
      },
    ]),
  }),
});
const LEND_SOL_BASE_PERCENT = BigInt(
  process.env.LEND_SOL_BASE_PERCENT
    ? Math.min(
        10000,
        Math.max(1000, parseInt(process.env.LEND_SOL_BASE_PERCENT, 10) || 7000)
      )
    : 7000
);
const LEND_SOL_RETRY_DECREMENT_PERCENT = BigInt(
  process.env.LEND_SOL_RETRY_DECREMENT_PERCENT
    ? Math.min(
        5000,
        Math.max(
          250,
          parseInt(process.env.LEND_SOL_RETRY_DECREMENT_PERCENT, 10) || 1500
        )
      )
    : 1500
);
const LEND_SOL_MIN_PERCENT = BigInt(
  process.env.LEND_SOL_MIN_PERCENT
    ? Math.min(
        9000,
        Math.max(
          500,
          parseInt(process.env.LEND_SOL_MIN_PERCENT, 10) || 2000
        )
      )
    : 2000
);
const MAX_SLIPPAGE_RETRIES = process.env.MAX_SLIPPAGE_RETRIES
  ? Math.max(1, parseInt(process.env.MAX_SLIPPAGE_RETRIES, 10) || 1)
  : 5;
const MIN_TRANSFER_LAMPORTS = process.env.MIN_TRANSFER_LAMPORTS
  ? BigInt(process.env.MIN_TRANSFER_LAMPORTS)
  : BigInt(50_000); // default ~0.00005 SOL to avoid dust txs
const ATA_CREATION_FEE_LAMPORTS = BigInt(5_000); // buffer for tx fee when creating ATA
const WALLET_DISABLE_THRESHOLD_LAMPORTS = BigInt(5_000_000); // 0.005 SOL guardrail
const WALLET_DISABLE_THRESHOLD_SOL = Number(WALLET_DISABLE_THRESHOLD_LAMPORTS) / 1e9;
const WALLET_DISABLE_THRESHOLD_LABEL = WALLET_DISABLE_THRESHOLD_SOL.toFixed(3);
const ESTIMATED_GAS_PER_SWAP_LAMPORTS = process.env.ESTIMATED_GAS_PER_SWAP_LAMPORTS
  ? BigInt(process.env.ESTIMATED_GAS_PER_SWAP_LAMPORTS)
  : BigInt(5_000); // estimated gas per swap transaction
const ESTIMATED_ATA_CREATION_LAMPORTS = process.env.ESTIMATED_ATA_CREATION_LAMPORTS
  ? BigInt(process.env.ESTIMATED_ATA_CREATION_LAMPORTS)
  : BigInt(2_000_000); // estimated cost for ATA creation (rent + fees)
const DISABLE_STATE_FILE = path.resolve(SCRIPT_DIR, ".wallet-disable-state.json");
const ENABLE_COLOR = process.stdout?.isTTY && process.env.NO_COLOR !== "1";
const COLORS = ENABLE_COLOR
  ? {
      reset: "\x1b[0m",
      info: "\x1b[36m",
      success: "\x1b[32m",
      warn: "\x1b[33m",
      error: "\x1b[31m",
      label: "\x1b[96m",
      muted: "\x1b[90m",
    }
  : {
      reset: "",
      info: "",
      success: "",
      warn: "",
      error: "",
      label: "",
      muted: "",
    };

const VERBOSE_ERROR_OUTPUT = process.env.JUPITER_SWAP_TOOL_VERBOSE_ERRORS === "1";
const ERROR_SUPPRESSION_WINDOW_MS = process.env.JUPITER_SWAP_TOOL_ERROR_SUPPRESSION_MS
  ? Math.max(0, parseInt(process.env.JUPITER_SWAP_TOOL_ERROR_SUPPRESSION_MS, 10) || 0)
  : 5000;
const LAUNCHER_GUARD_MAX_AGE_MS = process.env.JUPITER_SWAP_TOOL_LAUNCHER_GUARD_MAX_AGE_MS
  ? Math.max(0, parseInt(process.env.JUPITER_SWAP_TOOL_LAUNCHER_GUARD_MAX_AGE_MS, 10) || 0)
  : 5 * 60 * 1000;
const QUIET_MODE = process.env.QUIET_MODE === "1" || process.env.JUPITER_QUIET_MODE === "1";
const FLOW_WALLET_DELAY_MS = process.env.FLOW_WALLET_DELAY_MS
  ? Math.max(0, parseInt(process.env.FLOW_WALLET_DELAY_MS, 10) || 0)
  : 500;
const FLOW_LOOP_COOLDOWN_MS = process.env.FLOW_LOOP_COOLDOWN_MS
  ? Math.max(0, parseInt(process.env.FLOW_LOOP_COOLDOWN_MS, 10) || 0)
  : 60000;
const RPC_RETRY_BASE_DELAY_MS = process.env.RPC_RETRY_BASE_DELAY_MS
  ? Math.max(100, parseInt(process.env.RPC_RETRY_BASE_DELAY_MS, 10) || 1000)
  : 1000;
const MAX_CONCURRENT_WALLETS = process.env.MAX_CONCURRENT_WALLETS
  ? Math.max(1, parseInt(process.env.MAX_CONCURRENT_WALLETS, 10) || 5)
  : 5;
const RECENT_ERROR_LOGS = new Map();
let verboseErrorHintShown = false;
const DEFAULT_DERIVATION_PATH = "m/44'/501'/0'/0'";

const TIMING_ENABLED = process.env.JUPITER_SWAP_TOOL_TIMING === "1";

function recordTiming(label, durationMs) {
  if (!TIMING_ENABLED) return;
  console.log(paint(`[timing] ${label}: ${durationMs.toFixed(1)} ms`, "muted"));
}

async function measureAsync(label, fn) {
  const start = process.hrtime.bigint();
  try {
    return await fn();
  } finally {
    const end = process.hrtime.bigint();
    const durationMs = Number(end - start) / 1_000_000;
    recordTiming(label, durationMs);
  }
}

const TOKEN_CATALOG_FILE = path.resolve(SCRIPT_DIR, "token_catalog.json");
const JUPITER_PRICE_API_BASE =
  process.env.JUPITER_PRICE_API_BASE || "https://lite-api.jup.ag/price/v3";
const JUPITER_PRICE_ENDPOINT = `${JUPITER_PRICE_API_BASE}/price`;
// Using FREE TIER to avoid 401 Unauthorized errors
const JUPITER_TOKENS_API_BASE =
  process.env.JUPITER_TOKENS_API_BASE || "https://lite-api.jup.ag/tokens/v2";
const JUPITER_TOKENS_SEARCH_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/search`;
const JUPITER_TOKENS_TAG_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/tag`;
const JUPITER_TOKENS_CATEGORY_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/category`;
// Fallback URL for loadJupiterTokenMap() - uses strict token list from v1 API (still available on lite)
const JUPITER_TOKEN_LIST_URL = "https://lite-api.jup.ag/tokens/v1/strict";
const DEFAULT_LEND_EARN_BASE =
  process.env.JUPITER_LEND_API_BASE || "https://lite-api.jup.ag/lend/v1/earn";
const DEFAULT_PERPS_API_BASE =
  process.env.JUPITER_PERPS_API_BASE || "https://perps-api.jup.ag/v1";
const FALLBACK_LEND_EARN_BASES = [
  "https://lite-api.jup.ag/lend/v1/earn",
  "https://lite-api.jup.ag/lend/earn",
  "https://lite-api.jup.ag/jup-integrators/earn",
];
const FALLBACK_PERPS_BASES = [
  "https://perps-api.jup.ag/v1",
  "https://api.jup.ag/perps/v1",
  "https://lite-api.jup.ag/perps/v1",
  "https://lite-api.jup.ag/perps",
];
const LEND_EARN_BASES = Array.from(
  new Set([DEFAULT_LEND_EARN_BASE, ...FALLBACK_LEND_EARN_BASES])
);
const PERPS_API_BASES = Array.from(
  new Set([DEFAULT_PERPS_API_BASE, ...FALLBACK_PERPS_BASES])
);
const USE_ULTRA_ENGINE = JUPITER_SWAP_ENGINE !== "lite";
const UNIVERSAL_TOKEN_TAGS = Object.freeze(["swappable", "default-sweep"]);
const TERMINAL_TOKEN_TAGS = Object.freeze([
  ...UNIVERSAL_TOKEN_TAGS,
  "secondary-terminal",
]);
const FALLBACK_TOKEN_CATALOG = [
  {
    symbol: "SOL",
    mint: SOL_MINT,
    decimals: 9,
    program: "native",
    tags: ["terminal"],
  },
  {
    symbol: "USDC",
    mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    decimals: 6,
    program: "spl",
    tags: TERMINAL_TOKEN_TAGS,
  },
  {
    symbol: "USDT",
    mint: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    decimals: 6,
    program: "spl",
    tags: TERMINAL_TOKEN_TAGS,
  },
  {
    symbol: "POPCAT",
    mint: "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
    decimals: 9,
    program: "spl",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "PUMP",
    mint: "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn",
    decimals: 6,
    program: "token-2022",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "PENGU",
    mint: "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
    decimals: 6,
    program: "spl",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "FART",
    mint: "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
    decimals: 6,
    program: "spl",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "WIF",
    mint: "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    decimals: 6,
    program: "spl",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "URANUS",
    mint: "BFgdzMkTPdKKJeTipv2njtDEwhKxkgFueJQfJGt1jups",
    decimals: 6,
    program: "spl",
    tags: UNIVERSAL_TOKEN_TAGS,
  },
  {
    symbol: "wBTC",
    mint: "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",
    decimals: 8,
    program: "spl",
    tags: TERMINAL_TOKEN_TAGS,
  },
  {
    symbol: "cbBTC",
    mint: "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij",
    decimals: 8,
    program: "spl",
    tags: TERMINAL_TOKEN_TAGS,
  },
  {
    symbol: "wETH",
    mint: "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",
    decimals: 8,
    program: "spl",
    tags: TERMINAL_TOKEN_TAGS,
  },
];

function describePathForLog(targetPath) {
  const resolved = path.resolve(targetPath);
  const relative = path.relative(process.cwd(), resolved);
  if (relative && !relative.startsWith("..")) {
    return relative;
  }
  return resolved;
}

function ensureStartupResources() {
  // Ensure critical folders/files exist before the CLI proceeds.
  const defaultResources = [
    {
      id: "keypairs",
      kind: "directory",
      targetPath: path.resolve(KEYPAIR_DIR),
      ensure: () => fs.mkdirSync(path.resolve(KEYPAIR_DIR), { recursive: true }),
    },
    {
      id: "rpcList",
      kind: "file",
      targetPath: RPC_LIST_FILE,
      ensure: () => {
        const directory = path.dirname(RPC_LIST_FILE);
        if (!fs.existsSync(directory)) {
          fs.mkdirSync(directory, { recursive: true });
        }
        if (!fs.existsSync(RPC_LIST_FILE)) {
          const template =
            "# Add RPC endpoints here (one per line or separated by commas)\n";
          try {
            fs.writeFileSync(RPC_LIST_FILE, template, { flag: "wx" });
          } catch (err) {
            if (err.code !== "EEXIST") {
              throw err;
            }
          }
        }
      },
    },
  ];

  const summary = new Map();
  for (const resource of defaultResources) {
    const existed = fs.existsSync(resource.targetPath);
    if (!existed) {
      try {
        resource.ensure();
      } catch (err) {
        throw new Error(
          `Failed to create required ${resource.kind} at ${resource.targetPath}: ${err.message}`
        );
      }
    }
    summary.set(resource.id, {
      path: resource.targetPath,
      existed,
      created: !existed,
    });
  }

  return summary;
}

function announceStartupResources(summary, options = {}) {
  const { skipBanner = false } = options;
  if (skipBanner) return;

  const resourcesToReport = [
    {
      id: "keypairs",
      createdMessage: (location) => `Initialized keypair directory at ${location}`,
      readyMessage: (location) => `Keypair directory ready at ${location}`,
    },
    {
      id: "rpcList",
      createdMessage: (location) => `Created RPC endpoints template at ${location}`,
      readyMessage: (location) => `RPC endpoints file found at ${location}`,
    },
  ];

  for (const resource of resourcesToReport) {
    const details = summary.get(resource.id);
    if (!details) continue;
    const location = describePathForLog(details.path);
    const tone = details.created ? "success" : "muted";
    const message = details.created
      ? resource.createdMessage(location)
      : resource.readyMessage(location);
    console.log(paint(message, tone));
  }
}

function normaliseTokenRecord(raw, source = "catalog") {
  if (!raw || typeof raw !== "object") return null;
  const symbolRaw = typeof raw.symbol === "string" ? raw.symbol.trim() : "";
  const mintRaw = typeof raw.mint === "string" ? raw.mint.trim() : "";
  const decimalsRaw = raw.decimals;
  if (!symbolRaw || !mintRaw || !Number.isFinite(decimalsRaw)) return null;
  const programRaw =
    typeof raw.program === "string" ? raw.program.trim().toLowerCase() : "spl";
  const allowedPrograms = new Set(["spl", "token-2022", "native"]);
  const program = allowedPrograms.has(programRaw) ? programRaw : "spl";
  const tagsRaw = Array.isArray(raw.tags) ? raw.tags : [];
  const tags = tagsRaw
    .map((tag) => (typeof tag === "string" ? tag.trim().toLowerCase() : ""))
    .filter((tag) => tag.length > 0);
  return {
    symbol: symbolRaw.toUpperCase(),
    mint: mintRaw,
    decimals: Number(decimalsRaw),
    program,
    tags,
    source,
  };
}

const NORMALISED_FALLBACK_TOKENS = FALLBACK_TOKEN_CATALOG.map((entry) =>
  normaliseTokenRecord(entry, "fallback")
).filter(Boolean);

let FILE_TOKEN_CATALOG = [];
let fileCatalogError = null;
try {
  if (fs.existsSync(TOKEN_CATALOG_FILE)) {
    const raw = fs.readFileSync(TOKEN_CATALOG_FILE, "utf8");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error("token catalog must be a JSON array");
    }
    FILE_TOKEN_CATALOG = parsed
      .map((entry) => normaliseTokenRecord(entry, "file"))
      .filter(Boolean);
  }
} catch (err) {
  fileCatalogError = err;
  console.warn(
    paint(
      `Token catalog load warning: ${err.message}. Falling back to built-in list.`,
      "warn"
    )
  );
}

function mergeTokenSources(primaryTokens, ...fallbackSources) {
  const deduped = new Map();
  for (const list of [primaryTokens, ...fallbackSources]) {
    for (const entry of list) {
      if (!entry || !entry.symbol) continue;
      if (!deduped.has(entry.symbol)) {
        deduped.set(entry.symbol, entry);
      }
    }
  }
  return Array.from(deduped.values());
}

let tokenCatalogSourceLabel = FILE_TOKEN_CATALOG.length > 0 ? "file" : "fallback";

let TOKEN_CATALOG = mergeTokenSources(FILE_TOKEN_CATALOG, NORMALISED_FALLBACK_TOKENS);
let TOKEN_CATALOG_BY_SYMBOL = new Map(
  TOKEN_CATALOG.map((entry) => [entry.symbol, entry])
);
let TOKEN_CATALOG_BY_MINT = new Map(
  TOKEN_CATALOG.map((entry) => [entry.mint, entry])
);

function snapshotTokenCatalog() {
  return TOKEN_CATALOG.map((entry) => ({ ...entry }));
}

function rebuildTokenCatalog(primaryTokens, sourceLabel) {
  TOKEN_CATALOG = mergeTokenSources(primaryTokens, FILE_TOKEN_CATALOG, NORMALISED_FALLBACK_TOKENS);
  TOKEN_CATALOG_BY_SYMBOL = new Map(
    TOKEN_CATALOG.map((entry) => [entry.symbol, entry])
  );
  TOKEN_CATALOG_BY_MINT = new Map(
    TOKEN_CATALOG.map((entry) => [entry.mint, entry])
  );
  tokenCatalogSourceLabel = sourceLabel;
}

function resolveRandomCatalogMint(options = {}) {
  let rng = typeof options.rng === "function" ? options.rng : null;
  const candidates = TOKEN_CATALOG.filter(
    (entry) => entry?.mint && entry.mint !== SOL_MINT
  );
  if (candidates.length === 0) {
    throw new Error("Token catalog does not contain any non-SOL mints");
  }
  const excludeSet = (() => {
    const raw = options.exclude;
    const set = new Set();
    if (!raw) return set;
    const values = raw instanceof Set ? Array.from(raw) : Array.isArray(raw) ? raw : [raw];
    for (const value of values) {
      if (typeof value !== "string") continue;
      const trimmed = value.trim();
      if (!trimmed) continue;
      set.add(trimmed === SOL_MINT ? SOL_MINT : trimmed);
    }
    return set;
  })();
  let pool = candidates.filter((entry) => !excludeSet.has(entry.mint));
  if (pool.length === 0) {
    pool = candidates;
  }
  let randomValue = typeof rng === "function" ? rng() : Math.random();
  if (!Number.isFinite(randomValue)) {
    randomValue = Math.random();
  }
  if (randomValue < 0) randomValue = 0;
  if (randomValue >= 1) randomValue = 1 - Number.EPSILON;
  const span = pool.length;
  const pick = Math.floor(randomValue * span);
  const index = Math.min(span - 1, Math.max(0, pick));
  return pool[index].mint;
}

let jupiterTokenMapPromise = null;
let jupiterTokenMap = null;

let tokenCatalogRefreshPromise = null;

function transformTokensApiRecord(entry) {
  if (!entry || typeof entry !== "object") return null;
  const programId = typeof entry.tokenProgram === "string" ? entry.tokenProgram : "";
  let program = "spl";
  if (/Tokenkeg/i.test(programId)) program = "spl";
  else if (/TokenzQd/i.test(programId) || /Token22/i.test(programId)) program = "token-2022";
  else if (entry.id === SOL_MINT) program = "native";
  const tags = Array.isArray(entry.tags) ? entry.tags : [];
  const lendTags = entry.apy?.jupEarn ? ["lend"] : [];
  return normaliseTokenRecord(
    {
      symbol: entry.symbol,
      mint: entry.id,
      decimals: entry.decimals ?? 0,
      program,
      tags: [...tags, ...lendTags],
    },
    "api"
  );
}

async function refreshTokenCatalogFromApi(options = {}) {
  if (tokenCatalogRefreshPromise) return tokenCatalogRefreshPromise;
  tokenCatalogRefreshPromise = (async () => {
    const query = typeof options.query === "string" ? options.query : " ";
    const limit = Number.isFinite(options.limit) ? options.limit : 2000;
    const url = new URL(JUPITER_TOKENS_SEARCH_ENDPOINT);
    url.searchParams.set("query", query);
    url.searchParams.set("limit", String(limit));
    if (options.cursor) url.searchParams.set("cursor", options.cursor);
    try {
      const resp = await fetch(url.toString(), {
        method: "GET",
        headers: { accept: "application/json" },
      });
      if (!resp.ok) {
        throw new Error(`Tokens API responded ${resp.status}: ${await resp.text()}`);
      }
      const data = await resp.json();
      if (!Array.isArray(data)) {
        throw new Error("Tokens API returned non-array payload");
      }
      const normalised = data
        .map((record) => transformTokensApiRecord(record))
        .filter(Boolean);
      if (normalised.length > 0) {
        rebuildTokenCatalog(normalised, "api");
      }
    } catch (err) {
      console.warn(
        paint(
          `Tokens API v2 refresh failed: ${err.message}`,
          "warn"
        )
      );
    } finally {
      tokenCatalogRefreshPromise = null;
    }
  })();
  return tokenCatalogRefreshPromise;
}

if (IS_MAIN_EXECUTION) {
  refreshTokenCatalogFromApi().catch(() => {});
}

function tokenBySymbol(symbol) {
  if (!symbol) return null;
  return TOKEN_CATALOG_BY_SYMBOL.get(symbol.toUpperCase()) || null;
}

function tokenHasTag(token, tag) {
  if (!token) return false;
  const lowered = tag.toLowerCase();
  return token.tags?.includes(lowered);
}

function mintBySymbol(symbol, fallbackMint) {
  const token = tokenBySymbol(symbol);
  if (token && token.mint) return token.mint;
  if (fallbackMint) return fallbackMint;
  throw new Error(`Token catalog is missing symbol ${symbol}`);
}

function pickRandomCatalogMint(options = {}) {
  const normalized = normaliseRandomMintOptions(options);
  const allowSol = options.allowSol === true || normalized.allowSol === true;
  const matchAnyTags =
    options.matchAnyTags === true || normalized.matchAnyTags === true;
  const includeTags = normalized.includeTags;
  const excludeTags = normalized.excludeTags;
  const excludeSymbols = new Set(normalized.excludeSymbols);
  const excludeMints = new Set(normalized.excludeMints);
  const rng = options.rng;

  const candidates = TOKEN_CATALOG.filter((entry) => {
    if (!entry || typeof entry.mint !== "string") return false;
    if (!allowSol && SOL_LIKE_MINTS.has(entry.mint)) return false;
    if (excludeMints.has(entry.mint)) return false;
    if (entry.symbol && excludeSymbols.has(entry.symbol)) return false;
    const tags = Array.isArray(entry.tags) ? entry.tags : [];
    if (includeTags.length > 0) {
      if (matchAnyTags) {
        if (!includeTags.some((tag) => tags.includes(tag))) return false;
      } else {
        for (const tag of includeTags) {
          if (!tags.includes(tag)) return false;
        }
      }
    }
    if (excludeTags.length > 0) {
      for (const tag of excludeTags) {
        if (tags.includes(tag)) return false;
      }
    }
    return true;
  });

  if (candidates.length === 0) {
    throw new Error("No matching tokens found in catalog for random selection");
  }

  const index = Math.min(
    candidates.length - 1,
    Math.floor(randomFloat(rng) * candidates.length)
  );
  return candidates[index];
}

async function loadJupiterTokenMap() {
  if (jupiterTokenMap) return jupiterTokenMap;
  if (jupiterTokenMapPromise) return jupiterTokenMapPromise;
  jupiterTokenMapPromise = (async () => {
    try {
      const resp = await fetch(JUPITER_TOKEN_LIST_URL, {
        method: "GET",
        headers: { "accept": "application/json" },
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const data = await resp.json();
      if (!Array.isArray(data)) {
        throw new Error("unexpected JSON shape");
      }
      const map = new Map();
      for (const item of data) {
        const address = typeof item.address === "string" ? item.address : null;
        if (!address) continue;
        map.set(address, {
          symbol:
            typeof item.symbol === "string" && item.symbol.trim()
              ? item.symbol.trim()
              : null,
          name:
            typeof item.name === "string" && item.name.trim()
              ? item.name.trim()
              : null,
          decimals:
            typeof item.decimals === "number" && Number.isFinite(item.decimals)
              ? item.decimals
              : null,
        });
      }
      jupiterTokenMap = map;
    } catch (err) {
      console.warn(
        paint(
          `Token list fetch warning: unable to load Jupiter catalog (${err.message || err}).`,
          "warn"
        )
      );
      jupiterTokenMap = new Map();
    }
    return jupiterTokenMap;
  })();
  return jupiterTokenMapPromise;
}

async function ensureMintInfo(mint) {
  if (!mint || SOL_LIKE_MINTS.has(mint)) return;
  if (TOKEN_CATALOG_BY_MINT.has(mint)) return;
  const known = KNOWN_MINTS.get(mint);
  if (known?.symbol) return;
  const cached = mintMetadataCache.get(mint);
  if (cached?.symbol) return;
  const external = await lookupExternalTokenInfo(mint);
  if (external?.symbol) {
    const symbol = external.symbol.toUpperCase();
    const decimals =
      typeof external.decimals === "number" && Number.isFinite(external.decimals)
        ? external.decimals
        : 0;
    KNOWN_MINTS.set(mint, {
      decimals,
      programId: null,
      symbol,
      name: external.name || symbol,
    });
    mintMetadataCache.set(mint, {
      decimals,
      programId: null,
      symbol,
      name: external.name || symbol,
    });
  }
}

async function lookupExternalTokenInfo(mint) {
  if (!mint) return null;
  try {
    const tokenMap = await loadJupiterTokenMap();
    return tokenMap.get(mint) || null;
  } catch (_) {
    return null;
  }
}

const USDC_MAINNET_MINT = mintBySymbol(
  "USDC",
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
);
const POPCAT_MINT = mintBySymbol(
  "POPCAT",
  "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
);
const PUMP_MINT = mintBySymbol(
  "PUMP",
  "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"
);
const PENGU_MINT = mintBySymbol(
  "PENGU",
  "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
);
const FARTCOIN_MINT = mintBySymbol(
  "FART",
  "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"
);
const WIF_MINT = mintBySymbol(
  "WIF",
  "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
);
const URANUS_MINT = mintBySymbol(
  "URANUS",
  "BFgdzMkTPdKKJeTipv2njtDEwhKxkgFueJQfJGt1jups"
);
const WBTC_MINT = mintBySymbol(
  "wBTC",
  "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh"
);
const CBBTC_MINT = mintBySymbol(
  "cbBTC",
  "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij"
);
const WETH_MINT = mintBySymbol(
  "wETH",
  "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"
);

const RPC_REQUEST_URL_STACK = globalThis.__jupiterSwapToolRpcStack || (globalThis.__jupiterSwapToolRpcStack = []);
const RPC_429_COUNTER = new Map();
const RPC_429_SUPPRESS_THRESHOLD = 3;
const originalConsoleError = console.error;
console.error = (...args) => {
  if (args.length > 0 && typeof args[0] === "string") {
    let message = args[0];
    if (message.startsWith("Server responded with")) {
      const endpointRaw = RPC_REQUEST_URL_STACK.length > 0
        ? RPC_REQUEST_URL_STACK[RPC_REQUEST_URL_STACK.length - 1]
        : null;
      if (endpointRaw) {
        let hostTag = endpointRaw;
        try {
          const urlObj = new URL(endpointRaw);
          hostTag = urlObj.host || urlObj.href;
        } catch (_) {}
        message = `[${hostTag}] ${message}`;
        args[0] = message;
      }
    }
    if (message.includes("429")) {
      const key = message.replace(/\s+\(x\d+\)$/, "");
      const nextCount = (RPC_429_COUNTER.get(key) || 0) + 1;
      RPC_429_COUNTER.set(key, nextCount);
      if (nextCount < RPC_429_SUPPRESS_THRESHOLD) {
        return;
      }
      if (nextCount % RPC_429_SUPPRESS_THRESHOLD === 0) {
        args[0] = `${key} (x${nextCount})`;
      } else {
        return;
      }
    }
  }
  return originalConsoleError(...args);
};


// ---- RPC endpoint rotation and health tracking ----
function loadRpcEndpoints() {
  const endpoints = [];
  const seen = new Set();

  const pushEndpoint = (value) => {
    if (!value) return;
    const trimmed = value.trim();
    if (!trimmed) return;
    if (seen.has(trimmed)) return;
    seen.add(trimmed);
    endpoints.push(trimmed);
  };

  pushEndpoint(process.env.RPC_URL);

  try {
    const raw = fs.readFileSync(RPC_LIST_FILE, 'utf8');
    const items = raw
      .split(/\r?\n|,/)
      .map((item) => item.trim())
      .filter((item) => item && !item.startsWith('#'));
    for (const item of items) pushEndpoint(item);
    if (items.length > 0) {
      RPC_ENDPOINTS_FILE_USED = RPC_LIST_FILE;
    }
  } catch (_) {
    // optional file
  }

  if (endpoints.length === 0) {
    pushEndpoint(DEFAULT_RPC_URL);
  }

  return endpoints;
}

const RPC_ENDPOINTS = loadRpcEndpoints();
let rpcEndpointCursor = 0;

// Flag an endpoint as temporarily unusable. The RPC rotation logic will
// skip it until the cooldown expires, preventing repeated 401/403 failures.
function markRpcEndpointUnhealthy(url, reason = "", options = {}) {
  if (!url) return;
  if (typeof reason === "object" && reason !== null && !Array.isArray(reason)) {
    options = reason;
    reason = options.reason || "";
  }
  const cooldownMs =
    typeof options.cooldownMs === "number"
      ? Math.max(1000, options.cooldownMs)
      : /rate[-\s]?limit/i.test(reason)
      ? RPC_RATE_LIMIT_COOLDOWN_MS
      : RPC_GENERAL_COOLDOWN_MS;
  if (!UNHEALTHY_RPC_ENDPOINTS.has(url)) {
    const note = reason ? ` (${reason})` : "";
    console.warn(paint(`RPC endpoint ${url} marked unhealthy${note}`, "warn"));
  }
  UNHEALTHY_RPC_ENDPOINTS.set(url, Date.now() + cooldownMs);
}

function nextRpcEndpoint() {
  if (RPC_ENDPOINTS.length === 0) return DEFAULT_RPC_URL;
  const total = RPC_ENDPOINTS.length;
  let fallbackUrl = null;
  let fallbackExpiry = Number.POSITIVE_INFINITY;
  for (let i = 0; i < total; i += 1) {
    const url = RPC_ENDPOINTS[rpcEndpointCursor];
    rpcEndpointCursor = (rpcEndpointCursor + 1) % total;
    const unhealthyUntil = UNHEALTHY_RPC_ENDPOINTS.get(url);
    if (unhealthyUntil && unhealthyUntil > Date.now()) {
      if (unhealthyUntil < fallbackExpiry) {
        fallbackExpiry = unhealthyUntil;
        fallbackUrl = url;
      }
      continue;
    }
    UNHEALTHY_RPC_ENDPOINTS.delete(url);
    return url;
  }
  if (fallbackUrl) {
    UNHEALTHY_RPC_ENDPOINTS.delete(fallbackUrl);
    return fallbackUrl;
  }
  return DEFAULT_RPC_URL;
}

// Open a new RPC connection using the next healthy endpoint. Each
// connection carries helpers so downstream code can mark an endpoint unhealthy
// when it encounters auth or websocket failures mid-run.
function createRpcConnection(commitment = 'confirmed', forcedEndpoint = null) {
  const url = forcedEndpoint || nextRpcEndpoint();
  const connection = new Connection(url, commitment);
  connection.__rpcEndpoint = url;
  connection.__markUnhealthy = (reason, options) => markRpcEndpointUnhealthy(url, reason, options);
  attachRpcRequestHook(connection);
  const ws = connection._rpcWebSocket;
  if (ws && typeof ws.on === 'function') {
    ws.on('error', (err) => {
      const msg = err?.message || String(err);
      if (/(?:^|\D)(403|401)(?:\D|$)/.test(msg)) markRpcEndpointUnhealthy(url, msg);
    });
    ws.on('close', (code) => {
      if (code === 4008 || code === 4103 || code === 403 || code === 401) {
        markRpcEndpointUnhealthy(url, `ws close code ${code}`);
      }
    });
    ws.on('error', () => {
      try {
        ws.close();
      } catch (_) {}
    });
  }
  return connection;
}

function attachRpcRequestHook(connection) {
  if (!connection || typeof connection._rpcRequest !== "function") return;
  if (connection.__rpcRequestHooked) return;
  const originalRpcRequest = connection._rpcRequest;
  connection._rpcRequest = async function patchedRpcRequest(...rpcArgs) {
    const label = this?.__rpcEndpoint || this?._rpcEndpoint || DEFAULT_RPC_URL;
    RPC_REQUEST_URL_STACK.push(label);
    try {
      return await originalRpcRequest.apply(this, rpcArgs);
    } finally {
      RPC_REQUEST_URL_STACK.pop();
    }
  };
  connection.__rpcRequestHooked = true;
}

const PRIMARY_RPC_URL = RPC_ENDPOINTS[0] || DEFAULT_RPC_URL;
const DEFAULT_USDC_MINT =
  process.env.USDC_MINT ||
  USDC_MAINNET_MINT;
const MINT_SYMBOL_OVERRIDES = new Map(
  TOKEN_CATALOG.filter((entry) => entry.symbol).map((entry) => [
    entry.mint,
    entry.symbol,
  ])
);
// Cache for share token → underlying asset mapping
const LEND_SHARE_TOKEN_MAP = new Map(); // shareTokenMint → underlyingAssetMint
let LEND_TOKENS_CACHE = null;
let LEND_TOKENS_CACHE_TIME = 0;
const LEND_TOKENS_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const LEND_BASE_ASSET_MINTS = new Set([
  SOL_MINT,
  DEFAULT_USDC_MINT,
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
  WBTC_MINT,
  CBBTC_MINT,
  WETH_MINT,
]);
const ADDITIONAL_LEND_SHARE_TOKENS = new Set([
  "2uQsyo1fXXQkDtcpXnLofWy88PxcvnfH2L8FPSE62FVU", // Additional lend share token to withdraw
]);
function isLendShareToken(tokenRecord) {
  const symbol = tokenRecord?.symbol || "";
  const mint = tokenRecord?.mint || "";
  return (
    (typeof symbol === "string" && symbol.toUpperCase().startsWith("JL")) ||
    ADDITIONAL_LEND_SHARE_TOKENS.has(mint)
  );
}

function symbolForMint(mint) {
  if (mint === SOL_MINT) return "SOL";
  const override = MINT_SYMBOL_OVERRIDES.get(mint);
  if (override) return override;
  const catalogEntry = TOKEN_CATALOG_BY_MINT.get(mint);
  if (catalogEntry?.symbol) return catalogEntry.symbol;
  const known = KNOWN_MINTS.get(mint);
  if (known?.symbol) return known.symbol;
  const cached = mintMetadataCache.get(mint);
  if (cached?.symbol) return cached.symbol;
  return mint.slice(0, 4);
}

function nameForMint(mint) {
  if (mint === SOL_MINT) return "Solana";
  const catalogEntry = TOKEN_CATALOG_BY_MINT.get(mint);
  if (catalogEntry?.name) return catalogEntry.name;
  const known = KNOWN_MINTS.get(mint);
  if (known?.name) return known.name;
  const cached = mintMetadataCache.get(mint);
  if (cached?.name) return cached.name;
  return symbolForMint(mint);
}

async function fetchPricesForMints(mints) {
  const ids = Array.from(
    new Set(
      mints
        .filter((mint) => typeof mint === "string" && mint.length > 0)
        .map((mint) => mint.trim())
    )
  );
  if (ids.length === 0) return {};
  const url = new URL(JUPITER_PRICE_ENDPOINT);
  url.searchParams.set("ids", ids.join(","));
  try {
    const resp = await fetch(url.toString(), {
      method: "GET",
      headers: { accept: "application/json" },
    });
    if (!resp.ok) {
      throw new Error(`Price API responded ${resp.status}: ${await resp.text()}`);
    }
    const data = await resp.json();
    if (!data || typeof data !== "object") return {};
    return data;
  } catch (err) {
    console.warn(
      paint(`Price API v3 fetch failed: ${err.message}`, "warn")
    );
    return {};
  }
}

function buildJsonApiHeaders(headers = {}, { includeUltraKey = false } = {}) {
  const baseHeaders = {
    accept: "application/json",
    "content-type": "application/json",
    ...(headers || {}),
  };
  if (includeUltraKey && SHOULD_SEND_ULTRA_HEADER) {
    baseHeaders["x-api-key"] = JUPITER_ULTRA_API_KEY;
  }
  return baseHeaders;
}

async function namespaceApiRequest({
  base,
  path,
  method = "POST",
  body,
  query,
  headers,
  includeUltraKey = false,
} = {}) {
  if (!base) throw new Error("Namespace API base is not defined");
  if (!path) throw new Error("Namespace API path is required");
  const url = new URL(
    path.startsWith("http")
      ? path
      : `${base.replace(/\/$/, "")}/${path.replace(/^\//, "")}`
  );
  if (query && typeof query === "object") {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) continue;
      url.searchParams.set(key, String(value));
    }
  }
  const baseHeaders = buildJsonApiHeaders(headers, { includeUltraKey });
  const init = {
    method,
    headers: baseHeaders,
  };
  if (method && method.toUpperCase() !== "GET") {
    init.body = JSON.stringify(body ?? {});
  }
  let response;
  let text = "";
  try {
    response = await fetch(url.toString(), init);
    text = await response.text();
  } catch (err) {
    throw new Error(`Failed to reach ${url.toString()}: ${err.message}`);
  }
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = null;
  }
  const headersObject = {};
  try {
    for (const [key, value] of response.headers.entries()) {
      headersObject[key] = value;
    }
  } catch (_) {}
  return {
    ok: response.ok,
    status: response.status,
    data: parsed,
    raw: text,
    headers: headersObject,
    url: url.toString(),
  };
}

function createNamespaceClient({ name, bases }) {
  const label = typeof name === "string" && name.trim().length > 0 ? name.trim() : "namespace";
  const prettyLabel = label.replace(/\b\w/g, (ch) => ch.toUpperCase());
  const uniqueBases = Array.from(
    new Set((Array.isArray(bases) ? bases : []).filter((base) => typeof base === "string" && base.trim().length > 0))
  );
  if (uniqueBases.length === 0) {
    throw new Error(`No API bases configured for ${label}`);
  }
  let index = 0;
  function currentBase() {
    return uniqueBases[Math.min(index, uniqueBases.length - 1)];
  }
  function advanceBase() {
    if (index < uniqueBases.length - 1) {
      index += 1;
      return true;
    }
    return false;
  }
  async function request(options = {}) {
    while (true) {
      const base = currentBase();
      const result = await namespaceApiRequest({ base, ...options });
      if (result.status === 404 && advanceBase()) {
        console.warn(
          paint(
            `  ${prettyLabel} endpoint ${base} returned 404; switching to ${currentBase()}`,
            "warn"
          )
        );
        continue;
      }
      return result;
    }
  }
  return {
    request,
    currentBase,
  };
}

const lendEarnClient = createNamespaceClient({
  name: "lend earn",
  bases: LEND_EARN_BASES,
});

const perpsClient = createNamespaceClient({
  name: "perps",
  bases: PERPS_API_BASES,
});

async function lendEarnRequest(options) {
  return lendEarnClient.request(options);
}

async function perpsApiRequest(options) {
  return perpsClient.request(options);
}

// Fetch Jupiter Lend tokens and populate share token mapping
async function refreshLendTokensCache() {
  try {
    const now = Date.now();
    if (LEND_TOKENS_CACHE && (now - LEND_TOKENS_CACHE_TIME) < LEND_TOKENS_CACHE_TTL) {
      return LEND_TOKENS_CACHE;
    }

    const result = await lendEarnRequest({
      path: "tokens",
      method: "GET",
    });

    if (result.ok && Array.isArray(result.data)) {
      LEND_TOKENS_CACHE = result.data;
      LEND_TOKENS_CACHE_TIME = now;

      // Update share token → underlying asset mapping
      LEND_SHARE_TOKEN_MAP.clear();
      for (const token of result.data) {
        if (token.address && token.assetAddress) {
          LEND_SHARE_TOKEN_MAP.set(token.address, token.assetAddress);
          LEND_BASE_ASSET_MINTS.add(token.assetAddress);
        }
      }

      return LEND_TOKENS_CACHE;
    }
  } catch (err) {
    console.warn(paint("  Warning: Failed to refresh Lend tokens cache:", "warn"), err.message);
  }
  return LEND_TOKENS_CACHE || [];
}

// Get underlying asset mint from share token mint
function getUnderlyingAssetForShareToken(shareTokenMint) {
  return LEND_SHARE_TOKEN_MAP.get(shareTokenMint) || null;
}

function parseCliOptions(tokens = []) {
  const options = {};
  const rest = [];
  for (let i = 0; i < tokens.length; i += 1) {
    const token = tokens[i];
    if (typeof token === "string" && token.startsWith("--")) {
      const key = token.slice(2);
      const next = tokens[i + 1];
      if (next && !next.startsWith("--")) {
        options[key] = next;
        i += 1;
      } else {
        options[key] = true;
      }
    } else if (token !== undefined) {
      rest.push(token);
    }
  }
  return { options, rest };
}

function findWalletByName(identifier) {
  const wallets = listWallets();
  const directMatch = wallets.find((w) => w.name === identifier);
  if (directMatch) return directMatch;

  const registryEntry = walletRegistry.resolveWalletIdentifier(identifier);
  if (registryEntry) {
    const byNumber = typeof registryEntry.number === "number"
      ? wallets.find((w) => w.number === registryEntry.number)
      : null;
    const byFilename = registryEntry.filename
      ? wallets.find((w) => w.name === registryEntry.filename)
      : null;
    const resolved = byNumber || byFilename;
    if (resolved) return resolved;
  }

  throw new Error(`Wallet ${identifier} not found in keypairs directory`);
}

async function resolveTokenRecord(input, { allowRefresh = true } = {}) {
  if (!input) return null;
  const trimmed = input.trim();
  if (trimmed.length === 0) return null;
  let candidate = tokenBySymbol(trimmed);
  if (!candidate) {
    const mintMatch = TOKEN_CATALOG_BY_MINT.get(trimmed);
    if (mintMatch) candidate = mintMatch;
  }
  if (!candidate && trimmed.toLowerCase() === "sol") {
    candidate = tokenBySymbol("SOL");
  }
  if (!candidate && allowRefresh) {
    try {
      await refreshTokenCatalogFromApi({ query: trimmed, limit: 200 });
    } catch (_) {}
    candidate = tokenBySymbol(trimmed) || TOKEN_CATALOG_BY_MINT.get(trimmed);
  }
  return candidate || null;
}

function logLendApiResult(action, result) {
  const tone = result.ok ? "success" : "warn";
  console.log(
    paint(
      `[lend] ${action}: ${result.status} ${result.ok ? "ok" : "error"}`,
      tone
    )
  );
  console.log(paint(`  url: ${result.url}`, "muted"));
  if (result.headers && result.headers["x-request-id"]) {
    console.log(
      paint(`  request-id: ${result.headers["x-request-id"]}`, "muted")
    );
  }
  if (result.data !== null && result.data !== undefined) {
    try {
      const pretty = JSON.stringify(result.data, null, 2);
      console.log(pretty);
    } catch (_) {
      console.log(result.data);
    }
  } else if (result.raw) {
    console.log(result.raw);
  }
  if (!result.ok && result.status === 404) {
    console.log(
      paint(
        "  Tip: the default Lend endpoint may be gated. Set JUPITER_LEND_API_BASE to the correct integrator URL if you have one.",
        "warn"
      )
    );
  }
}

function logPerpsApiResult(action, result) {
  const tone = result.ok ? "success" : "warn";
  console.log(
    paint(
      `[perps] ${action}: ${result.status} ${result.ok ? "ok" : "error"}`,
      tone
    )
  );
  console.log(paint(`  url: ${result.url}`, "muted"));
  if (result.headers && result.headers["x-request-id"]) {
    console.log(
      paint(`  request-id: ${result.headers["x-request-id"]}`, "muted")
    );
  }
  if (result.data !== null && result.data !== undefined) {
    try {
      const pretty = JSON.stringify(result.data, null, 2);
      console.log(pretty);
    } catch (_) {
      console.log(result.data);
    }
  } else if (result.raw) {
    console.log(result.raw);
  }
  if (!result.ok && result.status === 404) {
    console.log(
      paint(
        "  Tip: set JUPITER_PERPS_API_BASE to a valid integrator endpoint if the default lite endpoint is unavailable.",
        "warn"
      )
    );
  }
}

function coerceCliBoolean(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    if (!trimmed) return false;
    return ["true", "t", "1", "yes", "y", "on"].includes(trimmed);
  }
  return false;
}

function normalizePerpsSide(value) {
  if (!value) return "long";
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "long" || normalized === "short") {
    return normalized;
  }
  if (normalized === "buy") return "long";
  if (normalized === "sell") return "short";
  throw new Error(
    `Unknown perps side '${value}'. Expected long/short or buy/sell.`
  );
}

function formatPerpsMarketSnippet(entry) {
  if (!entry || typeof entry !== "object") return "unknown market";
  const symbol =
    entry.symbol || entry.market || entry.name || entry.id || "unknown";
  const pair =
    entry.pair ||
    (entry.baseAsset && entry.quoteAsset
      ? `${entry.baseAsset}/${entry.quoteAsset}`
      : null);
  const leverage = entry.maxLeverage || entry.leverage || entry.maxLev;
  const status = entry.status || entry.state;
  const funding =
    entry.fundingRate ?? entry.currentFundingRate ?? entry.funding ?? null;
  const parts = [symbol];
  if (pair) parts.push(pair);
  if (leverage !== undefined && leverage !== null) {
    parts.push(`max ${leverage}x`);
  }
  if (status) parts.push(status);
  if (funding !== null && funding !== undefined) {
    parts.push(`funding ${funding}`);
  }
  return parts.join(" · ");
}

function formatPerpsPositionSnippet(entry) {
  if (!entry || typeof entry !== "object") return "position";
  const market = entry.market || entry.symbol || entry.pair || entry.ticker;
  const side = entry.side || entry.direction || entry.positionSide;
  const size = entry.size ?? entry.baseSize ?? entry.positionSize;
  const notional = entry.notional ?? entry.notionalUsd ?? entry.value;
  const pnl =
    entry.unrealizedPnl ?? entry.unrealizedPnL ?? entry.pnlUsd ?? entry.pnl;
  const leverage = entry.leverage || entry.currentLeverage;
  const pieces = [market || "?"];
  if (side) pieces.push(String(side).toLowerCase());
  if (size !== undefined && size !== null) pieces.push(`size ${size}`);
  if (notional !== undefined && notional !== null)
    pieces.push(`notional ${notional}`);
  if (leverage !== undefined && leverage !== null)
    pieces.push(`${leverage}x`);
  if (pnl !== undefined && pnl !== null) pieces.push(`PnL ${pnl}`);
  return pieces.join(" · ");
}

function parseJsonOption(value, label, { expectObject = false } = {}) {
  if (value === undefined || value === null) return {};
  if (typeof value !== "string") {
    throw new Error(`${label} must be provided as a JSON string`);
  }
  try {
    const parsed = JSON.parse(value);
    if (expectObject) {
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error(`${label} must be a JSON object`);
      }
    }
    return parsed;
  } catch (err) {
    if (err instanceof Error && err.message.startsWith(`${label} must`)) {
      throw err;
    }
    throw new Error(`${label} must be valid JSON (${err.message})`);
  }
}

function buildPerpsRequest(action, payload, options, { defaultPath, defaultMethod = "POST" } = {}) {
  if (!defaultPath) {
    throw new Error(`Perps ${action} handler is missing a default path`);
  }
  const normalizedOptions = options && typeof options === "object" ? options : {};
  const methodRaw = normalizedOptions.method;
  const method = methodRaw
    ? String(methodRaw).trim().toUpperCase()
    : (defaultMethod || "POST").toUpperCase();
  const pathRaw = normalizedOptions.path;
  const path = pathRaw ? String(pathRaw).trim() : defaultPath;
  const request = { path, method };
  if (method === "GET") {
    if (payload !== undefined) {
      request.query = payload;
    }
  } else if (payload !== undefined) {
    request.body = payload;
  }
  return request;
}

function resolveWalletIdentifiers(inputs) {
  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw new Error("At least one wallet identifier is required");
  }
  const expanded = [];
  for (const item of inputs) {
    const trimmed = item.trim();
    if (trimmed.length === 0) continue;
    for (const part of trimmed.split(",")) {
      const sub = part.trim();
      if (sub.length === 0) continue;
      expanded.push(sub);
    }
  }
  if (expanded.length === 0) {
    throw new Error("At least one wallet identifier is required");
  }
  const existing = listWallets();
  const walletsByName = new Map(existing.map((wallet) => [wallet.name, wallet]));
  const walletsByNumber = new Map(
    existing.filter((wallet) => typeof wallet.number === "number").map((wallet) => [wallet.number, wallet])
  );

  return expanded.map((item) => {
    const trimmed = item.trim();
    if (!trimmed) {
      throw new Error("Wallet identifier cannot be empty");
    }

    // Direct filename match
    const namedWallet = walletsByName.get(trimmed);
    if (namedWallet) {
      return { name: namedWallet.name, pubkey: namedWallet.kp.publicKey.toBase58() };
    }

    // Registry lookup (supports numeric identifiers)
    const registryEntry = walletRegistry.resolveWalletIdentifier(trimmed);
    if (registryEntry) {
      const byNumber = typeof registryEntry.number === "number" ? walletsByNumber.get(registryEntry.number) : null;
      const byFilename = registryEntry.filename ? walletsByName.get(registryEntry.filename) : null;
      const resolvedWallet = byNumber || byFilename;
      if (resolvedWallet) {
        return {
          name: resolvedWallet.name,
          pubkey: resolvedWallet.kp.publicKey.toBase58(),
        };
      }
    }

    // Public key fallback
    try {
      const pk = new PublicKey(trimmed);
      return { name: trimmed, pubkey: pk.toBase58() };
    } catch (err) {
      throw new Error(`Invalid wallet identifier ${trimmed}: ${err.message}`);
    }
  });
}

async function handleLendCommand(args) {
  const categoryRaw = args[0];
  if (!categoryRaw) {
    console.log(
      "lend usage: lend earn <action> [...options] | lend overview (borrow coming soon)"
    );
    return;
  }
  console.log(
    paint(
      "Jupiter Lend API integration is experimental — all requests/responses are logged verbatim. Proceed carefully.",
      "warn"
    )
  );
  const category = categoryRaw.toLowerCase();
  if (category === "overview" || category === "snapshot") {
    await lendOverviewAllWallets();
    return;
  }
  if (category === "borrow") {
    console.log(
      paint(
        "Borrow endpoints are temporarily unavailable (coming soon).",
        "warn"
      )
    );
    return;
  }
  if (category !== "earn") {
    throw new Error(
      `Unknown lend category '${categoryRaw}'. Expected 'earn' or 'overview'.`
    );
  }
  const rest = args.slice(1);
  await handleLendEarnCommand(rest);
}

async function handlePerpsCommand(args) {
  const actionRaw = args[0];
  if (!actionRaw) {
    console.log(
      "perps usage: perps <markets|positions|open|close> [...options]"
    );
    return;
  }
  console.log(
    paint(
      "Perpetual trading carries liquidation and leverage risk. Review every payload before broadcasting transactions.",
      "warn"
    )
  );
  const action = actionRaw.toLowerCase();
  const rest = args.slice(1);
  if (action === "markets" || action === "market") {
    await handlePerpsMarkets(rest);
    return;
  }
  if (action === "positions" || action === "position") {
    await handlePerpsPositions(rest);
    return;
  }
  if (action === "open") {
    await handlePerpsOpen(rest);
    return;
  }
  if (action === "close" || action === "exit") {
    await handlePerpsClose(rest);
    return;
  }
  throw new Error(
    `Unknown perps action '${actionRaw}'. Expected 'markets', 'positions', 'open', or 'close'.`
  );
}

async function handlePerpsMarkets(args) {
  const { options } = parseCliOptions(args);

  // perps-api.jup.ag uses /jlp-info to get all market data
  const request = buildPerpsRequest(
    "markets",
    undefined,  // jlp-info requires no parameters
    options,
    { defaultPath: "jlp-info", defaultMethod: "GET" }
  );

  try {
    const result = await perpsApiRequest(request);

    if (!result.ok || !result.data) {
      console.log(paint("  No market data available.", "muted"));
      if (result.status === 404) {
        console.log(paint("  Tip: perps-api.jup.ag may not have a markets endpoint. Try checking individual pools.", "muted"));
      }
      return;
    }

    // Extract custodies array from JLP info response
    const custodies = result.data.custodies || [];
    if (!Array.isArray(custodies) || custodies.length === 0) {
      console.log(paint("  No markets/custodies found.", "muted"));
      return;
    }

    console.log(paint(`\n  Available Jupiter Perps Markets (${custodies.length}):\n`, "info"));

    // Display markets in a table format
    for (const custody of custodies) {
      const symbol = custody.symbol || custody.tokenSymbol || 'Unknown';
      const mint = custody.mint || custody.address || 'N/A';
      const oracle = custody.oraclePrice || custody.price || 'N/A';
      const aum = custody.assets?.owned || custody.aumUsd || 'N/A';

      console.log(paint(`    ${symbol.padEnd(10)} `, "label") + paint(`${mint.substring(0, 8)}...`, "muted"));
      if (typeof oracle === 'number') {
        console.log(paint(`               Price: $${oracle.toFixed(4)}`, "muted"));
      }
      if (typeof aum === 'number') {
        console.log(paint(`               AUM: $${(aum / 1e6).toFixed(2)}M`, "muted"));
      }
      console.log('');
    }

    // Show JLP pool stats if available
    if (result.data.price || result.data.supply) {
      console.log(paint('  JLP Pool Stats:', 'info'));
      if (result.data.price) {
        console.log(paint(`    JLP Price: $${result.data.price.toFixed(4)}`, 'muted'));
      }
      if (result.data.supply) {
        console.log(paint(`    JLP Supply: ${(result.data.supply / 1e6).toFixed(2)}M`, 'muted'));
      }
      if (result.data.apy) {
        console.log(paint(`    APY: ${(result.data.apy * 100).toFixed(2)}%`, 'muted'));
      }
    }

  } catch (err) {
    console.error(paint("Perps markets request failed:", "error"), err.message);
  }
}

async function handlePerpsPositions(args) {
  const { options, rest } = parseCliOptions(args);
  let inputs = rest.slice();
  if (inputs.length === 0 && typeof options.wallet === "string") {
    inputs = [options.wallet];
  }
  if (inputs.length === 0 && typeof options.wallets === "string") {
    inputs = options.wallets.split(",");
  }
  const wantsAll =
    coerceCliBoolean(options.all) ||
    coerceCliBoolean(options["all-wallets"]) ||
    inputs.some((item) => typeof item === "string" && item.trim() === "*");
  if (wantsAll) {
    inputs = listWallets().map((wallet) => wallet.name);
  }
  if (!inputs || inputs.length === 0) {
    throw new Error(
      "perps positions usage: perps positions <walletName|pubkey>[,...] [--market <market>] [--all]"
    );
  }
  const identifiers = resolveWalletIdentifiers(inputs);
  const walletNameMap = new Map(
    identifiers.map((entry) => [entry.pubkey, entry.name || entry.pubkey])
  );
  if (wantsAll) {
    console.log(
      paint(
        `  Targeting all ${identifiers.length} wallet(s) for perps positions.`,
        "muted"
      )
    );
  } else if (identifiers.length > 1) {
    console.log(
      paint(
        `  Targeting ${identifiers.length} wallet(s) for perps positions.`,
        "muted"
      )
    );
  }
  const scopeNames = identifiers
    .map((entry) => entry.name || entry.pubkey)
    .filter(Boolean);
  if (scopeNames.length > 0 && scopeNames.length <= 5) {
    console.log(paint(`  Wallet scope: ${scopeNames.join(", ")}`, "muted"));
  }

  // perps-api.jup.ag requires walletAddress (singular), so query each wallet individually
  const allEntries = [];
  for (const identifier of identifiers) {
    const payload = {
      walletAddress: identifier.pubkey,  // API uses walletAddress (singular)
    };
    if (options.market) payload.market = options.market;
    if (options.markets) payload.market = options.markets;
    if (options.status) payload.status = options.status;
    if (options.side) payload.side = options.side;
    if (options.leverage) payload.leverage = options.leverage;
    if (options.limit !== undefined) {
      const parsedLimit = parseInt(options.limit, 10);
      if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
        throw new Error("perps positions: --limit must be a positive integer");
      }
      payload.limit = parsedLimit;
    }

    const request = buildPerpsRequest(
      "positions",
      payload,
      options,
      { defaultPath: "positions", defaultMethod: "GET" }
    );

    try {
      const result = await perpsApiRequest(request);
      if (result.ok && result.data) {
        // perps-api.jup.ag returns { count, dataList }
        const positions = result.data.dataList || result.data || [];
        if (Array.isArray(positions)) {
          // Tag each position with wallet info
          positions.forEach(pos => {
            pos._walletName = identifier.name || identifier.pubkey;
            pos._walletPubkey = identifier.pubkey;
          });
          allEntries.push(...positions);
        }
      }
    } catch (err) {
      console.warn(paint(`  Warning: Failed to fetch positions for ${identifier.name || identifier.pubkey}: ${err.message}`, "warn"));
    }
  }

  const entries = allEntries;
  if (!Array.isArray(entries) || entries.length === 0) {
    console.log(
      paint(
        "  No perps positions found for the requested wallet scope.",
        "muted"
      )
    );
    console.log(paint("  Note: Newly opened positions may take a few moments to appear in the API.", "muted"));
    return;
  }

  const owners = new Map();
  for (const entry of entries) {
    const label = entry._walletName || entry._walletPubkey;
    if (!label) continue;
    owners.set(label, (owners.get(label) || 0) + 1);
  }
    console.log(
      paint(
        `  Found ${entries.length} open position(s) across ${owners.size || 1} wallet(s).`,
        "info"
      )
    );

    // Show detailed position cards instead of one-line snippets
    for (const entry of entries) {
      const ownerKey =
        normalizeWalletIdentifier(entry.owner) ||
        normalizeWalletIdentifier(entry.wallet) ||
        normalizeWalletIdentifier(entry.account);
      const walletLabel = ownerKey && walletNameMap.get(ownerKey)
        ? walletNameMap.get(ownerKey)
        : ownerKey || entry._walletName || "unknown";

      console.log(paint(`\n  ━━━ Position: ${walletLabel} ━━━`, "info"));

      // Position ID
      const positionId = entry.positionPubkey || entry.position || entry.id || entry.pubkey;
      if (positionId) {
        console.log(paint(`    Position ID: ${positionId}`, "muted"));
      }

      // Market
      const market = entry.market || entry.symbol || entry.pair || "Unknown";
      console.log(paint(`    Market: ${market}`, "info"));

      // Side
      const side = entry.side || entry.direction || entry.positionSide;
      if (side) {
        const sideLabel = String(side).toUpperCase();
        const sideColor = sideLabel === "LONG" ? "success" : "warn";
        console.log(paint(`    Direction: ${sideLabel}`, sideColor));
      }

      // Leverage
      const leverage = entry.leverage || entry.currentLeverage;
      if (leverage) {
        console.log(paint(`    Leverage: ${leverage}x`, "info"));
      }

      // Entry Price
      const entryPrice = entry.entryPrice || entry.entryPriceUsd || entry.avgEntryPrice;
      if (entryPrice) {
        console.log(paint(`    Entry Price: $${parseFloat(entryPrice).toFixed(2)}`, "muted"));
      }

      // Current/Mark Price
      const markPrice = entry.markPrice || entry.currentPrice || entry.markPriceUsd;
      if (markPrice) {
        console.log(paint(`    Mark Price: $${parseFloat(markPrice).toFixed(2)}`, "muted"));
      }

      // Liquidation Price
      const liqPrice = entry.liquidationPrice || entry.liquidationPriceUsd || entry.liqPrice;
      if (liqPrice) {
        console.log(paint(`    Liquidation Price: $${parseFloat(liqPrice).toFixed(2)} ⚠️`, "warn"));

        // Calculate distance to liquidation
        if (markPrice && entryPrice) {
          const currentPrice = parseFloat(markPrice);
          const liquidationPrice = parseFloat(liqPrice);
          const distance = Math.abs(currentPrice - liquidationPrice);
          const distancePct = ((distance / currentPrice) * 100).toFixed(2);
          console.log(paint(`    Liquidation Distance: ${distancePct}%`, "muted"));
        }
      }

      // Position Size
      const posSize = entry.positionSize || entry.positionSizeUsd || entry.notionalUsd || entry.notional;
      if (posSize) {
        console.log(paint(`    Position Size: $${parseFloat(posSize).toFixed(2)}`, "info"));
      }

      // Collateral
      const collateral = entry.collateral || entry.collateralUsd || entry.marginUsd;
      if (collateral) {
        console.log(paint(`    Collateral: $${parseFloat(collateral).toFixed(2)}`, "muted"));
      }

      // PnL
      const pnl = entry.unrealizedPnl || entry.unrealizedPnL || entry.pnlUsd || entry.pnl;
      if (pnl !== undefined && pnl !== null) {
        const pnlValue = parseFloat(pnl);
        const pnlColor = pnlValue >= 0 ? "success" : "error";
        const pnlSign = pnlValue >= 0 ? "+" : "";
        console.log(paint(`    Unrealized PnL: ${pnlSign}$${pnlValue.toFixed(2)}`, pnlColor));

        // Show PnL percentage if we have collateral
        if (collateral) {
          const pnlPct = ((pnlValue / parseFloat(collateral)) * 100).toFixed(2);
          console.log(paint(`    PnL %: ${pnlSign}${pnlPct}%`, pnlColor));
        }
      }

      // Close instructions
      console.log(paint(`    💡 Close: node cli_trader.js perps close ${walletLabel} --market ${market}`, "muted"));

      // Show raw data if DEBUG mode
      if (process.env.DEBUG_PERPS || process.env.VERBOSE) {
        console.log(paint("    Raw data:", "muted"));
        console.log(JSON.stringify(entry, null, 2));
      }
    }
}

async function handlePerpsOpen(args) {
  const { options, rest } = parseCliOptions(args);
  let walletName = options.wallet || options.owner;
  let market = options.market || options.pair;
  let sideRaw = options.side || options.direction;
  let sizeRaw = options.size ?? options.amount ?? options.notional;
  let priceRaw = options.price ?? options.limit;
  if (!walletName && rest.length > 0) walletName = rest[0];
  if (!market && rest.length > 1) market = rest[1];
  if (!sideRaw && rest.length > 2) sideRaw = rest[2];
  if (sizeRaw === undefined && rest.length > 3) sizeRaw = rest[3];
  if (priceRaw === undefined && rest.length > 4) priceRaw = rest[4];
  walletName = walletName ? String(walletName).trim() : "";
  market = market ? String(market).trim() : "";
  const sizeValue = sizeRaw !== undefined ? String(sizeRaw).trim() : "";
  const priceValue = priceRaw !== undefined ? String(priceRaw).trim() : "";
  if (!walletName || !market || !sideRaw || !sizeValue) {
    throw new Error(
      "perps open usage: perps open <walletFile|'all'> <market> <side> <size|'max'> [price] [--options ...]"
    );
  }

  // Handle "all" wallets
  let wallets = [];
  if (walletName.toLowerCase() === 'all' || walletName === '*') {
    wallets = listWallets();
    console.log(paint(`  Processing ALL ${wallets.length} wallets...`, "info"));
  } else {
    const wallet = findWalletByName(walletName);
    wallets = [wallet];
  }

  // Process each wallet
  for (const wallet of wallets) {
    // Calculate collateral for this wallet
    let usdcCollateral = 0;
    let collateralTokenDelta = "0";

    if (sizeValue.toLowerCase() === 'max') {
      // Step 1: Check SOL balance
      try {
        const connection = createRpcConnection("confirmed");
        const balance = await connection.getBalance(wallet.kp.publicKey);
        const solBalance = balance / 1e9;

        // Reserve 0.01 SOL for gas (perps transactions need gas)
        const gasReserve = 0.01;
        const availableSol = Math.max(0, solBalance - gasReserve);

        console.log(paint(`  ${wallet.name}: ${solBalance.toFixed(4)} SOL available`, "muted"));

        if (availableSol < 0.001) {
          console.warn(paint(`  Skipping ${wallet.name}: insufficient SOL for swap`, "warn"));
          continue;
        }

        // Fetch SOL price to check if we'll meet $10 minimum before swapping
        const priceData = await fetchPricesForMints([SOL_MINT]);
        const solPrice = priceData[SOL_MINT]?.usdPrice || 0;

        if (!solPrice || solPrice <= 0) {
          console.warn(paint(`  Skipping ${wallet.name}: unable to fetch SOL price`, "warn"));
          continue;
        }

        // Calculate estimated USD value
        const estimatedUsdValue = availableSol * solPrice;

        // Check $10 minimum BEFORE swapping to avoid wasting gas
        if (estimatedUsdValue < 10) {
          console.warn(paint(`  Skipping ${wallet.name}: ${availableSol.toFixed(4)} SOL ≈ $${estimatedUsdValue.toFixed(2)} (need $10 minimum)`, "warn"));
          continue;
        }

        console.log(paint(`    Estimated value: $${estimatedUsdValue.toFixed(2)} (${availableSol.toFixed(4)} SOL × $${solPrice.toFixed(2)})`, "muted"));

        // Step 2: Swap SOL → USDC
        console.log(paint(`    Swapping ${availableSol.toFixed(4)} SOL → USDC...`, "muted"));

        const swapAmount = Math.floor(availableSol * 1e9); // Convert to lamports

        // Create Ultra swap order for SOL→USDC
        const orderResult = await createUltraOrder({
          inputMint: SOL_MINT,
          outputMint: DEFAULT_USDC_MINT,
          amountLamports: swapAmount,
          userPublicKey: wallet.kp.publicKey.toBase58(),
          slippageBps: 50, // 0.5% slippage
          wrapAndUnwrapSol: true
        });

        if (!orderResult || !orderResult.swapTransaction) {
          console.warn(paint(`  Skipping ${wallet.name}: swap quote failed`, "warn"));
          continue;
        }

        // Execute the swap - sign and submit the transaction
        try {
          const txbuf = Buffer.from(orderResult.swapTransaction, 'base64');
          const vtx = VersionedTransaction.deserialize(txbuf);
          vtx.sign([wallet.kp]);
          const rawSigned = vtx.serialize();
          const signedBase64 = Buffer.from(rawSigned).toString('base64');
          const derivedSignature = bs58.encode(vtx.signatures[0]);

          const executeResult = await executeUltraSwap({
            signedTransaction: signedBase64,
            clientOrderId: orderResult.clientOrderId,
            signatureHint: derivedSignature
          });

          if (!executeResult || !executeResult.signature) {
            console.warn(paint(`  Skipping ${wallet.name}: swap execution failed`, "warn"));
            continue;
          }

          console.log(paint(`    ✅ Swap submitted: ${executeResult.signature}`, "muted"));
        } catch (err) {
          console.warn(paint(`  Skipping ${wallet.name}: swap execution error - ${err.message}`, "warn"));
          continue;
        }

        // USDC has 6 decimals
        const usdcAmount = (orderResult.outAmount || 0);
        usdcCollateral = usdcAmount / 1e6;

        console.log(paint(`    ✅ Received $${usdcCollateral.toFixed(2)} USDC (→ swap back to SOL for collateral)`, "success"));

        // Safety check: verify USDC amount still meets minimum after slippage
        if (usdcCollateral < 10) {
          console.warn(paint(`  Skipping ${wallet.name}: swap slippage resulted in $${usdcCollateral.toFixed(2)} (below $10 minimum)`, "warn"));
          continue;
        }

        // Now swap USDC back to SOL for use as collateral (Jupiter Perps requires collateral in market token)
        console.log(paint(`    Swapping USDC → SOL for collateral...`, "muted"));

        try {
          const reverseOrderResult = await createUltraOrder({
            inputMint: DEFAULT_USDC_MINT,
            outputMint: SOL_MINT,
            amountLamports: usdcAmount,
            userPublicKey: wallet.kp.publicKey.toBase58(),
            slippageBps: 50,
            wrapAndUnwrapSol: true
          });

          if (!reverseOrderResult || !reverseOrderResult.swapTransaction) {
            console.warn(paint(`  Skipping ${wallet.name}: reverse swap quote failed`, "warn"));
            continue;
          }

          // Sign and execute the reverse swap
          const rtxbuf = Buffer.from(reverseOrderResult.swapTransaction, 'base64');
          const rvtx = VersionedTransaction.deserialize(rtxbuf);
          rvtx.sign([wallet.kp]);
          const rrawSigned = rvtx.serialize();
          const rsignedBase64 = Buffer.from(rrawSigned).toString('base64');
          const rderividSignature = bs58.encode(rvtx.signatures[0]);

          const reverseExecuteResult = await executeUltraSwap({
            signedTransaction: rsignedBase64,
            clientOrderId: reverseOrderResult.clientOrderId,
            signatureHint: rderividSignature
          });

          if (!reverseExecuteResult || !reverseExecuteResult.signature) {
            console.warn(paint(`  Skipping ${wallet.name}: reverse swap execution failed`, "warn"));
            continue;
          }

          console.log(paint(`    ✅ Reverse swap submitted: ${reverseExecuteResult.signature}`, "muted"));

          // Now we have SOL collateral - calculate from the swap output
          const solCollateralLamports = reverseOrderResult.outAmount || 0;
          collateralTokenDelta = solCollateralLamports.toString();

          console.log(paint(`    ✅ Received ${(solCollateralLamports / 1e9).toFixed(4)} SOL collateral`, "success"));
        } catch (err) {
          console.warn(paint(`  Skipping ${wallet.name}: reverse swap error - ${err.message}`, "warn"));
          continue;
        }

      } catch (err) {
        console.warn(paint(`  Skipping ${wallet.name}: swap failed - ${err.message}`, "warn"));
        continue;
      }
    } else {
      // Manual size specified - assume it's USDC collateral amount
      usdcCollateral = parseFloat(sizeValue);
      collateralTokenDelta = Math.floor(usdcCollateral * 1e6).toString();

      if (usdcCollateral < 10) {
        console.warn(paint(`  Skipping ${wallet.name}: need $10 minimum collateral for new positions`, "warn"));
        continue;
      }
    }

    const payload = {
      wallet: wallet.kp.publicKey.toBase58(),
      market,
      side: normalizePerpsSide(sideRaw),
      collateral: usdcCollateral,  // USDC collateral amount
    };
    if (priceValue) payload.price = priceValue;
    if (options.leverage !== undefined) payload.leverage = String(options.leverage).trim();
    if (options.margin !== undefined) payload.margin = String(options.margin).trim();
    if (options.intent !== undefined) payload.intent = String(options.intent).trim();
    if (options.type !== undefined) payload.type = String(options.type).trim();
    const tif = options["time-in-force"] || options.timeInForce || options.tif;
    if (tif !== undefined) payload.timeInForce = String(tif).trim();
    const clientOrderId =
      options["client-order-id"] || options.clientOrderId || options.cid;
    if (clientOrderId !== undefined) {
      payload.clientOrderId = String(clientOrderId).trim();
    }
    if (options.triggerPrice !== undefined || options["trigger-price"] !== undefined) {
      const triggerValue = options.triggerPrice ?? options["trigger-price"];
      if (triggerValue !== undefined) {
        payload.triggerPrice = String(triggerValue).trim();
      }
    }
    if (options.takeProfit !== undefined || options["take-profit"] !== undefined) {
      const tp = options.takeProfit ?? options["take-profit"];
      if (tp !== undefined) payload.takeProfit = String(tp).trim();
    }
    if (options.stopLoss !== undefined || options["stop-loss"] !== undefined) {
      const sl = options.stopLoss ?? options["stop-loss"];
      if (sl !== undefined) payload.stopLoss = String(sl).trim();
    }
    if (coerceCliBoolean(options["reduce-only"]) || coerceCliBoolean(options.reduceOnly)) {
      payload.reduceOnly = true;
    }
    if (coerceCliBoolean(options["post-only"]) || coerceCliBoolean(options.postOnly)) {
      payload.postOnly = true;
    }
    if (
      coerceCliBoolean(options["immediate-or-cancel"]) ||
      coerceCliBoolean(options.immediateOrCancel) ||
      coerceCliBoolean(options.ioc) ||
      coerceCliBoolean(options.fok) ||
      coerceCliBoolean(options.fillOrKill)
    ) {
      payload.immediateOrCancel = true;
    }
    if (options.extra !== undefined || options.body !== undefined || options.params !== undefined) {
      const extra = parseJsonOption(
        options.extra ?? options.body ?? options.params,
        "perps open extra",
        { expectObject: true }
      );
      if (extra && Object.keys(extra).length > 0) {
        Object.assign(payload, extra);
      }
    }
    // Calculate position size with leverage
    const leverageNum = payload.leverage ? parseFloat(payload.leverage) : 5;
    const positionSizeUsd = usdcCollateral * leverageNum;

    console.log(
      paint(
        `  ${wallet.name}: Opening ${payload.side} $${positionSizeUsd.toFixed(2)} position (${leverageNum}x leverage)`,
        "info"
      )
    );

    const dryRun =
      coerceCliBoolean(options["dry-run"]) || coerceCliBoolean(options.dryRun);
    if (dryRun) {
      console.log(paint("  Dry-run enabled — would open position:", "warn"));
      console.log(paint(`    Collateral: $${usdcCollateral.toFixed(2)} USDC`, "muted"));
      console.log(paint(`    Position: $${positionSizeUsd.toFixed(2)} ${payload.market}`, "muted"));
      console.log(paint(`    Leverage: ${leverageNum}x`, "muted"));
      continue;
    }

    // perps-api.jup.ag uses /positions/increase endpoint
    // Build proper payload according to OpenAPI spec

    // Get market token info (what we're trading - SOL, ETH, etc.)
    const marketToken = TOKEN_CATALOG.find(t =>
      t.symbol === payload.market.toUpperCase() ||
      t.mint === payload.market
    );
    const marketMint = marketToken?.mint || SOL_MINT;

    // API payload - collateral mint must match market mint for long positions
    // collateralTokenDelta is now SOL lamports (from reverse swap)
    const apiPayload = {
      walletAddress: payload.wallet,
      side: payload.side.toLowerCase(),  // 'long' or 'short' (lowercase!)
      sizeUsd: positionSizeUsd,  // Position size = collateral × leverage
      leverage: leverageNum.toString(),  // Leverage as string
      collateralTokenDelta: collateralTokenDelta,  // SOL amount in lamports (9 decimals)
      inputMint: marketMint,  // Collateral token (SOL for SOL markets, etc.)
      collateralMint: marketMint,  // Must match market mint for long positions
      marketMint: marketMint,  // Market we're trading (SOL, ETH, etc.)
      maxSlippageBps: "50",  // 0.5% slippage tolerance (as string!)
    };

    // Add optional parameters
    if (payload.price) apiPayload.triggerPrice = parseFloat(payload.price);
    if (payload.takeProfit) {
      if (!apiPayload.tpsl) apiPayload.tpsl = [];
      apiPayload.tpsl.push({
        type: 'takeProfit',
        triggerPrice: parseFloat(payload.takeProfit)
      });
    }
    if (payload.stopLoss) {
      if (!apiPayload.tpsl) apiPayload.tpsl = [];
      apiPayload.tpsl.push({
        type: 'stopLoss',
        triggerPrice: parseFloat(payload.stopLoss)
      });
    }

    const request = buildPerpsRequest(
      "increase-position",
      apiPayload,
      options,
      { defaultPath: "positions/increase", defaultMethod: "POST" }
    );

    // Debug logging (only in dev mode)
    if (process.env.DEBUG_PERPS) {
      console.log(paint(`  Debug payload for ${wallet.name}:`, "muted"));
      console.log(JSON.stringify(apiPayload, null, 2));
    }

    try {
      const result = await perpsApiRequest(request);

      if (result.ok && result.data) {
        console.log(paint(`\n  ✅ ${wallet.name}: Position opened successfully!`, "success"));

        // Extract readable position info from the quote
        const quote = result.data.quote || {};
        const positionPubkey = result.data.positionPubkey;

        if (positionPubkey) {
          console.log(paint(`    Position ID: ${positionPubkey}`, "info"));
        }

        // Show key position metrics in a user-friendly format
        if (quote.side) {
          const sideLabel = String(quote.side).toUpperCase();
          const sideColor = sideLabel === "LONG" ? "success" : "warn";
          console.log(paint(`    Direction: ${sideLabel}`, sideColor));
        }

        if (quote.leverage) {
          console.log(paint(`    Leverage: ${quote.leverage}x`, "info"));
        }

        if (quote.entryPriceUsd) {
          console.log(paint(`    Entry Price: $${parseFloat(quote.entryPriceUsd).toFixed(2)}`, "muted"));
        }

        if (quote.liquidationPriceUsd) {
          const liqPrice = parseFloat(quote.liquidationPriceUsd).toFixed(2);
          console.log(paint(`    Liquidation Price: $${liqPrice} ⚠️`, "warn"));
        }

        if (quote.positionCollateralSizeUsd) {
          const collateral = parseFloat(quote.positionCollateralSizeUsd).toFixed(2);
          console.log(paint(`    Collateral: $${collateral}`, "muted"));
        }

        if (quote.positionSizeUsd) {
          const posSize = parseFloat(quote.positionSizeUsd).toFixed(2);
          console.log(paint(`    Position Size: $${posSize}`, "info"));
        }

        // Calculate and show the liquidation distance
        if (quote.entryPriceUsd && quote.liquidationPriceUsd) {
          const entryPrice = parseFloat(quote.entryPriceUsd);
          const liqPrice = parseFloat(quote.liquidationPriceUsd);
          const distance = Math.abs(entryPrice - liqPrice);
          const distancePct = ((distance / entryPrice) * 100).toFixed(2);
          console.log(paint(`    Liquidation Distance: ${distancePct}% ($${distance.toFixed(2)})`, "muted"));
        }

        // Show fees
        const totalFees = (parseFloat(quote.openFeeUsd || 0) + parseFloat(quote.priceImpactFeeUsd || 0)).toFixed(2);
        if (parseFloat(totalFees) > 0) {
          console.log(paint(`    Total Fees: $${totalFees}`, "muted"));
        }

        console.log(paint(`\n    💡 To view this position: node cli_trader.js perps positions ${wallet.name}`, "muted"));
        console.log(paint(`    💡 To close this position: node cli_trader.js perps close ${wallet.name} --market ${market}`, "muted"));

        // Only show detailed JSON if DEBUG_PERPS or VERBOSE is set
        if (process.env.DEBUG_PERPS || process.env.VERBOSE) {
          logPerpsApiResult("increase-position", result);
        }
      } else {
        console.error(paint(`  ❌ ${wallet.name}: Failed to open position`, "error"));
        if (result.data?.error || result.data?.message) {
          console.error(paint(`    Error: ${result.data?.error || result.data?.message}`, "error"));
        }
        logPerpsApiResult("increase-position", result);
      }
    } catch (err) {
      console.error(paint(`  ${wallet.name}: Perps open request failed: ${err.message}`, "error"));
    }

    // Small delay between wallet requests to avoid rate limiting
    if (wallets.length > 1) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  } // End of wallet loop
}

async function handlePerpsClose(args) {
  const { options, rest } = parseCliOptions(args);
  let walletName = options.wallet || options.owner;
  let market = options.market || options.pair;
  let positionId = options.position || options["position-id"] || options.positionId;
  let sizeRaw = options.size ?? options.amount ?? options.notional;
  let priceRaw = options.price ?? options.limit;
  if (!walletName && rest.length > 0) walletName = rest[0];
  if (!market && rest.length > 1) market = rest[1];
  if (!positionId && rest.length > 2) positionId = rest[2];
  if (sizeRaw === undefined && rest.length > 3) sizeRaw = rest[3];
  if (priceRaw === undefined && rest.length > 4) priceRaw = rest[4];
  walletName = walletName ? String(walletName).trim() : "";
  const marketValue = market ? String(market).trim() : "";
  const positionValue = positionId ? String(positionId).trim() : "";
  const sizeValue = sizeRaw !== undefined ? String(sizeRaw).trim() : "";
  const priceValue = priceRaw !== undefined ? String(priceRaw).trim() : "";
  if (!walletName) {
    throw new Error(
      "perps close usage: perps close <walletFile> [market] [positionId] [size]"
    );
  }
  const wallet = findWalletByName(walletName);
  const closeAll =
    coerceCliBoolean(options["close-all"]) ||
    coerceCliBoolean(options.closeAll) ||
    coerceCliBoolean(options.all);
  if (!marketValue && !positionValue && !closeAll) {
    throw new Error(
      "perps close requires --market or --position to identify the exposure."
    );
  }
  const payload = {
    wallet: wallet.kp.publicKey.toBase58(),
  };
  if (marketValue) payload.market = marketValue;
  if (positionValue) payload.positionId = positionValue;
  if (sizeValue) payload.size = sizeValue;
  if (priceValue) payload.price = priceValue;
  if (closeAll) payload.closeAll = true;
  if (coerceCliBoolean(options["reduce-only"]) || coerceCliBoolean(options.reduceOnly)) {
    payload.reduceOnly = true;
  }
  if (options.intent !== undefined) payload.intent = String(options.intent).trim();
  if (options.type !== undefined) payload.type = String(options.type).trim();
  const tif = options["time-in-force"] || options.timeInForce || options.tif;
  if (tif !== undefined) payload.timeInForce = String(tif).trim();
  const clientOrderId =
    options["client-order-id"] || options.clientOrderId || options.cid;
  if (clientOrderId !== undefined) {
    payload.clientOrderId = String(clientOrderId).trim();
  }
  if (options.extra !== undefined || options.body !== undefined || options.params !== undefined) {
    const extra = parseJsonOption(
      options.extra ?? options.body ?? options.params,
      "perps close extra",
      { expectObject: true }
    );
    if (extra && Object.keys(extra).length > 0) {
      Object.assign(payload, extra);
    }
  }
  const targetLabel =
    payload.market || payload.positionId || marketValue || positionValue || "?";
  console.log(
    paint(
      `  Wallet ${wallet.name}: ${wallet.kp.publicKey.toBase58()} — closing ${targetLabel}`,
      "muted"
    )
  );
  if (payload.size) {
    console.log(paint(`  Requested size: ${payload.size}`, "muted"));
  }
  if (payload.closeAll) {
    console.log(paint("  closeAll=true", "muted"));
  }
  const dryRun =
    coerceCliBoolean(options["dry-run"]) || coerceCliBoolean(options.dryRun);
  if (dryRun) {
    console.log(paint("  Dry-run enabled — request payload:", "warn"));
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  // perps-api.jup.ag has two endpoints: /positions/decrease and /positions/close-all
  if (closeAll || payload.closeAll) {
    // Use close-all endpoint
    const apiPayload = {
      walletAddress: payload.wallet,
    };

    const request = buildPerpsRequest(
      "close-all",
      apiPayload,
      options,
      { defaultPath: "positions/close-all", defaultMethod: "POST" }
    );

    try {
      const result = await perpsApiRequest(request);

      if (result.ok && result.data) {
        console.log(paint("\n  ✅ All positions closed successfully!", "success"));
        if (Array.isArray(result.data.transactions)) {
          console.log(paint(`    ${result.data.transactions.length} transaction(s) processed`, "info"));
        }
        console.log(paint("    💡 Check your wallet balance to confirm closure", "muted"));

        // Only show detailed JSON if DEBUG_PERPS or VERBOSE is set
        if (process.env.DEBUG_PERPS || process.env.VERBOSE) {
          logPerpsApiResult("close-all", result);
        }
      } else {
        console.error(paint("  ❌ Failed to close all positions", "error"));
        if (result.data?.error) {
          console.error(paint(`  Error: ${result.data.error}`, "error"));
        }
        logPerpsApiResult("close-all", result);
      }
    } catch (err) {
      console.error(paint("Perps close-all request failed:", "error"), err.message);
    }
  } else {
    // Use decrease/close specific position endpoint
    const apiPayload = {
      walletAddress: payload.wallet,
      entire: !sizeValue,  // If no size specified, close entire position
    };

    if (positionValue) {
      apiPayload.positionPubkey = positionValue;
    }
    if (sizeValue) {
      apiPayload.sizeUsdDelta = parseFloat(sizeValue);
    }
    if (priceValue) {
      apiPayload.triggerPrice = parseFloat(priceValue);
    }

    const request = buildPerpsRequest(
      "decrease",
      apiPayload,
      options,
      { defaultPath: "positions/decrease", defaultMethod: "POST" }
    );

    try {
      const result = await perpsApiRequest(request);

      if (result.ok && result.data) {
        console.log(paint("\n  ✅ Position closed successfully!", "success"));

        if (result.data.closedPosition) {
          console.log(paint(`    Position ID: ${result.data.closedPosition}`, "muted"));
        }

        // Extract PnL info if available
        const quote = result.data.quote || {};
        if (quote.realizedPnl || quote.realizedPnlUsd) {
          const pnl = parseFloat(quote.realizedPnl || quote.realizedPnlUsd);
          const pnlColor = pnl >= 0 ? "success" : "error";
          const pnlSign = pnl >= 0 ? "+" : "";
          console.log(paint(`    Realized PnL: ${pnlSign}$${pnl.toFixed(2)}`, pnlColor));
        }

        // Show fees
        if (quote.closeFeeUsd || quote.feeUsd) {
          const fee = parseFloat(quote.closeFeeUsd || quote.feeUsd);
          console.log(paint(`    Close Fee: $${fee.toFixed(2)}`, "muted"));
        }

        console.log(paint("    💡 Check your wallet balance to confirm closure", "muted"));
        console.log(paint(`    💡 View remaining positions: node cli_trader.js perps positions ${wallet.name}`, "muted"));

        // Only show detailed JSON if DEBUG_PERPS or VERBOSE is set
        if (process.env.DEBUG_PERPS || process.env.VERBOSE) {
          logPerpsApiResult("decrease", result);
        }
      } else {
        console.error(paint("  ❌ Failed to close position", "error"));
        if (result.data?.error) {
          console.error(paint(`  Error: ${result.data.error}`, "error"));
        }
        logPerpsApiResult("decrease", result);
      }
    } catch (err) {
      console.error(paint("Perps close request failed:", "error"), err.message);
    }
  }
}

async function handleLendEarnCommand(args) {
  const actionRaw = args[0];
  if (!actionRaw) {
    console.log(
      "lend earn usage: lend earn <tokens|positions|earnings|deposit|withdraw|mint|redeem> [...options]"
    );
    return;
  }
  const action = actionRaw.toLowerCase();
  const rest = args.slice(1);
  switch (action) {
    case "tokens":
      await lendEarnTokens(rest);
      return;
    case "positions":
      await lendEarnPositions(rest);
      return;
    case "earnings":
      await lendEarnEarnings(rest);
      return;
    case "deposit":
    case "withdraw":
      await lendEarnTransferLike(action, rest, { valueField: "amount" });
      return;
    case "mint":
    case "redeem":
      await lendEarnTransferLike(action, rest, { valueField: "shares" });
      return;
    case "deposit-instructions":
    case "withdraw-instructions":
    case "mint-instructions":
    case "redeem-instructions": {
      const baseAction = action.replace(/-instructions$/, "");
      const defaultField =
        baseAction === "mint" || baseAction === "redeem" ? "shares" : "amount";
      await lendEarnTransferLike(action, rest, { valueField: defaultField });
      return;
    }
    default:
      throw new Error(`Unknown lend earn action '${actionRaw}'.`);
  }
}

async function lendEarnTokens(args) {
  const { options } = parseCliOptions(args);
  const query = options.query ?? " ";
  const limit = options.limit ? Number(options.limit) : undefined;
  const category = options.category;
  const tag = options.tag;
  try {
    const result = await lendEarnRequest({
      path: "tokens",
      method: "GET",
      query: {
        ...(query ? { query } : {}),
        ...(limit ? { limit } : {}),
        ...(category ? { category } : {}),
        ...(tag ? { tag } : {}),
      },
    });
    logLendApiResult("earn tokens", result);
    if (Array.isArray(result.data)) {
      const sample = result.data.slice(0, Math.min(result.data.length, 10));
      if (sample.length > 0) {
        console.log(paint("Sample tokens:", "muted"));
        for (const entry of sample) {
          const summary =
            `${entry.symbol || entry.name || entry.id} — mint ${entry.id}` +
            (entry.apy?.jupEarn ? ` — APY ${entry.apy.jupEarn}%` : "");
          console.log(paint(`  ${summary}`, "info"));
        }
        if (result.data.length > sample.length) {
          console.log(
            paint(
              `  ... ${result.data.length - sample.length} more token(s)`,
              "muted"
            )
          );
        }
      }
    }
  } catch (err) {
    console.error(paint("Lend earn tokens request failed:", "error"), err.message);
  }
}

async function lendEarnPositions(args) {
  const { options, rest } = parseCliOptions(args);
  let input = rest.length > 0 ? rest : (options.wallets ? options.wallets.split(",") : []);
  const needsAllWallets =
    !input ||
    input.length === 0 ||
    input.some((item) => item.trim().length === 0 || item.trim() === "*");
  if (needsAllWallets) {
    input = listWallets().map((wallet) => wallet.name);
  }
  if (!input || input.length === 0) {
    throw new Error("lend earn positions usage: lend earn positions <walletName|pubkey>[,...]");
  }
  const identifiers = resolveWalletIdentifiers(input);
  const walletNameMap = new Map(
    identifiers.map((entry) => [entry.pubkey, entry.name || entry.pubkey])
  );
  if (needsAllWallets) {
    console.log(
      paint(
        `  Targeting all ${identifiers.length} wallet(s) for positions.`,
        "muted"
      )
    );
  } else if (identifiers.length > 1) {
    console.log(
      paint(
        `  Targeting ${identifiers.length} wallet(s) for positions.`,
        "muted"
      )
    );
  }
  const scopeNames = identifiers
    .map((entry) => entry.name || entry.pubkey)
    .filter(Boolean);
  if (scopeNames.length > 0 && scopeNames.length <= 5) {
    console.log(paint(`  Wallet scope: ${scopeNames.join(", ")}`, "muted"));
  }
  try {
    const walletCsv = identifiers.map((entry) => entry.pubkey).join(",");
    const result = await lendEarnRequest({
      path: "positions",
      method: "GET",
      query: { users: walletCsv, wallets: walletCsv },
    });
    logLendApiResult("earn positions", result);
    const entries = extractIterableFromLendData(result.data) || [];
    if (entries.length === 0) {
      console.log(
        paint(
          "  No earn positions found for the requested wallet(s).",
          "muted"
        )
      );
    } else {
      const grouped = new Map();
      for (const entry of entries) {
        const ownerKey = resolvePositionOwner(entry) || "unknown";
        const label = walletNameMap.get(ownerKey) || ownerKey;
        if (!grouped.has(label)) grouped.set(label, []);
        grouped.get(label).push(entry);
      }
      console.log(
        paint(
          `  Found ${entries.length} position(s) across ${grouped.size} wallet(s).`,
          "info"
        )
      );
      for (const [label, list] of grouped.entries()) {
        const preview = list
          .slice(0, 3)
          .map((entry) => formatEarnPositionSnippet(entry))
          .join("; ");
        let line = `  ${label}: ${list.length} position(s)`;
        if (preview) {
          line += ` — ${preview}`;
          if (list.length > 3) line += " …";
        }
        console.log(paint(line, "muted"));
      }
      console.log(
        paint(
          "  Tip: run 'lend overview' for combined earnings/borrow details per wallet.",
          "muted"
        )
      );
    }
  } catch (err) {
    console.error(paint("Lend earn positions request failed:", "error"), err.message);
  }
}

async function lendEarnEarnings(args) {
  const { options, rest } = parseCliOptions(args);
  let input = rest.length > 0 ? rest : (options.wallets ? options.wallets.split(",") : []);
  const needsAllWallets =
    !input ||
    input.length === 0 ||
    input.some((item) => item.trim().length === 0 || item.trim() === "*");
  if (needsAllWallets) {
    input = listWallets().map((wallet) => wallet.name);
  }
  if (!input || input.length === 0) {
    throw new Error("lend earn earnings usage: lend earn earnings <walletName|pubkey>[,...]");
  }
  const identifiers = resolveWalletIdentifiers(input);
  if (needsAllWallets) {
    console.log(
      paint(
        `  Targeting all ${identifiers.length} wallet(s) for earnings.`,
        "muted"
      )
    );
  } else if (identifiers.length > 1) {
    console.log(
      paint(
        `  Targeting ${identifiers.length} wallet(s) for earnings.`,
        "muted"
      )
    );
  }
  const earningScope = identifiers
    .map((entry) => entry.name || entry.pubkey)
    .filter(Boolean);
  if (earningScope.length > 0 && earningScope.length <= 5) {
    console.log(paint(`  Wallet scope: ${earningScope.join(", ")}`, "muted"));
  }
  const summaries = [];
  for (const identifier of identifiers) {
    const label = identifier.name || identifier.pubkey;
    try {
      const result = await lendEarnRequest({
        path: "earnings",
        method: "GET",
        query: { user: identifier.pubkey, wallet: identifier.pubkey },
      });
      logLendApiResult(`earn earnings (${label})`, result);
      const entries = extractIterableFromLendData(result.data) || [];
      summaries.push({ label, count: entries.length });
    } catch (err) {
      console.error(
        paint(
          `Lend earn earnings request failed for ${label}:`,
          "error"
        ),
        err.message || err
      );
    }
  }
  if (summaries.length > 1) {
    console.log(paint("Earnings summary:", "muted"));
    for (const item of summaries) {
      console.log(
        paint(
          `  ${item.label}: ${item.count} entr${item.count === 1 ? "y" : "ies"}`,
          "muted"
        )
      );
    }
  }
}

function normalizeWalletIdentifier(value) {
  if (!value) return null;
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof PublicKey) {
    return value.toBase58();
  }
  if (typeof value === "object") {
    if (typeof value.toBase58 === "function") {
      try {
        return value.toBase58();
      } catch (_) {}
    }
    if (typeof value.pubkey === "string") return value.pubkey;
    if (value.pubkey) return normalizeWalletIdentifier(value.pubkey);
    if (typeof value.wallet === "string") return value.wallet;
    if (value.wallet) return normalizeWalletIdentifier(value.wallet);
    if (typeof value.owner === "string") return value.owner;
    if (value.owner) return normalizeWalletIdentifier(value.owner);
    if (typeof value.address === "string") return value.address;
    if (value.address) return normalizeWalletIdentifier(value.address);
  }
  try {
    const stringified = value.toString();
    if (stringified && stringified !== "[object Object]") {
      return stringified;
    }
  } catch (_) {}
  return null;
}

function extractIterableFromLendData(data) {
  if (!data) return null;
  if (Array.isArray(data)) return data;
  if (Array.isArray(data.positions)) return data.positions;
  if (Array.isArray(data.entries)) return data.entries;
  if (Array.isArray(data.items)) return data.items;
  if (Array.isArray(data.data)) return data.data;
  return null;
}

function summariseLendEntries(result, walletNameMap, { label }) {
  if (!result || !result.ok) return;
  const iterable = extractIterableFromLendData(result.data);
  if (!Array.isArray(iterable) || iterable.length === 0) {
    console.log(paint(`  ${label}: no entries returned.`, "muted"));
    return;
  }
  const counts = new Map();
  for (const entry of iterable) {
    const walletKey = resolvePositionOwner(entry);
    if (!walletKey) continue;
    const labelName = walletNameMap.get(walletKey) || walletKey;
    counts.set(labelName, (counts.get(labelName) || 0) + 1);
  }
  console.log(
    paint(
      `  ${label}: ${iterable.length} entr${iterable.length === 1 ? "y" : "ies"} across ${counts.size || 0} wallet(s).`,
      "muted"
    )
  );
  if (counts.size > 0 && counts.size <= 10) {
    const sorted = Array.from(counts.entries()).sort((a, b) =>
      a[0].localeCompare(b[0])
    );
    for (const [walletLabel, count] of sorted) {
      console.log(
        paint(
          `    ${walletLabel}: ${count} entr${count === 1 ? "y" : "ies"}`,
          "muted"
        )
      );
    }
  }
}

function resolvePositionOwner(entry) {
  if (!entry || typeof entry !== "object") return null;
  const candidates = [
    entry,
    entry.wallet,
    entry.owner,
    entry.walletAddress,
    entry.walletPubkey,
    entry.borrower,
    entry.accountOwner,
    entry.user,
    entry.userPubkey,
    entry.pubkey,
  ];
  for (const candidate of candidates) {
    const resolved = normalizeWalletIdentifier(candidate);
    if (resolved) return resolved;
  }
  return null;
}

function collectBorrowPositionIds(entries, walletPubkey = null) {
  if (!Array.isArray(entries)) return [];
  const ids = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const owner = resolvePositionOwner(entry);
    if (walletPubkey && owner && owner !== walletPubkey) continue;
    const candidates = [
      entry.positionId,
      entry.id,
      entry.positionAccount,
      entry.account,
      entry.publicKey,
      entry.pubkey,
      entry.position?.id,
      entry.position?.positionId,
      entry.position?.account,
      entry.position?.publicKey,
      entry.depositAccount,
    ];
    const match = candidates.find(
      (value) => typeof value === "string" && value.trim().length > 0
    );
    if (match) {
      ids.push(match.trim());
      continue;
    }
    if (Array.isArray(entry.positions)) {
      for (const nested of entry.positions) {
        const nestedCandidates = [
          nested?.positionId,
          nested?.id,
          nested?.account,
          nested?.publicKey,
        ];
        const nestedMatch = nestedCandidates.find(
          (value) => typeof value === "string" && value.trim().length > 0
        );
        if (nestedMatch) {
          ids.push(nestedMatch.trim());
        }
      }
    }
  }
  return Array.from(new Set(ids));
}

function isLikelyBase64Transaction(value) {
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  if (trimmed.length < 80) return false;
  if (!/^[A-Za-z0-9+/=]+$/.test(trimmed)) return false;
  try {
    const buf = Buffer.from(trimmed, "base64");
    return buf.length > 32;
  } catch (_) {
    return false;
  }
}

function extractTransactionsFromLendResponse(data) {
  const collected = [];
  const seen = new Set();
  const pushTx = (str) => {
    if (!isLikelyBase64Transaction(str)) return;
    const id = str.slice(0, 32);
    if (seen.has(id)) return;
    seen.add(id);
    collected.push(str);
  };
  const scan = (value, keyHint = "") => {
    if (!value) return;
    if (typeof value === "string") {
      if (/transaction/i.test(keyHint) || isLikelyBase64Transaction(value)) {
        pushTx(value);
      }
      return;
    }
    if (Array.isArray(value)) {
      for (const element of value) {
        scan(element, keyHint);
      }
      return;
    }
    if (typeof value === "object") {
      for (const [key, val] of Object.entries(value)) {
        if (typeof val === "string") {
          if (/transaction/i.test(key)) {
            pushTx(val);
          } else {
            scan(val, key);
          }
        } else {
          scan(val, key);
        }
      }
    }
  };
  scan(data, "");
  return collected;
}

function formatNumericish(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return null;
    return trimmed;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toString();
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "object") {
    if (typeof value.uiAmountString === "string") return value.uiAmountString;
    if (typeof value.uiAmount === "number") return value.uiAmount.toString();
    if (typeof value.amount === "string") return value.amount;
    if (typeof value.amount === "number") return value.amount.toString();
  }
  try {
    const json = JSON.stringify(value);
    if (json.length <= 60) return json;
  } catch (_) {}
  return null;
}

function formatEarnPositionSnippet(entry) {
  const amountCandidate = findInObject(entry, [
    "balance.uiAmountString",
    "balance.uiAmount",
    "balance.amount",
    "balance",
    "amount.uiAmountString",
    "amount.uiAmount",
    "amount.amount",
    "amount",
    "shares.uiAmountString",
    "shares.amount",
    "shares",
    "principal.uiAmountString",
    "principal.amount",
    "principal",
    "depositedAmount.uiAmountString",
    "depositedAmount.amount",
    "depositedAmount",
    "value",
    "usdValue",
  ]);
  const amountStr = formatNumericish(amountCandidate);
  const rawSymbol = findInObject(entry, [
    "assetSymbol",
    "tokenSymbol",
    "depositSymbol",
    "symbol",
    "sharesSymbol",
    "asset.symbol",
    "token.symbol",
  ]);
  let symbolStr = formatNumericish(rawSymbol);
  if (!symbolStr) {
    const mint = findInObject(entry, [
      "asset",
      "depositMint",
      "mint",
      "tokenMint",
      "shareMint",
    ]);
    if (typeof mint === "string" && mint.trim()) {
      symbolStr = symbolForMint(mint.trim());
    }
  }
  if (!symbolStr) symbolStr = "share";
  if (amountStr) {
    return `${symbolStr} ≈ ${amountStr}`;
  }
  return symbolStr;
}

async function resolveTokenProgramForMint(connection, mintPubkey) {
  if (!connection) {
    throw new Error("resolveTokenProgramForMint requires a connection");
  }
  if (!mintPubkey) {
    throw new Error("resolveTokenProgramForMint requires a mint public key");
  }
  const info = await connection.getAccountInfo(mintPubkey);
  if (!info) {
    throw new Error(`Mint account ${mintPubkey.toBase58()} not found`);
  }
  if (info.owner?.equals?.(TOKEN_PROGRAM_ID)) {
    return TOKEN_PROGRAM_ID;
  }
  if (info.owner?.equals?.(TOKEN_2022_PROGRAM_ID)) {
    return TOKEN_2022_PROGRAM_ID;
  }
  throw new Error(
    `Unsupported mint owner ${info.owner?.toBase58?.() || "unknown"} for ${mintPubkey.toBase58()}`
  );
}

async function ensureAtaForMint(connection, wallet, mintPubkey, tokenProgram, options = {}) {
  if (!connection) throw new Error("ensureAtaForMint requires a connection");
  if (!wallet?.kp?.publicKey) throw new Error("ensureAtaForMint requires a wallet with a keypair");
  const programId = tokenProgram || (await resolveTokenProgramForMint(connection, mintPubkey));
  const ata = await getAssociatedTokenAddress(
    mintPubkey,
    wallet.kp.publicKey,
    false,
    programId,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const existing = await connection.getAccountInfo(ata);
  if (existing) {
    return false;
  }
  return sharedEnsureAtaForMint(connection, wallet, mintPubkey, programId, options);
}

async function ensureAtasForTransaction({ connection, wallet, txBase64, label }) {
  if (!txBase64) return 0;
  let message;
  const buf = Buffer.from(txBase64, "base64");
  try {
    const vtx = VersionedTransaction.deserialize(buf);
    message = vtx.message;
  } catch (err) {
    try {
      const legacyTx = Transaction.from(buf);
      message = legacyTx.compileMessage();
    } catch (_) {
      return 0;
    }
  }
  let accountKeys = [];
  if (typeof message.getAccountKeys === "function") {
    const keyStruct = message.getAccountKeys();
    accountKeys = [
      ...(keyStruct?.staticAccountKeys || []),
      ...((keyStruct?.accountKeysFromLookups && keyStruct.accountKeysFromLookups.readonly) || []),
      ...((keyStruct?.accountKeysFromLookups && keyStruct.accountKeysFromLookups.writable) || []),
    ];
  } else if (Array.isArray(message.accountKeys)) {
    accountKeys = [...message.accountKeys];
  }
  if (!accountKeys.length) return 0;

  const infos = await connection.getMultipleAccountsInfo(accountKeys);
  const infoMap = new Map();
  accountKeys.forEach((key, idx) => {
    infoMap.set(key.toBase58(), infos[idx] || null);
  });

  const mintCandidates = [];
  for (let i = 0; i < accountKeys.length; i += 1) {
    const info = infos[i];
    if (!info) continue;
    const owner = info.owner;
    const isTokenProgram = owner.equals(TOKEN_PROGRAM_ID) || owner.equals(TOKEN_2022_PROGRAM_ID);
    if (!isTokenProgram) continue;
    const dataLen = info.data?.length || 0;
    if (dataLen === MintLayout.span || (owner.equals(TOKEN_2022_PROGRAM_ID) && dataLen >= MintLayout.span)) {
      mintCandidates.push({
        mint: accountKeys[i],
        programId: owner,
      });
    }
  }
  if (!mintCandidates.length) return 0;

  const missing = [];
  for (const candidate of mintCandidates) {
    const tokenProgram = candidate.programId.equals(TOKEN_2022_PROGRAM_ID)
      ? TOKEN_2022_PROGRAM_ID
      : TOKEN_PROGRAM_ID;
    const ata = await getAssociatedTokenAddress(
      candidate.mint,
      wallet.kp.publicKey,
      false,
      tokenProgram,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    let ataInfo = infoMap.get(ata.toBase58());
    if (!ataInfo) {
      ataInfo = await connection.getAccountInfo(ata);
    }
    if (!ataInfo) {
      missing.push({ mint: candidate.mint, programId: tokenProgram, ata });
    }
  }

  if (missing.length === 0) return 0;
  console.log(
    paint(
      `  Preparing ${missing.length} associated token account(s) for ${label}.`,
      "muted"
    )
  );
  let createdTotal = 0;
  for (const entry of missing) {
    try {
      const created = await ensureAtaForMint(connection, wallet, entry.mint, entry.programId, { label });
      if (created) {
        createdTotal += 1;
      }
    } catch (err) {
      console.error(
        paint(
          `  Failed to create ATA for mint ${entry.mint.toBase58()}:`,
          "error"
        ),
        err.message || err
      );
    }
  }
  return createdTotal;
}

async function ensureWrappedSolBalance(
  connection,
  wallet,
  requiredLamports,
  existingLamportsOverride = null
) {
  return sharedEnsureWrappedSolBalance(
    connection,
    wallet,
    requiredLamports,
    existingLamportsOverride
  );
}

async function getWrappedSolAccountInfo(connection, ownerPubkey) {
  const mintPubkey = new PublicKey(SOL_MINT);
  const tokenProgram = TOKEN_PROGRAM_ID;
  const ata = await getAssociatedTokenAddress(
    mintPubkey,
    ownerPubkey,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const info = await connection.getAccountInfo(ata);
  if (!info) {
    return { ata, exists: false, lamports: 0n };
  }
  let lamports = 0n;
  try {
    const balanceInfo = await connection.getTokenAccountBalance(ata);
    lamports = BigInt(balanceInfo?.value?.amount ?? "0");
  } catch (_) {}
  return { ata, exists: true, lamports };
}

async function autoUnwrapWrappedSol(
  connection,
  wallet,
  { minLamports = 0n, reason = "unwrap wSOL" } = {}
) {
  const { ata, exists, lamports } = await getWrappedSolAccountInfo(
    connection,
    wallet.kp.publicKey
  );
  if (!exists || lamports <= minLamports) {
    return false;
  }
  const humanAmount = formatBaseUnits(lamports, 9);
  console.log(
    paint(
      `  ${reason}: unwrapping ${humanAmount} SOL for ${wallet.name}.`,
      "muted"
    )
  );
  try {
    const sig = await closeAccount(
      connection,
      wallet.kp,
      ata,
      wallet.kp.publicKey,
      wallet.kp,
      [],
      undefined,
      TOKEN_PROGRAM_ID
    );
    console.log(
      paint(
        `  Unwrapped ${humanAmount} SOL for ${wallet.name} — tx ${sig}`,
        "success"
      )
    );
  } catch (err) {
    console.error(
      paint(
        `  Failed to unwrap wSOL for ${wallet.name}:`,
        "error"
      ),
      err.message || err
    );
    throw err;
  }
  return true;
}

async function getSolAndWrappedSolBalances(connection, wallet) {
  const solLamports = BigInt(
    await getSolBalance(connection, wallet.kp.publicKey)
  );
  const { lamports: wsolLamports } = await getWrappedSolAccountInfo(
    connection,
    wallet.kp.publicKey
  );
  return {
    solLamports,
    wsolLamports,
  };
}

async function lendOverviewAllWallets() {
  const wallets = listWallets();
  if (!wallets.length) {
    console.log(
      paint("Lend overview aborted: no wallets found in keypairs directory.", "warn")
    );
    return;
  }
  const identifiers = wallets.map((wallet) => ({
    name: wallet.name,
    pubkey: wallet.kp.publicKey.toBase58(),
  }));
  const walletNameMap = new Map(
    identifiers.map((entry) => [entry.pubkey, entry.name])
  );
  console.log(
    paint(
      `Lend overview across ${identifiers.length} wallet(s).`,
      "label"
    )
  );
  if (identifiers.length <= 10) {
    console.log(
      paint(
        `  wallets: ${identifiers.map((entry) => entry.name).join(", ")}`,
        "muted"
      )
    );
  }
  const walletCsv = identifiers.map((entry) => entry.pubkey).join(",");

  console.log(paint("\n=== earn positions ===", "label"));
  try {
    const positionsResult = await lendEarnRequest({
      path: "positions",
      method: "GET",
      query: { users: walletCsv, wallets: walletCsv },
    });
    logLendApiResult("earn positions", positionsResult);
    summariseLendEntries(positionsResult, walletNameMap, {
      label: "earn positions",
    });
  } catch (err) {
    console.error(
      paint("earn positions request failed:", "error"),
      err.message || err
    );
  }

  console.log(paint("\n=== earn earnings ===", "label"));
  const earningsSummaries = [];
  for (const identifier of identifiers) {
    const label = identifier.name || identifier.pubkey;
    try {
      const earningsResult = await lendEarnRequest({
        path: "earnings",
        method: "GET",
        query: { user: identifier.pubkey, wallet: identifier.pubkey },
      });
      logLendApiResult(`earn earnings (${label})`, earningsResult);
      const entries = extractIterableFromLendData(earningsResult.data) || [];
      earningsSummaries.push({ label, count: entries.length });
    } catch (err) {
      console.error(
        paint(
          `earn earnings request failed for ${label}:`,
          "error"
        ),
        err.message || err
      );
    }
  }
  if (earningsSummaries.length > 1) {
    console.log(paint("Earnings summary:", "muted"));
    for (const item of earningsSummaries) {
      console.log(
        paint(
          `  ${item.label}: ${item.count} entr${item.count === 1 ? "y" : "ies"}`,
          "muted"
        )
      );
    }
  }

  console.log(
    paint(
      "\nBorrow endpoints are temporarily unavailable (coming soon).",
      "warn"
    )
  );
}

async function computeSpendableSolBalance(connection, ownerPubkey) {
  const lamports = BigInt(await getSolBalance(connection, ownerPubkey));
  const solRecord = tokenBySymbol("SOL");
  const decimals = solRecord?.decimals ?? 9;
  if (lamports <= 0n) {
    return {
      totalLamports: lamports,
      spendableLamports: 0n,
      reserveLamports: 0n,
      requiresAtaCreation: false,
      decimals,
    };
  }
  if (cachedAtaRentLamports === null) {
    const rent = await connection.getMinimumBalanceForRentExemption(165);
    cachedAtaRentLamports = BigInt(rent);
  }
  let requiresAtaCreation = false;
  try {
    const ata = await getAssociatedTokenAddress(
      new PublicKey(SOL_MINT),
      ownerPubkey,
      false,
      TOKEN_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    const ataInfo = await connection.getAccountInfo(ata);
    requiresAtaCreation = ataInfo === null;
  } catch (_) {
    requiresAtaCreation = false;
  }
  let reserve = lamports > GAS_RESERVE_LAMPORTS
    ? GAS_RESERVE_LAMPORTS
    : lamports / 10n;
  reserve += JUPITER_SOL_BUFFER_LAMPORTS;
  if (requiresAtaCreation) {
    reserve += cachedAtaRentLamports + ATA_CREATION_FEE_LAMPORTS;
  }
  if (reserve < MIN_SOL_PER_SWAP_LAMPORTS) {
    reserve = MIN_SOL_PER_SWAP_LAMPORTS;
  }
  if (reserve >= lamports) {
    reserve = lamports;
  }
  const spendable = lamports > reserve ? lamports - reserve : 0n;
  return {
    totalLamports: lamports,
    spendableLamports: spendable,
    reserveLamports: reserve,
    requiresAtaCreation,
    decimals,
  };
}

async function loadWalletTokenBalances(wallet) {
  const connection = createRpcConnection("confirmed");
  const tokens = [];
  const includeSol = true;
  if (includeSol) {
    const solInfo = await computeSpendableSolBalance(connection, wallet.kp.publicKey);
    const solRecord =
      tokenBySymbol("SOL") || {
        mint: SOL_MINT,
        symbol: "SOL",
        decimals: solInfo.decimals,
      };
    const amountDecimal = formatBaseUnits(solInfo.totalLamports, solInfo.decimals);
    const spendableDecimal = formatBaseUnits(solInfo.spendableLamports, solInfo.decimals);
    tokens.push({
      tokenRecord: {
        ...solRecord,
        mint: solRecord.mint || SOL_MINT,
        decimals: solRecord.decimals ?? solInfo.decimals,
        symbol: solRecord.symbol || "SOL",
      },
      amountRaw: solInfo.totalLamports,
      spendableRaw: solInfo.spendableLamports,
      decimals: solRecord.decimals ?? solInfo.decimals,
      amountDecimal,
      spendableDecimal,
      isSolLike: true,
      reserveLamports: solInfo.reserveLamports,
      requiresAtaCreation: solInfo.requiresAtaCreation,
    });
  }
  const parsedAccounts = await getAllParsedTokenAccounts(connection, wallet.kp.publicKey);
  for (const { account } of parsedAccounts) {
    const info = account.data.parsed.info;
    const amountRaw = BigInt(info.tokenAmount.amount);
    if (amountRaw === 0n) continue;
    const decimals = info.tokenAmount.decimals ?? 0;
    const mint = info.mint;
    let tokenRecord = await resolveTokenRecord(mint);
    if (!tokenRecord) {
      tokenRecord = {
        mint,
        symbol: symbolForMint(mint),
        decimals,
      };
    }
    const amountDecimal = formatBaseUnits(amountRaw, decimals);
    tokens.push({
      tokenRecord,
      amountRaw,
      spendableRaw: amountRaw,
      decimals,
      amountDecimal,
      spendableDecimal: amountDecimal,
      isSolLike: SOL_LIKE_MINTS.has(tokenRecord.mint),
      isLendShareToken: isLendShareToken(tokenRecord),
    });
  }
  return tokens;
}

async function lendEarnTransferLike(action, args, { valueField }) {
  let walletArg = args[0];
  let mintArg = args[1];
  let amountArg = args[2];
  walletArg = walletArg === undefined ? "*" : walletArg.trim();
  mintArg = mintArg === undefined ? "*" : mintArg.trim();
  amountArg = amountArg === undefined ? "*" : amountArg.trim();
  if (walletArg.length === 0) walletArg = "*";
  if (mintArg.length === 0) mintArg = "*";
  if (amountArg.length === 0) amountArg = "*";

  const tail = args.slice(3);
  const { options } = parseCliOptions(tail);
  const rawAmount = options.raw === true || options.raw === "true";
  const field = options.field || valueField;
  const extra = parseJsonOption(options.extra, "--extra");
  const skipSendFlag =
    options["no-send"] === true ||
    options["no-send"] === "true" ||
    options["no_send"] === true ||
    options["no_send"] === "true" ||
    options.dry === true ||
    options.dry === "true" ||
    options["dry-run"] === true ||
    options["dry-run"] === "true" ||
    options["dry_run"] === true ||
    options["dry_run"] === "true";
  const ignoreKeys = new Set([
    "raw",
    "field",
    "extra",
    "no-send",
    "no_send",
    "dry",
    "dry-run",
    "dry_run",
  ]);
  const isInstructionRequest = action.endsWith("-instructions");
  const normalizedAction = action.replace(/-instructions$/, "");
  const usesBaseAssets =
    normalizedAction === "deposit" || normalizedAction === "mint";
  const usesShareTokens =
    normalizedAction === "withdraw" || normalizedAction === "redeem";

  let lendTokens = [];
  try {
    lendTokens = await refreshLendTokensCache();
  } catch (_) {
    lendTokens = [];
  }
  const shareTokenIndex = new Map();
  const baseAssetYieldBps = new Map();
  for (const token of lendTokens) {
    if (token && typeof token === "object") {
      if (typeof token.symbol === "string") {
        shareTokenIndex.set(token.symbol.toLowerCase(), token);
      }
      if (typeof token.address === "string") {
        shareTokenIndex.set(token.address, token);
      }
      if (token.assetAddress) {
        const totalRate = Number(token.totalRate ?? token.supplyRate ?? 0);
        if (Number.isFinite(totalRate)) {
          const existing = baseAssetYieldBps.get(token.assetAddress);
          if (existing === undefined || totalRate > existing) {
            baseAssetYieldBps.set(token.assetAddress, totalRate);
          }
        }
      }
    }
  }

  let wallets;
  if (walletArg === "*") {
    wallets = listWallets();
    if (!wallets.length) {
      console.log(
        paint(
          `  No wallets found in ${KEYPAIR_DIR}; skipping ${normalizedAction}.`,
          "warn"
        )
      );
      return;
    }
    const walletWord = wallets.length === 1 ? "wallet" : "wallets";
    console.log(
      paint(
        `  Targeting ${wallets.length} ${walletWord} for ${normalizedAction}.`,
        "muted"
      )
    );
  } else {
    const targetWallet = findWalletByName(walletArg);
    wallets = [targetWallet];
    console.log(
      paint(
        `  Targeting wallet ${targetWallet.name} for ${normalizedAction}.`,
        "muted"
      )
    );
  }
  const tokenFilterRecord = mintArg !== "*" ? await resolveTokenRecord(mintArg) : null;
  if (mintArg !== "*" && !tokenFilterRecord && !shareTokenIndex.size) {
    throw new Error(`Token ${mintArg} not found in catalog (try running 'tokens --refresh')`);
  }

  let targetMintForFilter = null;
  let tokenFilterLabel;
  let filterSymbolForLogs;

  if (mintArg === "*") {
    if (usesBaseAssets) {
      const baseSymbols = Array.from(LEND_BASE_ASSET_MINTS)
        .map((mint) => symbolForMint(mint))
        .filter(Boolean);
      const uniqueSymbols = [...new Set(baseSymbols)];
      tokenFilterLabel = uniqueSymbols.length
        ? `eligible base assets (${uniqueSymbols.join("/")})`
        : "eligible base assets";
    } else if (usesShareTokens) {
      tokenFilterLabel = "all Jupiter lend share tokens (JL-*)";
    } else {
      tokenFilterLabel = "all tokens with balances";
    }
  } else if (usesBaseAssets) {
    const directMint = tokenFilterRecord?.mint || mintArg;
    const lowerSymbol = typeof mintArg === "string" ? mintArg.toLowerCase() : "";
    const shareEntry =
      shareTokenIndex.get(directMint) ||
      shareTokenIndex.get(lowerSymbol);
    let baseMint = null;
    if (typeof directMint === "string") {
      const mapped = getUnderlyingAssetForShareToken(directMint);
      if (mapped) baseMint = mapped;
    }
    if (!baseMint && shareEntry?.assetAddress) {
      baseMint = shareEntry.assetAddress;
    }
    if (!baseMint && shareEntry?.asset?.address) {
      baseMint = shareEntry.asset.address;
    }
    if (!baseMint && typeof directMint === "string" && LEND_BASE_ASSET_MINTS.has(directMint)) {
      baseMint = directMint;
    }
    if (!baseMint && tokenFilterRecord?.mint && LEND_BASE_ASSET_MINTS.has(tokenFilterRecord.mint)) {
      baseMint = tokenFilterRecord.mint;
    }
    if (!baseMint) {
      const available = Array.from(LEND_BASE_ASSET_MINTS)
        .map((mint) => symbolForMint(mint))
        .filter(Boolean)
        .join(", ");
      throw new Error(
        `Token ${mintArg} is not a supported Lend base asset. Eligible base assets: ${available}`
      );
    }
    targetMintForFilter = baseMint;
    const baseSymbol = symbolForMint(baseMint) || baseMint.slice(0, 4);
    if (shareEntry?.symbol && shareEntry.symbol.toLowerCase() !== baseSymbol.toLowerCase()) {
      tokenFilterLabel = `${shareEntry.symbol} (underlying ${shareEntry.asset?.symbol || baseSymbol})`;
      filterSymbolForLogs = shareEntry.symbol;
    } else if (tokenFilterRecord?.symbol) {
      tokenFilterLabel = tokenFilterRecord.symbol;
      filterSymbolForLogs = tokenFilterRecord.symbol;
    } else {
      tokenFilterLabel = baseSymbol;
      filterSymbolForLogs = baseSymbol;
    }
  } else {
    targetMintForFilter = tokenFilterRecord?.mint || mintArg;
    tokenFilterLabel = tokenFilterRecord?.symbol || mintArg;
    filterSymbolForLogs = tokenFilterRecord?.symbol || mintArg;
  }

  if (!filterSymbolForLogs) {
    filterSymbolForLogs = tokenFilterLabel;
  }

  console.log(paint(`  Token filter: ${tokenFilterLabel}`, "muted"));
  const amountLabel = (() => {
    if (amountArg === "*") {
      const scope = usesBaseAssets
        ? "max spendable per wallet"
        : "full available balance per wallet";
      return rawAmount ? `${scope} (raw units)` : scope;
    }
    return rawAmount ? `${amountArg} (raw units)` : amountArg;
  })();
  console.log(paint(`  Amount: ${amountLabel}`, "muted"));

  for (const wallet of wallets) {
    const balances = await loadWalletTokenBalances(wallet);
    if (!balances.length) {
      console.log(paint(`  Wallet ${wallet.name} has no token balances for ${action}.`, "muted"));
      continue;
    }

    let eligibleBalances = balances;
    if (mintArg === "*") {
      if (usesBaseAssets) {
        eligibleBalances = balances.filter((entry) =>
          LEND_BASE_ASSET_MINTS.has(entry.tokenRecord.mint)
        );
      } else if (usesShareTokens) {
        eligibleBalances = balances.filter((entry) =>
          isLendShareToken(entry.tokenRecord)
        );
      }
    }

    if (usesBaseAssets && mintArg === "*" && eligibleBalances.length > 1 && baseAssetYieldBps.size > 0) {
      eligibleBalances = [...eligibleBalances].sort((a, b) => {
        const yieldA = baseAssetYieldBps.get(a.tokenRecord.mint) ?? -Infinity;
        const yieldB = baseAssetYieldBps.get(b.tokenRecord.mint) ?? -Infinity;
        if (yieldA !== yieldB) {
          return yieldB - yieldA;
        }
        const spendableA =
          typeof a.spendableRaw === "bigint"
            ? a.spendableRaw
            : BigInt(a.spendableRaw ?? 0);
        const spendableB =
          typeof b.spendableRaw === "bigint"
            ? b.spendableRaw
            : BigInt(b.spendableRaw ?? 0);
        if (spendableA !== spendableB) {
          return spendableB > spendableA ? 1 : -1;
        }
        const symbolA = a.tokenRecord.symbol || a.tokenRecord.mint;
        const symbolB = b.tokenRecord.symbol || b.tokenRecord.mint;
        return symbolA.localeCompare(symbolB);
      });
    }

    const effectiveFilterMint = mintArg === "*"
      ? null
      : targetMintForFilter || tokenFilterRecord?.mint || mintArg;

    const filteredBalances = effectiveFilterMint
      ? eligibleBalances.filter((entry) => entry.tokenRecord.mint === effectiveFilterMint)
      : eligibleBalances;

    if (!filteredBalances.length) {
      const label = mintArg === "*" ? "selected tokens" : filterSymbolForLogs;
      console.log(
        paint(
          `  Wallet ${wallet.name} has no balance for ${label}.`,
          "muted"
        )
      );
      continue;
    }

    let walletHadSuccess = false;
    let wrapConnection = null;

    for (const balance of filteredBalances) {
      let currentBalance = balance;
      let succeeded = false;
      for (let attempt = 0; attempt < 3 && currentBalance; attempt += 1) {
        let displayAmount =
          amountArg === "*"
            ? usesBaseAssets
              ? currentBalance.spendableDecimal
              : currentBalance.amountDecimal
            : amountArg;
        if (!displayAmount || /^0(?:\.0+)?$/.test(displayAmount)) {
          if (amountArg === "*" && usesBaseAssets) {
            console.log(
              paint(
                `  Skipping ${wallet.name}: no spendable balance for ${currentBalance.tokenRecord.symbol}.`,
                "muted"
              )
            );
          }
          break;
        }

        let baseAmount;
        if (amountArg === "*") {
          const targetRaw = usesBaseAssets
            ? currentBalance.spendableRaw ?? currentBalance.amountRaw
            : currentBalance.amountRaw;
          let targetRawBigInt =
            typeof targetRaw === "bigint" ? targetRaw : BigInt(targetRaw ?? 0);
          if (targetRawBigInt <= 0n) {
            if (usesBaseAssets) {
              console.log(
                paint(
                  `  Skipping ${wallet.name}: spendable balance for ${currentBalance.tokenRecord.symbol} is zero after reserves.`,
                  "muted"
                )
              );
            }
            break;
          }
          if (usesBaseAssets && !rawAmount) {
            if (targetRawBigInt <= LEND_SOL_WRAP_BUFFER_LAMPORTS) {
              console.log(
                paint(
                  `  Skipping ${wallet.name}: spendable SOL ${formatBaseUnits(targetRawBigInt, currentBalance.decimals)} is below the wrap buffer (${formatBaseUnits(LEND_SOL_WRAP_BUFFER_LAMPORTS, currentBalance.decimals)}).`,
                  "muted"
                )
              );
              break;
            }
            targetRawBigInt -= LEND_SOL_WRAP_BUFFER_LAMPORTS;
          }
          if (usesBaseAssets && targetRawBigInt < MIN_LEND_SOL_DEPOSIT_LAMPORTS) {
            console.log(
              paint(
                `  Skipping ${wallet.name}: spendable SOL (${formatBaseUnits(targetRawBigInt, currentBalance.decimals)} SOL) is below the minimum deposit threshold (${formatBaseUnits(MIN_LEND_SOL_DEPOSIT_LAMPORTS, currentBalance.decimals)} SOL).`,
                "muted"
              )
            );
            break;
          }
          if (usesBaseAssets && !rawAmount) {
            let percent =
              LEND_SOL_BASE_PERCENT -
              BigInt(attempt) * LEND_SOL_RETRY_DECREMENT_PERCENT;
            if (percent < LEND_SOL_MIN_PERCENT) percent = LEND_SOL_MIN_PERCENT;
            targetRawBigInt = (targetRawBigInt * percent) / 10000n;
          }
          if (targetRawBigInt <= 0n) {
            break;
          }
          baseAmount = targetRawBigInt.toString();
          const human = formatBaseUnits(targetRawBigInt, currentBalance.decimals);
          displayAmount = rawAmount ? baseAmount : human;
        } else {
          baseAmount = rawAmount
            ? displayAmount
            : decimalToBaseUnits(displayAmount, currentBalance.decimals).toString();
        }

        const baseAmountBigInt = BigInt(baseAmount);
        if (baseAmountBigInt <= 0n) {
          break;
        }

        if (
          usesBaseAssets &&
          currentBalance.isSolLike &&
          !skipSendFlag &&
          !isInstructionRequest
        ) {
          let wrapFailed = false;
          try {
            if (!wrapConnection) {
              wrapConnection = createRpcConnection("confirmed");
            }
            const { lamports: existingWsolLamports } =
              await getWrappedSolAccountInfo(wrapConnection, wallet.kp.publicKey);
            await ensureWrappedSolBalance(
              wrapConnection,
              wallet,
              baseAmountBigInt,
              existingWsolLamports
            );
          } catch (wrapErr) {
            console.error(
              paint(
                `  ${normalizedAction} abort: failed to wrap SOL for ${wallet.name}:`,
                "error"
              ),
              wrapErr.message || wrapErr
            );
            wrapFailed = true;
          }
          if (wrapFailed) {
            break;
          }
        }

        // Official Jupiter Lend API request body
        // deposit/withdraw use "amount", mint/redeem use "shares"
        // For withdraw/redeem, asset must be the UNDERLYING asset, not the share token
        let assetMint = currentBalance.tokenRecord.mint;

        if (usesShareTokens) {
          // Get underlying asset from share token
          const underlyingAsset = getUnderlyingAssetForShareToken(assetMint);
          if (underlyingAsset) {
            assetMint = underlyingAsset;
          } else {
            console.warn(
              paint(
                `  Warning: Could not map share token ${currentBalance.tokenRecord.symbol || assetMint} to underlying asset`,
                "warn"
              )
            );
          }
        }

        const body = {
          asset: assetMint,
          signer: wallet.kp.publicKey.toBase58(),
        };

        // Use correct field name based on endpoint type
        if (field === "shares") {
          body.shares = baseAmount;
        } else {
          body.amount = baseAmount;
        }

        // Add any extra fields from --extra option if provided
        if (extra && typeof extra === 'object') {
          Object.assign(body, extra);
        }

        if (amountArg === "*") {
          const humanAmount = rawAmount
            ? `${displayAmount} (raw)`
            : displayAmount;
          const symbol =
            currentBalance.tokenRecord.symbol || symbolForMint(currentBalance.tokenRecord.mint);
          console.log(
            paint(
              `  plan → ${wallet.name}: ${normalizedAction} ${humanAmount} ${symbol}`,
              "info"
            )
          );
          if (usesBaseAssets && currentBalance.isSolLike) {
            const reserveLamports =
              typeof currentBalance.reserveLamports === "bigint"
                ? currentBalance.reserveLamports
                : BigInt(currentBalance.reserveLamports ?? 0);
            if (reserveLamports > 0n) {
              console.log(
                paint(
                  `    retaining ${formatBaseUnits(reserveLamports, 9)} SOL for rent/fees${currentBalance.requiresAtaCreation ? " (ATA)" : ""}`,
                  "muted"
                )
              );
            }
          }
        }

        console.log(
          paint(
            `  request payload (${action}) → ${wallet.name}`,
            "muted"
          ),
          JSON.stringify(body, null, 2)
        );

        // For withdrawals/redeems, pre-create the destination token account (underlying asset)
        // The lend API expects this account to exist before withdrawal
        if (usesShareTokens && !skipSendFlag && !isInstructionRequest) {
          try {
            const connection = createRpcConnection("confirmed");
            // The asset field contains the underlying asset mint (not the share token)
            // For jlSOL → wSOL, for jlUSDC → USDC, etc.
            const destinationMint = new PublicKey(body.asset);
            const tokenSymbol = currentBalance.tokenRecord.symbol || symbolForMint(body.asset);
            const destinationAta = await getAssociatedTokenAddress(
              destinationMint,
              wallet.kp.publicKey,
              false,
              TOKEN_PROGRAM_ID,
              ASSOCIATED_TOKEN_PROGRAM_ID
            );
            // Check if ATA exists
            const ataInfo = await connection.getAccountInfo(destinationAta);
            if (!ataInfo) {
              console.log(
                paint(
                  `  Creating destination ${tokenSymbol} account for ${wallet.name} before ${normalizedAction}...`,
                  "muted"
                )
              );
              await ensureAtaForMint(connection, wallet, destinationMint, TOKEN_PROGRAM_ID);
              await delay(500); // Give time for ATA creation to settle
            }
          } catch (ataErr) {
            console.warn(
              paint(
                `  Warning: Could not pre-create destination account for ${wallet.name}: ${ataErr.message || ataErr}`,
                "warn"
              )
            );
          }
        }

        let ataRetryNeeded = false;
        try {
          const result = await lendEarnRequest({
            path: action,
            method: "POST",
            body,
          });

          // Enhanced error handling
          if (!result.ok) {
            const errorMsg = result.data?.message || result.data?.error || result.raw || `HTTP ${result.status}`;
            console.error(
              paint(
                `  ${normalizedAction} failed for ${wallet.name}: ${errorMsg}`,
                "error"
              )
            );
            console.log(paint(`  API URL: ${result.url}`, "muted"));
            if (result.data) {
              console.log(paint(`  Response:`, "muted"), JSON.stringify(result.data, null, 2));
            }
            throw new Error(`Lend API ${normalizedAction} failed: ${errorMsg}`);
          }

          logLendApiResult(`earn ${action}`, result);

          if (!skipSendFlag && !isInstructionRequest) {
            const transactions = extractTransactionsFromLendResponse(result.data);

            if (transactions.length === 0) {
              console.warn(
                paint(
                  `  Warning: No transaction returned from ${normalizedAction} API for ${wallet.name}`,
                  "warn"
                )
              );
              console.log(paint(`  Response data:`, "muted"), JSON.stringify(result.data, null, 2));
              break;
            }

            console.log(
              paint(
                `  Submitting ${transactions.length} transaction(s) for ${wallet.name}.`,
                "info"
              )
            );
            const connection = createRpcConnection("confirmed");
            for (let i = 0; i < transactions.length; i += 1) {
              const txBase64 = transactions[i];
              const createdCount = await ensureAtasForTransaction({
                connection,
                wallet,
                txBase64,
                label: normalizedAction,
              });
              if (createdCount > 0) {
                ataRetryNeeded = true;
                break;
              }
              try {
                await submitSignedSolanaTransaction(connection, wallet, txBase64, {
                  label: normalizedAction,
                  index: i,
                  total: transactions.length,
                });
              } catch (sendErr) {
                console.error(
                  paint(
                    `  Failed to submit transaction ${i + 1}/${transactions.length} for ${wallet.name}:`,
                    "error"
                  ),
                  sendErr.message || sendErr
                );
                throw sendErr;
              }
              await delay(DELAY_BETWEEN_CALLS_MS);
            }
          }

          if (ataRetryNeeded) {
            console.log(
              paint(
                "  Associated token account(s) created; recalculating deposit...",
                "muted"
              )
            );
            await delay(DELAY_BETWEEN_CALLS_MS);
            const refreshedBalances = await loadWalletTokenBalances(wallet);
            currentBalance = refreshedBalances.find(
              (entry) => entry.tokenRecord.mint === balance.tokenRecord.mint
            );
            if (!currentBalance) {
              console.warn(
                paint(
                  `  Unable to refresh balances for ${wallet.name}; deposit aborted.`,
                  "warn"
                )
              );
              break;
            }
            continue;
          }

          succeeded = true;
          break;
        } catch (err) {
          const errMessage = err?.message || String(err);
          if (
            attempt < 4 &&
            /insufficient funds/i.test(errMessage) &&
            usesBaseAssets &&
            typeof currentBalance.spendableRaw !== "undefined"
          ) {
            const currentRaw =
              typeof currentBalance.spendableRaw === "bigint"
                ? currentBalance.spendableRaw
                : BigInt(currentBalance.spendableRaw ?? 0);
            if (currentRaw <= MIN_LEND_SOL_DEPOSIT_LAMPORTS) {
              console.warn(
                paint(
                  `  ${normalizedAction} failed: spendable SOL ${formatBaseUnits(currentRaw, currentBalance.decimals)} < minimum ${formatBaseUnits(MIN_LEND_SOL_DEPOSIT_LAMPORTS, currentBalance.decimals)} SOL. Top up the wallet before retrying.`,
                  "warn"
                )
              );
              break;
            }
            let percent =
              LEND_SOL_BASE_PERCENT -
              BigInt(attempt + 1) * LEND_SOL_RETRY_DECREMENT_PERCENT;
            if (percent < LEND_SOL_MIN_PERCENT) percent = LEND_SOL_MIN_PERCENT;
            let adjustedRaw = (currentRaw * percent) / 10000n;
            if (adjustedRaw <= LEND_SOL_WRAP_BUFFER_LAMPORTS) {
              console.warn(
                paint(
                  `  ${normalizedAction} failed: adjusted amount ${formatBaseUnits(adjustedRaw, currentBalance.decimals)} SOL leaves no room for wrap buffer (${formatBaseUnits(LEND_SOL_WRAP_BUFFER_LAMPORTS, currentBalance.decimals)} SOL).`,
                  "warn"
                )
              );
              break;
            }
            adjustedRaw -= LEND_SOL_WRAP_BUFFER_LAMPORTS;
            if (adjustedRaw > 0n) {
              currentBalance = {
                ...currentBalance,
                spendableRaw: adjustedRaw,
                spendableDecimal: formatBaseUnits(adjustedRaw, currentBalance.decimals),
              };
              console.warn(
                paint(
                  `  Retry ${attempt + 1}: reducing deposit amount after insufficient funds error (leaving additional buffer).`,
                  "warn"
                )
              );
              continue;
            }
          }
          console.error(
            paint(`Lend earn ${action} request failed for ${wallet.name}:`, "error"),
            errMessage
          );
          break;
        }
      }

      if (!succeeded) {
        console.warn(
          paint(
            `  ${normalizedAction} skipped for ${wallet.name}; see logs above.`,
            "warn"
          )
        );
      } else {
        walletHadSuccess = true;
      }
    }

    if (
      walletHadSuccess &&
      !skipSendFlag &&
      !isInstructionRequest &&
      (normalizedAction === "withdraw" || normalizedAction === "redeem")
    ) {
      const unwrapConnection =
        wrapConnection || createRpcConnection("confirmed");
      try {
        await autoUnwrapWrappedSol(unwrapConnection, wallet, {
          minLamports: 0n,
          reason: `${normalizedAction} cleanup`,
        });
      } catch (err) {
        console.warn(
          paint(
            `  ${normalizedAction} cleanup: failed to auto-unwrap wSOL for ${wallet.name} — ${err.message || err}`,
            "warn"
          )
        );
      }
    }
  }
}

function describeMintLabel(mint, options = {}) {
  const showAddress =
    options.showAddress !== undefined ? options.showAddress : true;
  const symbol = symbolForMint(mint);
  const name = nameForMint(mint);
  const distinctName =
    name && name.length > 0 && name.toUpperCase() !== symbol.toUpperCase()
      ? name
      : null;
  const parts = [];
  parts.push(symbol);
  if (distinctName) parts.push(`(${distinctName})`);
  if (showAddress) parts.push(`[${mint}]`);
  return parts.join(" ");
}
function isSimulationError(err) {
  const message = err?.message || '';
  return /simulation failed/i.test(message);
}

function isRateLimitMessage(message) {
  if (!message) return false;
  const lowered = message.toLowerCase();
  return (
    lowered.includes("429") ||
    lowered.includes("too many requests") ||
    lowered.includes("rate limit")
  );
}

function isRateLimitError(err) {
  if (!err) return false;
  if (typeof err === "string") {
    return isRateLimitMessage(err);
  }
  if (typeof err.code === "number" && err.code === 429) return true;
  if (typeof err.status === "number" && err.status === 429) return true;
  if (
    (typeof err.name === "string" && err.name === "TypeError" &&
      typeof err.message === "string" && err.message.toLowerCase().includes("fetch failed")) ||
    (typeof err.message === "string" && err.message.toLowerCase().includes("failed to fetch"))
  ) {
    return true;
  }
  if (isRateLimitMessage(err.message)) return true;
  if (Array.isArray(err.logs)) {
    const joined = err.logs.join("\n");
    if (isRateLimitMessage(joined)) return true;
  }
  return false;
}

function listAllRpcEndpoints() {
  if (RPC_ENDPOINTS.length > 0) return [...RPC_ENDPOINTS];
  return [DEFAULT_RPC_URL];
}

function getRpcEndpointByIndex(index) {
  const endpoints = listAllRpcEndpoints();
  if (index < 0 || index >= endpoints.length) {
    throw new Error(`RPC index ${index} out of range (0-${endpoints.length - 1})`);
  }
  return endpoints[index];
}


// ---- Helper utilities for logging / parsing ----
function paint(text, tone = "info") {
  const color = COLORS[tone] || "";
  const reset = COLORS.reset || "";
  return `${color}${text}${reset}`;
}

function parseTokenArgs(args) {
  if (!args || args.length === 0) return [];
  const tokens = [];
  for (const raw of args) {
    if (!raw) continue;
    const [mintRaw, symbolRaw] = raw.split(":");
    const mint = mintRaw?.trim();
    if (!mint) continue;
    const symbol = symbolRaw?.trim();
    tokens.push({ mint, symbol: symbol || null });
  }
  return tokens;
}

function listTokenCatalog(options = {}) {
  const { verbose = false } = options;
  const sorted = [...TOKEN_CATALOG].sort((a, b) =>
    a.symbol.localeCompare(b.symbol)
  );
  console.log(
    paint(`Token catalog (${sorted.length} entries)`, "label")
  );

  for (const token of sorted) {
    const programLabel =
      token.program === "token-2022"
        ? "Token-2022"
        : token.program === "native"
        ? "Native"
        : "SPL";
    const symbolCell = token.symbol.padEnd(10, " ");
    const baseLine = `${symbolCell} ${token.mint}`;
    const metaParts = [
      `decimals=${token.decimals}`,
      `program=${programLabel}`,
    ];
    if (token.symbol === "USDC" && DEFAULT_USDC_MINT !== token.mint) {
      metaParts.push(`default=${DEFAULT_USDC_MINT}`);
    }
    if (verbose) {
      if (token.tags?.length) metaParts.push(`tags=${token.tags.join(", ")}`);
      if (token.source) metaParts.push(`source=${token.source}`);
    }
    console.log(paint(`  ${baseLine} (${metaParts.join(", ")})`, "muted"));
  }

  if (!verbose) {
    console.log(
      paint(
        "  (run tokens --verbose for tag coverage and source metadata)",
        "muted"
      )
    );
  }
}

function logRandomMintResolution(resolution) {
  if (!resolution || !resolution.entry) return;
  const descriptor = resolution.entry.symbol
    ? `${resolution.entry.symbol} (${resolution.entry.mint})`
    : resolution.entry.mint;
  const label =
    typeof resolution.label === "string" && resolution.label.length > 0
      ? ` (${resolution.label})`
      : "";
  console.log(paint(`  Random mint resolved${label}: ${descriptor}`, "muted"));
}

function logDetailedError(prefix, err) {
  const baseMsg = err?.message || String(err);
  const key = `${prefix}::${baseMsg}`;
  const now = Date.now();
  const cached = RECENT_ERROR_LOGS.get(key);

  if (
    !VERBOSE_ERROR_OUTPUT &&
    ERROR_SUPPRESSION_WINDOW_MS > 0 &&
    cached &&
    now - cached.timestamp < ERROR_SUPPRESSION_WINDOW_MS
  ) {
    const nextRepeats = cached.repeats + 1;
    if (!cached.noticeIssued) {
      console.error(
        paint(
          `${prefix} (repeat x${nextRepeats + 1}; details suppressed — set JUPITER_SWAP_TOOL_VERBOSE_ERRORS=1 to inspect)`,
          "warn"
        )
      );
    }
    RECENT_ERROR_LOGS.set(key, {
      timestamp: now,
      repeats: nextRepeats,
      noticeIssued: true,
    });
    return;
  }

  RECENT_ERROR_LOGS.set(key, { timestamp: now, repeats: 0, noticeIssued: false });

  console.error(paint(prefix, "error"), baseMsg);

  if (!VERBOSE_ERROR_OUTPUT) {
    if (!verboseErrorHintShown) {
      console.error(
        paint(
          "  ↳ set JUPITER_SWAP_TOOL_VERBOSE_ERRORS=1 for full stack traces, logs, and RPC payloads.",
          "muted"
        )
      );
      verboseErrorHintShown = true;
    }
    return;
  }

  if (err?.logs) {
    console.error(paint("  logs:", "muted"), JSON.stringify(err.logs, null, 2));
  }

  if (err?.value) {
    console.error(paint("  rpc value:", "muted"), JSON.stringify(err.value, null, 2));
  }

  if (err?.response?.data) {
    console.error(
      paint("  response:", "muted"),
      typeof err.response.data === "string"
        ? err.response.data
        : JSON.stringify(err.response.data, null, 2)
    );
  }

  if (err?.stack) {
    console.error(
      paint("  stack:", "muted"),
      err.stack.split("\n").slice(0, 10).join("\n")
    );
  }

  const extra = {};
  for (const key in err || {}) {
    if (!Object.prototype.hasOwnProperty.call(err, key)) continue;
    if (["message", "logs", "stack", "response", "value"].includes(key)) continue;
    extra[key] = err[key];
  }
  if (Object.keys(extra).length > 0) {
    try {
      console.error(paint("  extra:", "muted"), JSON.stringify(extra, null, 2));
    } catch (_) {}
  }
}

function isLamportShortageError(err) {
  if (!err) return false;
  const msg = (err.message || "").toLowerCase();
  if (msg.includes("insufficient lamports")) return true;
  if (Array.isArray(err.logs)) {
    const joined = err.logs.join("\n").toLowerCase();
    if (joined.includes("insufficient lamports")) return true;
  }
  return false;
}
function isSlippageError(err) {
  if (!err) return false;
  const msg = (err.message || "").toLowerCase();
  if (msg.includes("slippage")) return true;
  if (msg.includes("price impact")) return true;
  if (Array.isArray(err.logs)) {
    const joined = err.logs.join("\n").toLowerCase();
    if (joined.includes("slippage")) return true;
    if (joined.includes("price impact")) return true;
  }
  return false;
}

// Enhanced error classification for better retry logic
function classifySwapError(err) {
  if (!err) return { type: 'unknown', retryable: false, message: 'Unknown error' };
  
  const msg = (err.message || "").toLowerCase();
  const logs = Array.isArray(err.logs) ? err.logs.join("\n").toLowerCase() : "";
  const combined = `${msg} ${logs}`;
  
  // Rate limiting
  if (isRateLimitError(err)) {
    return { type: 'rate_limit', retryable: true, message: 'Rate limit exceeded' };
  }
  
  // Slippage issues
  if (isSlippageError(err)) {
    return { type: 'slippage', retryable: true, message: 'Slippage tolerance exceeded' };
  }
  
  // Minimum output threshold (dust / rounding) issues
  if (
    /cannot compute other amount threshold/i.test(combined) ||
    /minimum output (?:not )?met/i.test(combined) ||
    /min(?:imum)? output/i.test(combined) ||
    /amount (?:became )?too small/i.test(combined)
  ) {
    return { type: 'min_output', retryable: false, message: 'Minimum output not satisfied for route' };
  }

  // Insufficient funds
  if (isLamportShortageError(err) || msg.includes("insufficient")) {
    return { type: 'insufficient_funds', retryable: true, message: 'Insufficient funds' };
  }
  
  // Simulation failures
  if (isSimulationError(err)) {
    return { type: 'simulation', retryable: true, message: 'Transaction simulation failed' };
  }
  
  // Route not found
  if (/route not found/i.test(msg) || /no route/i.test(msg)) {
    return { type: 'no_route', retryable: false, message: 'No trading route found' };
  }

  if (msg.includes("ultra order failed")) {
    return { type: 'simulation', retryable: true, message: err.message || 'Ultra order failed' };
  }

  if (msg.includes("ultra execute failed")) {
    return { type: 'confirmation', retryable: true, message: err.message || 'Ultra execute failed' };
  }
  
  // Network/RPC issues
  if (/(?:^|\D)(403|401)(?:\D|$)/.test(msg)) {
    return { type: 'rpc_auth', retryable: true, message: 'RPC authentication failed' };
  }
  
  // Transaction confirmation issues
  if (/transaction.*not.*confirmed/i.test(msg) || /timeout/i.test(msg)) {
    return { type: 'confirmation', retryable: true, message: 'Transaction confirmation timeout' };
  }
  
  // Token account issues
  if (/insufficient funds for rent/i.test(combined) || /account.*not.*found/i.test(combined)) {
    return { type: 'account_issue', retryable: false, message: 'Token account issue' };
  }
  
  return { type: 'unknown', retryable: false, message: msg };
}

// Enhanced retry manager with exponential backoff and better logging
class SwapRetryManager {
  constructor(options = {}) {
    this.maxRetries = options.maxRetries || 5;
    this.baseDelay = options.baseDelay || 1000;
    this.maxDelay = options.maxDelay || 30000;
    this.retryCounts = new Map();
  }
  
  shouldRetry(errorType, currentCount) {
    const retryableTypes = ['rate_limit', 'slippage', 'insufficient_funds', 'simulation', 'rpc_auth', 'confirmation'];
    return retryableTypes.includes(errorType) && currentCount < this.maxRetries;
  }
  
  getRetryDelay(retryCount) {
    const delay = Math.min(this.baseDelay * Math.pow(2, retryCount), this.maxDelay);
    // Add jitter to prevent thundering herd
    const jitter = Math.random() * 0.1 * delay;
    return Math.floor(delay + jitter);
  }
  
  formatRetryMessage(errorType, retryCount, maxRetries, errorInfo) {
    const messages = {
      rate_limit: `Rate limited, retrying ${retryCount}/${maxRetries}...`,
      slippage: `Slippage exceeded, retrying ${retryCount}/${maxRetries}...`,
      insufficient_funds: `Insufficient funds, retrying with reduced amount ${retryCount}/${maxRetries}...`,
      simulation: `Simulation failed, retrying ${retryCount}/${maxRetries}...`,
      rpc_auth: `RPC auth failed, switching endpoint and retrying ${retryCount}/${maxRetries}...`,
      confirmation: `Confirmation timeout, retrying ${retryCount}/${maxRetries}...`
    };
    return messages[errorType] || `Retrying ${retryCount}/${maxRetries}...`;
  }
}
const SOL_LIKE_MINTS = new Set([
  SOL_MINT,
  "11111111111111111111111111111111",
]);

const KNOWN_MINTS = new Map(
  TOKEN_CATALOG.map((entry) => {
    let programId = null;
    if (entry.program === "spl") {
      programId = TOKEN_PROGRAM_ID;
    } else if (entry.program === "token-2022") {
      programId = TOKEN_2022_PROGRAM_ID;
    }
    return [
      entry.mint,
      {
        decimals: entry.decimals,
        programId,
        symbol: entry.symbol,
      },
    ];
  })
);

const DEFAULT_SWEEP_TOKEN_SYMBOLS = TOKEN_CATALOG.filter((entry) =>
  tokenHasTag(entry, "default-sweep")
).map((entry) => entry.symbol);

const DEFAULT_SWEEP_MINTS = Array.from(
  new Set(
    DEFAULT_SWEEP_TOKEN_SYMBOLS.map((symbol) => {
      if (symbol === "USDC") return DEFAULT_USDC_MINT;
      const entry = tokenBySymbol(symbol);
      return entry?.mint || null;
    }).filter((mint) => mint && !SOL_LIKE_MINTS.has(mint))
  )
);

const RECENT_RANDOM_CATALOG_LIMIT = 24;
const recentCatalogMintHistory = [];

function rememberRecentCatalogMint(mint, limit = RECENT_RANDOM_CATALOG_LIMIT) {
  if (!mint) return;
  const normalized = typeof mint === "string" ? mint : String(mint ?? "");
  const existingIndex = recentCatalogMintHistory.indexOf(normalized);
  if (existingIndex !== -1) {
    recentCatalogMintHistory.splice(existingIndex, 1);
  }
  const effectiveLimit = Math.max(1, Number.isFinite(limit) ? Math.floor(limit) : RECENT_RANDOM_CATALOG_LIMIT);
  recentCatalogMintHistory.push(normalized);
  while (recentCatalogMintHistory.length > effectiveLimit) {
    recentCatalogMintHistory.shift();
  }
}

function combineTagLists(...lists) {
  const combined = new Set();
  for (const list of lists) {
    for (const tag of normaliseTagList(list)) {
      combined.add(tag);
    }
  }
  return [...combined];
}

function combineMintExclusions(...lists) {
  const result = new Set();
  const addMint = (mint) => {
    if (typeof mint === "string" && mint.length > 0) {
      result.add(mint);
    }
  };
  for (const list of lists) {
    if (!list) continue;
    if (list instanceof Set) {
      for (const mint of list) addMint(mint);
      continue;
    }
    if (Array.isArray(list)) {
      for (const mint of list) addMint(mint);
      continue;
    }
    if (typeof list === "string") {
      addMint(list);
    }
  }
  return result;
}

function sampleMintFromCatalog(options = {}) {
  const rngFn = typeof options.rng === "function" ? options.rng : DEFAULT_RNG;
  const skipSolLike = options.skipSolLike !== false;
  const avoidRecent = options.avoidRecent !== false;
  const remember = options.remember !== false;
  const rememberLimit = Number.isFinite(options.rememberLimit)
    ? Math.max(1, Math.floor(options.rememberLimit))
    : RECENT_RANDOM_CATALOG_LIMIT;
  const requireTags = combineTagLists(options.requireTags, options.tags);
  const anyTags = combineTagLists(options.anyTags);
  const exclude = combineMintExclusions(options.exclude);
  const preferFile = options.preferFile !== false;
  const filterFn = typeof options.filter === "function" ? options.filter : null;

  const sources = [];
  if (FILE_TOKEN_CATALOG.length > 0 && preferFile) {
    sources.push(FILE_TOKEN_CATALOG);
  }
  sources.push(TOKEN_CATALOG);

  const seen = new Set();
  const pool = [];
  for (const source of sources) {
    for (const entry of source) {
      if (!entry || typeof entry.mint !== "string") continue;
      const mint = entry.mint;
      if (seen.has(mint)) continue;
      seen.add(mint);
      if (skipSolLike && SOL_LIKE_MINTS.has(mint)) continue;
      if (exclude.has(mint)) continue;
      if (requireTags.length > 0 && !requireTags.every((tag) => tokenHasTag(entry, tag))) continue;
      if (anyTags.length > 0 && !anyTags.some((tag) => tokenHasTag(entry, tag))) continue;
      if (filterFn && !filterFn(entry)) continue;
      pool.push(entry);
    }
  }

  if (pool.length === 0) return null;

  let candidates = pool;
  if (avoidRecent && recentCatalogMintHistory.length > 0) {
    const recentSet = new Set(recentCatalogMintHistory);
    const filtered = pool.filter((entry) => !recentSet.has(entry.mint));
    if (filtered.length > 0) {
      candidates = filtered;
    }
  }

  const pickIndex = Math.floor(randomFloat(rngFn) * candidates.length);
  const chosen = candidates[pickIndex];
  if (!chosen) return null;
  if (remember) {
    rememberRecentCatalogMint(chosen.mint, rememberLimit);
  }
  return chosen;
}


const mintMetadataCache = new Map();
let cachedAtaRentLamports = null;
const ASCII_BANNER = String.raw`     ____.                   _____    ________    _____                      
    |    |__ ________       /  _  \  /  _____/  _/ ____\____ _______  _____  
    |    |  |  \____ \     /  /_\  \/   \  ___  \   __\\__  \\_  __ \/     \ 
/\__|    |  |  /  |_> >   /    |    \    \_\  \  |  |   / __ \|  | \/  Y Y  \
\________|____/|   __/ /\ \____|__  /\______  /  |__|  (____  /__|  |__|_|  /
               |__|    \/         \/        \/              \/            \/`;

function describeTokenCatalogSource() {
  if (tokenCatalogSourceLabel === "api") {
    return "Jupiter Tokens API v2 (+ local fallback)";
  }
  if (tokenCatalogSourceLabel === "file") {
    const relative = path.relative(process.cwd(), TOKEN_CATALOG_FILE);
    if (relative && !relative.startsWith("..")) return `${relative}`;
    return TOKEN_CATALOG_FILE;
  }
  return "built-in defaults";
}

function printStartupBanner() {
  console.log(paint(ASCII_BANNER, "label"));
  console.log(
    paint(`Jupiter Swap Tool v${TOOL_VERSION} — made by zayd / cold`, "label")
  );
  console.log(
    paint(
      `Loaded ${TOKEN_CATALOG.length} tokens from ${describeTokenCatalogSource()}.`,
      "muted"
    )
  );
  const activeEngineMode = USE_ULTRA_ENGINE ? "ultra" : "lite";
  const engineLabel = formatSwapEngineLabel(activeEngineMode);
  if (activeEngineMode === "ultra") {
    const tone = JUPITER_ULTRA_API_KEY ? "info" : "warn";
    console.log(paint(`Swap engine: ${engineLabel}`, tone));
    if (!JUPITER_ULTRA_API_KEY) {
      console.log(paint("  Tip: set JUPITER_ULTRA_API_KEY for higher limits.", "warn"));
    }
  } else {
    console.log(paint(`Swap engine: ${engineLabel}`, "muted"));
  }
  console.log(
    paint(
      "Tip: run `tokens --verbose` to inspect the catalog used by automated flows.",
      "muted"
    )
  );
}


const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const SESSION_RNG_NAMESPACE =
  process.env.JUPITER_SWAP_TOOL_SESSION_SEED ||
  process.env.JUPITER_SWAP_TOOL_RANDOM_SEED ||
  "jupiter-swap-tool-session";

function hashStringToUint32(input) {
  const str =
    typeof input === "string"
      ? input
      : JSON.stringify(input, (_, value) =>
          typeof value === "bigint" ? value.toString() : value
        );
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i += 1) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

function createDeterministicRng(seedInput) {
  let state = hashStringToUint32(seedInput) || 0x811c9dc5;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function deriveWalletSessionRng(wallet, scope = "default") {
  const components = [
    SESSION_RNG_NAMESPACE,
    scope,
    wallet?.name || "",
    wallet?.kp?.publicKey ? wallet.kp.publicKey.toBase58() : "",
  ];
  return createDeterministicRng(components.join("|"));
}

const pickFirstDefined = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return undefined;
};

const parseDurationOverride = (raw) => {
  if (raw === undefined || raw === null) return null;
  if (typeof raw === "number") {
    if (!Number.isFinite(raw)) return null;
    return raw;
  }
  const str = String(raw).trim();
  if (str.length === 0) return null;
  const match = str.match(
    /^(-?\d+(?:\.\d+)?)\s*(ms|millisecond(?:s)?|s|sec(?:ond)?(?:s)?|m|min(?:ute)?(?:s)?|h|hr(?:s)?|hour(?:s)?)?$/i
  );
  if (!match) {
    const numeric = Number(str);
    if (!Number.isFinite(numeric)) return null;
    return numeric;
  }
  const value = Number(match[1]);
  if (!Number.isFinite(value)) return null;
  const unit = (match[2] || "ms").toLowerCase();
  if (unit.startsWith("ms") || unit.startsWith("millisecond")) {
    return value;
  }
  if (unit.startsWith("h") || unit.startsWith("hr") || unit.startsWith("hour")) {
    return value * HOUR_MS;
  }
  if (unit.startsWith("m")) {
    return value * MINUTE_MS;
  }
  if (unit.startsWith("s")) {
    return value * 1000;
  }
  return value;
};

const extractDurationOverride = (options, candidates) => {
  if (!options || typeof options !== "object") return null;
  for (const candidate of candidates) {
    if (!(candidate in options)) continue;
    const raw = options[candidate];
    if (raw === undefined || raw === null) continue;
    const keyLower = candidate.toLowerCase();
    if (keyLower.endsWith("minutes") || keyLower.endsWith("minute") || keyLower.endsWith("mins")) {
      const numeric = Number(raw);
      if (Number.isFinite(numeric)) {
        return numeric * MINUTE_MS;
      }
    }
    const parsed = parseDurationOverride(raw);
    if (parsed !== null) {
      return parsed;
    }
  }
  return null;
};

const pickIntInclusive = (rng, min, max) =>
  randomIntInclusive(min, max, rng);

const applyScalingToSegments = (segments, rawTotal, target) => {
  if (!Number.isFinite(rawTotal) || rawTotal <= 0) {
    return segments.map((segment) => ({ ...segment }));
  }
  const ratio = target / rawTotal;
  return segments.map((segment) => {
    const scaled = Math.round(segment.waitMs * ratio);
    return {
      ...segment,
      waitMs: clamp(scaled, segment.minMs, segment.maxMs),
    };
  });
};

const rebalanceSegmentDurations = (segments, target) => {
  if (!Array.isArray(segments) || segments.length === 0) return 0;
  let current = segments.reduce((sum, entry) => sum + entry.waitMs, 0);
  const tolerance = Math.max(500, Math.round(target * 0.0025));
  let guard = 0;
  while (Math.abs(current - target) > tolerance && guard < 2000) {
    const delta = target - current;
    if (delta === 0) break;
    const direction = delta > 0 ? 1 : -1;
    const candidates = segments.filter((entry) =>
      direction > 0 ? entry.waitMs < entry.maxMs : entry.waitMs > entry.minMs
    );
    if (candidates.length === 0) break;
    const step = Math.max(1, Math.round(Math.abs(delta) / Math.max(4, candidates.length)));
    let applied = 0;
    for (const entry of candidates) {
      if (Math.abs(current - target) <= tolerance) break;
      const room = direction > 0 ? entry.maxMs - entry.waitMs : entry.waitMs - entry.minMs;
      if (room <= 0) continue;
      const amount = Math.min(room, step);
      entry.waitMs += direction * amount;
      current += direction * amount;
      applied += direction * amount;
      if (Math.abs(current - target) <= tolerance) break;
    }
    if (applied === 0) {
      break;
    }
    guard += 1;
  }
  return current;
};

const resolveDurationTargets = (definition, options, loops, minPossible, maxPossible) => {
  const baseDuration = Math.max(1, Number(definition?.defaultDurationMs || 0)) * loops;
  const minOverride = extractDurationOverride(options, [
    "durationMinMs",
    "durationMin",
    "durationLowerMs",
    "durationLower",
    "minDurationMs",
    "minDuration",
    "durationMinMinutes",
    "durationMinMins",
    "minDurationMinutes",
  ]);
  const maxOverride = extractDurationOverride(options, [
    "durationMaxMs",
    "durationMax",
    "durationUpperMs",
    "durationUpper",
    "maxDurationMs",
    "maxDuration",
    "durationMaxMinutes",
    "durationMaxMins",
    "maxDurationMinutes",
  ]);
  const targetOverride = extractDurationOverride(options, [
    "durationMs",
    "duration",
    "durationTargetMs",
    "durationTarget",
    "targetDurationMs",
    "targetDuration",
    "durationMinutes",
    "durationMins",
  ]);

  let minTarget = Number.isFinite(minOverride) ? Math.max(1, minOverride) : baseDuration;
  let maxTarget = Number.isFinite(maxOverride) ? Math.max(1, maxOverride) : baseDuration;

  if (!Number.isFinite(minOverride) && Number.isFinite(maxOverride)) {
    minTarget = Math.min(baseDuration, maxTarget);
  }

  if (!Number.isFinite(maxOverride) && Number.isFinite(minOverride)) {
    maxTarget = Math.max(baseDuration, minTarget);
  }

  if (minTarget > maxTarget) {
    const tmp = minTarget;
    minTarget = maxTarget;
    maxTarget = tmp;
  }
  let targetDuration = Number.isFinite(targetOverride) ? targetOverride : baseDuration;
  targetDuration = clamp(targetDuration, minTarget, maxTarget);

  const boundedMin = clamp(minTarget, minPossible, maxPossible);
  const boundedMax = clamp(maxTarget, boundedMin, maxPossible);
  const boundedTarget = clamp(targetDuration, boundedMin, boundedMax);

  return {
    minTarget: boundedMin,
    maxTarget: boundedMax,
    targetDuration: boundedTarget,
  };
};

export async function runPrewrittenFlow(flowKey, options = {}) {
  const keyCandidate = pickFirstDefined(flowKey, options.flowKey, options.name);
  const normalizedKey = typeof keyCandidate === "string"
    ? keyCandidate.trim().toLowerCase()
    : "";
  const definition = PREWRITTEN_FLOW_DEFINITIONS[normalizedKey];
  if (!definition) {
    const available = Object.keys(PREWRITTEN_FLOW_DEFINITIONS);
    throw new Error(
      `Unknown prewritten flow '${flowKey}'. Available flows: ${available.join(", ")}`
    );
  }

  let rng = typeof options.rng === "function" ? options.rng : null;
  if (!rng) {
    rng = createDeterministicRng(`${normalizedKey}:scheduler`);
  }

  const swapRange = definition.swapCountRange || {};
  const swapsPerCycle = Math.max(
    1,
    Math.floor(
      pickFirstDefined(
        definition.swapsPerCycle,
        definition.cycleSwapCount,
        definition.cycleLength,
        Array.isArray(definition.legs) ? definition.legs.length : 1
      )
    )
  );
  const baseCycleLegs = Array.isArray(definition.legs)
    ? definition.legs.map((leg, legIndex) => ({ legIndex, leg }))
    : [];
  const cycleLegSequence =
    swapsPerCycle > 0 && baseCycleLegs.length > 0
      ? Array.from({ length: swapsPerCycle }, (_, cycleIndex) => {
          const source = baseCycleLegs[cycleIndex % baseCycleLegs.length];
          return {
            legIndex: source.legIndex,
            leg: source.leg,
            cycleHopIndex: cycleIndex,
            legRepetition: Math.floor(cycleIndex / baseCycleLegs.length),
          };
        })
      : [];
  const swapRangeMin = Math.max(
    swapsPerCycle,
    Math.floor(swapRange.min ?? swapsPerCycle)
  );
  const swapRangeMax = Math.max(
    swapRangeMin,
    Math.floor(swapRange.max ?? swapRangeMin)
  );

  const swapTargetOverride = pickFirstDefined(
    options.swapTarget,
    options.swapCount,
    options.targetSwapCount,
    options.targetSwaps,
    options.targetHopCount
  );

  const normalizedOverride = Number(swapTargetOverride);
  const hasOverride = Number.isFinite(normalizedOverride) && normalizedOverride > 0;
  let sampledSwapTarget = hasOverride
    ? Math.max(swapRangeMin, Math.floor(normalizedOverride))
    : pickIntInclusive(rng, swapRangeMin, swapRangeMax);

  const minimumCycles = Math.max(
    1,
    Math.floor(pickFirstDefined(options.minimumCycles, definition.minimumCycles, 1))
  );
  const minimumSwapCount = Math.max(
    swapsPerCycle,
    minimumCycles * swapsPerCycle,
    swapRangeMin,
    Math.floor(
      pickFirstDefined(
        options.minimumSwapCount,
        definition.minimumSwapCount,
        swapRangeMin
      )
    )
  );

  let effectiveSwapTarget = Math.max(sampledSwapTarget, minimumSwapCount);

  const loopsRaw = pickFirstDefined(
    options.loops,
    options.loopCount,
    options.loop,
    definition.defaultLoops,
    null
  );
  let loopsOverride = Number(loopsRaw);
  if (!Number.isFinite(loopsOverride) || loopsOverride <= 0) {
    loopsOverride = null;
  } else {
    loopsOverride = Math.max(1, Math.floor(loopsOverride));
  }

  let loops;
  if (loopsOverride !== null) {
    loops = Math.max(loopsOverride, minimumCycles);
  } else {
    loops = Math.ceil(effectiveSwapTarget / swapsPerCycle);
    if (loops < minimumCycles) loops = minimumCycles;
  }
  if (!Number.isFinite(loops) || loops <= 0) {
    loops = minimumCycles;
  }

  const requiredLoops = Math.max(
    loops,
    Math.ceil(minimumSwapCount / swapsPerCycle),
    Math.ceil(effectiveSwapTarget / swapsPerCycle)
  );
  loops = Number.isFinite(requiredLoops) && requiredLoops > 0 ? requiredLoops : minimumCycles;

  const minimumPlannedSwaps = Math.max(minimumSwapCount, effectiveSwapTarget);
  let loopAlignedSwapTarget = loops * swapsPerCycle;
  let plannedSwapTarget = Math.max(minimumPlannedSwaps, loopAlignedSwapTarget);
  const loopRequirementFromPlanned = Math.ceil(plannedSwapTarget / swapsPerCycle);
  if (loopRequirementFromPlanned > loops) {
    loops = loopRequirementFromPlanned;
    loopAlignedSwapTarget = loops * swapsPerCycle;
    plannedSwapTarget = Math.max(minimumPlannedSwaps, loopAlignedSwapTarget);
  }
  effectiveSwapTarget = plannedSwapTarget;

  const sampledSegments = [];
  if (cycleLegSequence.length > 0 && plannedSwapTarget > 0) {
    const cycleLength = cycleLegSequence.length;
    const totalSegments = plannedSwapTarget;
    for (let segmentIndex = 0; segmentIndex < totalSegments; segmentIndex += 1) {
      const loopIndex = Math.floor(segmentIndex / cycleLength);
      const cycleIndex = segmentIndex % cycleLength;
      const { legIndex, leg, cycleHopIndex, legRepetition } =
        cycleLegSequence[cycleIndex] || {};
      const minMs = Math.max(0, Math.round(leg?.segmentWaitsMs?.minMs ?? 0));
      const maxMs = Math.max(minMs, Math.round(leg?.segmentWaitsMs?.maxMs ?? minMs));
      const waitMs = pickIntInclusive(rng, minMs, maxMs);
      sampledSegments.push({
        flowKey: normalizedKey,
        loopIndex,
        cycleIndex,
        swapIndex: segmentIndex,
        legIndex,
        cycleHopIndex,
        legRepetition,
        legKey: leg?.key,
        label: leg?.label,
        minMs,
        maxMs,
        waitMs,
      });
    }
  }

  if (sampledSegments.length === 0) {
    return {
      definition,
      flowKey: normalizedKey,
      flowLabel: definition.label,
      loops,
      targetDurationMs: 0,
      totalPlannedWaitMs: 0,
      rawSampledDurationMs: 0,
      segments: [],
    };
  }

  const minPossible = sampledSegments.reduce((sum, segment) => sum + segment.minMs, 0);
  const maxPossible = sampledSegments.reduce((sum, segment) => sum + segment.maxMs, 0);
  const rawTotal = sampledSegments.reduce((sum, segment) => sum + segment.waitMs, 0);

  const { minTarget, maxTarget, targetDuration } = resolveDurationTargets(
    definition,
    options,
    loops,
    minPossible,
    maxPossible
  );
  const desiredDuration = clamp(targetDuration ?? rawTotal, minTarget, maxTarget);

  const scaledSegments = applyScalingToSegments(sampledSegments, rawTotal, desiredDuration);
  rebalanceSegmentDurations(scaledSegments, desiredDuration);
  const finalTotal = scaledSegments.reduce((sum, segment) => sum + segment.waitMs, 0);

  const schedule = {
    definition,
    flowKey: normalizedKey,
    flowLabel: definition.label,
    loops,
    swapsPerCycle,
    swapTarget: {
      sampled: sampledSwapTarget,
      minimum: minimumSwapCount,
      planned: plannedSwapTarget,
      range: { min: swapRangeMin, max: swapRangeMax },
      effective: effectiveSwapTarget,
    },
    requireTerminalSolHop: definition.requireTerminalSolHop === true,
    targetDurationMs: desiredDuration,
    totalPlannedWaitMs: finalTotal,
    rawSampledDurationMs: rawTotal,
    segments: scaledSegments.map((segment) => ({ ...segment })),
  };

  const shouldExecuteWaits = options.executeWaits !== false;
  if (shouldExecuteWaits) {
    for (const segment of schedule.segments) {
      if (typeof options.onSegment === "function") {
        await options.onSegment(segment, schedule);
      }
      await delay(segment.waitMs);
    }
  } else if (typeof options.onSegment === "function") {
    for (const segment of schedule.segments) {
      await options.onSegment(segment, schedule);
    }
  }

  return schedule;
}

function passiveSleep() {
  if (PASSIVE_STEP_DELAY_MS <= 0 && PASSIVE_STEP_JITTER_MS <= 0) {
    return Promise.resolve();
  }
  const base = Math.max(0, PASSIVE_STEP_DELAY_MS);
  const jitter = Math.max(0, PASSIVE_STEP_JITTER_MS);
  let extra = 0;
  if (jitter > 0) {
    // Random jitter in the range [-jitter, +jitter] for a more human cadence.
    const span = jitter * 2;
    extra = Math.floor(Math.random() * (span + 1)) - jitter;
  }
  const totalDelay = Math.max(0, base + extra);
  return delay(totalDelay);
}

function balanceRpcDelay() {
  if (BALANCE_RPC_DELAY_MS <= 0) return Promise.resolve();
  return delay(BALANCE_RPC_DELAY_MS);
}

function shuffleArray(array, rng = DEFAULT_RNG) {
  const generator = normaliseRng(rng);
  const result = Array.isArray(array) ? [...array] : [];
  for (let i = result.length - 1; i > 0; i -= 1) {
    const randomValue = randomFloat(generator);
    const j = Math.floor(randomValue * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

async function ensureCachedAtaRentLamports(connection) {
  if (cachedAtaRentLamports === null) {
    const rent = await connection.getMinimumBalanceForRentExemption(165);
    cachedAtaRentLamports = BigInt(rent);
  }
  return cachedAtaRentLamports;
}

function ensureKeypairDir() {
  if (!fs.existsSync(KEYPAIR_DIR)) {
    try {
      fs.mkdirSync(KEYPAIR_DIR, { recursive: true });
      console.log(paint(`Created keypairs directory: ${KEYPAIR_DIR}`, "success"));
    } catch (err) {
      throw new Error(`Failed to create keypairs directory: ${err.message}`);
    }
  }
}

function loadDisableStateFromDisk() {
  try {
    const raw = fs.readFileSync(DISABLE_STATE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return { disabledWallets: [], forceResetActive: false, lastComputedAt: null };
    }
    const disabledWallets = Array.isArray(parsed.disabledWallets)
      ? parsed.disabledWallets
          .filter((item) => typeof item === "string")
          .filter((item) => item !== "crew_1.json")
      : [];
    const forceResetActive = parsed.forceResetActive === true;
    const lastComputedAt =
      typeof parsed.lastComputedAt === "number" ? parsed.lastComputedAt : null;
    return { disabledWallets, forceResetActive, lastComputedAt };
  } catch (_) {
    return { disabledWallets: [], forceResetActive: false, lastComputedAt: null };
  }
}

let disableState = loadDisableStateFromDisk();
const DISABLED_WALLETS = new Set(disableState.disabledWallets || []);
DISABLED_WALLETS.delete("crew_1.json");

function persistDisableState() {
  const payload = {
    disabledWallets: Array.from(DISABLED_WALLETS),
    forceResetActive: disableState.forceResetActive === true,
    lastComputedAt: disableState.lastComputedAt ?? Date.now(),
  };
  try {
    fs.writeFileSync(DISABLE_STATE_FILE, JSON.stringify(payload, null, 2));
  } catch (err) {
    console.warn(
      paint(
        `warning: unable to persist wallet disable state — ${err.message || err}`,
        "warn"
      )
    );
  }
}

function isWalletDisabledByGuard(walletName) {
  if (walletName === "crew_1.json") return false;
  return DISABLED_WALLETS.has(walletName);
}

async function refreshWalletDisableStatus(options = {}) {
  const {
    connection: providedConnection = null,
    wallets: providedWallets = null,
    silent = false,
  } = options;
  const wallets = Array.isArray(providedWallets) ? providedWallets : listWallets();
  const lamportsMap = new Map();
  if (wallets.length === 0) {
    DISABLED_WALLETS.clear();
    disableState.forceResetActive = false;
    disableState.lastComputedAt = Date.now();
    persistDisableState();
    return lamportsMap;
  }

  const connection = providedConnection || createRpcConnection("confirmed");
  const newDisabled = [];
  for (const wallet of wallets) {
    try {
      await balanceRpcDelay();
      const lamports = BigInt(await getSolBalance(connection, wallet.kp.publicKey));
      lamportsMap.set(wallet.name, lamports);
      if (wallet.name !== "crew_1.json" && lamports < WALLET_DISABLE_THRESHOLD_LAMPORTS) {
        newDisabled.push(wallet.name);
      }
    } catch (err) {
      if (!silent) {
        console.warn(
          paint(
            `  warning: unable to read SOL balance for ${wallet.name} — ${err.message || err}`,
            "warn"
          )
        );
      }
    }
  }

  DISABLED_WALLETS.clear();
  for (const name of newDisabled) {
    DISABLED_WALLETS.add(name);
  }
  disableState.forceResetActive = false;
  disableState.lastComputedAt = Date.now();
  persistDisableState();
  return lamportsMap;
}

function forceResetWalletDisableState() {
  DISABLED_WALLETS.clear();
  disableState.forceResetActive = true;
  disableState.lastComputedAt = Date.now();
  persistDisableState();
}

async function autoRefreshWalletDisables() {
  if (disableState.forceResetActive) {
    return;
  }
  await refreshWalletDisableStatus({ silent: true });
}

function getWalletGuardSummary(options = {}) {
  const walletList = Array.isArray(options.wallets) ? options.wallets : listWallets();
  const total = walletList.length;
  const guardSuspended = disableState.forceResetActive === true;
  const disabledNames = [];
  if (!guardSuspended) {
    for (const wallet of walletList) {
      if (isWalletDisabledByGuard(wallet.name)) {
        disabledNames.push(wallet.name);
      }
    }
  }
  const disabled = guardSuspended ? 0 : disabledNames.length;
  const active = total - disabled;
  return {
    total,
    active: active >= 0 ? active : 0,
    disabled,
    guardSuspended,
    disabledNames,
  };
}

function listWallets(...args) {
  return sharedListWallets(...args);
}

/* -------------------------------------------------------------------------- */
/* Campaign runtime glue                                                      */
/* -------------------------------------------------------------------------- */

const campaignWalletRegistry = new Map();
let campaignHooksRegistered = false;
let campaignDryRun = false;

function getCampaignWallet(pubkeyBase58) {
  if (!campaignWalletRegistry.has(pubkeyBase58)) {
    throw new Error(`campaign wallet ${pubkeyBase58} not registered`);
  }
  return campaignWalletRegistry.get(pubkeyBase58);
}

function ensureCampaignHooksRegistered() {
  if (campaignHooksRegistered) return;
  registerCampaignHooks({
    getSolLamports: async (pubkeyBase58) => {
      const connection = createRpcConnection("confirmed");
      try {
        const lamports = await connection.getBalance(new PublicKey(pubkeyBase58));
        return BigInt(lamports);
      } finally {
        try {
          connection?.destroy?.();
        } catch (_) {}
      }
    },
    getSplLamports: async (pubkeyBase58, mint) => {
      return campaignGetSplLamports(pubkeyBase58, mint);
    },
    jupiterLiteSwap: async (pubkeyBase58, inMint, outMint, lamports) => {
      return performCampaignSwap({
        pubkeyBase58,
        inMint,
        outMint,
        amountLamports: lamports,
      });
    },
    findLargestSplHolding: async (pubkeyBase58) => {
      return campaignFindLargestHolding(pubkeyBase58);
    },
    findSplHoldingByMint: async (pubkeyBase58, mint) => {
      return campaignFindHolding(pubkeyBase58, mint);
    },
    splToLamports: async (pubkeyBase58, mint, uiAmount) => {
      return campaignSplToLamports(pubkeyBase58, mint, uiAmount);
    },
    getSplBalanceLamports: async (pubkeyBase58, mint) => {
      return campaignGetSplBalanceLamports(pubkeyBase58, mint);
    },
  });
  campaignHooksRegistered = true;
}

async function ensureCampaignAta(connection, ownerKeypair, mint, programId) {
  const mintPubkey = new PublicKey(mint);
  const ata = await getAssociatedTokenAddress(
    mintPubkey,
    ownerKeypair.publicKey,
    false,
    programId || TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const info = await connection.getAccountInfo(ata);
  if (info) {
    return { ata, created: false };
  }
  const transaction = new Transaction().add(
    createAssociatedTokenAccountInstruction(
      ownerKeypair.publicKey,
      ata,
      ownerKeypair.publicKey,
      mintPubkey,
      programId || TOKEN_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID
    )
  );
  transaction.feePayer = ownerKeypair.publicKey;
  const { blockhash } = await connection.getLatestBlockhash();
  transaction.recentBlockhash = blockhash;
  transaction.sign(ownerKeypair);
  const raw = transaction.serialize();
  const signature = await connection.sendRawTransaction(raw);
  await connection.confirmTransaction(signature, "confirmed");
  return { ata, created: true };
}

async function performCampaignSwap({ pubkeyBase58, inMint, outMint, amountLamports }) {
  const entry = getCampaignWallet(pubkeyBase58);
  const wallet = entry.wallet;
  const lamportsBig = BigInt(amountLamports ?? 0);
  if (lamportsBig <= 0n) {
    throw new Error("campaign swap requires positive lamports");
  }
  const userPubkey = wallet.kp.publicKey;
  const connection = createRpcConnection("confirmed");
  const metadataConnection = createRpcConnection("confirmed");
  let inputMeta = null;
  let outputMeta = null;
  try {
    if (!SOL_LIKE_MINTS.has(inMint)) {
      inputMeta = await resolveMintMetadata(metadataConnection, inMint);
    }
    if (!SOL_LIKE_MINTS.has(outMint)) {
      outputMeta = await resolveMintMetadata(metadataConnection, outMint);
      await ensureCampaignAta(connection, wallet.kp, outMint, outputMeta.programId);
    }
  } catch (err) {
    throw new Error(`metadata prep failed: ${err?.message || err}`);
  }

  let quote;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      quote = await fetchLegacyQuote(
        inMint,
        outMint,
        lamportsBig,
        userPubkey.toBase58(),
        SLIPPAGE_BPS
      );
      break;
    } catch (err) {
      const message = err?.message || String(err || "");
      if (/rate limit/i.test(message) && attempt < 3) {
        await delay(500 * (attempt + 1));
        continue;
      }
      throw err;
    }
  }

  if (!quote) {
    throw new Error("quote unavailable after retries");
  }

  if (campaignDryRun) {
    const outAmountLamports = BigInt(quote.outAmount || quote.outAmountWithSlippage || 0);
    const inputDecimals = inputMeta?.decimals ?? 9;
    const outputDecimals = outputMeta?.decimals ?? (SOL_LIKE_MINTS.has(outMint) ? 9 : 6);
    console.log(
      paint(
        `[dry-run] ${entry.name}: ${symbolForMint(inMint)}→${symbolForMint(outMint)} amount=${formatBaseUnits(lamportsBig, inputDecimals)} expectedOut=${formatBaseUnits(outAmountLamports, outputDecimals)}`,
        "muted"
      )
    );
    return `dry-run-${Date.now()}`;
  }

  const swapPayload = await fetchLegacySwap(
    quote,
    userPubkey.toBase58(),
    SOL_LIKE_MINTS.has(inMint) || SOL_LIKE_MINTS.has(outMint)
  );
  const txBuffer = Buffer.from(swapPayload.swapTransaction, "base64");
  const vtx = VersionedTransaction.deserialize(txBuffer);
  vtx.sign([wallet.kp]);
  const raw = vtx.serialize();
  const signature = await connection.sendRawTransaction(raw);
  await connection.confirmTransaction(signature, "confirmed");
  return signature;
}

async function campaignFindLargestHolding(pubkeyBase58) {
  const entry = getCampaignWallet(pubkeyBase58);
  const owner = entry.wallet.kp.publicKey;
  const connection = createRpcConnection("confirmed");
  try {
    const parsed = await getAllParsedTokenAccounts(connection, owner);
    let best = null;
    for (const { account } of parsed) {
      const info = account?.data?.parsed?.info;
      if (!info) continue;
      const mint = info.mint;
      if (!mint || SOL_LIKE_MINTS.has(mint)) continue;
      const rawAmount = info.tokenAmount?.amount ?? "0";
      let amount;
      try {
        amount = BigInt(rawAmount);
      } catch (_) {
        amount = 0n;
      }
      if (amount <= 0n) continue;
      if (!best || amount > best.amount) {
        best = {
          mint,
          amount,
          uiAmount: rawAmount,
          decimals: info.tokenAmount?.decimals ?? 0,
        };
      }
    }
    if (!best) return null;
    return { mint: best.mint, uiAmount: best.uiAmount, decimals: best.decimals };
  } finally {
    try {
      connection?.destroy?.();
    } catch (_) {}
  }
}

async function campaignGetSplBalanceLamports(pubkeyBase58, mint) {
  if (!mint) {
    return 0n;
  }
  const entry = getCampaignWallet(pubkeyBase58);
  const owner = entry.wallet.kp.publicKey;
  const connection = createRpcConnection("confirmed");
  try {
    const parsed = await getAllParsedTokenAccounts(connection, owner);
    let total = 0n;
    for (const { account } of parsed) {
      const info = account?.data?.parsed?.info;
      if (!info) continue;
      if (info.mint !== mint) continue;
      const rawAmount = info.tokenAmount?.amount ?? "0";
      try {
        total += BigInt(rawAmount);
      } catch (_) {
        continue;
      }
    }
    return total;
  } finally {
    try {
      connection?.destroy?.();
    } catch (_) {}
  }
}

async function campaignSplToLamports(pubkeyBase58, mint, uiAmount) {
  if (typeof uiAmount === "bigint") {
    return uiAmount;
  }
  if (typeof uiAmount === "number") {
    const connection = createRpcConnection("confirmed");
    try {
      const meta = await resolveMintMetadata(connection, mint);
      const decimals = meta?.decimals ?? 0;
      return BigInt(Math.floor(uiAmount * 10 ** decimals));
    } finally {
      try {
        connection?.destroy?.();
      } catch (_) {}
    }
  }
  if (typeof uiAmount === "string") {
    if (/^\d+$/.test(uiAmount)) {
      return BigInt(uiAmount);
    }
    const parsed = Number(uiAmount);
    if (!Number.isNaN(parsed)) {
      const connection = createRpcConnection("confirmed");
      try {
        const meta = await resolveMintMetadata(connection, mint);
        const decimals = meta?.decimals ?? 0;
        return BigInt(Math.floor(parsed * 10 ** decimals));
      } finally {
        try {
          connection?.destroy?.();
        } catch (_) {}
      }
    }
  }
  if (uiAmount && typeof uiAmount === "object") {
    if (typeof uiAmount.amount === "string" && /^\d+$/.test(uiAmount.amount)) {
      return BigInt(uiAmount.amount);
    }
    if (typeof uiAmount.uiAmount === "number") {
      return campaignSplToLamports(pubkeyBase58, mint, uiAmount.uiAmount);
    }
    if (typeof uiAmount.uiAmountString === "string") {
      return campaignSplToLamports(pubkeyBase58, mint, uiAmount.uiAmountString);
    }
  }
  return 0n;
}

function filterWalletsByBatch(wallets, batchRaw) {
  const normalized = (batchRaw || "all").toString().trim().toLowerCase();
  if (normalized === "all" || normalized.length === 0) {
    return wallets;
  }
  if (normalized === "1" || normalized === "2") {
    const target = parseInt(normalized, 10) - 1;
    return wallets.filter((_, index) => index % 2 === target);
  }
  throw new Error("campaign --batch must be 1, 2, or all");
}

async function handleCampaignCommand(rawArgs) {
  const { options, rest } = parseCliOptions(rawArgs);
  const [campaignKeyRaw, durationKeyRaw] = rest;
  const RANDOM_PLACEHOLDER = "RANDOM";
  if (!campaignKeyRaw || !durationKeyRaw) {
    throw new Error(
      "campaign usage: campaign <meme-carousel|scatter-then-converge|btc-eth-circuit|icarus|zenith|aurora> <30m|1h|2h|6h> [--batch <1|2|all>] [--dry-run]"
    );
  }
  const campaignKey = campaignKeyRaw.toLowerCase();
  const durationKey = durationKeyRaw.toLowerCase();
  const preset = CAMPAIGNS[campaignKey];
  if (!preset) {
    throw new Error(`campaign ${campaignKeyRaw} not recognised`);
  }
  if (!preset.durations[durationKey]) {
    throw new Error(`campaign duration ${durationKeyRaw} not supported`);
  }

  let wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found in keypairs directory.", "warn"));
    return;
  }

  const batchRaw = options.batch || options.Batch || options.BATCH;
  wallets = filterWalletsByBatch(wallets, batchRaw);
  wallets = wallets.filter((wallet) => !isWalletDisabledByGuard(wallet.name));
  if (wallets.length === 0) {
    console.log(paint("No eligible wallets after filtering/batch selection.", "warn"));
    return;
  }

  const dryRun =
    coerceCliBoolean(options["dry-run"]) ||
    coerceCliBoolean(options.dryRun) ||
    coerceCliBoolean(options.dryrun);

  campaignWalletRegistry.clear();
  for (const wallet of wallets) {
    const pubkey = wallet.kp.publicKey.toBase58();
    campaignWalletRegistry.set(pubkey, { wallet, name: wallet.name });
  }
  ensureCampaignHooksRegistered();

  const walletHoldingsMap = new Map();
  const walletSolBalanceMap = new Map();
  if (campaignKey === "btc-eth-circuit") {
    const sweepConnection = createRpcConnection("confirmed");
    for (const wallet of wallets) {
      const pubkey = wallet.kp.publicKey.toBase58();
      try {
        await balanceRpcDelay();
        const lamports = await sweepConnection.getBalance(wallet.kp.publicKey);
        walletSolBalanceMap.set(pubkey, BigInt(lamports));
      } catch (err) {
        console.warn(
          paint(
            `  Warning: failed to fetch SOL balance for ${wallet.name}: ${err?.message || err}`,
            "warn"
          )
        );
      }
      try {
        await balanceRpcDelay();
        const parsedAccounts = await getAllParsedTokenAccounts(sweepConnection, wallet.kp.publicKey);
        const holdings = [];
        for (const { account } of parsedAccounts) {
          const info = account?.data?.parsed?.info;
          if (!info) continue;
          const mint = info.mint;
          if (!mint || SOL_LIKE_MINTS.has(mint)) continue;
          const state = info.state;
          const locked = state && state !== "initialized";
          if (locked) continue;
          const rawAmount = info.tokenAmount?.amount ?? "0";
          let amount = 0n;
          try {
            amount = BigInt(rawAmount);
          } catch (_) {
            amount = 0n;
          }
          if (amount <= 0n) continue;
          const decimals = info.tokenAmount?.decimals ?? 0;
          const isFrozen = state === "frozen" || info.isFrozen === true;
          holdings.push({ mint, amountLamports: amount, decimals, locked, isFrozen });
        }
        walletHoldingsMap.set(pubkey, holdings);
      } catch (err) {
        console.warn(
          paint(
            `  Warning: failed to fetch token holdings for ${wallet.name}: ${err?.message || err}`,
            "warn"
          )
        );
        walletHoldingsMap.set(pubkey, []);
      }
    }
    try {
      sweepConnection?.destroy?.();
    } catch (_) {}
  }

  const pubkeys = wallets.map((wallet) => wallet.kp.publicKey.toBase58());
  const { plansByWallet } = instantiateCampaignForWallets({
    campaignKey,
    durationKey,
    walletPubkeys: pubkeys,
    walletHoldings: walletHoldingsMap,
    walletSolBalances: walletSolBalanceMap,
  });

  const preparedPlans = new Map();
  let balanceConnection = null;
  for (const wallet of wallets) {
    const pubkey = wallet.kp.publicKey.toBase58();
    const plan = plansByWallet.get(pubkey);
    if (!plan) continue;
    let balance = 0n;
    try {
      if (walletSolBalanceMap.has(pubkey)) {
        balance = walletSolBalanceMap.get(pubkey);
      } else {
        if (!balanceConnection) {
          balanceConnection = createRpcConnection("confirmed");
        }
        await balanceRpcDelay();
        balance = BigInt(await balanceConnection.getBalance(wallet.kp.publicKey));
      }
    } catch (err) {
      console.warn(
        paint(
          `  Failed to fetch balance for ${wallet.name}: ${err?.message || err}`,
          "warn"
        )
      );
      continue;
    }
    const truncated = truncatePlanToBudget(plan.schedule, balance);
    if (truncated.length === 0) {
      console.log(
        paint(
          `  Skipping ${wallet.name}: insufficient SOL after reserve (balance ${formatBaseUnits(balance, 9)} SOL).`,
          "warn"
        )
      );
      continue;
    }
      preparedPlans.set(pubkey, {
        schedule: truncated,
        rng: plan.rng,
        randomSessions: plan.randomSessions,
        poolMints: plan.poolMints,
      });
  }
  try {
    balanceConnection?.destroy?.();
  } catch (_) {}

  if (preparedPlans.size === 0) {
    console.log(paint("No wallets have sufficient balance to participate.", "warn"));
    return;
  }

  campaignDryRun = dryRun;
  const swapCounts = [];
  for (const [pubkey, planEntry] of preparedPlans.entries()) {
    const schedule = Array.isArray(planEntry.schedule) ? planEntry.schedule : [];
    const swapSteps = schedule.filter((step) => step.kind === "swapHop").length;
    const fanOutSteps = schedule.filter((step) => step.kind === "fanOutSwap").length;
    const sweepSteps = schedule.filter((step) => step.kind === "sweepToSOL").length;
    const checkpointSteps = schedule.filter((step) => step.kind === "checkpointToSOL").length;
    const label = campaignWalletRegistry.get(pubkey)?.name || pubkey;
    swapCounts.push({ label, swapSteps, fanOutSteps, sweepSteps, checkpointSteps });
  }

  if (dryRun) {
    console.log(paint("Dry-run hop preview (per wallet):", "info"));
    for (const [pubkey, { schedule }] of preparedPlans.entries()) {
      const label = campaignWalletRegistry.get(pubkey)?.name || pubkey;
      let previewLastMint = SOL_MINT;
      const hops = [];
      for (const step of schedule) {
        if (step.kind !== "swapHop") continue;
        const logical = step.logicalStep || {};
        let fromMint = logical.inMint ?? previewLastMint ?? SOL_MINT;
        if (fromMint === RANDOM_PLACEHOLDER) {
          fromMint = previewLastMint ?? SOL_MINT;
        }
        let toMint = logical.outMint ?? SOL_MINT;
        if (toMint === RANDOM_PLACEHOLDER) {
          toMint = previewLastMint ?? SOL_MINT;
        }
        hops.push(`${symbolForMint(fromMint)}→${symbolForMint(toMint)}`);
        previewLastMint = toMint || SOL_MINT;
      }
      const MAX_PREVIEW_HOPS = 40;
      let preview;
      if (hops.length === 0) {
        preview = "(no swaps scheduled)";
      } else if (hops.length > MAX_PREVIEW_HOPS) {
        const visible = hops.slice(0, MAX_PREVIEW_HOPS).join(" | ");
        preview = `${visible} | ... (+${hops.length - MAX_PREVIEW_HOPS} more)`;
      } else {
        preview = hops.join(" | ");
      }
      console.log(paint(`  ${label}: ${preview}`, "muted"));
    }
  }

  console.log(
    paint(
      `Starting campaign ${campaignKey} (${durationKey}) across ${preparedPlans.size} wallet(s) — dryRun=${dryRun ? "yes" : "no"}.`,
      "info"
    )
  );
  swapCounts.forEach(({ label, swapSteps, fanOutSteps, sweepSteps, checkpointSteps }) => {
    const parts = [];
    if (sweepSteps > 0) parts.push(`${sweepSteps} sweep${sweepSteps === 1 ? "" : "s"}`);
    if (fanOutSteps > 0) parts.push(`${fanOutSteps} fan-out swap${fanOutSteps === 1 ? "" : "s"}`);
    if (swapSteps > 0) parts.push(`${swapSteps} swap${swapSteps === 1 ? "" : "s"}`);
    if (checkpointSteps > 0) parts.push(`${checkpointSteps} checkpoint${checkpointSteps === 1 ? "" : "s"}`);
    const summary = parts.length > 0 ? parts.join(" + ") : "no scheduled steps";
    console.log(
      paint(
        `  ${label}: ${summary}.`,
        "muted"
      )
    );
  });

  await executeTimedPlansAcrossWallets({ plansByWallet: preparedPlans });
  console.log(paint("Campaign complete.", "success"));
}

let decimalPrecisionWarnings = 0;

function decimalToBaseUnits(amountStr, decimals) {
  if (typeof amountStr !== "string") amountStr = String(amountStr);
  const normalized = amountStr.trim();
  if (!/^[0-9]+(\.[0-9]+)?$/.test(normalized)) {
    throw new Error(`Invalid decimal amount: ${amountStr}`);
  }
  const [wholePart, rawFractionalPart = ""] = normalized.split(".");
  let fractionalPart = rawFractionalPart;
  if (fractionalPart.length > decimals) {
    const truncated = fractionalPart.slice(0, decimals);
    const dropped = fractionalPart.slice(decimals);
    fractionalPart = truncated;
    if (decimalPrecisionWarnings < 5) {
      console.warn(
        paint(
          `⚠️  Truncating amount ${amountStr} to ${wholePart}.${truncated} to respect ${decimals} decimal places (dropped ${dropped.length} digit${dropped.length === 1 ? "" : "s"})`,
          "warn"
        )
      );
      decimalPrecisionWarnings += 1;
      if (decimalPrecisionWarnings === 5) {
        console.warn(
          paint("⚠️  Further decimal truncation warnings suppressed.", "warn")
        );
      }
    }
  }
  const base = BigInt(10) ** BigInt(decimals);
  const whole = BigInt(wholePart) * base;
  const fracPadded = (fractionalPart + "0".repeat(decimals)).slice(0, decimals);
  const fraction = fracPadded.length ? BigInt(fracPadded) : BigInt(0);
  return whole + fraction;
}

function formatBaseUnits(amount, decimals) {
  const base = BigInt(10) ** BigInt(decimals);
  const whole = amount / base;
  const fraction = amount % base;
  if (fraction === 0n) return whole.toString();
  const fracStr = fraction
    .toString()
    .padStart(decimals, "0")
    .replace(/0+$/, "");
  return `${whole}.${fracStr}`;
}

function formatSignedBaseUnits(amount, decimals) {
  if (typeof amount === "string") amount = BigInt(amount);
  if (typeof amount !== "bigint") amount = BigInt(amount);
  const negative = amount < 0n;
  const formatted = formatBaseUnits(negative ? -amount : amount, decimals);
  return negative ? `-${formatted}` : formatted;
}

configureWalletHelpers({
  keypairDir: KEYPAIR_DIR,
  loadKeypairFromFile,
  paint,
  formatBaseUnits,
  symbolForMint,
  logger: console,
});

// Present lamport deltas using the existing decimal formatter while preserving sign.
function formatLamportsDelta(delta) {
  const negative = delta < 0n;
  const magnitude = negative ? -delta : delta;
  const formatted = formatBaseUnits(magnitude, 9);
  return negative ? `-${formatted}` : formatted;
}

function parsePublicKeyStrict(value, label) {
  try {
    return new PublicKey(value);
  } catch (err) {
    const detail = err?.message ? `: ${err.message}` : "";
    throw new Error(`${label || "Public key"} is invalid${detail}`);
  }
}

function bnToBigInt(value) {
  if (!value) return 0n;
  if (typeof value === "bigint") return value;
  if (typeof value === "number") return BigInt(value);
  if (typeof value === "string") return BigInt(value);
  if (typeof value.toString === "function") {
    return BigInt(value.toString());
  }
  throw new Error("Cannot convert value to bigint");
}

function formatTimestampSeconds(secondsLike) {
  if (secondsLike === null || secondsLike === undefined) return "n/a";
  const seconds = Number(bnToBigInt(secondsLike));
  if (!Number.isFinite(seconds) || seconds <= 0) return "n/a";
  const millis = seconds * 1000;
  if (!Number.isFinite(millis)) return "n/a";
  const date = new Date(millis);
  if (Number.isNaN(date.getTime())) return "n/a";
  return date.toISOString();
}

function perpsKnownCustodyLabels() {
  return KNOWN_CUSTODIES.map((entry) => entry.symbol).join(", ");
}

function ensurePerpsProgramMatchesConfiguration(connection) {
  const configuredProgramId = getPerpsProgramId();
  const program = getPerpsProgram(connection);
  if (!program.programId.equals(configuredProgramId)) {
    const configuredId = configuredProgramId.toBase58();
    const activeId = program.programId.toBase58();
    throw new Error(
      `Perps program mismatch: active program ${activeId} does not match configured program ${configuredId}. ` +
        "Verify JUPITER_PERPS_PROGRAM_ID or PERPS_PROGRAM_ID is set correctly."
    );
  }
  return program;
}

function pickRandomPortion(total) {
  if (total <= 1n) return total;
  const denominators = [3n, 4n, 5n, 6n];
  const denom = denominators[Math.floor(Math.random() * denominators.length)];
  const portion = total - total / denom;
  return portion > 0n ? portion : 1n;
}

// Calculate total gas requirements for a multi-hop swap sequence
// Note: This is an ESTIMATION only - actual ATA checks happen during execution
async function calculateTotalGasRequirements(steps, connection, walletPublicKey) {
  if (!steps || steps.length === 0) return { totalGas: 0n, ataCreations: 0, breakdown: [] };

  let totalGas = 0n;
  let ataCreations = 0;
  const breakdown = [];

  // Count unique non-SOL output tokens (conservative estimate: assume we need to create ATA for each)
  const uniqueNonSolOutputs = new Set();
  for (const step of steps) {
    if (!SOL_LIKE_MINTS.has(step.to)) {
      uniqueNonSolOutputs.add(step.to);
    }
  }

  // Conservative estimate: assume we might need to create ATA for each unique non-SOL token
  // but don't actually check with RPC (to avoid rate limiting)
  ataCreations = uniqueNonSolOutputs.size;
  totalGas += BigInt(ataCreations) * ESTIMATED_ATA_CREATION_LAMPORTS;

  for (const tokenMint of uniqueNonSolOutputs) {
    breakdown.push({
      type: 'ata_creation',
      token: symbolForMint(tokenMint),
      cost: ESTIMATED_ATA_CREATION_LAMPORTS,
      note: 'estimated (actual check during execution)'
    });
  }

  // Estimate gas for each swap transaction
  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const swapGas = ESTIMATED_GAS_PER_SWAP_LAMPORTS;
    totalGas += swapGas;
    breakdown.push({
      type: 'swap',
      step: i + 1,
      from: symbolForMint(step.from),
      to: symbolForMint(step.to),
      cost: swapGas
    });
  }

  // Add base gas reserve
  const baseReserve = GAS_RESERVE_LAMPORTS + JUPITER_SOL_BUFFER_LAMPORTS;
  totalGas += baseReserve;
  breakdown.push({
    type: 'base_reserve',
    description: 'Base gas reserve + Jupiter buffer',
    cost: baseReserve
  });

  return { totalGas, ataCreations, breakdown };
}

// Fetch token accounts from both legacy SPL and Token-2022 programs so
// callers always see a complete view without worrying about program IDs.
async function getAllParsedTokenAccounts(connection, ownerPubkey) {
  const programs = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
  const combined = [];
  const seen = new Set();

  for (const programId of programs) {
    let retries = 3;
    let lastError = null;

    while (retries > 0) {
      try {
        const resp = await connection.getParsedTokenAccountsByOwner(ownerPubkey, {
          programId,
        });
        for (const entry of resp.value) {
          const key = entry.pubkey.toBase58();
          if (seen.has(key)) continue;
          seen.add(key);
          combined.push(entry);
        }
        break; // Success, exit retry loop
      } catch (err) {
        lastError = err;
        retries--;
        if (retries > 0) {
          // Wait before retry with exponential backoff
          await delay(500 * (4 - retries));
        }
      }
    }

    // Log if all retries failed
    if (lastError && retries === 0) {
      console.warn(
        paint(
          `  Warning: Failed to fetch token accounts for ${ownerPubkey.toBase58().slice(0, 8)}... (${lastError.message || 'unknown error'})`,
          "warn"
        )
      );
    }
  }

  return combined;
}

async function resolveMintMetadata(connection, mint) {
  if (SOL_LIKE_MINTS.has(mint)) {
    return { decimals: 9, programId: null, symbol: "SOL" };
  }
  if (mintMetadataCache.has(mint)) return mintMetadataCache.get(mint);

  const fallback = KNOWN_MINTS.get(mint) || null;
  const catalogEntry = TOKEN_CATALOG_BY_MINT.get(mint) || null;

  const mintPubkey = new PublicKey(mint);
  const tryLoad = async (program, label) => {
    try {
      const mintInfo = await getMint(connection, mintPubkey, "confirmed", program);
      return { decimals: mintInfo.decimals, programId: program };
    } catch (error) {
      throw new Error(`${label}: ${error.message ?? error}`);
  }
};

  let lastError;
  for (const [program, label] of [
    [TOKEN_PROGRAM_ID, "token-program"],
    [TOKEN_2022_PROGRAM_ID, "token-2022"],
  ]) {
    try {
      const meta = await tryLoad(program, label);
      const overrideSymbol = MINT_SYMBOL_OVERRIDES.get(mint) || null;
      if (overrideSymbol) meta.symbol = overrideSymbol;
      else if (catalogEntry?.symbol) meta.symbol = catalogEntry.symbol;
      else if (fallback?.symbol) meta.symbol = fallback.symbol;
      if (!meta.symbol) {
        const external = await lookupExternalTokenInfo(mint);
        if (external?.symbol) {
          meta.symbol = external.symbol.toUpperCase();
          if (external.name) meta.name = external.name;
          if (
            typeof external.decimals === "number" &&
            Number.isFinite(external.decimals) &&
            external.decimals !== meta.decimals
          ) {
            meta.decimals = external.decimals;
          }
        }
      }
      if (!meta.symbol && fallback?.symbol) {
        meta.symbol = fallback.symbol;
      }
      if (!meta.symbol && catalogEntry?.symbol) {
        meta.symbol = catalogEntry.symbol;
      }
      if (!meta.symbol) {
        meta.symbol = mint.slice(0, 4);
      }
      if (!meta.name && catalogEntry?.symbol) {
        meta.name = catalogEntry.symbol;
      }
      if (!meta.name && fallback?.name) {
        meta.name = fallback.name;
      }
      if (!meta.name && meta.symbol) {
        meta.name = meta.symbol;
      }
      mintMetadataCache.set(mint, meta);
      KNOWN_MINTS.set(mint, {
        decimals: meta.decimals,
        programId: meta.programId,
        symbol: meta.symbol,
        name: meta.name || meta.symbol,
      });
      return meta;
    } catch (err) {
      lastError = err;
    }
  }

  if (fallback) {
    const fallbackMeta = { ...fallback };
    const overrideSymbol = MINT_SYMBOL_OVERRIDES.get(mint) || null;
    if (overrideSymbol) fallbackMeta.symbol = overrideSymbol;
    if (!fallbackMeta.symbol && catalogEntry?.symbol) {
      fallbackMeta.symbol = catalogEntry.symbol;
    }
    if (!fallbackMeta.name && catalogEntry?.symbol) {
      fallbackMeta.name = catalogEntry.symbol;
    }
    mintMetadataCache.set(mint, fallbackMeta);
    console.warn(
      paint(
        `  warning: using cached mint metadata for ${mint} (${lastError?.message ?? lastError}); verify this mint exists on the current cluster`,
        "warn"
      )
    );
    return fallbackMeta;
  }

  throw new Error(`Unable to load mint ${mint}: ${lastError?.message ?? lastError}`);
}

async function getTokenBalanceBaseUnits(connection, walletPubkey, mintPubkey, programId) {
  try {
    const ata = await getAssociatedTokenAddress(
      mintPubkey,
      walletPubkey,
      false,
      programId || TOKEN_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    const resp = await connection.getTokenAccountBalance(ata);
    return BigInt(resp.value.amount);
  } catch (e) {
    if (isRateLimitError(e)) {
      throw e;
    }
    return 0n;
  }
}

// ---- Token sweeping helpers ----
// Consolidate SPL balances back into SOL, either for a provided mint list or
// by scanning every wallet. Used by the sweep commands and BTC/ETH allocator.
// Estimate USD value of dust tokens using Jupiter Price API
async function estimateDustValueUsd(mint, amount, decimals) {
  try {
    const url = new URL(JUPITER_PRICE_ENDPOINT);
    url.searchParams.set("ids", mint);
    const resp = await fetch(url.toString(), {
      method: "GET",
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(5000)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    // Price API v3 returns prices directly, no data wrapper
    const priceData = data?.[mint];
    if (!priceData?.usdPrice) return null;

    const tokenAmount = Number(amount) / Math.pow(10, decimals);
    const usdValue = tokenAmount * priceData.usdPrice;
    return usdValue;
  } catch {
    return null;
  }
}

async function sweepTokensToSol(mints, label = "") {
  const uniqueMints = [...new Set(mints)].filter((mint) => mint && !SOL_LIKE_MINTS.has(mint));
  if (uniqueMints.length === 0) {
    console.log(
      paint(
        `No token balances to sweep${label ? ` (${label})` : ""}.`,
        "muted"
      )
    );
    return;
  }

  // Filter out Jupiter Lend share tokens
  const mintSet = new Set();
  const skippedShareTokens = [];

  for (const mint of uniqueMints) {
    const tokenRecord = await resolveTokenRecord(mint);
    if (tokenRecord && isLendShareToken(tokenRecord)) {
      skippedShareTokens.push(tokenRecord.symbol || mint);
    } else {
      mintSet.add(mint);
    }
  }

  if (skippedShareTokens.length > 0) {
    console.log(
      paint(
        `  Skipped ${skippedShareTokens.length} Jupiter Lend share token(s): ${skippedShareTokens.join(", ")}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  Tip: Use 'lend earn withdraw' to redeem share tokens.`,
        "muted"
      )
    );
  }

  if (mintSet.size === 0) {
    console.log(
      paint(
        `No swappable tokens to sweep${label ? ` (${label})` : ""}.`,
        "muted"
      )
    );
    return;
  }

  console.log(
    paint(
      `Sweeping tokens back to SOL${label ? ` (${label})` : ""}...`,
      "label"
    )
  );

  // Per-wallet sweep: only swap tokens each wallet actually holds
  const wallets = listWallets();
  const connection = createRpcConnection("confirmed");

  for (const wallet of wallets) {
    try {
      const parsedAccounts = await getAllParsedTokenAccounts(connection, wallet.kp.publicKey);
      const walletMints = [];
      const solDustAccounts = [];

      for (const { account, pubkey } of parsedAccounts) {
        const info = account.data.parsed.info;
        const mint = info.mint;
        const amount = BigInt(info.tokenAmount.amount);
        const decimals = info.tokenAmount.decimals ?? 0;

        if (amount <= 0n) continue;

        if (mintSet.has(mint)) {
          walletMints.push({ mint, amount, accountPubkey: pubkey });
          continue;
        }

        if (SOL_LIKE_MINTS.has(mint)) {
          let programId = TOKEN_PROGRAM_ID;
          try {
            programId = new PublicKey(account.owner);
          } catch (_) {}
          solDustAccounts.push({
            accountPubkey: pubkey,
            amount,
            decimals,
            programId,
          });
        }
      }

      // Swap each token this wallet holds
      for (const { mint, amount, accountPubkey } of walletMints) {
        try {
          await doSwapAcross(mint, SOL_MINT, "all", {
            wallets: [wallet],
            quietSkips: false,
            failOnMinOutput: true,
          });
        } catch (err) {
          // If swap failed (likely due to dust amount), decide whether to rescue or burn
          const tokenSymbol = symbolForMint(mint) || mint.substring(0, 8);
          logDetailedError(`  ${wallet.name}: sweep ${tokenSymbol} failed`, err);

          // Check if it's a dust amount issue
          if (/too small|simulation error|failed to get quotes|amount became too small/i.test(err.message)) {
            try {
              // Get token info for value calculation
              const parsedData = await connection.getParsedAccountInfo(accountPubkey);
              const tokenInfo = parsedData?.value?.data?.parsed?.info?.tokenAmount;
              const remainingAmount = tokenInfo?.amount ? BigInt(tokenInfo.amount) : 0n;
              const decimals = tokenInfo?.decimals ?? 6;

              // Calculate dust USD value
              const dustValueUsd = await estimateDustValueUsd(mint, remainingAmount, decimals);
              // Get SOL price to calculate threshold (0.002 SOL worth)
              const solPrice = await (async () => {
                try {
                  const url = new URL(JUPITER_PRICE_ENDPOINT);
                  url.searchParams.set("ids", SOL_MINT);
                  const resp = await fetch(url.toString(), { method: "GET", headers: { accept: "application/json" }, signal: AbortSignal.timeout(5000) });
                  if (!resp.ok) return 150; // Fallback to ~$150/SOL
                  const data = await resp.json();
                  return data?.[SOL_MINT]?.usdPrice || 150;
                } catch { return 150; }
              })();
              const DUST_RESCUE_THRESHOLD_USD = solPrice * 0.002; // 0.002 SOL worth

              if (dustValueUsd !== null && dustValueUsd >= DUST_RESCUE_THRESHOLD_USD) {
                // RESCUE: Dust is worth rescuing
                console.log(
                  paint(
                    `  ${wallet.name}: ${tokenSymbol} dust worth $${dustValueUsd.toFixed(4)} - attempting rescue...`,
                    "info"
                  )
                );

                try {
                  // Step 1: Buy more of the token (0.01 SOL worth)
                  await doSwapAcross(SOL_MINT, mint, { mode: "range", min: 0.01, max: 0.01 }, {
                    wallets: [wallet],
                    quietSkips: true,
                  });

                  // Step 2: Sweep all (original dust + new purchase) back to SOL
                  await delay(1000); // Brief pause for transaction settlement
                  await doSwapAcross(mint, SOL_MINT, "all", {
                    wallets: [wallet],
                    quietSkips: true,
                    failOnMinOutput: true,
                  });

                  console.log(
                    paint(
                      `  ${wallet.name}: successfully rescued ${tokenSymbol} dust worth $${dustValueUsd.toFixed(4)}`,
                      "success"
                    )
                  );
                  continue; // Skip burn logic, rescue successful
                } catch (rescueErr) {
                  console.warn(
                    paint(
                      `  ${wallet.name}: dust rescue failed for ${tokenSymbol}, falling back to burn`,
                      "warn"
                    )
                  );
                  // Fall through to burn logic below
                }
              }

              // BURN: Dust is not worth rescuing or price unavailable or rescue failed
              const valueMsg = dustValueUsd !== null
                ? ` (value $${dustValueUsd.toFixed(4)} < $${DUST_RESCUE_THRESHOLD_USD.toFixed(4)} [0.002 SOL])`
                : " (price unavailable)";
              console.log(
                paint(
                  `  ${wallet.name}: burning ${tokenSymbol} dust${valueMsg}...`,
                  "muted"
                )
              );

              if (remainingAmount > 0n) {
                // Burn remaining dust tokens first (required before closing)
                const burnIx = createBurnInstruction(
                  accountPubkey,
                  new PublicKey(mint),
                  wallet.kp.publicKey,
                  remainingAmount,
                  [],
                  TOKEN_PROGRAM_ID
                );

                // Close account to reclaim rent (~0.00203 SOL)
                const closeIx = createCloseAccountInstruction(
                  accountPubkey,
                  wallet.kp.publicKey, // destination for rent
                  wallet.kp.publicKey, // authority
                  [],
                  TOKEN_PROGRAM_ID
                );

                // Build and send transaction
                const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
                const tx = new Transaction({
                  feePayer: wallet.kp.publicKey,
                  blockhash,
                  lastValidBlockHeight,
                }).add(burnIx, closeIx);

                tx.sign(wallet.kp);
                const sig = await connection.sendRawTransaction(tx.serialize(), {
                  skipPreflight: false,
                  maxRetries: 2,
                });

                await connection.confirmTransaction({
                  signature: sig,
                  blockhash,
                  lastValidBlockHeight,
                }, "confirmed");

                console.log(
                  paint(
                    `  ${wallet.name}: burned ${formatBaseUnits(remainingAmount, parsedData.value.data.parsed.info.tokenAmount.decimals)} ${tokenSymbol} dust and closed account (tx: ${sig.substring(0, 8)}...)`,
                    "success"
                  )
                );
              } else {
                // Already zero balance, just close
                const closeIx = createCloseAccountInstruction(
                  accountPubkey,
                  wallet.kp.publicKey,
                  wallet.kp.publicKey,
                  [],
                  TOKEN_PROGRAM_ID
                );

                const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
                const tx = new Transaction({
                  feePayer: wallet.kp.publicKey,
                  blockhash,
                  lastValidBlockHeight,
                }).add(closeIx);

                tx.sign(wallet.kp);
                const sig = await connection.sendRawTransaction(tx.serialize(), {
                  skipPreflight: false,
                  maxRetries: 2,
                });

                await connection.confirmTransaction({
                  signature: sig,
                  blockhash,
                  lastValidBlockHeight,
                }, "confirmed");

                console.log(
                  paint(
                    `  ${wallet.name}: closed ${tokenSymbol} account (tx: ${sig.substring(0, 8)}...)`,
                    "success"
                  )
                );
              }
            } catch (closeErr) {
              console.warn(
                paint(
                  `  ${wallet.name}: failed to burn/close ${tokenSymbol} account: ${closeErr.message}`,
                  "warn"
                )
              );
            }
          }
        }
      }

      for (const solEntry of solDustAccounts) {
        try {
          const dustValueUsd = await estimateDustValueUsd(SOL_MINT, solEntry.amount, solEntry.decimals || 9);
          if (dustValueUsd === null) {
            console.log(
              paint(
                `  ${wallet.name}: skipping SOL dust account ${solEntry.accountPubkey.toBase58()} — price unavailable`,
                "muted"
              )
            );
            continue;
          }
          if (dustValueUsd >= SOL_DUST_AUTOCLOSE_THRESHOLD_USD) {
            continue;
          }

          const amountLabel = formatBaseUnits(solEntry.amount, solEntry.decimals || 9);
          console.log(
            paint(
              `  ${wallet.name}: unwrapping SOL dust ${amountLabel} (≈$${dustValueUsd.toFixed(4)})`,
              "muted"
            )
          );

          const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
          const tx = new Transaction({
            feePayer: wallet.kp.publicKey,
            blockhash,
            lastValidBlockHeight,
          }).add(
            createCloseAccountInstruction(
              solEntry.accountPubkey,
              wallet.kp.publicKey,
              wallet.kp.publicKey,
              [],
              solEntry.programId
            )
          );

          tx.sign(wallet.kp);
          const sig = await connection.sendRawTransaction(tx.serialize(), {
            skipPreflight: false,
            maxRetries: 2,
          });

          await connection.confirmTransaction(
            {
              signature: sig,
              blockhash,
              lastValidBlockHeight,
            },
            "confirmed"
          );

          console.log(
            paint(
              `  ${wallet.name}: reclaimed SOL dust from ${solEntry.accountPubkey.toBase58()} (tx: ${sig.substring(0, 8)}...)`,
              "success"
            )
          );
        } catch (solDustErr) {
          console.warn(
            paint(
              `  ${wallet.name}: failed to unwrap SOL dust for ${solEntry.accountPubkey.toBase58()}: ${solDustErr.message || solDustErr}`,
              "warn"
            )
          );
        }
      }
    } catch (err) {
      console.error(paint(`  ${wallet.name}: failed to scan token accounts:`, "error"), err.message);
    }
    await balanceRpcDelay();
  }
}

async function sweepAllTokensToSol() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }

  const mintSet = new Set();
  const skippedShareTokens = new Set();

  for (const w of wallets) {
    const connection = createRpcConnection("confirmed");
    const parsedAccounts = await getAllParsedTokenAccounts(connection, w.kp.publicKey);
    for (const { account } of parsedAccounts) {
      const info = account.data.parsed.info;
      const mint = info.mint;
      const amount = BigInt(info.tokenAmount.amount);
      if (amount > 0n && !SOL_LIKE_MINTS.has(mint)) {
        // Skip Jupiter Lend share tokens (JL-* tokens) - they can't be swapped
        const tokenRecord = await resolveTokenRecord(mint);
        if (tokenRecord && isLendShareToken(tokenRecord)) {
          skippedShareTokens.add(tokenRecord.symbol || mint);
          continue;
        }
        mintSet.add(mint);
      }
    }
    // Add small delay between wallets to avoid RPC rate limiting
    await delay(100);
  }

  if (skippedShareTokens.size > 0) {
    console.log(
      paint(
        `  Skipped ${skippedShareTokens.size} Jupiter Lend share token(s): ${[...skippedShareTokens].join(", ")}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  Tip: Use 'lend earn withdraw' to redeem share tokens.`,
        "muted"
      )
    );
  }

  await sweepTokensToSol([...mintSet], "all tokens");
}

// Consolidate all wallets back to SOL and then fan SOL out to wBTC/cbBTC/wETH.
// This is used by the dedicated sweep command triggered via the launcher hotkey.
async function sweepTokensToBtcEthTargets() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }

  console.log(paint("Sweeping all non-SOL balances into SOL before allocating to wBTC/cbBTC/wETH", "label"));
  await sweepAllTokensToSol();

  // Target tokens for sweep (excludes USDT/USDC, includes other swappable tokens)
  const allTargets = [
    { mint: WBTC_MINT, label: "wBTC" },
    { mint: CBBTC_MINT, label: "cbBTC" },
    { mint: WETH_MINT, label: "wETH" },
  ];

  // Add other swappable tokens (SPYX, NVDAX, CRCLX, JUPSOL, etc.) from token catalog
  const catalogTargets = TOKEN_CATALOG.filter(entry =>
    entry.tags?.includes("swappable") &&
    !SOL_LIKE_MINTS.has(entry.mint) &&
    entry.mint !== USDC_MINT &&
    entry.mint !== USDT_MINT &&
    entry.mint !== WBTC_MINT &&
    entry.mint !== CBBTC_MINT &&
    entry.mint !== WETH_MINT
  ).map(entry => ({ mint: entry.mint, label: entry.symbol }));

  const targets = [...allTargets, ...catalogTargets];

  if (cachedAtaRentLamports === null) {
    const rentConnection = createRpcConnection("confirmed");
    const rent = await rentConnection.getMinimumBalanceForRentExemption(165);
    cachedAtaRentLamports = BigInt(rent);
  }

  const planEntries = await measureAsync("sweep-to-btc-eth:plan-wallets", async () => {
    const entries = [];
    for (const wallet of wallets) {
      const connection = createRpcConnection("confirmed");
      const solBalanceLamports = BigInt(await getSolBalance(connection, wallet.kp.publicKey));
      let reserve = solBalanceLamports > GAS_RESERVE_LAMPORTS
        ? GAS_RESERVE_LAMPORTS
        : solBalanceLamports / 10n;
      reserve += JUPITER_SOL_BUFFER_LAMPORTS;
      // No ATA rent pre-reservation: we'll close the account after swap to reclaim SOL
      if (reserve >= solBalanceLamports) {
        console.log(
          paint(
            `Skipping ${wallet.name}: SOL balance ${formatBaseUnits(solBalanceLamports, 9)} below reserve ${formatBaseUnits(reserve, 9)}`,
            "muted"
          )
        );
        continue;
      }
      const spendable = solBalanceLamports - reserve;
      if (spendable <= 0n) {
        console.log(paint(`Skipping ${wallet.name}: nothing spendable after reserve`, "muted"));
        continue;
      }

      // Pick ONE random target token for this wallet
      const selectedTarget = targets[Math.floor(Math.random() * targets.length)];
      const summary = `Sweeping ${formatBaseUnits(spendable, 9)} SOL from ${wallet.name} → ${selectedTarget.label}`;
      const allocations = [{
        mint: selectedTarget.mint,
        label: selectedTarget.label,
        amountDecimal: formatBaseUnits(spendable, 9),
      }];

      entries.push({
        wallet,
        summary,
        allocations,
      });

      await balanceRpcDelay();
    }
    return entries;
  });

  if (planEntries.length === 0) {
    console.log(paint("No wallets qualified for BTC/ETH allocation after sweeping.", "muted"));
    return;
  }

  await measureAsync("sweep-to-btc-eth:execute-swaps", async () => {
    for (const entry of planEntries) {
      console.log(paint(entry.summary, "label"));
      for (const allocation of entry.allocations) {
        console.log(
          paint(
            `  ${entry.wallet.name}: swapping ${allocation.amountDecimal} SOL -> ${allocation.label}`,
            "info"
          )
        );
        await doSwapAcross(SOL_MINT, allocation.mint, allocation.amountDecimal, {
          wallets: [entry.wallet],
          quietSkips: true,
          suppressMetadata: false,
        });
        await passiveSleep();
      }
      await passiveSleep();
    }
  });
}


// Convenience wrapper to execute a predefined list of swaps with human-
// readable logging. Used by secondary flows such as sol-usdc-popcat.
async function runSwapSequence(steps, label) {
  console.log(
    paint(
      `${label} — ${steps.length} step${steps.length === 1 ? '' : 's'}`,
      "label"
    )
  );
  let index = 0;
  const resolutionState = createMintResolutionState();
  for (const step of steps) {
    index += 1;

    const excludeForFrom = combineMintExclusions(resolutionState.used);
    const resolvedFrom = resolveMintDescriptor(step.from, {
      exclude: excludeForFrom,
    });
    const excludeForTo = combineMintExclusions(
      resolutionState.used,
      [resolvedFrom.mint]
    );
    const resolvedTo = resolveMintDescriptor(step.to, {
      exclude: excludeForTo,
    });

    const fromMint = normaliseSolMint(resolvedFrom.mint);
    const toMint = normaliseSolMint(resolvedTo.mint);
    resolutionState.used.add(fromMint);
    resolutionState.used.add(toMint);

    const fromLabel =
      resolvedFrom.description || resolvedFrom.symbol || symbolForMint(fromMint);
    const toLabel =
      resolvedTo.description || resolvedTo.symbol || symbolForMint(toMint);
    const descriptorNeedsUpdate =
      !step.description || resolvedFrom.random || resolvedTo.random;

    step.from = fromMint;
    step.to = toMint;
    step.resolvedFrom = resolvedFrom;
    step.resolvedTo = resolvedTo;
    if (descriptorNeedsUpdate) {
      step.description = `${fromLabel} -> ${toLabel}`;
    }
    const descriptor = step.description || `${fromLabel} -> ${toLabel}`;

    console.log(paint(`Step ${index}/${steps.length}: ${descriptor}`, "info"));
    if (step.noop) {
      console.log(paint("  (no on-chain action required)", "muted"));
      continue;
    }
    const amountArg = step.amount === undefined ? null : step.amount;
    await doSwapAcross(step.from, step.to, amountArg);
    await delay(DELAY_BETWEEN_CALLS_MS);
  }
}

function normaliseSolMint(mint) {
  return SOL_LIKE_MINTS.has(mint) ? SOL_MINT : mint;
}

function createMintResolutionState(initial = []) {
  const state = { used: new Set() };
  if (Array.isArray(initial)) {
    for (const mint of initial) {
      if (typeof mint === "string" && mint.length > 0) {
        state.used.add(normaliseSolMint(mint));
      }
    }
  }
  return state;
}

function resolveMintDescriptor(candidate, options = {}) {
  const baseSampleOptions =
    options.sampleOptions && typeof options.sampleOptions === "object"
      ? { ...options.sampleOptions }
      : {};
  if (options.requireTags) {
    baseSampleOptions.requireTags = combineTagLists(
      baseSampleOptions.requireTags,
      options.requireTags
    );
  }
  if (options.anyTags) {
    baseSampleOptions.anyTags = combineTagLists(
      baseSampleOptions.anyTags,
      options.anyTags
    );
  }
  const combinedExclude = combineMintExclusions(
    baseSampleOptions.exclude,
    options.exclude
  );
  baseSampleOptions.exclude = combinedExclude;

  const skipSolDefault = pickFirstDefined(
    baseSampleOptions.skipSolLike,
    options.skipSolLike
  );
  const avoidRecentDefault = pickFirstDefined(
    baseSampleOptions.avoidRecent,
    options.avoidRecent
  );

  const coerceBoolean = (value, fallback) => {
    if (value === true) return true;
    if (value === false) return false;
    return fallback;
  };

  if (typeof candidate === "string") {
    const trimmed = candidate.trim();
    if (trimmed.length === 0) {
      throw new Error("Empty mint descriptor encountered");
    }

    const randomMatch = trimmed.match(/^RANDOM(?::(.+))?$/i);
    if (randomMatch) {
      const directiveRaw = randomMatch[1] || "";
      const segments = directiveRaw
        .split(",")
        .map((segment) => segment.trim())
        .filter((segment) => segment.length > 0);

      let skipSolLike = coerceBoolean(skipSolDefault, true);
      let avoidRecent = coerceBoolean(avoidRecentDefault, true);
      let requireTags = [];
      let anyTags = [];

      for (const segment of segments) {
        const lower = segment.toLowerCase();
        if (lower === "sol-ok" || lower === "allow-sol") {
          skipSolLike = false;
          continue;
        }
        if (lower === "allow-recent" || lower === "recent-ok") {
          avoidRecent = false;
          continue;
        }
        if (lower === "no-recent" || lower === "avoid-recent") {
          avoidRecent = true;
          continue;
        }
        const eqIndex = segment.indexOf("=");
        if (eqIndex !== -1) {
          const key = segment.slice(0, eqIndex).trim().toLowerCase();
          const value = segment.slice(eqIndex + 1).trim();
          if (!value) continue;
          if (key === "tag" || key === "tags") {
            requireTags = requireTags.concat(value.split("|"));
            continue;
          }
          if (key === "any" || key === "anytag" || key === "anytags") {
            anyTags = anyTags.concat(value.split("|"));
            continue;
          }
          if (key === "exclude") {
            for (const part of value.split("|")) {
              const trimmedPart = part.trim();
              if (trimmedPart) combinedExclude.add(trimmedPart);
            }
            continue;
          }
        }
        requireTags.push(segment);
      }

      const descriptorLabel =
        segments.length > 0 ? `RANDOM:${segments.join(",")}` : "RANDOM";

      const includeTags = combineTagLists(
        baseSampleOptions.requireTags,
        requireTags
      );
      const anyTagList = combineTagLists(
        baseSampleOptions.anyTags,
        anyTags
      );
      const excludeTags = combineTagLists(baseSampleOptions.excludeTags);
      const entry = pickRandomCatalogMint({
        includeTags,
        matchAnyTags: anyTagList.length > 0,
        excludeTags,
        excludeMints: Array.from(combinedExclude),
        allowSol: !skipSolLike,
        rng: options.rng,
      });

      if (!entry) {
        throw new Error(
          `Unable to resolve ${descriptorLabel}: no eligible catalog entries found`
        );
      }

      const symbol = entry.symbol || symbolForMint(entry.mint);
      return {
        mint: entry.mint,
        symbol,
        description: `${descriptorLabel} (${symbol})`,
        random: true,
        catalogEntry: entry,
        label: descriptorLabel,
      };
    }

    const symbolEntry = tokenBySymbol(trimmed);
    if (symbolEntry?.mint) {
      const symbol = symbolEntry.symbol || symbolForMint(symbolEntry.mint);
      return {
        mint: symbolEntry.mint,
        symbol,
        description: symbol,
        random: false,
        catalogEntry: symbolEntry,
        label: symbol,
      };
    }

    const symbol = symbolForMint(trimmed);
    return {
      mint: trimmed,
      symbol,
      description: symbol,
      random: false,
      catalogEntry: TOKEN_CATALOG_BY_MINT.get(trimmed) || null,
      label: symbol,
    };
  }

  if (candidate && typeof candidate === "object") {
    const objectMint =
      typeof candidate.mint === "string" ? candidate.mint : null;
    if (objectMint) {
      const symbol = candidate.symbol || symbolForMint(objectMint);
      return {
        mint: objectMint,
        symbol,
        description: candidate.label || symbol,
        random: false,
        catalogEntry: TOKEN_CATALOG_BY_MINT.get(objectMint) || null,
        label: candidate.label || symbol,
      };
    }
  }

  throw new Error(`Unsupported mint descriptor: ${String(candidate)}`);
}

const CREW1_CYCLE_TOKENS = [
  { mint: POPCAT_MINT, symbol: 'POPCAT' },
  { mint: PUMP_MINT, symbol: 'PUMP' },
  { mint: PENGU_MINT, symbol: 'PENGU' },
  { mint: FARTCOIN_MINT, symbol: 'FART' },
  { mint: WIF_MINT, symbol: 'WIF' },
  { mint: URANUS_MINT, symbol: 'URANUS' },
  { mint: WBTC_MINT, symbol: 'wBTC' },
  { mint: CBBTC_MINT, symbol: 'cbBTC' },
  { mint: WETH_MINT, symbol: 'wETH' },
];

// Legacy hardcoded segments - replaced by dynamic token selection from "long-circle" tag
// Kept for reference but buildDynamicLongCircleSegments() is now used instead
const LONG_CHAIN_SEGMENTS_BASE_LEGACY = [
  { name: 'sol-usdc-uranus', mints: [SOL_MINT, DEFAULT_USDC_MINT, URANUS_MINT, SOL_MINT] },
  { name: 'sol-pump', mints: [SOL_MINT, PUMP_MINT, SOL_MINT] },
  { name: 'sol-pengu-fart', mints: [SOL_MINT, PENGU_MINT, FARTCOIN_MINT, SOL_MINT] },
  { name: 'sol-wif', mints: [SOL_MINT, WIF_MINT, SOL_MINT] },
  { name: 'sol-uranus-loop', mints: [SOL_MINT, DEFAULT_USDC_MINT, URANUS_MINT, POPCAT_MINT, PENGU_MINT, DEFAULT_USDC_MINT, SOL_MINT] },
  { name: 'sol-wbtc', mints: [SOL_MINT, WBTC_MINT, SOL_MINT] },
  { name: 'sol-cbbtc', mints: [SOL_MINT, CBBTC_MINT, SOL_MINT] },
  { name: 'sol-weth', mints: [SOL_MINT, WETH_MINT, SOL_MINT] },
];

// Build dynamic long-circle segments from tokens tagged "long-circle"
function buildDynamicLongCircleSegments() {
  const longCircleTokens = TOKEN_CATALOG.filter(
    (entry) =>
      entry &&
      typeof entry.mint === "string" &&
      tokenHasTag(entry, "swappable") &&
      !tokenHasTag(entry, "secondary-terminal") &&
      !SOL_LIKE_MINTS.has(entry.mint)
  );

  if (longCircleTokens.length === 0) {
    // Fallback to legacy segments if no tokens tagged
    return LONG_CHAIN_SEGMENTS_BASE_LEGACY;
  }

  const segments = [];

  // Generate simple SOL->Token->SOL segments for each token
  for (const token of longCircleTokens) {
    segments.push({
      name: `sol-${token.symbol.toLowerCase()}`,
      mints: [SOL_MINT, token.mint, SOL_MINT]
    });
  }

  // Generate a few multi-hop segments using 2-3 random tokens
  const minMultiHop = Math.min(3, longCircleTokens.length);
  for (let i = 0; i < minMultiHop; i++) {
    const shuffled = shuffleArray([...longCircleTokens], Math.random);
    const hopCount = Math.min(2 + Math.floor(Math.random() * 2), shuffled.length); // 2-3 hops
    const mints = [SOL_MINT, ...shuffled.slice(0, hopCount).map(t => t.mint), SOL_MINT];
    segments.push({
      name: `sol-multi-${i + 1}`,
      mints
    });
  }

  return segments;
}

const LONG_CHAIN_SEGMENTS_BASE = buildDynamicLongCircleSegments();

const BUCKSHOT_TOKEN_MINTS = Array.from(
  new Set(
    LONG_CHAIN_SEGMENTS_BASE.flatMap((segment) =>
      segment.mints
        .map((mint) => normaliseSolMint(mint))
        .filter((mint) => !SOL_LIKE_MINTS.has(mint))
    )
  )
);

function isRandomMintSentinel(value) {
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  if (!trimmed) return false;
  return trimmed.toLowerCase() === "random";
}

function toMintExclusionSet(raw) {
  const set = new Set();
  if (!raw) return set;
  const values = raw instanceof Set ? Array.from(raw) : Array.isArray(raw) ? raw : [raw];
  for (const value of values) {
    if (typeof value !== "string") continue;
    const trimmed = value.trim();
    if (!trimmed) continue;
    set.add(trimmed === SOL_MINT ? SOL_MINT : trimmed);
  }
  return set;
}

function stepsFromMints(mints, options = {}) {
  const steps = [];
  if (!Array.isArray(mints) || mints.length < 2) return steps;

  const rng = typeof options.rng === "function" ? options.rng : DEFAULT_RNG;
  const baseExclude = combineMintExclusions(options.exclude);
  const resolutionState = createMintResolutionState(baseExclude);
  const resolvedDescriptors = [];

  for (let i = 0; i < mints.length; i += 1) {
    const candidate = mints[i];
    const exclude = combineMintExclusions(resolutionState.used);
    const descriptor = resolveMintDescriptor(candidate, {
      rng,
      exclude,
      sampleOptions: options.sampleOptions,
      requireTags: options.requireTags,
      anyTags: options.anyTags,
      skipSolLike: options.skipSolLike,
      avoidRecent: options.avoidRecent,
    });
    const normalizedMint = normaliseSolMint(descriptor.mint);
    resolutionState.used.add(normalizedMint);
    resolvedDescriptors.push(descriptor);
  }

  for (let i = 0; i < resolvedDescriptors.length - 1; i += 1) {
    const fromDescriptor = resolvedDescriptors[i];
    const toDescriptor = resolvedDescriptors[i + 1];
    const fromMint = normaliseSolMint(fromDescriptor.mint);
    const toMint = normaliseSolMint(toDescriptor.mint);
    if (!fromMint || !toMint || fromMint === toMint) continue;

    const fromLabel =
      fromDescriptor.description ||
      fromDescriptor.symbol ||
      symbolForMint(fromMint);
    const toLabel =
      toDescriptor.description ||
      toDescriptor.symbol ||
      symbolForMint(toMint);

    const step = {
      from: fromMint,
      to: toMint,
      description: `${fromLabel} -> ${toLabel}`,
      forceAll: options.forceAll === true,
      resolvedFrom: fromDescriptor,
      resolvedTo: toDescriptor,
    };

    const randomResolutions = [];
    if (fromDescriptor.random) {
      randomResolutions.push({
        role: "from",
        placeholder: mints[i],
        entry: fromDescriptor,
      });
    }
    if (toDescriptor.random) {
      randomResolutions.push({
        role: "to",
        placeholder: mints[i + 1],
        entry: toDescriptor,
      });
    }
    if (randomResolutions.length > 0) {
      step.randomResolutions = randomResolutions;
    }

    steps.push(step);
  }

  return steps;
}

function flattenSegmentsToSteps(segments, options = {}) {
  const steps = [];
  const resolutionState = createMintResolutionState();
  for (const segment of segments) {
    steps.push(
      ...stepsFromMints(segment.mints, {
        ...options,
        forceAll: segment.forceAll,
      })
    );
  }
  return steps;
}

function describeStepSequence(steps) {
  return steps
    .map((step) => `${symbolForMint(step.from)}→${symbolForMint(step.to)}`)
    .join(' | ');
}

function determineAutomationAmountArg(forceAll = false) {
  if (forceAll) return 'all';
  return DEFAULT_SWAP_AMOUNT_MODE === 'all' ? 'all' : null;
}

// Choose which swap segments a wallet should execute. Random mode ensures
// every wallet performs a meaningful number of swaps by expanding the subset
// when the dice roll comes back too small.
function selectSegmentsForWallet(randomMode, options = {}) {
  if (!randomMode) return LONG_CHAIN_SEGMENTS_BASE;
  const rng = typeof options.rng === "function" ? options.rng : Math.random;
  const shuffled = shuffleArray(LONG_CHAIN_SEGMENTS_BASE, rng);
  const maxSegments = LONG_CHAIN_SEGMENTS_BASE.length;
  const minSegments = Math.min(2, maxSegments);
  for (let attempt = 0; attempt < maxSegments; attempt += 1) {
    const chosenCount = randomIntInclusive(minSegments, maxSegments, rng);
    const candidate = shuffled.slice(0, chosenCount);
    if (flattenSegmentsToSteps(candidate, { rng, exclude: new Set() }).length >= 3) {
      return candidate;
    }
  }
  return shuffled;
}

// Generates the optional post-chain random sweep path. Ensures at least
// three hops so the run is meaningful, falling back to the full token list
// if random selection still ends up too short.
function buildSecondaryPathMints(randomMode, options = {}) {
  const rng = typeof options.rng === "function" ? options.rng : Math.random;
  const excludeSet =
    options.exclude instanceof Set
      ? options.exclude
      : new Set(
          Array.isArray(options.exclude)
            ? options.exclude
            : options.exclude
            ? [options.exclude]
            : []
        );

  if (!randomMode) {
    const path = [SOL_MINT, DEFAULT_USDC_MINT];
    for (const mint of path) {
      if (!SOL_LIKE_MINTS.has(mint)) excludeSet.add(mint);
    }
    return path;
  }

  const poolMints = Array.from(
    new Set(
      TOKEN_CATALOG.filter(
        (entry) =>
          entry &&
          typeof entry.mint === "string" &&
          tokenHasTag(entry, "swappable") &&
          !tokenHasTag(entry, "secondary-terminal") &&
          !SOL_LIKE_MINTS.has(entry.mint)
      ).map((entry) => entry.mint)
    )
  );

  const availableIntermediates = poolMints.length;
  if (availableIntermediates === 0) {
    return [SOL_MINT, DEFAULT_USDC_MINT];
  }

  const maxIntermediateCount = Math.max(1, Math.min(availableIntermediates, 4));

  for (let attempt = 0; attempt < 5; attempt += 1) {
    const resolutionState = createMintResolutionState([SOL_MINT]);
    const path = [SOL_MINT];
    const intermediateCount = pickIntInclusive(
      Math.random,
      1,
      maxIntermediateCount
    );

    for (let i = 0; i < intermediateCount; i += 1) {
      const entry = sampleMintFromCatalog({
        requireTags: ["swappable"],
        exclude: resolutionState.used,
        skipSolLike: true,
        filter: (candidate) => !tokenHasTag(candidate, "secondary-terminal"),
      });
      if (!entry) break;
      path.push(entry.mint);
      resolutionState.used.add(entry.mint);
    }

    if (path.length <= 1) continue;

    let terminalEntry = sampleMintFromCatalog({
      requireTags: ["secondary-terminal"],
      exclude: resolutionState.used,
      skipSolLike: false,
    });
    let terminalMint = terminalEntry?.mint;
    if (!terminalMint || resolutionState.used.has(terminalMint)) {
      if (!resolutionState.used.has(DEFAULT_USDC_MINT)) {
        terminalMint = DEFAULT_USDC_MINT;
      } else if (!resolutionState.used.has(SOL_MINT)) {
        terminalMint = SOL_MINT;
      } else {
        const fallbackTerminal = sampleMintFromCatalog({
          requireTags: ["swappable"],
          exclude: resolutionState.used,
          skipSolLike: true,
          avoidRecent: false,
          filter: (candidate) => !tokenHasTag(candidate, "secondary-terminal"),
        });
        terminalMint = fallbackTerminal?.mint || DEFAULT_USDC_MINT;
      }
    }
    path.push(terminalMint);

    const deduped = [];
    for (const mint of path) {
      if (deduped.length === 0 || deduped[deduped.length - 1] !== mint) {
        deduped.push(mint);
      }
    }

    const unique = [];
    const seen = new Set();
    for (const mint of deduped) {
      if (seen.has(mint)) continue;
      seen.add(mint);
      unique.push(mint);
    }

    if (unique.length - 1 >= 3) {
      return unique;
    }
  }

  const fallback = [SOL_MINT];
  const seenFallback = new Set([SOL_MINT]);
  for (const entry of TOKEN_CATALOG) {
    if (!entry || typeof entry.mint !== "string") continue;
    if (!tokenHasTag(entry, "swappable")) continue;
    if (tokenHasTag(entry, "secondary-terminal")) continue;
    if (SOL_LIKE_MINTS.has(entry.mint)) continue;
    if (seenFallback.has(entry.mint)) continue;
    seenFallback.add(entry.mint);
    fallback.push(entry.mint);
  }
  if (!seenFallback.has(DEFAULT_USDC_MINT)) {
    fallback.push(DEFAULT_USDC_MINT);
  }
  return fallback;
}
async function executeSwapPlanForWallet(wallet, steps, label, options = {}) {
  if (!steps || steps.length === 0) return;
  if (isWalletDisabledByGuard(wallet.name)) {
    console.log(
      paint(
        `Skipping ${wallet.name}: disabled for swaps (<${WALLET_DISABLE_THRESHOLD_LABEL} SOL).`,
        "muted"
      )
    );
    return;
  }
  const walletSkipRegistry = options.skipRegistry instanceof Set ? options.skipRegistry : new Set();

  const stateConnection = createRpcConnection("confirmed");
  if (cachedAtaRentLamports === null) {
    const rent = await stateConnection.getMinimumBalanceForRentExemption(165);
    cachedAtaRentLamports = BigInt(rent);
  }
  const initialSolLamports = BigInt(
    await getSolBalance(stateConnection, wallet.kp.publicKey)
  );
  const parsedAccounts = await getAllParsedTokenAccounts(
    stateConnection,
    wallet.kp.publicKey
  );
  const tokenBalances = new Map();
  for (const { account } of parsedAccounts) {
    const info = account.data.parsed.info;
    const mint = info.mint;
    if (SOL_LIKE_MINTS.has(mint)) continue;
    const amount = BigInt(info.tokenAmount.amount);
    if (amount > 0n) tokenBalances.set(mint, amount);
  }

  // Calculate comprehensive gas requirements for the entire sequence
  const gasRequirements = await calculateTotalGasRequirements(steps, stateConnection, wallet.kp.publicKey);
  
  console.log(
    paint(
      `\n=== ${label} for ${wallet.name} (${wallet.kp.publicKey.toBase58()}) — ${steps.length} swap${steps.length === 1 ? '' : 's'} ===`,
      'label'
    )
  );
  
  // Display gas breakdown
  console.log(paint(`Gas estimation breakdown:`, "info"));
  for (const item of gasRequirements.breakdown) {
    const costFormatted = formatBaseUnits(item.cost, 9);
    if (item.type === 'ata_creation') {
      console.log(paint(`  ATA creation for ${item.token}: ${costFormatted} SOL`, "muted"));
    } else if (item.type === 'swap') {
      console.log(paint(`  Swap ${item.step} (${item.from}→${item.to}): ${costFormatted} SOL`, "muted"));
    } else if (item.type === 'base_reserve') {
      console.log(paint(`  ${item.description}: ${costFormatted} SOL`, "muted"));
    }
  }
  console.log(paint(`  Total estimated gas: ${formatBaseUnits(gasRequirements.totalGas, 9)} SOL`, "info"));
  console.log(paint(`  ATA creations needed: ${gasRequirements.ataCreations}`, "muted"));

  const firstStep = steps[0];
  if (firstStep) {
    if (SOL_LIKE_MINTS.has(firstStep.from)) {
      if (initialSolLamports <= gasRequirements.totalGas) {
        console.log(
          paint(
            `Skipping ${wallet.name}: SOL balance ${formatBaseUnits(initialSolLamports, 9)} insufficient for complete sequence (need ${formatBaseUnits(gasRequirements.totalGas, 9)} SOL)`,
            "error"
          )
        );
        return;
      }
    } else {
      const requiredBalance = tokenBalances.get(firstStep.from) || 0n;
      if (requiredBalance === 0n) {
        if (!SOL_LIKE_MINTS.has(firstStep.from)) {
          walletSkipRegistry.add(firstStep.from);
        }
        console.log(
          paint(
            `Skipping ${wallet.name}: no balance for initial token ${symbolForMint(firstStep.from)}`,
            "muted"
          )
        );
        return;
      }
    }
  }

  for (const step of steps) {
    const amountArg = determineAutomationAmountArg(step.forceAll);
    await doSwapAcross(step.from, step.to, amountArg, {
      wallets: [wallet],
      quietSkips: true,
      suppressMetadata: true,
      walletDelayMs: PASSIVE_STEP_DELAY_MS,
      walletSkipRegistry,
    });
    await passiveSleep();
  }

  const finalConnection = createRpcConnection("confirmed");
  const finalSolLamports = BigInt(
    await getSolBalance(finalConnection, wallet.kp.publicKey)
  );
  const solDelta = finalSolLamports - initialSolLamports;
  console.log(
    paint(
      `  SOL summary: ${formatBaseUnits(initialSolLamports, 9)} → ${formatBaseUnits(finalSolLamports, 9)} (Δ ${formatLamportsDelta(solDelta)})`,
      "muted"
    )
  );
}

// ---- Long circle orchestration (multi-hop randomised swap chains) ----
// Builds per-wallet plans from the configured segments, honours random mode,
// and triggers the optional secondary sweep when requested.
async function runWalletIntervalCycle() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint('No wallets found; cannot run interval cycle.', 'warn'));
    return;
  }

  const SWAP_DELAY_MS = 60_000;
  const LAP_RESTS_MS = [120_000, 180_000];
  const laps = 3;

  const walletCount = wallets.length;
  console.log(paint(`Interval cycle starting for ${walletCount} wallet${walletCount === 1 ? '' : 's'}`, 'label'));

  for (let lap = 0; lap < laps; lap += 1) {
    console.log(paint(`
-- interval cycle lap ${lap + 1}/${laps} --`, 'label'));

    const options = {
      wallets: wallets,
      quietSkips: true,
      suppressMetadata: false,
      walletDelayMs: 0,
    };

    try {
      await doSwapAcross(SOL_MINT, DEFAULT_USDC_MINT, 'all', options);
    } catch (err) {
      logDetailedError('  SOL -> USDC lap initial swap failed', err);
    }
    await delay(SWAP_DELAY_MS);

    for (const token of CREW1_CYCLE_TOKENS) {
      try {
        await doSwapAcross(DEFAULT_USDC_MINT, token.mint, 'random', options);
      } catch (err) {
        logDetailedError(`  USDC -> ${token.symbol} swap failed`, err);
      }
      await delay(SWAP_DELAY_MS);
      try {
        await doSwapAcross(token.mint, DEFAULT_USDC_MINT, 'all', options);
      } catch (err) {
        logDetailedError(`  ${token.symbol} -> USDC swap failed`, err);
      }
      await delay(SWAP_DELAY_MS);
    }

    if (lap < LAP_RESTS_MS.length) {
      const rest = LAP_RESTS_MS[lap];
      console.log(paint(`  resting ${rest / 60000} minute(s) before next lap`, 'muted'));
      await delay(rest);
    }
  }

  console.log(paint('Interval cycle completed.', 'success'));
}

// Legacy alias for backward compatibility
async function runCrew1Cycle() {
  return runWalletIntervalCycle();
}

async function runLongCircle(options = {}) {
  const enableSecondary = options.enableSecondary === true;
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint('No wallets found', 'muted'));
    return;
  }

  const randomMode = DEFAULT_SWAP_AMOUNT_MODE === 'random';
  console.log(
    paint(
      `Long chain starting in ${randomMode ? 'random' : 'deterministic'} mode across ${wallets.length} wallet${wallets.length === 1 ? '' : 's'}`,
      'label'
    )
  );

  const plans = await measureAsync("long-circle:plan-wallets", async () => {
    const built = wallets.map((wallet) => {
      const rng = deriveWalletSessionRng(wallet, "long-circle");
      let segments = selectSegmentsForWallet(randomMode, { rng });
      let usedMints = new Set();
      let steps = flattenSegmentsToSteps(segments, { rng, exclude: usedMints });
      if (randomMode && steps.length < 3) {
        const extended = new Set(segments);
        let attempt = 0;
        for (const segment of LONG_CHAIN_SEGMENTS_BASE) {
          if (extended.has(segment)) {
            attempt += 1;
            continue;
          }
          extended.add(segment);
          const candidateSegments = Array.from(extended);
          const candidateUsedMints = new Set();
          const candidateSteps = flattenSegmentsToSteps(candidateSegments, {
            rng,
            exclude: candidateUsedMints,
          });
          if (candidateSteps.length >= 3) {
            segments = candidateSegments;
            steps = candidateSteps;
            usedMints = candidateUsedMints;
            break;
          }
        }
      }

      const finalSteps = flattenSegmentsToSteps(segments, {
        rng: createDeterministicRng(`${wallet.name}:long-circle:steps-final`),
      });
      return {
        wallet,
        steps: finalSteps,
        summary: describeStepSequence(finalSteps),
        skipRegistry: new Set(),
        rng,
        usedMints,
      };
    });
    return built;
  });

  if (randomMode) {
    console.log(paint('Randomised segment order per wallet (quiet skips enabled).', 'muted'));
  }

  await measureAsync("long-circle:execute-wallets", async () => {
    for (const plan of plans) {
      if (plan.steps.length === 0) continue;
      console.log(paint(`  plan for ${plan.wallet.name}: ${plan.summary}`, 'muted'));
      await executeSwapPlanForWallet(plan.wallet, plan.steps, 'Long chain', { skipRegistry: plan.skipRegistry });
      await passiveSleep();
    }

    if (enableSecondary) {
      console.log(paint('\n-- secondary random order sweep --', 'label'));
      for (const plan of plans) {
        const secondaryPath = buildSecondaryPathMints(randomMode, {
          rng: plan.rng,
          exclude: plan.usedMints,
        });
        const secondarySteps = stepsFromMints(secondaryPath, {
          rng: plan.rng,
          exclude: plan.usedMints,
        });
        if (secondarySteps.length === 0) continue;
        const summary = describeStepSequence(secondarySteps);
        console.log(
          paint(
            `  secondary path for ${plan.wallet.name}: ${summary}`,
            'muted'
          )
        );
        await executeSwapPlanForWallet(plan.wallet, secondarySteps, 'Secondary chain', { skipRegistry: plan.skipRegistry });
        await passiveSleep();
      }
    } else {
      console.log(paint('\nSecondary random sweep disabled.', 'muted'));
    }
  });
}

function computeBuckshotSpendable(solLamports, ataRent) {
  if (solLamports <= MIN_SOL_PER_SWAP_LAMPORTS) return 0n;
  let reserve = solLamports > GAS_RESERVE_LAMPORTS
    ? GAS_RESERVE_LAMPORTS
    : solLamports / 10n;
  reserve += ataRent;
  reserve += JUPITER_SOL_BUFFER_LAMPORTS;
  if (reserve < MIN_SOL_PER_SWAP_LAMPORTS) {
    reserve = MIN_SOL_PER_SWAP_LAMPORTS;
  }
  if (reserve >= solLamports) return 0n;
  return solLamports - reserve;
}

function buildBuckshotTargets() {
  // Build ALL swappable tokens (no limit) for buckshot distribution
  const eligibleEntries = TOKEN_CATALOG.filter(
    (entry) =>
      entry &&
      typeof entry.mint === "string" &&
      tokenHasTag(entry, "swappable") &&
      !SOL_LIKE_MINTS.has(entry.mint)
  );
  const uniqueMints = Array.from(new Set(eligibleEntries.map((entry) => entry.mint)));
  if (uniqueMints.length === 0) return [];

  // Return ALL swappable tokens (each wallet will pick one randomly)
  return eligibleEntries.map((entry) => ({
    mint: entry.mint,
    symbol: entry.symbol || symbolForMint(entry.mint),
    name: entry.name || entry.symbol || symbolForMint(entry.mint),
  }));
}

async function runBuckshot() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }
  const targets = buildBuckshotTargets();
  if (targets.length === 0) {
    console.log(paint("Buckshot token list empty; nothing to do.", "muted"));
    return;
  }

  console.log(
    paint(
      `Buckshot mode — ${targets.length} available token${targets.length === 1 ? '' : 's'} (ONE token per wallet)`,
      "label"
    )
  );
  const targetSummary = targets.map((target) => target.symbol).join(', ');
  if (targetSummary) {
    console.log(paint(`  Available tokens: ${targetSummary}`, 'muted'));
  }

  const walletHoldings = new Map();
  // Track which tokens have been assigned to avoid duplicates across wallets
  const usedTokenIndices = new Set();
  const availableIndices = Array.from({ length: targets.length }, (_, i) => i);

  const planEntries = await measureAsync("buckshot:plan-wallets", async () => {
    const entries = [];
    for (const wallet of wallets) {
      if (isWalletDisabledByGuard(wallet.name)) {
        console.log(
          paint(
            `Skipping ${wallet.name}: disabled for swaps (<${WALLET_DISABLE_THRESHOLD_LABEL} SOL).`,
            "muted"
          )
        );
        continue;
      }

      const connection = createRpcConnection("confirmed");
      // No ATA rent pre-reservation: we'll close the account after swap to reclaim SOL
      const solLamports = BigInt(await getSolBalance(connection, wallet.kp.publicKey));
      let reserve = solLamports > GAS_RESERVE_LAMPORTS
        ? GAS_RESERVE_LAMPORTS
        : solLamports / 10n;
      reserve += JUPITER_SOL_BUFFER_LAMPORTS;

      if (reserve >= solLamports) {
        console.log(
          paint(
            `Skipping ${wallet.name}: SOL balance ${formatBaseUnits(solLamports, 9)} below reserve ${formatBaseUnits(reserve, 9)}`,
            "muted"
          )
        );
        continue;
      }

      const spendable = solLamports - reserve;
      if (spendable <= MIN_SOL_PER_SWAP_LAMPORTS) {
        console.log(
          paint(
            `Skipping ${wallet.name}: spendable SOL ${formatBaseUnits(spendable, 9)} below minimum swap amount.`,
            "muted"
          )
        );
        continue;
      }

      // Pick ONE random token for this wallet (avoid already-used tokens)
      const remainingIndices = availableIndices.filter(i => !usedTokenIndices.has(i));
      if (remainingIndices.length === 0) {
        console.log(
          paint(
            `Skipping ${wallet.name}: all ${targets.length} tokens have been assigned to other wallets.`,
            "muted"
          )
        );
        continue;
      }

      const randomIndex = remainingIndices[Math.floor(Math.random() * remainingIndices.length)];
      const selectedToken = targets[randomIndex];
      usedTokenIndices.add(randomIndex);

      entries.push({
        wallet,
        token: selectedToken,
        spendableDecimal: formatBaseUnits(spendable, 9),
      });
      walletHoldings.set(wallet.name, new Set([selectedToken.mint]));
      await balanceRpcDelay();
    }
    return entries;
  });

  if (planEntries.length === 0) {
    console.log(paint("No eligible wallets for buckshot after applying reserves.", "muted"));
    return;
  }

  console.log(
    paint(
      `\nPrepared buckshot plans for ${planEntries.length} wallet${planEntries.length === 1 ? "" : "s"}.`,
      "label"
    )
  );
  for (const plan of planEntries) {
    console.log(
      paint(
        `  ${plan.wallet.name} → ${plan.token.symbol}: ${plan.spendableDecimal} SOL`,
        "muted"
      )
    );
  }

  await measureAsync("buckshot:execute-swaps", async () => {
    for (const plan of planEntries) {
      const { wallet, token, spendableDecimal } = plan;
      console.log(
        paint(
          `\n=== Buckshot: ${wallet.name} swapping ${spendableDecimal} SOL → ${token.symbol} ===`,
          "label"
        )
      );

      await doSwapAcross(SOL_MINT, token.mint, spendableDecimal, {
        wallets: [wallet],
        quietSkips: true,
        suppressMetadata: false,
        maxSlippageRetries: 7,
        slippageBoostAfter: 3,
        slippageBoostStrategy: "add",
        slippageBoostIncrementBps: 200,
      });

      await passiveSleep();
    }
  });

  console.log(
    paint(
      "\nBuckshot distribution complete. Each wallet holds ONE token position.",
      "success"
    )
  );
  const buckshotSummary = buildHotkeyInlineSummary("buckshot-rotation");
  if (buckshotSummary) {
    console.log(paint(`Commands: ${buckshotSummary}.`, "info"));
  }
  const buckshotExitLabel =
    formatHotkeyKeys("buckshot-rotation", "exit") || "<enter>";
  console.log(
    paint(
      `Enter a mint address to rotate all held tokens into the new target (${buckshotExitLabel} to exit).`,
      "info"
    )
  );

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const prompt = () =>
    new Promise((resolve) => rl.question("buckshot target mint> ", resolve));

  while (true) {
    const rawInput = (await prompt())?.trim();
    if (!rawInput) {
      break;
    }
    if (isHotkeyMatch("buckshot-rotation", "exit", rawInput)) {
      break;
    }
    let targetMint;
    try {
      const pk = new PublicKey(rawInput);
      targetMint = pk.toBase58();
    } catch (err) {
      console.log(
        paint(
          `Invalid mint provided (${rawInput}). Please paste a valid mint address.`,
          "warn"
        )
      );
      continue;
    }

    console.log(
      paint(
        `\nRotating held tokens into ${symbolForMint(targetMint)} (${targetMint})`,
        "label"
      )
    );

    for (const wallet of wallets) {
      if (isWalletDisabledByGuard(wallet.name)) {
        console.log(
          paint(
            `Skipping ${wallet.name}: disabled for swaps (<${WALLET_DISABLE_THRESHOLD_LABEL} SOL).`,
            "muted"
          )
        );
        continue;
      }
      const holdingsSet = walletHoldings.get(wallet.name);
      if (!holdingsSet || holdingsSet.size === 0) {
        console.log(
          paint(
            `Skipping ${wallet.name}: no recorded token holdings from buckshot.`,
            "muted"
          )
        );
        continue;
      }
      if (holdingsSet.size === 1 && holdingsSet.has(targetMint)) {
        console.log(
          paint(
            `Skipping ${wallet.name}: already holding ${symbolForMint(targetMint)}.`,
            "muted"
          )
        );
        continue;
      }

      for (const mint of holdingsSet) {
        if (mint === targetMint) continue;
        const fromSymbol = symbolForMint(mint);
        const toSymbol = symbolForMint(targetMint);
        console.log(
          paint(
            `  ${wallet.name}: swapping ${fromSymbol} -> ${toSymbol}`,
            "info"
          )
        );
        await doSwapAcross(mint, targetMint, "all", {
          wallets: [wallet],
          quietSkips: true,
          suppressMetadata: true,
          maxSlippageRetries: 7,
          slippageBoostAfter: 3,
          slippageBoostStrategy: "add",
          slippageBoostIncrementBps: 200,
        });
        await passiveSleep();
      }

      walletHoldings.set(wallet.name, new Set([targetMint]));
    }

    console.log(
      paint(
        `Rotation complete. Wallets now target ${symbolForMint(targetMint)} (${targetMint}).`,
        "success"
      )
    );
    console.log(
      paint(
        "Paste another mint to rotate again, or press Enter to exit buckshot mode.",
        "info"
      )
    );
  }

  rl.close();

  console.log(
    paint(
      "Buckshot mode finished. Current holdings remain in the last selected token.",
      "muted"
    )
  );
}

/* -------------------------------------------------------------------------- */
/* Prewritten flows                                                           */
/* -------------------------------------------------------------------------- */

const PREWRITTEN_FLOW_PLAN_MAP = new Map([
  [
    "arpeggio",
    {
      key: "arpeggio",
      label: "Arpeggio",
      description:
        "Loops SOL through USDC, POPCAT, and PUMP with SOL consolidation between legs.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: null,
          description: "Seed USDC runway from SOL holdings",
        },
        {
          fromMint: DEFAULT_USDC_MINT,
          toMint: POPCAT_MINT,
          amount: "all",
          description: "Take a randomized POPCAT entry from USDC",
        },
        {
          fromMint: POPCAT_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Flatten POPCAT exposure back to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: PUMP_MINT,
          amount: "random",
          description: "Rotate part of SOL into PUMP",
        },
        {
          fromMint: PUMP_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest PUMP back to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: null,
          description: "Rebuild USDC buffer before the next cycle",
        },
      ],
      swapCountRange: { min: 10, max: 100 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 45_000, max: 120_000 },
      defaultDurationMs: 45 * 60 * 1000,
    },
  ],
  [
    "horizon",
    {
      key: "horizon",
      label: "Horizon",
      description:
        "Mid-duration rotation cycling through USDC, URANUS, WIF, and POPCAT with SOL anchors.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: { mode: "range", min: 0.02, max: 0.35 },
          description: "Establish USDC base from SOL",
        },
        {
          fromMint: DEFAULT_USDC_MINT,
          toMint: URANUS_MINT,
          amount: "all",
          description: "Rotate USDC into URANUS position",
        },
        {
          fromMint: URANUS_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Consolidate URANUS back to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: WIF_MINT,
          amount: { mode: "range", min: 0.08, max: 0.25 },
          description: "Deploy SOL into WIF",
        },
        {
          fromMint: WIF_MINT,
          toMint: POPCAT_MINT,
          amount: "all",
          description: "Pivot WIF into POPCAT",
        },
        {
          fromMint: POPCAT_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest POPCAT position to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: null,
          description: "Rebalance into USDC for next cycle",
        },
      ],
      swapCountRange: { min: 50, max: 300 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 90_000, max: 180_000 },
      defaultDurationMs: 60 * 60 * 1000,
    },
  ],
  [
    "echo",
    {
      key: "echo",
      label: "Echo",
      description:
        "Extended multi-hour loop alternating between major pairs with deep USDC buffers.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: { mode: "range", min: 0.18, max: 0.42 },
          description: "Build substantial USDC runway",
        },
        {
          fromMint: DEFAULT_USDC_MINT,
          toMint: URANUS_MINT,
          amount: { mode: "range", min: 0.25, max: 0.5 },
          description: "Take measured URANUS entry from USDC",
        },
        {
          fromMint: URANUS_MINT,
          toMint: WIF_MINT,
          amount: "all",
          description: "Rotate URANUS exposure into WIF",
        },
        {
          fromMint: WIF_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Consolidate WIF back to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: POPCAT_MINT,
          amount: { mode: "range", min: 0.1, max: 0.3 },
          description: "Deploy SOL into POPCAT",
        },
        {
          fromMint: POPCAT_MINT,
          toMint: PUMP_MINT,
          amount: "all",
          description: "Pivot POPCAT into PUMP",
        },
        {
          fromMint: PUMP_MINT,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest PUMP position to SOL",
        },
        {
          fromMint: SOL_MINT,
          toMint: DEFAULT_USDC_MINT,
          amount: null,
          description: "Rebuild USDC buffer for continuation",
        },
      ],
      swapCountRange: { min: 250, max: 750 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 120_000, max: 300_000 },
      defaultDurationMs: 6 * 60 * 60 * 1000,
    },
  ],
  [
    "icarus",
    {
      key: "icarus",
      label: "Icarus",
      description:
        "High-tempo random meme rotations that return to SOL between bursts.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.08, max: 0.22 },
          description: "Deploy SOL into a random fanout token",
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "icarus-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest the random position back to SOL",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "icarus-core",
          },
        },
      ],
      swapCountRange: { min: 18, max: 120 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 60_000, max: 180_000 },  // 1-3 minutes between hops for better randomization
      defaultDurationMs: 40 * 60 * 1000,
    },
  ],
  [
    "zenith",
    {
      key: "zenith",
      label: "Zenith",
      description:
        "Mid-tempo rotations that pivot between random pools before settling in SOL.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.1, max: 0.28 },
          description: "Seed a random long-circle token from SOL",
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "zenith-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: "all",
          description: "Rotate into another random pool before returning",
          randomization: {
            mode: "session-to-random",
            sessionGroup: "zenith-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Realise the position back to SOL",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "zenith-core",
          },
        },
      ],
      swapCountRange: { min: 24, max: 150 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 90_000, max: 240_000 },  // 1.5-4 minutes between hops
      defaultDurationMs: 55 * 60 * 1000,
    },
  ],
  [
    "aurora",
    {
      key: "aurora",
      label: "Aurora",
      description:
        "Slow and steady random accumulations cycling through secondary pools.",
      startMint: SOL_MINT,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.05, max: 0.16 },
          description: "Feather SOL into a random secondary token",
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "aurora-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Rebalance back to SOL",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "aurora-core",
          },
        },
      ],
      swapCountRange: { min: 12, max: 90 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      waitBoundsMs: { min: 60_000, max: 150_000 },
      defaultDurationMs: 70 * 60 * 1000,
    },
  ],
  [
    "titan",
    {
      key: "titan",
      label: "Titan",
      description:
        "High-value version of Icarus with 0.02 SOL minimum swaps and extended delays (1min minimum) for whale positions.",
      startMint: SOL_MINT,
      minSwapSol: 0.02,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.15, max: 0.4 },
          description: "Deploy SOL into a random token (whale-sized)",
          delayAfterMs: { min: 60_000, max: 600_000 },  // 1min-10min delay AFTER acquiring token
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "titan-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest the random position back to SOL (full balance)",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "titan-core",
          },
          delayAfterMs: { min: 60_000, max: 600_000 },
        },
      ],
      swapCountRange: { min: 18, max: 120 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      forceSolReturnEvery: { min: 4, max: 7 },  // Return to SOL every 4-7 swaps for safety
      waitBoundsMs: { min: 60_000, max: 600_000 },  // 1 minute to 10 minutes between swaps
      defaultDurationMs: 30 * 60 * 1000,
      loopable: true,
    },
  ],
  [
    "odyssey",
    {
      key: "odyssey",
      label: "Odyssey",
      description:
        "High-value version of Zenith with 0.02 SOL minimum swaps and extended delays for substantial holdings.",
      startMint: SOL_MINT,
      minSwapSol: 0.02,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.2, max: 0.45 },
          description: "Seed a random token from SOL (whale-sized)",
          delayAfterMs: { min: 30_000, max: 600_000 },  // 30s-10min delay AFTER acquiring token
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "odyssey-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: "all",
          description: "Rotate into another random pool (full balance)",
          delayAfterMs: { min: 30_000, max: 600_000 },  // 30s-10min delay while holding token
          randomization: {
            mode: "session-to-random",
            sessionGroup: "odyssey-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Realise the position back to SOL (full balance)",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "odyssey-core",
          },
          delayAfterMs: { min: 30_000, max: 600_000 },
        },
      ],
      swapCountRange: { min: 30, max: 180 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      forceSolReturnEvery: { min: 4, max: 7 },  // Return to SOL every 4-7 swaps for safety
      waitBoundsMs: { min: 30_000, max: 600_000 },  // 30 seconds to 10 minutes between swaps
      defaultDurationMs: 90 * 60 * 1000,
      loopable: true,
    },
  ],
  [
    "sovereign",
    {
      key: "sovereign",
      label: "Sovereign",
      description:
        "High-value version of Aurora with 0.02 SOL minimum swaps and extended delays for commanding long-term positions.",
      startMint: SOL_MINT,
      minSwapSol: 0.02,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.02, max: 0.35 },
          description: "Feather SOL into a random token (whale-sized)",
          delayAfterMs: { min: 30_000, max: 600_000 },  // 30s-10min delay AFTER acquiring token
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "sovereign-core",
            // Using poolTags to pull ALL swappable tokens from token_catalog.json
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Rebalance back to SOL (full balance)",
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "sovereign-core",
          },
          delayAfterMs: { min: 30_000, max: 600_000 },
        },
      ],
      swapCountRange: { min: 20, max: 150 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      forceSolReturnEvery: { min: 4, max: 7 },  // Return to SOL every 4-7 swaps for safety
      waitBoundsMs: { min: 30_000, max: 600_000 },  // 30 seconds to 10 minutes between swaps
      defaultDurationMs: 8 * 60 * 60 * 1000,
      loopable: true,
    },
  ],
  [
    "nova",
    {
      key: "nova",
      label: "Nova",
      description:
        "Supernova version of Icarus with 0.01 SOL minimum swaps, 30s-10min holding periods, and infinite loop capability.",
      startMint: SOL_MINT,
      minSwapSol: 0.01,
      cycleTemplate: [
        {
          fromMint: SOL_MINT,
          toMint: RANDOM_MINT_PLACEHOLDER,
          amount: { mode: "range", min: 0.15, max: 0.4 },
          description: "Deploy SOL into a random token (supernova-sized)",
          delayAfterMs: { min: 30_000, max: 600_000 },  // 30s-10min delay AFTER acquiring token
          randomization: {
            mode: "sol-to-random",
            sessionGroup: "nova-core",
            poolTags: ["swappable"],
            excludeMints: [SOL_MINT],
          },
        },
        {
          fromMint: RANDOM_MINT_PLACEHOLDER,
          toMint: SOL_MINT,
          amount: "all",
          description: "Harvest the random position back to SOL (full balance)",
          delayAfterMs: { min: 1_000, max: 3_000 },  // Minimal delay before next SOL deployment
          randomization: {
            mode: "session-to-sol",
            sessionGroup: "nova-core",
          },
        },
      ],
      swapCountRange: { min: 18, max: 120 },
      minimumCycles: 2,
      requireTerminalSolHop: true,
      forceSolReturnEvery: { min: 4, max: 7 },  // Return to SOL every 4-7 swaps for safety
      waitBoundsMs: { min: 1_000, max: 600_000 },  // Allow per-step delays to override
      defaultDurationMs: 30 * 60 * 1000,
      loopable: true,  // Supports infinite looping
    },
  ],
]);

function normalizePrewrittenFlowKey(key) {
  if (typeof key !== "string") return key;
  return key.trim().toLowerCase();
}

function sampleIntegerInRange(minValue, maxValue, rng = DEFAULT_RNG) {
  const min = Math.max(0, Math.floor(minValue));
  const max = Math.max(min, Math.floor(maxValue));
  if (max === min) return min;
  return randomIntInclusive(min, max, rng);
}

function formatDurationMs(totalMs) {
  if (!Number.isFinite(totalMs) || totalMs <= 0) return "0s";
  const totalSeconds = Math.round(totalMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(" ");
}

function allocateHopDelays(totalDurationMs, hopCount, options = {}, rng = DEFAULT_RNG) {
  const count = Math.max(0, hopCount | 0);
  if (count === 0) return [];

  const total = Math.max(0, Math.floor(Number(totalDurationMs) || 0));
  if (total === 0) return new Array(count).fill(0);

  const rawMin = options.min ?? 0;
  const rawMax = options.max ?? null;
  const minMs = Math.max(0, Math.floor(Number(rawMin) || 0));
  const maxMs =
    rawMax === null || rawMax === undefined
      ? null
      : Math.max(minMs, Math.floor(Number(rawMax) || 0));

  const baseTotal = minMs * count;
  if (baseTotal >= total) {
    const per = Math.floor(total / count);
    const remainder = total - per * count;
    return Array.from({ length: count }, (_, index) =>
      per + (index < remainder ? 1 : 0)
    );
  }

  const generator = normaliseRng(rng);
  const result = new Array(count).fill(minMs);
  let remaining = total - baseTotal;

  const capacities = new Array(count).fill(
    maxMs === null ? Number.POSITIVE_INFINITY : Math.max(0, maxMs - minMs)
  );

  const weights = Array.from({ length: count }, () => randomFloat(generator) + 0.01);
  const weightTotal = weights.reduce((sum, value) => sum + value, 0);

  let allocated = 0;
  for (let i = 0; i < count && remaining - allocated > 0; i += 1) {
    const desired = Math.floor((weights[i] / weightTotal) * remaining);
    if (desired <= 0) continue;
    const capacity = capacities[i];
    const addition = Math.min(capacity, desired, remaining - allocated);
    if (addition <= 0) continue;
    result[i] += addition;
    capacities[i] = capacity - addition;
    allocated += addition;
  }

  let leftover = remaining - allocated;
  if (leftover > 0) {
    let guard = 0;
    const maxIterations = count * 12;
    while (leftover > 0 && guard < maxIterations) {
      let progress = false;
      for (let i = 0; i < count && leftover > 0; i += 1) {
        if (capacities[i] <= 0) continue;
        result[i] += 1;
        capacities[i] -= 1;
        leftover -= 1;
        progress = true;
      }
      if (!progress) break;
      guard += 1;
    }

    if (leftover > 0) {
      for (let i = count - 1; i >= 0 && leftover > 0; i -= 1) {
        const addition = Math.min(capacities[i], leftover);
        if (addition <= 0) continue;
        result[i] += addition;
        capacities[i] -= addition;
        leftover -= addition;
      }
    }
  }

  const currentTotal = result.reduce((sum, value) => sum + value, 0);
  let difference = total - currentTotal;
  if (difference !== 0) {
    const minBound = minMs;
    const maxBound = maxMs === null ? Number.POSITIVE_INFINITY : maxMs;
    let iteration = 0;
    const maxIterations = count * 20;
    while (difference !== 0 && iteration < maxIterations) {
      const index = iteration % count;
      if (difference > 0) {
        if (result[index] < maxBound) {
          result[index] += 1;
          difference -= 1;
        }
      } else if (difference < 0) {
        if (result[index] > minBound) {
          result[index] -= 1;
          difference += 1;
        }
      }
      iteration += 1;
    }
    if (difference !== 0) {
      const index = count - 1;
      const adjusted = Math.max(
        minBound,
        Math.min(maxBound, result[index] + difference)
      );
      difference -= adjusted - result[index];
      result[index] = adjusted;
    }
  }

  return result;
}

function cloneFlowAmount(amount) {
  if (amount && typeof amount === "object") {
    return { ...amount };
  }
  return amount;
}

function normalizeFlowAmount(amount, options = {}) {
  const rng = options.rng;
  const source = cloneFlowAmount(amount);
  if (source === null || source === undefined) return null;
  if (typeof source === "string") {
    const trimmed = source.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof source === "number") {
    if (!Number.isFinite(source)) return null;
    // Round to 9 decimals max to prevent "too many fractional digits" errors
    const rounded = Math.floor(source * 1_000_000_000) / 1_000_000_000;
    return rounded.toString();
  }
  if (typeof source === "object") {
    const mode = typeof source.mode === "string" ? source.mode.toLowerCase() : null;
    if (mode === "all" || mode === "random") return mode;
    if (mode === "explicit" && source.value !== undefined && source.value !== null) {
      const value = Number(source.value);
      if (Number.isFinite(value)) {
        const rounded = Math.floor(value * 1_000_000_000) / 1_000_000_000;
        return rounded.toString();
      }
      return source.value.toString();
    }
    if (mode === "range") {
      const min = Number(source.min ?? 0);
      const max = Number(source.max ?? min);
      if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
      const lower = Math.min(min, max);
      const upper = Math.max(min, max);
      const sampled = lower + randomFloat(rng) * (upper - lower);
      // Round to 9 decimals max to prevent "too many fractional digits" errors
      const rounded = Math.floor(sampled * 1_000_000_000) / 1_000_000_000;
      return rounded.toString();
    }
    if (source.value !== undefined && source.value !== null) {
      const value = Number(source.value);
      if (Number.isFinite(value)) {
        const rounded = Math.floor(value * 1_000_000_000) / 1_000_000_000;
        return rounded.toString();
      }
      return source.value.toString();
    }
  }
  return null;
}

function describeFlowAmount(normalizedAmount) {
  if (normalizedAmount === null) return "(session default amount)";
  const lowered = normalizedAmount.toLowerCase();
  if (lowered === "all") return "(all holdings)";
  if (lowered === "random") return "(randomized amount)";
  return `(amount ${normalizedAmount})`;
}

// Helper function to resolve mint candidates (missing from imports)
function resolveMintCandidate(candidate, options = {}) {
  const fallbackMint = options.fallbackMint || SOL_MINT;

  // If it's already a valid mint address, return it
  if (typeof candidate === "string" && candidate.length > 32) {
    return {
      mint: normaliseSolMint(candidate),
      resolution: null,
    };
  }

  // If it's a symbol, look it up
  if (typeof candidate === "string") {
    const entry = tokenBySymbol(candidate);
    if (entry?.mint) {
      return {
        mint: entry.mint,
        resolution: { symbol: entry.symbol, source: "catalog" },
      };
    }
  }

  // Fall back to default
  return {
    mint: fallbackMint,
    resolution: null,
  };
}

// Helper object for mint resolution (missing from code)
const mintResolver = {
  resolveMint(candidate, options = {}) {
    if (!candidate) return null;

    // Handle RANDOM_MINT_PLACEHOLDER
    if (candidate === RANDOM_MINT_PLACEHOLDER) {
      return RANDOM_MINT_PLACEHOLDER;
    }

    // If it's already a valid mint, return it
    if (typeof candidate === "string" && candidate.length > 32) {
      return normaliseSolMint(candidate);
    }

    // If it's a symbol, look it up
    if (typeof candidate === "string") {
      const entry = tokenBySymbol(candidate);
      if (entry?.mint) return entry.mint;
    }

    return candidate;
  }
};

// USD Validation Helper: Validates and auto-adjusts swap amounts to meet minimum USD value
async function validateAndAdjustSwapAmountUSD(fromMint, plannedSolAmount, minUSD) {
  try {
    // Fetch current SOL price in USD
    const url = new URL(JUPITER_PRICE_ENDPOINT);
    url.searchParams.set("ids", SOL_MINT);
    const resp = await fetch(url.toString(), {
      method: "GET",
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(5000)
    });

    if (!resp.ok) {
      console.warn(paint("⚠️  Failed to fetch SOL price for USD validation, proceeding with planned amount", "warn"));
      return {
        isValid: true,
        adjustedAmount: plannedSolAmount,
        usdValue: null,
        pricePerSol: null,
        skipped: false
      };
    }

    const data = await resp.json();
    // Price API v3 returns prices directly, no data wrapper
    const priceData = data?.[SOL_MINT];

    if (!priceData?.usdPrice) {
      console.warn(paint("⚠️  SOL price unavailable, proceeding with planned amount", "warn"));
      return {
        isValid: true,
        adjustedAmount: plannedSolAmount,
        usdValue: null,
        pricePerSol: null,
        skipped: false
      };
    }

    const pricePerSol = priceData.usdPrice;
    const plannedUsdValue = plannedSolAmount * pricePerSol;

    // Check if planned amount meets minimum
    if (plannedUsdValue >= minUSD) {
      return {
        isValid: true,
        adjustedAmount: plannedSolAmount,
        usdValue: plannedUsdValue,
        pricePerSol: pricePerSol,
        skipped: false
      };
    }

    // Calculate adjusted amount to meet minimum USD value
    const adjustedAmount = minUSD / pricePerSol;
    const adjustedUsdValue = adjustedAmount * pricePerSol;

    return {
      isValid: false,
      adjustedAmount: adjustedAmount,
      usdValue: adjustedUsdValue,
      plannedUsdValue: plannedUsdValue,
      pricePerSol: pricePerSol,
      skipped: false
    };

  } catch (err) {
    console.warn(paint(`⚠️  USD validation error: ${err.message}, proceeding with planned amount`, "warn"));
    return {
      isValid: true,
      adjustedAmount: plannedSolAmount,
      usdValue: null,
      pricePerSol: null,
      skipped: false
    };
  }
}

async function runPrewrittenFlowPlan(flowKey, options = {}) {
  const normalizedKey = normalizePrewrittenFlowKey(flowKey);
  const flow =
    PREWRITTEN_FLOW_PLAN_MAP.get(normalizedKey) ||
    PREWRITTEN_FLOW_PLAN_MAP.get(flowKey);
  if (!flow) {
    throw new Error(`Unknown prewritten flow: ${flowKey}`);
  }

  const flowRandomSessions = new Map();

  // Create per-wallet session maps for independent random token selection
  const perWalletSessions = new Map();

  let walletList = Array.isArray(options.wallets) && options.wallets.length > 0
    ? options.wallets
    : listWallets();
  if (walletList.length === 0) {
    console.log(paint("No wallets found for prewritten flow", "muted"));
    return {
      key: flow.key,
      plannedSwaps: 0,
      cycles: 0,
      waitTotalMs: 0,
      targetWaitTotalMs: 0,
      finalMint: flow.startMint || SOL_MINT,
    };
  }

  const seedSource =
    walletList[0]?.kp?.publicKey?.toBase58?.() ?? flow.key ?? "prewritten";
  let flowRng = typeof options.rng === "function" ? options.rng : null;
  if (!flowRng) {
    flowRng = createDeterministicRng(`${flow.key}:${seedSource}:flow`);
  }

  const runtimeProfile = PREWRITTEN_FLOW_DEFINITIONS?.[normalizedKey]?.runtimeProfile;
  const selectMintForFlow = (randomMeta = {}, rng = flowRng) => {
    let pool = TOKEN_CATALOG.filter((entry) => entry && entry.mint);
    if (Array.isArray(randomMeta.poolTags) && randomMeta.poolTags.length > 0) {
      const tagSet = new Set(
        randomMeta.poolTags
          .map((tag) => (typeof tag === "string" ? tag.trim().toLowerCase() : ""))
          .filter((tag) => tag.length > 0)
      );
      pool = pool.filter((entry) =>
        Array.isArray(entry.tags) && entry.tags.some((tag) => tagSet.has(tag))
      );
    }
    if (Array.isArray(randomMeta.poolMints) && randomMeta.poolMints.length > 0) {
      const allowed = new Set(
        randomMeta.poolMints
          .map((mint) => (typeof mint === "string" ? mint : null))
          .filter(Boolean)
      );
      pool = pool.filter((entry) => allowed.has(entry.mint));
    }
    const excludeSet = new Set(
      (randomMeta.excludeMints || [])
        .map((mint) => (typeof mint === "string" ? normaliseSolMint(mint) : null))
        .filter(Boolean)
    );
    const candidates = pool.filter((entry) => {
      if (!entry || !entry.mint) return false;
      const normalized = normaliseSolMint(entry.mint);
      if (excludeSet.has(normalized)) return false;
      return !SOL_LIKE_MINTS.has(entry.mint);
    });
    if (candidates.length === 0) {
      return null;
    }
    const pickIndex = Math.floor(rng() * candidates.length) % candidates.length;
    const pick = candidates[pickIndex] || candidates[0];
    if (!pick) return null;
    return {
      mint: pick.mint,
      symbol: pick.symbol,
      decimals: pick.decimals ?? 6,
    };
  };

  const cycleTemplate = Array.isArray(flow.cycleTemplate)
    ? flow.cycleTemplate
    : [];
  if (cycleTemplate.length === 0) {
    throw new Error(
      `Prewritten flow ${flow.label} has no cycle template defined`
    );
  }

  const swapRange = flow.swapCountRange || runtimeProfile?.swapCountRange || {};
  const cycleLength = cycleTemplate.length;
  const rangeMinBase = Math.max(cycleLength, Math.floor(swapRange.min ?? cycleLength));
  const rangeMaxBase = Math.max(rangeMinBase, Math.floor(swapRange.max ?? rangeMinBase));
  const clampSwapTarget = (value) => {
    const numeric = Math.floor(Number(value) || 0);
    if (!Number.isFinite(numeric)) return rangeMinBase;
    return Math.max(rangeMinBase, Math.min(rangeMaxBase, numeric));
  };
  const overrideTarget = options.swapTarget ?? options.swapCount ?? null;

  let sampledTarget;
  if (overrideTarget !== null && overrideTarget !== undefined) {
    sampledTarget = Math.max(
      rangeMinBase,
      Math.floor(Number(overrideTarget) || rangeMinBase)
    );
  } else {
    sampledTarget = sampleIntegerInRange(rangeMinBase, rangeMaxBase, flowRng);
  }

  const minimumCycles = Math.max(1, Math.floor(flow.minimumCycles ?? 1));
  const perFlowMinimumSwaps = Math.max(
    0,
    Math.floor(
      flow.minimumSwapCount ?? runtimeProfile?.minimumSwapCount ?? rangeMinBase
    )
  );
    const minimumSwapCount = Math.max(rangeMinBase, perFlowMinimumSwaps);
    const desiredSwapTarget = Math.max(sampledTarget, minimumSwapCount);

    const startCandidate = pickFirstDefined(
      options.startMint,
      flow.startMint,
      SOL_MINT
    );
    const normalizedStartMint =
      typeof startCandidate === "string"
        ? normaliseSolMint(startCandidate)
        : null;
    const templateTerminalMint =
      cycleTemplate.length > 0
        ? cycleTemplate[cycleTemplate.length - 1]?.toMint
        : null;
    const normalizedTemplateTerminalMint =
      typeof templateTerminalMint === "string"
        ? normaliseSolMint(templateTerminalMint)
        : null;

    let fullCycles = Math.floor(desiredSwapTarget / cycleLength);
    let partialCycleHops = desiredSwapTarget % cycleLength;
    const normalizedPartialCycleTerminalMint =
      partialCycleHops > 0 && partialCycleHops <= cycleTemplate.length
        ? (() => {
            const terminalStep = cycleTemplate[partialCycleHops - 1] || null;
            const terminalMint =
              typeof terminalStep?.toMint === "string"
                ? terminalStep.toMint
                : null;
            return terminalMint ? normaliseSolMint(terminalMint) : null;
          })()
        : null;
    let executedCycles = fullCycles + (partialCycleHops > 0 ? 1 : 0);
    if (executedCycles < minimumCycles) {
      fullCycles = minimumCycles;
      partialCycleHops = 0;
      executedCycles = minimumCycles;
    }

    let executedSwapTarget = fullCycles * cycleLength + partialCycleHops;
    if (
      partialCycleHops > 0 &&
      !flow.requireTerminalSolHop &&
      normalizedStartMint &&
      normalizedTemplateTerminalMint &&
      normalizedStartMint === normalizedTemplateTerminalMint
    ) {
      const shouldClosePartialCycle =
        !normalizedPartialCycleTerminalMint ||
        normalizedPartialCycleTerminalMint !== normalizedTemplateTerminalMint;

      if (shouldClosePartialCycle) {
        fullCycles += 1;
        partialCycleHops = 0;
        executedCycles = Math.max(executedCycles, fullCycles);
        executedSwapTarget = fullCycles * cycleLength;
      }
    }

    const combinedRandomOptions = combineRandomMintOptions(
      flow.randomMintOptions || EMPTY_RANDOM_MINT_OPTIONS,
      options.randomMintOptions || EMPTY_RANDOM_MINT_OPTIONS
    );
  const logRandomResolutions = options.logRandomResolutions !== false;
  const resolutionHandler =
    typeof options.onRandomMintResolved === "function"
      ? options.onRandomMintResolved
      : logRandomResolutions
      ? logRandomMintResolution
      : null;

    const startResult = resolveMintCandidate(startCandidate, {
      fallbackMint: SOL_MINT,
      rng: flowRng,
      baseRandomOptions: combinedRandomOptions,
      label: `${flow.label} start`,
  });
  let currentMint = startResult.mint;
  if (startResult.resolution && resolutionHandler) {
    resolutionHandler({
      ...startResult.resolution,
      role: "start",
      stepIndex: -1,
    });
  }

  const schedule = [];
  const cycles = executedCycles;

  for (let cycleIndex = 0; cycleIndex < cycles; cycleIndex += 1) {
    const sessionGroups = new Map();
    const limit = cycleIndex === cycles - 1 && partialCycleHops > 0 ? partialCycleHops : cycleTemplate.length;
    for (let stepIndex = 0; stepIndex < limit; stepIndex += 1) {
      const step = cycleTemplate[stepIndex];
      const fromMint = step.fromMint || currentMint;
      const resolvedFromMint = mintResolver.resolveMint(fromMint);
      const toMint = step.toMint;
      const resolvedToMint = mintResolver.resolveMint(toMint, {
        exclude: [
          resolvedFromMint,
          currentMint,
          ...(Array.isArray(step.avoidMints) ? step.avoidMints : []),
        ],
      });
      if (!resolvedToMint) {
        throw new Error(`Flow ${flow.label} step is missing a toMint value`);
      }

      const amount = cloneFlowAmount(step.amount);
      const randomResolutions = [];
      let randomization = null;
      if (step.randomization) {
        randomization = { ...step.randomization };
        if (Array.isArray(step.randomization.poolTags)) {
          randomization.poolTags = step.randomization.poolTags
            .map((tag) => (typeof tag === "string" ? tag.trim().toLowerCase() : ""))
            .filter((tag, idx, arr) => tag.length > 0 && arr.indexOf(tag) === idx);
        }
        if (Array.isArray(step.randomization.poolMints)) {
          const seen = new Set();
          randomization.poolMints = step.randomization.poolMints
            .map((mint) => (typeof mint === "string" ? mint : null))
            .filter((mint) => {
              if (!mint || seen.has(mint)) return false;
              seen.add(mint);
              return true;
            });
        }
        if (Array.isArray(step.randomization.excludeMints)) {
          const seen = new Set();
          randomization.excludeMints = step.randomization.excludeMints
            .map((mint) => (typeof mint === "string" ? normaliseSolMint(mint) : null))
            .filter((mint) => {
              if (!mint || seen.has(mint)) return false;
              seen.add(mint);
              return true;
            });
        }
        const groupLabel =
          typeof randomization.sessionGroup === "string"
            ? randomization.sessionGroup.trim()
            : "";
        if (groupLabel.length > 0) {
          let assigned = sessionGroups.get(groupLabel);
          if (!assigned) {
            assigned = `${flow.key}-${groupLabel}-${cycleIndex}`;
            sessionGroups.set(groupLabel, assigned);
          }
          randomization.sessionKey = assigned;
        } else if (
          typeof randomization.sessionKey === "string" &&
          randomization.sessionKey.length > 0
        ) {
          randomization.sessionKey = `${randomization.sessionKey}-${cycleIndex}-${stepIndex}`;
        } else {
          randomization.sessionKey = `${flow.key}-${cycleIndex}-${stepIndex}`;
        }
      }
      const entry = {
        ...step,
        fromMint: resolvedFromMint,
        toMint: resolvedToMint,
        amount,
        randomization,
      };
      if (randomResolutions.length > 0) {
        entry.randomResolutions = randomResolutions;
      }
      schedule.push(entry);
      currentMint = resolvedToMint;
    }
  };

  if (flow.requireTerminalSolHop && currentMint !== SOL_MINT) {
    schedule.push({
      fromMint: currentMint,
      toMint: SOL_MINT,
      amount: "all",
      description: "Return to SOL to finish the session",
      autoAppended: true,
    });
    currentMint = SOL_MINT;
  }

  const swapExecutionCount = schedule.length;
  const plannedSwaps = schedule.length;
  const requestedDurationMsRaw =
    options.totalDurationMs ??
    options.durationMs ??
    flow.defaultDurationMs ??
    0;
  const requestedDurationMs = Math.max(
    0,
    Math.floor(Number(requestedDurationMsRaw) || 0)
  );

  const waitOptions = {
    min: options.waitBounds?.min ?? flow.waitBoundsMs?.min ?? 0,
    max: options.waitBounds?.max ?? flow.waitBoundsMs?.max ?? null,
  };
  const perHopDelays = allocateHopDelays(
    requestedDurationMs,
    plannedSwaps,
    waitOptions,
    flowRng
  );
  const actualWaitTotal = perHopDelays.reduce((acc, value) => acc + value, 0);

  console.log(
    paint(`\n== Prewritten flow: ${flow.label} (${flow.key}) ==`, "label")
  );
  const swapRangeLabel = `${rangeMinBase}-${rangeMaxBase}`;
  const coverageLabel = runtimeProfile?.label
    || (runtimeProfile?.targetDurationMs
      ? `≈${formatDurationMs(runtimeProfile.targetDurationMs)}`
      : flow.defaultDurationMs
        ? `≈${formatDurationMs(flow.defaultDurationMs)}`
        : null);
  const cycleSummaryParts = [];
  if (fullCycles > 0) {
    cycleSummaryParts.push(`${fullCycles} full`);
  }
  if (partialCycleHops > 0) {
    cycleSummaryParts.push(`1 partial (${partialCycleHops} hop${partialCycleHops === 1 ? "" : "s"})`);
  }
  if (cycleSummaryParts.length === 0) {
    cycleSummaryParts.push("0 cycle");
  }
  const cycleSummary = cycleSummaryParts.join(" + ");

  const extraStopovers = Math.max(0, swapExecutionCount - executedSwapTarget);
  const executionLabel =
    extraStopovers > 0
      ? `${swapExecutionCount} hop(s) (includes ${extraStopovers} terminal hop${extraStopovers === 1 ? "" : "s"})`
      : `${swapExecutionCount} hop(s)`;

  console.log(
    paint(
      `Swap target sampled at ${sampledTarget} hop(s) (min ${minimumSwapCount}, range ${swapRangeLabel}); executing ${executionLabel} across ${cycleSummary}.`,
      "muted"
    )
  );
  if (coverageLabel) {
    console.log(
      paint(`Coverage goal: ${coverageLabel}.`, "muted")
    );
  }
  if (flow.description) {
    console.log(paint(flow.description, "muted"));
  }

  if (requestedDurationMs > 0) {
    const waitLabel =
      actualWaitTotal === requestedDurationMs
        ? formatDurationMs(actualWaitTotal)
        : `${formatDurationMs(actualWaitTotal)} (target ${formatDurationMs(requestedDurationMs)})`;
    console.log(paint(`Planned wait budget: ${waitLabel}.`, "muted"));
  }

  const walletNames = walletList.map((wallet) => wallet.name).join(", ");
  if (walletNames) {
    console.log(paint(`Wallet scope: ${walletNames}.`, "muted"));
  }

  // First-hop SOL minimum validation (if minSwapSol is set)
  if (typeof flow.minSwapSol === 'number' && flow.minSwapSol > 0 && schedule.length > 0) {
    console.log(paint(`\n💰 First-hop SOL minimum enabled: ${flow.minSwapSol.toFixed(3)} SOL minimum`, "info"));

    const firstStep = schedule[0];

    // Only validate if first hop is from SOL
    if (SOL_LIKE_MINTS.has(firstStep.fromMint)) {
      const validWallets = [];
      const skippedWallets = [];

      // Create RPC connection for balance checks
      const balanceConnection = createRpcConnection("confirmed");

      for (const wallet of walletList) {
        try {
          // Get wallet SOL balance
          const solBalance = await getSolBalance(balanceConnection, wallet.kp.publicKey);
          const solBalanceSol = Number(solBalance) / 1e9;
          const solBalanceFormatted = formatBaseUnits(BigInt(solBalance), 9);

          // Check if wallet meets the minimum SOL requirement
          // Reserve some for gas/fees
          const spendable = Math.max(0, solBalanceSol - 0.01); // Reserve 0.01 SOL for gas

          if (spendable < flow.minSwapSol) {
            console.log(
              paint(
                `  ⏭️  Skipping ${wallet.name}: spendable balance ${spendable.toFixed(4)} SOL is below ${flow.minSwapSol.toFixed(3)} SOL minimum`,
                "warn"
              )
            );
            skippedWallets.push({ wallet, reason: `Insufficient balance (${solBalanceFormatted} SOL)` });
            continue;
          }

          // Wallet passes validation
          console.log(
            paint(
              `  ✓ ${wallet.name}: ${solBalanceFormatted} SOL meets ${flow.minSwapSol.toFixed(3)} SOL minimum`,
              "muted"
            )
          );

          validWallets.push(wallet);

        } catch (err) {
          console.warn(
            paint(`  ⚠️  ${wallet.name}: SOL balance check error (${err.message}), including anyway`, "warn")
          );
          validWallets.push(wallet);
        }
      }

      // Update wallet list to only include valid wallets
      walletList = validWallets;

      if (validWallets.length === 0) {
        console.log(paint("\n❌ No wallets meet the minimum USD requirement for first hop. Flow aborted.", "error"));
        return {
          key: flow.key,
          plannedSwaps: 0,
          cycles: 0,
          waitTotalMs: 0,
          targetWaitTotalMs: 0,
          finalMint: flow.startMint || SOL_MINT,
          skippedWallets: skippedWallets.length,
        };
      }

      if (skippedWallets.length > 0) {
        console.log(paint(`\n📊 Validation summary: ${validWallets.length} wallet(s) will execute, ${skippedWallets.length} skipped`, "info"));
      }
    } else {
      console.log(paint("  ℹ️  First hop is not from SOL, skipping USD validation", "muted"));
    }
  }

  // Initialize swap counter for forced SOL returns
  let swapCounter = 0;
  let nextSolReturnAt = null;
  if (flow.forceSolReturnEvery && typeof flow.forceSolReturnEvery === 'object') {
    const min = Number(flow.forceSolReturnEvery.min) || 4;
    const max = Number(flow.forceSolReturnEvery.max) || min;
    nextSolReturnAt = Math.floor(min + flowRng() * (max - min + 1));
    console.log(paint(`Forced SOL return enabled: every ${nextSolReturnAt} swaps.`, "muted"));
  }

  for (let index = 0; index < schedule.length; index += 1) {
    const step = schedule[index];
    const normalizedAmount = normalizeFlowAmount(step.amount, { rng: flowRng });
    const amountLabel = describeFlowAmount(normalizedAmount);
    const hopLabel = `Hop ${index + 1}/${plannedSwaps}`;

    // Check if this step has randomization that needs per-wallet resolution
    const hasRandomization = step.randomization && typeof step.randomization === 'object';
    const needsPerWalletRandomization = hasRandomization &&
      (step.randomization.mode === 'sol-to-random' ||
       step.randomization.mode === 'random-to-random' ||
       step.randomization.mode === 'session-to-random' ||
       step.randomization.mode === 'session-to-sol');

    if (needsPerWalletRandomization) {
      // Per-wallet randomization: each wallet picks its own random token
      const mode = step.randomization.mode;
      let hopDescription;
      if (mode === 'sol-to-random' || mode === 'random-to-random' || mode === 'session-to-random') {
        hopDescription = `${describeMintLabel(step.fromMint)} → <random tokens>`;
      } else if (mode === 'session-to-sol') {
        hopDescription = `<random tokens> → ${describeMintLabel(step.toMint)}`;
      } else {
        hopDescription = `${describeMintLabel(step.fromMint)} → ${describeMintLabel(step.toMint)}`;
      }
      const descriptionParts = [
        hopLabel,
        hopDescription,
        amountLabel,
      ];
      if (step.description) descriptionParts.push(`— ${step.description}`);
      console.log(paint(descriptionParts.join(" "), "info"));

      // Execute all wallets concurrently for true parallel execution
      await Promise.all(walletList.map(async (wallet, walletIdx) => {
        // Get or create per-wallet session map for independent token selection
        if (!perWalletSessions.has(wallet.name)) {
          perWalletSessions.set(wallet.name, new Map());
        }
        const walletSessionState = perWalletSessions.get(wallet.name);

        // Create a wallet-specific RNG based on wallet index and hop index
        const walletSeed = (flowRng() * 1000000 + walletIdx * 1000 + index) >>> 0;
        const walletRng = (() => {
          let seed = walletSeed;
          return () => {
            seed = (seed * 1664525 + 1013904223) >>> 0;
            return seed / 0x100000000;
          };
        })();

        let resolvedStep;
        try {
          resolvedStep = resolveRandomizedStep(step, walletRng, {
            sessionState: walletSessionState,
            selectMint: (meta) => selectMintForFlow(meta, walletRng),
          });
        } catch (err) {
          console.error(
            paint(`  ${wallet.name}: flow hop skipped: ${err?.message || err}`, "warn")
          );
          return;
        }

        const resolvedFromMint = resolvedStep?.inMint ?? step.fromMint;
        const resolvedToMint = resolvedStep?.outMint ?? step.toMint;

        try {
          await doSwapAcross(resolvedFromMint, resolvedToMint, normalizedAmount, {
            wallets: [wallet],
            quietSkips: false,
            suppressMetadata: false,
            walletDelayMs: FLOW_WALLET_DELAY_MS,
          });
        } catch (err) {
          console.error(
            paint(`  ${wallet.name}: flow hop failed: ${err?.message || err}`, "error")
          );
        }
      }));
      currentMint = step.toMint; // Keep as placeholder since each wallet has different token
    } else {
      // Standard non-randomized or session-based randomization: all wallets use same token
      let resolvedStep;
      try {
        resolvedStep = resolveRandomizedStep(step, flowRng, {
          sessionState: flowRandomSessions,
          selectMint: selectMintForFlow,
        });
      } catch (err) {
        console.error(
          paint(`  Flow hop skipped: ${err?.message || err}`, "warn")
        );
        continue;
      }
      const resolvedFromMint = resolvedStep?.inMint ?? step.fromMint;
      const resolvedToMint = resolvedStep?.outMint ?? step.toMint;
      const descriptionParts = [
        hopLabel,
        `${describeMintLabel(resolvedFromMint)} → ${describeMintLabel(resolvedToMint)}`,
        amountLabel,
      ];
      if (step.description) descriptionParts.push(`— ${step.description}`);
      console.log(paint(descriptionParts.join(" "), "info"));

      try {
        await doSwapAcross(resolvedFromMint, resolvedToMint, normalizedAmount, {
          wallets: walletList,
          quietSkips: true,
          suppressMetadata: true,
          walletDelayMs:
            step.walletDelayMs ??
            flow.walletDelayMs ??
            options.walletDelayMs ??
            FLOW_WALLET_DELAY_MS,
        });
        currentMint = resolvedToMint;
      } catch (err) {
        console.error(
          paint(`  Flow hop failed: ${err?.message || err}`, "error")
        );
      }
    }

    // Increment swap counter and check for forced SOL return
    if (nextSolReturnAt !== null) {
      swapCounter++;

      // Check if we should force return to SOL
      if (swapCounter >= nextSolReturnAt && currentMint !== SOL_MINT) {
        console.log(
          paint(
            `  → Forced SOL return triggered (swap ${swapCounter}/${nextSolReturnAt})`,
            "info"
          )
        );

        try {
          await doSwapAcross(currentMint, SOL_MINT, "all", {
            wallets: walletList,
            quietSkips: true,
            suppressMetadata: true,
            walletDelayMs: FLOW_WALLET_DELAY_MS,
          });
          currentMint = SOL_MINT;
          console.log(paint(`  ✓ Forced SOL return complete`, "success"));
        } catch (err) {
          console.error(
            paint(`  ✗ Forced SOL return failed: ${err?.message || err}`, "error")
          );
        }

        // Reset counter and pick new threshold
        swapCounter = 0;
        const min = Number(flow.forceSolReturnEvery.min) || 4;
        const max = Number(flow.forceSolReturnEvery.max) || min;
        nextSolReturnAt = Math.floor(min + flowRng() * (max - min + 1));
        console.log(
          paint(
            `  → Next forced SOL return in ${nextSolReturnAt} swaps`,
            "muted"
          )
        );
      }
    }

    // Use step-specific delay if defined, otherwise use allocated delay
    let waitMs = perHopDelays[index] ?? 0;

    // Check if this step has a custom delayAfterMs setting
    if (step.delayAfterMs && typeof step.delayAfterMs === 'object') {
      const minDelay = Number(step.delayAfterMs.min) || 0;
      const maxDelay = Number(step.delayAfterMs.max) || minDelay;
      waitMs = Math.floor(minDelay + flowRng() * (maxDelay - minDelay));
    }

    if (waitMs > 0) {
      console.log(
        paint(
          `  waiting ${formatDurationMs(waitMs)} before next hop`,
          "muted"
        )
      );
      await delay(waitMs);
    }
  }

  console.log(
    paint(
      `Prewritten flow ${flow.label} complete: ${plannedSwaps} hop(s) executed.`,
      "success"
    )
  );

  return {
    key: flow.key,
    plannedSwaps,
    cycles: executedCycles,
    swapExecutionCount,
    sampledSwapTarget: sampledTarget,
    desiredSwapTarget,
    executedSwapTarget,
    partialCycleHops,
    waitTotalMs: actualWaitTotal,
    targetWaitTotalMs: requestedDurationMs,
    finalMint: currentMint,
  };
}

async function runInteractiveTargetLoop(startMintRaw = SOL_MINT) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(
      paint("No wallets found. Generate wallets before starting target loop.", "warn")
    );
    return;
  }

  let currentMint = SOL_MINT;
  if (typeof startMintRaw === "string" && startMintRaw.trim().length > 0) {
    const trimmed = startMintRaw.trim();
    if (trimmed.toLowerCase() === "sol") {
      currentMint = SOL_MINT;
    } else {
      try {
        currentMint = new PublicKey(trimmed).toBase58();
      } catch (err) {
        console.log(
          paint(
            `Starting mint ${trimmed} is invalid; defaulting to SOL.`,
            "warn"
          )
        );
        currentMint = SOL_MINT;
      }
    }
  }

  const flattenKeysLabel =
    formatHotkeyKeys("target-loop", "flatten-to-sol") || "'sol'";
  const exitKeysLabel =
    formatHotkeyKeys("target-loop", "exit") || "'exit'";
  const helpKeysLabel =
    formatHotkeyKeys("target-loop", "show-help") || "'help'";

  console.log(
    paint(
      `\nTarget loop mode — paste mint addresses to rotate holdings. Type ${flattenKeysLabel} to flatten back to SOL, or ${exitKeysLabel} to leave.`,
      "label"
    )
  );
  console.log(
    paint(
      `Current position: ${symbolForMint(currentMint)}.`,
      "info"
    )
  );

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const prompt = () =>
    new Promise((resolve) =>
      rl.question(`target-loop [${symbolForMint(currentMint)}]> `, resolve)
    );

  const printHelp = () => {
    const summary = buildHotkeyInlineSummary("target-loop");
    if (summary) {
      console.log(paint(`Commands: ${summary}.`, "info"));
    }
  };

  const runSegment = async (fromMint, toMint, description, options = {}) => {
    const { suppressMetadata = true } = options;
    console.log(paint(description, "info"));
    try {
      await doSwapAcross(fromMint, toMint, "all", {
        suppressMetadata,
      });
      currentMint = toMint;
      return true;
    } catch (err) {
      console.error(
        paint("  swap segment failed:", "error"),
        err?.message || err
      );
      return false;
    }
  };

  printHelp();

  try {
    while (true) {
      const rawInput = (await prompt())?.trim();
      if (!rawInput) {
        continue;
      }
      if (isHotkeyMatch("target-loop", "exit", rawInput)) {
        break;
      }
      if (isHotkeyMatch("target-loop", "show-help", rawInput)) {
        printHelp();
        continue;
      }
      if (isHotkeyMatch("target-loop", "show-catalog", rawInput)) {
        listTokenCatalog({ verbose: false });
        continue;
      }
      if (isHotkeyMatch("target-loop", "flatten-to-sol", rawInput)) {
        if (SOL_LIKE_MINTS.has(currentMint)) {
          console.log(
            paint("Already holding SOL. Awaiting next mint address.", "muted")
          );
          continue;
        }
        const fromSymbol = symbolForMint(currentMint);
        const description = `\nSwapping ${fromSymbol} -> SOL (all available balance)`;
        const flattened = await runSegment(currentMint, SOL_MINT, description);
        if (flattened) {
          console.log(
            paint(
              "Flattened back to SOL. Paste a mint address when you're ready to rotate.",
              "success"
            )
          );
        }
        continue;
      }

      let targetMint;
      try {
        targetMint = new PublicKey(rawInput).toBase58();
      } catch (_) {
        console.log(
          paint(
            `Input not recognised. Paste a valid mint address or type ${helpKeysLabel}.`,
            "warn"
          )
        );
        continue;
      }

      if (targetMint === currentMint) {
        console.log(
          paint(
            `Already positioned in ${symbolForMint(targetMint)}. Waiting for next mint.`,
            "muted"
          )
        );
        continue;
      }

      const targetSymbol = symbolForMint(targetMint);

      if (!SOL_LIKE_MINTS.has(currentMint)) {
        const fromSymbol = symbolForMint(currentMint);
        const description = `\nPreparing rotation: ${fromSymbol} -> SOL (all holdings)`;
        const flattened = await runSegment(currentMint, SOL_MINT, description);
        if (!flattened) {
          console.log(
            paint(
              "Rotation to SOL failed; still holding prior token. Resolve the issue or try again.",
              "warn"
            )
          );
          continue;
        }
      }

      const description = `Swapping SOL -> ${targetSymbol} using available spendable balance`;
      const swapped = await runSegment(
        SOL_MINT,
        targetMint,
        description
      );
      if (swapped) {
        console.log(
          paint(
            `Holding ${targetSymbol}. Paste another mint, type ${flattenKeysLabel} to flatten, or ${exitKeysLabel} to leave.`,
            "success"
          )
        );
      }
    }
  } finally {
    rl.close();
    console.log(
      paint(
        "Target loop complete. Wallets remain in the last selected position.",
        "muted"
      )
    );
  }
}

async function swapSolToUsdcThenPopcat() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }

  const popcatMint = new PublicKey(POPCAT_MINT);

  console.log(paint("Ensuring POPCAT ATAs exist before chaining swaps", "muted"));
  for (const w of wallets) {
    const connection = createRpcConnection("confirmed");
    try {
      await ensureAta(connection, w.kp.publicKey, popcatMint, w.kp, TOKEN_PROGRAM_ID);
    } catch (err) {
      console.warn(
        paint(
          `  warning: skipping ATA prep for ${w.name} — ${err.message || err}`,
          "warn"
        )
      );
    }
    await delay(DELAY_BETWEEN_CALLS_MS);
  }

  const steps = [
    { from: SOL_MINT, to: DEFAULT_USDC_MINT, description: 'SOL -> USDC' },
    { from: DEFAULT_USDC_MINT, to: POPCAT_MINT, description: 'USDC -> POPCAT', amount: 'all' },
  ];
  await runSwapSequence(steps, 'SOL -> USDC -> POPCAT chain');
}

// Iterate every wallet and close zero-balance SPL/Token-2022 accounts.
// Shared helper so the CLI command and automated flows behave consistently.
async function closeEmptyTokenAccounts() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }

  const MAX_CLOSE_ACCOUNTS_PER_TX = 12;

  // Process wallets sequentially with delays to avoid RPC rate limiting
  for (const w of wallets) {
    const lookupConnection = createRpcConnection("confirmed");
    const parsed = await getAllParsedTokenAccounts(lookupConnection, w.kp.publicKey);

    const closable = [];
    for (const { pubkey, account } of parsed) {
      const info = account.data.parsed.info;
      const amount = BigInt(info.tokenAmount.amount);
      if (amount !== 0n) continue;

      let programId;
      try {
        programId = new PublicKey(account.owner);
      } catch (err) {
        console.warn(
          paint(
            `  skipping ${pubkey.toBase58()} — unable to parse owner program: ${err.message || err}`,
            "warn"
          )
        );
        continue;
      }

      if (
        !programId.equals(TOKEN_PROGRAM_ID) &&
        !programId.equals(TOKEN_2022_PROGRAM_ID)
      ) {
        console.warn(
          paint(
            `  skipping ${pubkey.toBase58()} — unsupported token program ${programId.toBase58()}`,
            "warn"
          )
        );
        continue;
      }

      if (Array.isArray(info.extensions)) {
        const transferFeeExt = info.extensions.find(
          (ext) => ext?.extension === "transferFeeAmount"
        );
        const withheldRaw = BigInt(transferFeeExt?.state?.withheldAmount || 0);
        if (withheldRaw > 0n) {
          const decimals = info.tokenAmount.decimals || 0;
          const formatted = formatBaseUnits(withheldRaw, decimals);
          console.warn(
            paint(
              `  skipping ${pubkey.toBase58()} — ${formatted} withheld fee remains for mint ${info.mint}; withdraw or harvest fees before closing`,
              "warn"
            )
          );
          continue;
        }
      }

      const symbol = symbolForMint(info.mint);
      closable.push({
        accountPubkey: pubkey,
        programId,
        mint: info.mint,
        symbol,
      });
    }

    if (closable.length === 0) {
      console.log(paint(`No empty token accounts for ${w.name}.`, "muted"));
      continue; // Skip to next wallet
    }

    const closeConnection = createRpcConnection("confirmed");
    let closedCount = 0;

    const chunkLabels = (chunk) =>
      chunk
        .map((entry) => {
          const base = entry.symbol || symbolForMint(entry.mint) || entry.mint.slice(0, 4);
          const suffix = entry.accountPubkey.toBase58().slice(-6);
          return `${base}:${suffix}`;
        })
        .join(", ");

    const chunkTasks = [];
    for (let i = 0; i < closable.length; i += MAX_CLOSE_ACCOUNTS_PER_TX) {
      const chunk = closable.slice(i, i + MAX_CLOSE_ACCOUNTS_PER_TX);
      chunkTasks.push(async () => {
        const tx = new Transaction();
        for (const entry of chunk) {
          tx.add(
            createCloseAccountInstruction(
              entry.accountPubkey,
              w.kp.publicKey,
              w.kp.publicKey,
              [],
              entry.programId
            )
          );
        }
        tx.feePayer = w.kp.publicKey;

        try {
          const { blockhash, lastValidBlockHeight } = await closeConnection.getLatestBlockhash("confirmed");
          tx.recentBlockhash = blockhash;
          tx.sign(w.kp);
          const raw = tx.serialize();
          const signature = await closeConnection.sendRawTransaction(raw);
          await closeConnection.confirmTransaction(
            { signature, blockhash, lastValidBlockHeight },
            "confirmed"
          );
          closedCount += chunk.length;
          console.log(
            paint(
              `Closed ${chunk.length} account(s) for ${w.name}: ${chunkLabels(chunk)} — tx ${signature}`,
              "success"
            )
          );
        } catch (bundleErr) {
          console.warn(
            paint(
              `  bundle close failed (${bundleErr.message || bundleErr}); retrying individually.`,
              "warn"
            )
          );
          for (const entry of chunk) {
            try {
              const signature = await closeAccount(
                closeConnection,
                w.kp,
                entry.accountPubkey,
                w.kp.publicKey,
                w.kp,
                [],
                undefined,
                entry.programId
              );
              closedCount += 1;
              console.log(
                paint(
                  `  Closed token account ${entry.accountPubkey.toBase58()} (${entry.symbol || symbolForMint(entry.mint)}) — tx ${signature}`,
                  "success"
                )
              );
            } catch (err) {
              logDetailedError(`  close ${entry.accountPubkey.toBase58()} failed`, err);
            }
          }
        }
      });
    }

    const CHUNK_CONCURRENCY = 3;
    for (let idx = 0; idx < chunkTasks.length; idx += CHUNK_CONCURRENCY) {
      const batch = chunkTasks.slice(idx, idx + CHUNK_CONCURRENCY);
      await Promise.all(batch.map((task) => task()));
    }

    if (closedCount > 0) {
      const balanceConnection = createRpcConnection("confirmed");
      const solBalance = await getSolBalance(balanceConnection, w.kp.publicKey);
      console.log(
        paint(
          `Post-cleanup SOL for ${w.name}: ${formatBaseUnits(BigInt(solBalance), 9)} SOL`,
          "muted"
        )
      );
    }

    // Add delay between wallets to avoid RPC rate limiting
    await delay(100);
  }
}

function toSafeNumber(value) {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("Value exceeds JavaScript safe integer range");
  }
  if (value < BigInt(Number.MIN_SAFE_INTEGER)) {
    throw new Error("Value is below JavaScript safe integer range");
  }
  return Number(value);
}

function nextIndexForPrefix(prefix) {
  if (!fs.existsSync(KEYPAIR_DIR)) return 1;
  const files = fs
    .readdirSync(KEYPAIR_DIR)
    .filter((f) => f.startsWith(`${prefix}_`) && f.endsWith(".json"));
  let max = 0;
  for (const f of files) {
    const body = f.slice(prefix.length + 1, -5);
    const idx = parseInt(body, 10);
    if (!Number.isNaN(idx) && idx > max) max = idx;
  }
  return max + 1;
}

function nextWalletFilename(prefix, startingIndex) {
  let idx = startingIndex;
  while (true) {
    const candidate = `${prefix}_${idx}.json`;
    const fp = path.join(KEYPAIR_DIR, candidate);
    if (!fs.existsSync(fp)) {
      return { filename: candidate, nextIndex: idx + 1 };
    }
    idx += 1;
  }
}

function persistKeypairToDisk(kp, options = {}) {
  ensureKeypairDir();
  const prefix = options.prefix && options.prefix.trim().length > 0
    ? options.prefix.trim()
    : "wallet";
  const metadata = options.metadata && typeof options.metadata === "object"
    ? options.metadata
    : {};
  let targetFilename = options.filename || null;
  let nextIndex =
    typeof options.nextIndex === "number" && options.nextIndex >= 0
      ? options.nextIndex
      : nextIndexForPrefix(prefix);
  if (!targetFilename) {
    const allocation = nextWalletFilename(prefix, nextIndex);
    targetFilename = allocation.filename;
    nextIndex = allocation.nextIndex;
  }
  const secretKeyArray = Array.from(kp.secretKey);
  const secretKeyBase58 = bs58.encode(kp.secretKey);
  const payload = {
    publicKey: kp.publicKey.toBase58(),
    secretKey: secretKeyArray,
    secretKeyBase58,
    createdAt: new Date().toISOString(),
    ...metadata,
  };
  fs.writeFileSync(
    path.join(KEYPAIR_DIR, targetFilename),
    JSON.stringify(payload, null, 2)
  );
  return {
    filename: targetFilename,
    nextIndex,
    publicKey: payload.publicKey,
    secretKeyBase58,
  };
}

function findWalletFilenameByPublicKey(publicKey) {
  if (!publicKey) return null;
  if (!fs.existsSync(KEYPAIR_DIR)) return null;
  const entries = fs.readdirSync(KEYPAIR_DIR);
  for (const entry of entries) {
    if (!entry.endsWith(".json")) continue;
    try {
      const raw = fs.readFileSync(path.join(KEYPAIR_DIR, entry), "utf8");
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed.publicKey === "string" && parsed.publicKey === publicKey) {
        return entry;
      }
    } catch (_) {}
  }
  return null;
}

// generate new wallets and save
function generateWallets(n, prefix = "wallet") {
  console.log(paint(`Generating ${n} wallet${n === 1 ? '' : 's'} with prefix "${prefix}"...`, "info"));
  ensureKeypairDir();
  const created = [];
  let nextIndex = nextIndexForPrefix(prefix);
  for (let i = 0; i < n; i++) {
    const kp = Keypair.generate();
    const persisted = persistKeypairToDisk(kp, {
      prefix,
      nextIndex,
      metadata: { generated: true },
    });
    nextIndex = persisted.nextIndex;
    const info = {
      name: persisted.filename,
      publicKey: persisted.publicKey,
    };
    if (PRINT_SECRET_KEYS) {
      info.secretKeyBase58 = persisted.secretKeyBase58;
    }
    created.push(info);
  }
  console.log(paint(`Successfully generated ${created.length} wallet${created.length === 1 ? '' : 's'} in ${KEYPAIR_DIR}`, "success"));
  return created;
}

function normalizeMnemonicInput(input) {
  return input
    .trim()
    .split(/\s+/)
    .map((word) => word.toLowerCase())
    .join(" ");
}

function keypairFromMnemonic(mnemonic, options = {}) {
  const cleaned = normalizeMnemonicInput(mnemonic);
  if (!bip39.validateMnemonic(cleaned)) {
    throw new Error("Provided mnemonic is not valid BIP39.");
  }
  const passphrase = typeof options.passphrase === "string" ? options.passphrase : "";
  const path = (options.path && options.path.trim()) || DEFAULT_DERIVATION_PATH;
  if (!/^m\//i.test(path)) {
    throw new Error(`Invalid derivation path "${path}". Paths must start with \"m/\".`);
  }
  const seed = bip39.mnemonicToSeedSync(cleaned, passphrase);
  const derived = derivePath(path, seed.toString("hex"));
  if (!derived || !derived.key) {
    throw new Error(`Unable to derive key for path ${path}.`);
  }
  const seedBytes = derived.key.slice(0, 32);
  return Keypair.fromSeed(seedBytes);
}

function keypairFromSecretArray(arrayLike) {
  if (!Array.isArray(arrayLike)) {
    throw new Error("Secret key array must be a list of numbers.");
  }
  const numbers = arrayLike.map((value) => {
    const num = Number(value);
    if (!Number.isInteger(num) || num < 0 || num > 255) {
      throw new Error(`Secret key array contains invalid byte value: ${value}`);
    }
    return num;
  });
  const secret = Uint8Array.from(numbers);
  if (secret.length === 32) {
    return Keypair.fromSeed(secret);
  }
  if (secret.length === 64) {
    return Keypair.fromSecretKey(secret);
  }
  throw new Error(`Secret key array must contain 32 or 64 numbers (received ${secret.length}).`);
}

function keypairFromSecretInput(secretInput, options = {}) {
  const trimmed = typeof secretInput === "string" ? secretInput.trim() : "";
  if (!trimmed) {
    throw new Error("Secret input is empty.");
  }

  const tryJson = () => {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return keypairFromSecretArray(parsed);
    }
    if (parsed && typeof parsed === "object") {
      if (Array.isArray(parsed.secretKey)) {
        return keypairFromSecretArray(parsed.secretKey);
      }
      if (typeof parsed.secretKeyBase58 === "string") {
        const decoded = bs58.decode(parsed.secretKeyBase58);
        if (decoded.length !== 64 && decoded.length !== 32) {
          throw new Error("secretKeyBase58 must decode to 32 or 64 bytes.");
        }
        return decoded.length === 32
          ? Keypair.fromSeed(decoded)
          : Keypair.fromSecretKey(decoded);
      }
      if (Array.isArray(parsed._keypair?.secretKey)) {
        return keypairFromSecretArray(parsed._keypair.secretKey);
      }
    }
    throw new Error("JSON input must contain a secretKey array or secretKeyBase58 string.");
  };

  const tryNumericList = () => {
    const parts = trimmed.split(/[, \t\r\n]+/).filter(Boolean);
    if (parts.length === 0) throw new Error("Numeric secret array is empty.");
    const values = parts.map((part) => {
      const num = Number(part);
      if (!Number.isInteger(num) || num < 0 || num > 255) {
        throw new Error(`Invalid byte value in numeric list: ${part}`);
      }
      return num;
    });
    return keypairFromSecretArray(values);
  };

  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    return tryJson();
  }

  const wordCount = trimmed.split(/\s+/).length;
  if (wordCount >= 12 && bip39.validateMnemonic(normalizeMnemonicInput(trimmed))) {
    return keypairFromMnemonic(trimmed, {
      path: options.path,
      passphrase: options.passphrase,
    });
  }

  if (/^[0-9,\s]+$/.test(trimmed)) {
    return tryNumericList();
  }

  try {
    const decoded = bs58.decode(trimmed);
    if (decoded.length === 64) {
      return Keypair.fromSecretKey(decoded);
    }
    if (decoded.length === 32) {
      return Keypair.fromSeed(decoded);
    }
    throw new Error(`Base58 secret must decode to 32 or 64 bytes (received ${decoded.length}).`);
  } catch (err) {
    throw new Error(`Unable to parse secret input. Expected base58 string, JSON array/object, or mnemonic phrase. (${err.message || err})`);
  }
}

function importWalletFromSecret(secretInput, options = {}) {
  const kp = keypairFromSecretInput(secretInput, options);
  const publicKey = kp.publicKey.toBase58();
  const existingFilename = findWalletFilenameByPublicKey(publicKey);
  if (existingFilename && options.force !== true) {
    throw new Error(
      `Wallet ${publicKey} already exists in ${existingFilename}. Use --force to overwrite or delete the existing file first.`
    );
  }
  const prefix =
    options.prefix && options.prefix.trim().length > 0
      ? options.prefix.trim()
      : existingFilename
      ? existingFilename.replace(/\.json$/, "")
      : "imported";

  const metadata = {
    importedAt: new Date().toISOString(),
    source: options.source || "manual",
  };

  const persistenceOptions = {
    prefix,
    metadata,
  };
  if (existingFilename && options.force === true) {
    persistenceOptions.filename = existingFilename;
  }
  const persisted = persistKeypairToDisk(kp, persistenceOptions);
  return {
    filename: persisted.filename,
    publicKey: persisted.publicKey,
    secretKeyBase58: persisted.secretKeyBase58,
    overwritten: Boolean(existingFilename && options.force === true),
  };
}

// get SOL balance
async function getSolBalance(connection, pubkey) {
  const lam = await connection.getBalance(pubkey);
  return lam;
}

// get token balance (returns uiAmount or 0)
async function getTokenBalance(connection, walletPubkey, mintPubkey, programId) {
  try {
    const ata = await getAssociatedTokenAddress(
      mintPubkey,
      walletPubkey,
      false,
      programId || TOKEN_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    const resp = await connection.getTokenAccountBalance(ata);
    return resp.value.uiAmount;
  } catch (e) {
    return 0;
  }
}

// check & create ATA if missing
async function ensureAta(connection, ownerPubkey, mint, payerKeypair, programId) {
  const tokenProgram = programId || TOKEN_PROGRAM_ID;
  const ata = await getAssociatedTokenAddress(
    mint,
    ownerPubkey,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const info = await connection.getAccountInfo(ata);
  if (info === null) {
    try {
      const mintInfo = await connection.getAccountInfo(mint);
      if (!mintInfo) {
        throw new Error("mint account not found on current RPC");
      }
      console.log(
        paint(
          `  mint ${mint.toBase58()} owner ${mintInfo.owner.toBase58()} dataLen ${mintInfo.data.length}`,
          "muted"
        )
      );
    } catch (mintErr) {
      throw new Error(
        `mint lookup failed (${mint.toBase58()}): ${mintErr.message || mintErr}`
      );
    }
    if (cachedAtaRentLamports === null) {
      const rent = await metadataConnection.getMinimumBalanceForRentExemption(165);
      cachedAtaRentLamports = BigInt(rent);
    }
    const payerBalance = BigInt(await connection.getBalance(payerKeypair.publicKey));
    if (payerBalance < cachedAtaRentLamports + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS) {
      throw new Error(
        `insufficient SOL to create ATA (need ${(Number(cachedAtaRentLamports + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS)/1e9).toFixed(6)} SOL incl reserve, have ${(Number(payerBalance)/1e9).toFixed(6)} SOL)`
      );
    }
    // create ATA
    const ix = createAssociatedTokenAccountInstruction(
      payerKeypair.publicKey,
      ata,
      ownerPubkey,
      mint,
      tokenProgram,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    const tx = new Transaction().add(ix);
    tx.feePayer = payerKeypair.publicKey;
    const { blockhash } = await connection.getLatestBlockhash();
    tx.recentBlockhash = blockhash;
    tx.sign(payerKeypair);
    const raw = tx.serialize();
    const sig = await connection.sendRawTransaction(raw);
    await connection.confirmTransaction(sig, "confirmed");
    console.log(`Created ATA ${ata.toBase58()} for ${ownerPubkey.toBase58()}`);
  }
  return { ata, created: info === null };
}

async function ultraApiRequest({ path, method = "POST", body, query, headers } = {}) {
  if (!path) throw new Error("Ultra API path is required");
  let target = path;
  if (!target.startsWith("http")) {
    while (target.startsWith("/")) {
      target = target.slice(1);
    }
    target = `${JUPITER_ULTRA_API_BASE}/${target}`;
  }
  const url = new URL(target);
  if (query && typeof query === "object") {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) continue;
      url.searchParams.set(key, String(value));
    }
  }
  const init = {
    method,
    headers: buildJsonApiHeaders(headers, { includeUltraKey: true }),
  };
  if (method && method.toUpperCase() !== "GET") {
    init.body = JSON.stringify(body ?? {});
  }
  let response;
  let text = "";
  try {
    response = await fetch(url.toString(), init);
    text = await response.text();
  } catch (err) {
    throw new Error(`Ultra API request failed (${url.toString()}): ${err.message}`);
  }
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch (_) {
    parsed = null;
  }
  const headersObject = {};
  try {
    for (const [key, value] of response.headers.entries()) {
      headersObject[key] = value;
    }
  } catch (_) {}
  return {
    ok: response.ok,
    status: response.status,
    data: parsed,
    raw: text,
    headers: headersObject,
    url: url.toString(),
  };
}

function findInObject(obj, candidates) {
  for (const key of candidates) {
    const value = key.split(".").reduce((acc, segment) => (acc && acc[segment] !== undefined ? acc[segment] : undefined), obj);
    if (typeof value === "string" && value.length > 0) return value;
    if (typeof value === "number" && !Number.isNaN(value)) return value;
    if (Array.isArray(value) && value.length > 0) {
      for (const element of value) {
        if (typeof element === "string" && element.length > 0) return element;
      }
    }
  }
  return null;
}

function extractUltraSwapTransaction(response) {
  if (!response || typeof response !== "object") return null;
  const direct = findInObject(response, [
    "swapTransaction",
    "transaction",
    "tx",
    "txBase64",
    "data.swapTransaction",
    "order.swapTransaction",
    "result.swapTransaction",
  ]);
  if (typeof direct === "string") return direct;
  if (Array.isArray(response.transactions) && response.transactions.length > 0) {
    const candidate = response.transactions[0];
    if (typeof candidate === "string") return candidate;
    if (candidate && typeof candidate.transaction === "string") {
      return candidate.transaction;
    }
  }
  return null;
}

function extractUltraOutAmount(response) {
  const amount = findInObject(response, [
    "outAmount",
    "outputAmount",
    "amountOut",
    "quoteResponse.outAmount",
    "route.outAmount",
    "result.outAmount",
    "data.outAmount",
  ]);
  if (typeof amount === "number") return amount.toString();
  if (typeof amount === "string") return amount;
  return null;
}

function extractUltraClientOrderId(response) {
  const id = findInObject(response, [
    "requestId",
    "clientOrderId",
    "orderId",
    "id",
    "data.requestId",
    "data.clientOrderId",
    "result.requestId",
    "result.clientOrderId",
  ]);
  return typeof id === "string" && id.length > 0 ? id : null;
}

function extractUltraSignature(response) {
  const sig = findInObject(response, [
    "signature",
    "txid",
    "txId",
    "transactionSignature",
    "result.signature",
    "data.signature",
  ]);
  if (typeof sig === "string" && sig.length > 0) return sig;
  if (Array.isArray(response.signatures) && response.signatures.length > 0) {
    const first = response.signatures[0];
    if (typeof first === "string") return first;
    if (first && typeof first.signature === "string") return first.signature;
  }
  return null;
}

// Legacy Lite API helpers --------------------------------------------------
async function fetchWithTimeout(resource, options = {}, { timeoutMs = JUP_HTTP_TIMEOUT_MS, timeoutMessage } = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  if (typeof timeoutId.unref === "function") timeoutId.unref();
  try {
    return await fetch(resource, { ...options, signal: controller.signal });
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(timeoutMessage || `Request timed out after ${timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchLegacyQuote(inputMint, outputMint, amountLamports, userPubkey, slippageBps = SLIPPAGE_BPS) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount: amountLamports.toString(),
    slippageBps: slippageBps.toString(),
    restrictIntermediateTokens: "true",
    swapMode: "ExactIn",
  });
  if (userPubkey) params.set("userPublicKey", userPubkey);

  let res;
  try {
    res = await fetchWithTimeout(`${JUPITER_SWAP_QUOTE_URL}?${params.toString()}`, {}, {
      timeoutMessage: `Quote request timed out after ${JUP_HTTP_TIMEOUT_MS}ms`,
    });
  } catch (err) {
    throw new Error(`Quote request failed to reach Jupiter: ${err.message}`);
  }

  const body = await res.text();
  let json;
  try {
    json = JSON.parse(body);
  } catch (err) {
    throw new Error(`Quote API returned invalid JSON: ${body.substring(0, 200)}`);
  }

  if (!res.ok || json.error) {
    throw new Error(
      `Quote API error ${res.status}: ${json.error || body.substring(0, 200)}`
    );
  }

  if (!json.outAmount) {
    throw new Error("Quote response missing outAmount");
  }

  return json;
}

async function fetchLegacySwap(quoteResponse, userPublicKey, wrapAndUnwrapSol) {
  const payload = {
    quoteResponse,
    userPublicKey,
    wrapAndUnwrapSol,
    dynamicSlippage: null,
    prioritizationFeeLamports: null,
  };

  let res;
  try {
    res = await fetchWithTimeout(
      JUPITER_SWAP_URL,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      },
      { timeoutMessage: `Swap request timed out after ${JUP_HTTP_TIMEOUT_MS}ms` }
    );
  } catch (err) {
    throw new Error(`Swap request failed to reach Jupiter: ${err.message}`);
  }

  const body = await res.text();
  let json;
  try {
    json = JSON.parse(body);
  } catch (err) {
    throw new Error(`Swap API returned invalid JSON: ${body.substring(0, 200)}`);
  }

  if (!res.ok || json.error) {
    throw new Error(
      `Swap API error ${res.status}: ${json.error || body.substring(0, 200)}`
    );
  }

  if (!json.swapTransaction) {
    throw new Error("Swap payload missing");
  }

  return json;
}

async function createUltraOrder({
  inputMint,
  outputMint,
  amountLamports,
  userPublicKey,
  slippageBps = SLIPPAGE_BPS,
  wrapAndUnwrapSol = true,
}) {
  // Official Ultra API uses GET with query parameters
  const queryParams = {
    inputMint,
    outputMint,
    amount: amountLamports.toString(),
    taker: userPublicKey,
    restrictIntermediateTokens: "true",
  };

  // Add optional parameters if needed
  if (slippageBps && slippageBps !== SLIPPAGE_BPS) {
    queryParams.slippageBps = slippageBps;
  }

  const result = await ultraApiRequest({
    path: "order",
    method: "GET",
    query: queryParams,
  });
  if (!result.ok) {
    const message =
      (result.data && result.data.error) ||
      (result.data && result.data.message) ||
      result.raw ||
      `status ${result.status}`;
    const error = new Error(`Ultra order failed: ${message}`);
    error.status = result.status;
    error.payload = result.data || result.raw;
    throw error;
  }
  const swapTransaction = extractUltraSwapTransaction(result.data);
  if (!swapTransaction) {
    throw new Error("Ultra order response missing swap transaction payload");
  }
  const outAmount = extractUltraOutAmount(result.data);
  const clientOrderId = extractUltraClientOrderId(result.data);
  return {
    payload: result.data,
    swapTransaction,
    outAmount,
    clientOrderId,
  };
}

async function executeUltraSwap({
  signedTransaction,
  clientOrderId,
  signatureHint,
  extraHeaders,
}) {
  // Official Ultra API execute endpoint expects: signedTransaction and requestId
  const body = {
    signedTransaction: signedTransaction,
  };
  // requestId is the clientOrderId from the order response
  if (clientOrderId) {
    body.requestId = clientOrderId;
  }
  // Optional signature hint (not required by API but may help with tracking)
  if (Array.isArray(signatureHint) && signatureHint.length > 0) {
    body.signatures = signatureHint;
  } else if (typeof signatureHint === "string" && signatureHint.length > 0) {
    body.signature = signatureHint;
    body.signatures = [signatureHint];
  }
  const result = await ultraApiRequest({
    path: "execute",
    method: "POST",
    body,
    headers: extraHeaders,
  });
  if (!result.ok) {
    const message =
      (result.data && result.data.error) ||
      (result.data && result.data.message) ||
      result.raw ||
      `status ${result.status}`;
    const error = new Error(`Ultra execute failed: ${message}`);
    error.status = result.status;
    error.payload = result.data || result.raw;
    throw error;
  }
  const signature = extractUltraSignature(result.data) || null;
  return {
    payload: result.data,
    signature,
    response: result,
  };
}

async function checkRpcHealth(options = {}) {
  const { endpointOverride = null, endpointIndex = null } = options;
  let selectedEndpoint = null;
  if (typeof endpointOverride === "string" && endpointOverride.trim().length > 0) {
    selectedEndpoint = endpointOverride.trim();
  } else if (typeof endpointIndex === "number" && Number.isFinite(endpointIndex)) {
    selectedEndpoint = getRpcEndpointByIndex(endpointIndex);
  }

  const connection = selectedEndpoint
    ? new Connection(selectedEndpoint, "confirmed")
    : createRpcConnection("confirmed");
  const start = Date.now();
  try {
    await connection.getVersion();
    const latency = Date.now() - start;
    const endpointRaw =
      selectedEndpoint ||
      connection.__rpcEndpoint ||
      connection._rpcEndpoint ||
      "unknown";
    let hostLabel = endpointRaw;
    try {
      const parsed = new URL(endpointRaw);
      hostLabel = parsed.host || parsed.href;
    } catch (_) {}
    console.log(
      paint(`RPC ok ${hostLabel} (${latency}ms)`, "success")
    );
  } catch (e) {
    const endpointRaw =
      selectedEndpoint ||
      connection.__rpcEndpoint ||
      connection._rpcEndpoint ||
      "unknown";
    throw new Error(`RPC health check failed for ${endpointRaw}: ${e.message}`);
  }
}

function resolveRpcTestTargets(targetRaw) {
  const endpoints = listAllRpcEndpoints();
  if (!targetRaw || targetRaw.trim().length === 0 || targetRaw.trim().toLowerCase() === "all") {
    return endpoints;
  }
  const target = targetRaw.trim();
  if (/^\d+$/.test(target)) {
    const index = parseInt(target, 10) - 1;
    return [getRpcEndpointByIndex(index)];
  }
  if (target.startsWith("http://") || target.startsWith("https://")) {
    return [target];
  }
  const filtered = endpoints.filter((url) => url.includes(target));
  if (filtered.length === 0) {
    throw new Error(`No RPC endpoints match selector "${target}"`);
  }
  return filtered;
}

async function testRpcEndpoints(targetRaw, options = {}) {
  const swapTestRequested = options.swapTest === true;
  const swapLoops = Math.max(1, parseInt(options.swapLoops ?? 10, 10) || 10);
  const swapDelayMs = Math.max(0, parseInt(options.swapDelayMs ?? 1000, 10) || 1000);
  const swapAmountOverride =
    typeof options.swapAmount === "string" && options.swapAmount.trim().length > 0
      ? options.swapAmount.trim()
      : null;
  const swapConfirmed = options.swapConfirm === true;
  let endpoints;
  try {
    endpoints = resolveRpcTestTargets(targetRaw);
  } catch (err) {
    console.error(paint("Error:", "error"), err.message);
    return;
  }
  if (endpoints.length === 0) {
    console.log(paint("No RPC endpoints to test.", "muted"));
    return;
  }
  console.log(
    paint(
      `Testing ${endpoints.length} RPC endpoint${endpoints.length === 1 ? '' : 's'}...`,
      "label"
    )
  );
  for (let i = 0; i < endpoints.length; i += 1) {
    const url = endpoints[i];
    console.log(
      paint(
        `\n[${i + 1}/${endpoints.length}] ${url}`,
        "info"
      )
    );
    const connection = new Connection(url, "confirmed");
    const started = Date.now();
    try {
      const version = await connection.getVersion();
      const latency = Date.now() - started;
      const core = version["solana-core"] || version["solanaCore"] || "unknown";
      const blockhashStart = Date.now();
      const { blockhash } = await connection.getLatestBlockhash();
      const blockhashLatency = Date.now() - blockhashStart;
      let healthStatus = "unavailable";
      try {
        const health = await connection.getHealth();
        if (typeof health === "string") {
          healthStatus = health;
        } else if (health === null) {
          healthStatus = "ok";
        }
      } catch (_) {
        // many RPCs do not implement getHealth; ignore
      }
      console.log(
        paint(
          `  ✓ version ${core} — latency ${latency}ms — blockhash ${blockhashLatency}ms — health ${healthStatus}`,
          "success"
        )
      );
      if (!blockhash) {
        console.log(paint("    warning: empty blockhash response", "warn"));
      }
    } catch (err) {
      console.error(
        paint(
          `  ✗ failed: ${err.message || err}`,
          "error"
        )
      );
    }
  }
  if (swapTestRequested) {
    await runRpcSwapTest(endpoints, {
      loops: swapLoops,
      delayMs: swapDelayMs,
      amount: swapAmountOverride,
      confirm: swapConfirmed,
    });
  }
}

async function runRpcSwapTest(endpoints, options = {}) {
  const confirm = options.confirm === true;
  if (!confirm) {
    console.log(
      paint(
        "Swap test requested but --confirm flag missing; skipping swap stress test.",
        "warn"
      )
    );
    return;
  }
  const loops = Math.max(1, parseInt(options.loops ?? 10, 10) || 10);
  const delayMs = Math.max(0, parseInt(options.delayMs ?? 1000, 10) || 1000);
  const amountStr =
    typeof options.amount === "string" && options.amount.trim().length > 0
      ? options.amount.trim()
      : "0.001";

  const wallets = listWallets();
  const crewWallet = wallets.find((w) => w.name === "crew_1.json");
  if (!crewWallet) {
    console.log(
      paint(
        "Swap test aborted: crew_1.json wallet not found.",
        "error"
      )
    );
    return;
  }

  console.log(
    paint(
      `\nStarting swap stress test across ${endpoints.length} endpoint${endpoints.length === 1 ? '' : 's'} — ${loops} round${loops === 1 ? '' : 's'}, ${delayMs}ms delay, amount ${amountStr} SOL`,
      "label"
    )
  );
  for (let round = 0; round < loops; round += 1) {
    const endpoint = endpoints[round % endpoints.length];
    console.log(
      paint(
        `\nSwap round ${round + 1}/${loops} via ${endpoint}`,
        "info"
      )
    );
    try {
      await doSwapAcross(SOL_MINT, DEFAULT_USDC_MINT, amountStr, {
        wallets: [crewWallet],
        quietSkips: false,
        suppressMetadata: false,
        includeDisabled: true,
        forcedRpcEndpoint: endpoint,
        maxSlippageRetries: 7,
        slippageBoostAfter: 3,
        slippageBoostStrategy: "add",
        slippageBoostIncrementBps: 200,
      });
      await doSwapAcross(DEFAULT_USDC_MINT, SOL_MINT, "all", {
        wallets: [crewWallet],
        quietSkips: false,
        suppressMetadata: false,
        includeDisabled: true,
        forcedRpcEndpoint: endpoint,
        maxSlippageRetries: 7,
        slippageBoostAfter: 3,
        slippageBoostStrategy: "add",
        slippageBoostIncrementBps: 200,
      });
      console.log(paint("  swap round complete", "success"));
    } catch (err) {
      console.error(
        paint("  swap round failed:", "error"),
        err?.message || err
      );
    }
    if (round < loops - 1) {
      await delay(delayMs);
    }
  }
}


async function testJupiterUltraOrder({
  inputMint: inputMintRaw,
  outputMint: outputMintRaw,
  amount: amountRaw,
  walletName,
  submit = false,
  slippageBps: slippageRaw,
} = {}) {
  const inputArg = (inputMintRaw ?? SOL_MINT).toString().trim();
  const outputArg = (outputMintRaw ?? DEFAULT_USDC_MINT).toString().trim();
  const amountArg = (amountRaw ?? "0.001").toString().trim();
  const slippageBps =
    slippageRaw !== undefined && slippageRaw !== null
      ? Math.max(1, parseInt(slippageRaw, 10) || SLIPPAGE_BPS)
      : SLIPPAGE_BPS;

  if (!JUPITER_ULTRA_API_KEY) {
    console.warn(
      paint(
        "Ultra API key not configured; proceeding with anonymous request.",
        "warn"
      )
    );
  }

  let wallet;
  if (walletName) {
    try {
      wallet = findWalletByName(walletName);
    } catch (err) {
      console.error(paint("Ultra test aborted:", "error"), err.message);
      return;
    }
  } else {
    const wallets = listWallets();
    if (wallets.length === 0) {
      console.error(
        paint("Ultra test aborted: no wallets found in keypairs directory.", "error")
      );
      return;
    }
    wallet =
      wallets.find((entry) => entry.name === "crew_1.json") || wallets[0];
  }

  const walletPubkey = wallet.kp.publicKey.toBase58();
  console.log(
    paint(
      `Using wallet ${wallet.name} (${walletPubkey}) for Ultra order.`,
      "muted"
    )
  );
  if (isWalletDisabledByGuard(wallet.name)) {
    console.warn(
      paint(
        `  Warning: wallet guard disabled ${wallet.name}. Ensure it has at least ${WALLET_DISABLE_THRESHOLD_LABEL} SOL before submitting.`,
        "warn"
      )
    );
  }

  const resolveMintInput = async (value, label) => {
    const trimmed = value.trim();
    if (!trimmed) {
      throw new Error(`${label} token is required`);
    }
    const record = await resolveTokenRecord(trimmed);
    if (record?.mint) {
      return {
        mint: record.mint,
        symbol: record.symbol || symbolForMint(record.mint),
        decimals:
          typeof record.decimals === "number" ? record.decimals : undefined,
      };
    }
    if (/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(trimmed)) {
      return {
        mint: trimmed,
        symbol: symbolForMint(trimmed),
        decimals: undefined,
      };
    }
    throw new Error(
      `${label} token ${trimmed} not found in catalog (provide a mint address)`
    );
  };

  let inputInfo;
  let outputInfo;
  try {
    inputInfo = await resolveMintInput(inputArg, "Input");
    outputInfo = await resolveMintInput(outputArg, "Output");
  } catch (err) {
    console.error(paint("Ultra test aborted:", "error"), err.message);
    return;
  }

  const connection = createRpcConnection("confirmed");
  let inputMeta;
  let outputMeta;
  try {
    inputMeta = await resolveMintMetadata(connection, inputInfo.mint);
  } catch (err) {
    console.error(
      paint(
        `Ultra test aborted: failed to load input mint metadata (${inputInfo.mint}):`,
        "error"
      ),
      err.message || err
    );
    return;
  }
  try {
    outputMeta = await resolveMintMetadata(connection, outputInfo.mint);
  } catch (err) {
    console.error(
      paint(
        `Ultra test aborted: failed to load output mint metadata (${outputInfo.mint}):`,
        "error"
      ),
      err.message || err
    );
    return;
  }

  const inputDecimals =
    inputMeta?.decimals ??
    inputInfo.decimals ??
    KNOWN_MINTS.get(inputInfo.mint)?.decimals ??
    0;
  const outputDecimals =
    outputMeta?.decimals ??
    outputInfo.decimals ??
    KNOWN_MINTS.get(outputInfo.mint)?.decimals ??
    0;

  let amountLamports;
  try {
    amountLamports = decimalToBaseUnits(amountArg, inputDecimals);
  } catch (err) {
    console.error(
      paint("Ultra test aborted: invalid amount (expected decimal).", "error"),
      err.message
    );
    return;
  }
  if (amountLamports <= 0n) {
    console.error(
      paint("Ultra test aborted: amount must be greater than zero.", "error")
    );
    return;
  }

  const wrapSol =
    SOL_LIKE_MINTS.has(inputInfo.mint) || SOL_LIKE_MINTS.has(outputInfo.mint);

  console.log(
    paint(
      `Ultra order plan: ${describeMintLabel(inputInfo.mint)} → ${describeMintLabel(outputInfo.mint)}`,
      "label"
    )
  );
  console.log(
    paint(
      `  amount: ${amountArg} (${amountLamports.toString()} base units) — slippage ${slippageBps} bps`,
      "muted"
    )
  );
  console.log(
    paint(`  wrap/unwrap SOL: ${wrapSol ? "yes" : "no"}`, "muted")
  );

  let orderInfo;
  try {
    orderInfo = await createUltraOrder({
      inputMint: inputInfo.mint,
      outputMint: outputInfo.mint,
      amountLamports,
      userPublicKey: walletPubkey,
      slippageBps,
      wrapAndUnwrapSol: wrapSol,
    });
  } catch (err) {
    const message = err?.message || err;
    console.error(paint("Ultra order request failed:", "error"), message);
    if (err?.payload) {
      try {
        const preview = JSON.stringify(err.payload, null, 2);
        console.error(paint("  payload:", "muted"), preview);
      } catch (_) {}
    }
    const payloadText =
      typeof err?.payload === "string" ? err.payload : "";
    if (/route not found/i.test(String(message)) || /route not found/i.test(payloadText)) {
      console.warn(
        paint(
          "  Hint: try increasing the amount or confirm the selected mints are supported by Jupiter Ultra.",
          "warn"
        )
      );
    } else if (/(?:^|\D)(401|403)(?:\D|$)/.test(String(message))) {
      console.warn(
        paint(
          "  Hint: Ultra returned 401/403 — verify JUPITER_ULTRA_API_KEY or JUPITER_ULTRA_API_BASE.",
          "warn"
        )
      );
    }
    return;
  }

  console.log(paint("Ultra order created successfully.", "success"));
  if (orderInfo.clientOrderId) {
    console.log(
      paint(`  clientOrderId: ${orderInfo.clientOrderId}`, "muted")
    );
  }
  if (orderInfo.outAmount) {
    const outDecimal = (() => {
      try {
        return formatBaseUnits(BigInt(orderInfo.outAmount), outputDecimals);
      } catch (_) {
        return null;
      }
    })();
    const label = outDecimal
      ? `${orderInfo.outAmount} (${outDecimal} ${symbolForMint(outputInfo.mint)})`
      : orderInfo.outAmount;
    console.log(paint(`  expected outAmount: ${label}`, "muted"));
  }
  if (orderInfo.payload) {
    try {
      const preview = JSON.stringify(orderInfo.payload);
      const snippet = preview.length > 400 ? `${preview.slice(0, 400)}…` : preview;
      console.log(paint("  payload snippet:", "muted"), snippet);
    } catch (_) {}
  }

  let vtx;
  try {
    const txbuf = Buffer.from(orderInfo.swapTransaction, "base64");
    vtx = VersionedTransaction.deserialize(txbuf);
    const ixCount = vtx.message.compiledInstructions.length;
    const accountCount = vtx.message.staticAccountKeys.length;
    console.log(
      paint(
        `  transaction decoded: ${accountCount} static accounts, ${ixCount} instruction(s)`,
        "muted"
      )
    );
  } catch (err) {
    console.error(
      paint("Failed to decode Ultra swap transaction:", "error"),
      err.message || err
    );
    return;
  }

  if (!submit) {
    console.log(
      paint(
        "Dry run complete — transaction not signed or broadcast. Re-run with --submit to execute.",
        "info"
      )
    );
    return;
  }

  vtx.sign([wallet.kp]);
  const rawSigned = vtx.serialize();
  const signedBase64 = Buffer.from(rawSigned).toString("base64");
  const derivedSignature = bs58.encode(vtx.signatures[0]);

  console.log(
    paint(
      "Submitting signed swap via Ultra execute endpoint...",
      "info"
    )
  );
  let executeResult;
  try {
    await delay(DELAY_BETWEEN_CALLS_MS);
    executeResult = await executeUltraSwap({
      signedTransaction: signedBase64,
      clientOrderId: orderInfo.clientOrderId,
      signatureHint: derivedSignature,
    });
  } catch (err) {
    console.error(paint("Ultra execute request failed:", "error"), err.message || err);
    if (err?.payload) {
      try {
        const preview = JSON.stringify(err.payload, null, 2);
        console.error(paint("  payload:", "muted"), preview);
      } catch (_) {}
    }
    return;
  }

  const networkSignature =
    executeResult.signature || derivedSignature || null;
  if (networkSignature) {
    console.log(
      paint(`Ultra execute accepted signature ${networkSignature}`, "success")
    );
  } else {
    console.log(
      paint("Ultra execute response did not include a signature.", "warn")
    );
  }
  if (executeResult?.payload) {
    try {
      const preview = JSON.stringify(executeResult.payload);
      const snippet = preview.length > 400 ? `${preview.slice(0, 400)}…` : preview;
      console.log(paint("  execute payload snippet:", "muted"), snippet);
    } catch (_) {}
  }

  if (networkSignature) {
    try {
      await connection.confirmTransaction(networkSignature, "confirmed");
      console.log(paint("  transaction confirmed on-chain.", "success"));
    } catch (err) {
      console.warn(
        paint(
          "  confirmation warning:",
          "warn"
        ),
        err.message || err
      );
    }
  }

  console.log(paint("Ultra swap test finished.", "info"));
}

async function submitSignedSolanaTransaction(connection, wallet, base64Tx, { label, index, total }) {
  const labelPrefix =
    total && total > 1 ? `${label} ${index + 1}/${total}` : label;
  let serialized = null;
  let derivedSignature = null;
  try {
    const buf = Buffer.from(base64Tx, "base64");
    const vtx = VersionedTransaction.deserialize(buf);
    vtx.sign([wallet.kp]);
    serialized = vtx.serialize();
    derivedSignature = bs58.encode(vtx.signatures[0]);
  } catch (versionedErr) {
    try {
      const bufLegacy = Buffer.from(base64Tx, "base64");
      const tx = Transaction.from(bufLegacy);
      tx.sign(wallet.kp);
      serialized = tx.serialize();
      derivedSignature = tx.signature ? bs58.encode(tx.signature) : null;
    } catch (legacyErr) {
      throw new Error(
        `Unable to deserialize transaction for ${labelPrefix} (${versionedErr.message || versionedErr} / ${legacyErr.message || legacyErr})`
      );
    }
  }
  if (!serialized) {
    throw new Error(`Failed to serialize transaction for ${labelPrefix}`);
  }
  let networkSignature;
  try {
    networkSignature = await connection.sendRawTransaction(serialized, {
      skipPreflight: false,
    });
    await connection.confirmTransaction(networkSignature, "confirmed");
  } catch (err) {
    throw new Error(
      `RPC submission failed for ${labelPrefix}: ${err.message || err}`
    );
  }
  const finalSignature = networkSignature || derivedSignature;
  if (finalSignature) {
    console.log(
      paint(
        `  ${labelPrefix}: executed tx ${finalSignature}`,
        "success"
      )
    );
  } else {
    console.log(
      paint(`  ${labelPrefix}: transaction submitted (no signature returned)`, "success")
    );
  }
}

// send a signed native SOL transfer tx
async function sendSolTransfer(connection, fromKeypair, toPubkey, lamports) {
  const tx = new Transaction().add(
    SystemProgram.transfer({
      fromPubkey: fromKeypair.publicKey,
      toPubkey,
      lamports,
    })
  );
  tx.feePayer = fromKeypair.publicKey;
  const { blockhash } = await connection.getLatestBlockhash();
  tx.recentBlockhash = blockhash;
  tx.sign(fromKeypair);
  const raw = tx.serialize();
  const sig = await connection.sendRawTransaction(raw);
  await connection.confirmTransaction(sig, "confirmed");
  return sig;
}

// listing addresses + balances
async function listWalletAddresses() {
  const wallets = listWallets();
  const walletMenuHotkey =
    formatHotkeyPrimaryKey("launcher", "wallet-tools") || "'w'";
  if (wallets.length === 0) {
    if (!fs.existsSync(KEYPAIR_DIR)) {
      console.log(paint(`No keypairs directory found at ${KEYPAIR_DIR}`, "warn"));
      console.log(
        paint(
          `Use wallet menu (hotkey ${walletMenuHotkey}) or 'generate <n> [prefix]' command to create wallets`,
          "info"
        )
      );
    } else {
      console.log(paint(`No wallets found in ${KEYPAIR_DIR}`, "warn"));
      console.log(
        paint(
          `Use wallet menu (hotkey ${walletMenuHotkey}) or 'generate <n> [prefix]' command to create wallets`,
          "info"
        )
      );
    }
    return;
  }
  const connection = createRpcConnection("confirmed");
  for (const w of wallets) {
    try {
      const lamports = BigInt(await getSolBalance(connection, w.kp.publicKey));
      const solDisplay = formatBaseUnits(lamports, 9);
      const { lamports: wsolLamports } = await getWrappedSolAccountInfo(
        connection,
        w.kp.publicKey
      );
      const wsolDisplay = formatBaseUnits(wsolLamports, 9);
      console.log(
        paint(
          `${w.name}  ${w.kp.publicKey.toBase58()}  ${solDisplay} SOL  (wSOL ${wsolDisplay})`,
          "info"
        )
      );
    } catch (e) {
      console.log(
        paint(
          `${w.name}  ${w.kp.publicKey.toBase58()}  error reading balance: ${e.message}`,
          "warn"
        )
      );
    }
  }
}

async function handleWalletWrap(args) {
  const walletName = args[0];
  const usage =
    "wallet wrap usage: wallet wrap <wallet #|filename> [amount|all] [--raw]";
  if (!walletName) {
    console.log(usage);
    return;
  }
  const wallet = findWalletByName(walletName);
  const { options, rest } = parseCliOptions(args.slice(1));
  const rawAmount =
    options.raw === true ||
    options.raw === "true" ||
    options.raw === "1";
  const amountArg = (rest[0] || "all").trim();
  const connection = createRpcConnection("confirmed");
  const beforeBalances = await getSolAndWrappedSolBalances(connection, wallet);
  const spendableInfo = await computeSpendableSolBalance(
    connection,
    wallet.kp.publicKey
  );
  let wrapLamports;
  if (amountArg === "*" || amountArg.toLowerCase() === "all") {
    wrapLamports = spendableInfo.spendableLamports;
  } else {
    try {
      wrapLamports = rawAmount
        ? BigInt(amountArg)
        : decimalToBaseUnits(amountArg, 9);
    } catch (err) {
      throw new Error(
        `Invalid amount for wallet wrap: ${amountArg} (${err.message || err})`
      );
    }
  }
  if (wrapLamports < 0n) wrapLamports = 0n;
  const availableLamports = spendableInfo.spendableLamports;
  if (availableLamports <= 0n) {
    console.log(
      paint(
        `  Wallet ${wallet.name} has no spendable SOL after gas/reserve buffers.`,
        "warn"
      )
    );
    return;
  }
  const feeBuffer = ESTIMATED_GAS_PER_SWAP_LAMPORTS;
  const maxWrap =
    availableLamports > feeBuffer ? availableLamports - feeBuffer : 0n;
  if (wrapLamports > maxWrap) {
    console.warn(
      paint(
        `  Adjusting wrap amount to ${formatBaseUnits(maxWrap, 9)} SOL to leave room for fees.`,
        "warn"
      )
    );
    wrapLamports = maxWrap;
  }
  if (wrapLamports <= 0n) {
    console.log(paint("  Nothing to wrap after reserves/fees.", "muted"));
    return;
  }

  const humanWrap = formatBaseUnits(wrapLamports, 9);
  console.log(
    paint(
      `  Wrapping ${humanWrap} SOL for ${wallet.name}.`,
      "info"
    )
  );
  console.log(
    paint(
      `    before → SOL ${formatBaseUnits(beforeBalances.solLamports, 9)} | wSOL ${formatBaseUnits(beforeBalances.wsolLamports, 9)}`,
      "muted"
    )
  );

  const requiredTotal = beforeBalances.wsolLamports + wrapLamports;
  await ensureWrappedSolBalance(
    connection,
    wallet,
    requiredTotal,
    beforeBalances.wsolLamports
  );

  const afterBalances = await getSolAndWrappedSolBalances(connection, wallet);
  console.log(
    paint(
      `    after  → SOL ${formatBaseUnits(afterBalances.solLamports, 9)} | wSOL ${formatBaseUnits(afterBalances.wsolLamports, 9)}`,
      "muted"
    )
  );
}

async function handleWalletUnwrap(args) {
  const walletName = args[0];
  const usage =
    "wallet unwrap usage: wallet unwrap <wallet #|filename> [amount|all] [--raw]";
  if (!walletName) {
    console.log(usage);
    return;
  }
  const wallet = findWalletByName(walletName);
  const { options, rest } = parseCliOptions(args.slice(1));
  const rawAmount =
    options.raw === true ||
    options.raw === "true" ||
    options.raw === "1";
  const amountArg = (rest[0] || "all").trim();
  const connection = createRpcConnection("confirmed");
  const beforeBalances = await getSolAndWrappedSolBalances(connection, wallet);
  if (beforeBalances.wsolLamports <= 0n) {
    console.log(
      paint(
        `  Wallet ${wallet.name} has no wrapped SOL to unwrap.`,
        "muted"
      )
    );
    return;
  }

  let unwrapLamports;
  if (amountArg === "*" || amountArg.toLowerCase() === "all") {
    unwrapLamports = beforeBalances.wsolLamports;
  } else {
    try {
      unwrapLamports = rawAmount
        ? BigInt(amountArg)
        : decimalToBaseUnits(amountArg, 9);
    } catch (err) {
      throw new Error(
        `Invalid amount for wallet unwrap: ${amountArg} (${err.message || err})`
      );
    }
  }
  if (unwrapLamports < 0n) unwrapLamports = 0n;
  if (unwrapLamports <= 0n) {
    console.log(paint("  Nothing to unwrap.", "muted"));
    return;
  }

  if (unwrapLamports > beforeBalances.wsolLamports) {
    console.warn(
      paint(
        `  Requested unwrap exceeds wSOL balance; capping to ${formatBaseUnits(beforeBalances.wsolLamports, 9)} SOL.`,
        "warn"
      )
    );
    unwrapLamports = beforeBalances.wsolLamports;
  }

  let remainingLamports = beforeBalances.wsolLamports - unwrapLamports;
  if (
    remainingLamports > 0n &&
    remainingLamports < LEND_SOL_WRAP_BUFFER_LAMPORTS
  ) {
    console.warn(
      paint(
        `  Remaining wSOL would drop below the wrap buffer; unwrapping entire balance instead.`,
        "warn"
      )
    );
    unwrapLamports = beforeBalances.wsolLamports;
    remainingLamports = 0n;
  }

  const { ata, exists } = await getWrappedSolAccountInfo(
    connection,
    wallet.kp.publicKey
  );
  if (!exists) {
    console.log(
      paint(
        `  No active wSOL account found for ${wallet.name}.`,
        "warn"
      )
    );
    return;
  }

  const humanUnwrap = formatBaseUnits(unwrapLamports, 9);
  console.log(
    paint(
      `  Unwrapping ${humanUnwrap} SOL for ${wallet.name}.`,
      "info"
    )
  );
  console.log(
    paint(
      `    before → SOL ${formatBaseUnits(beforeBalances.solLamports, 9)} | wSOL ${formatBaseUnits(beforeBalances.wsolLamports, 9)}`,
      "muted"
    )
  );

  const sig = await closeAccount(
    connection,
    wallet.kp,
    ata,
    wallet.kp.publicKey,
    wallet.kp,
    [],
    undefined,
    TOKEN_PROGRAM_ID
  );
  console.log(
    paint(
      `  Closed wSOL account for ${wallet.name} — tx ${sig}`,
      "success"
    )
  );

  if (remainingLamports > 0n) {
    const wrapConnection = connection;
    const solAfterClose = BigInt(
      await getSolBalance(wrapConnection, wallet.kp.publicKey)
    );
    if (
      remainingLamports + ESTIMATED_GAS_PER_SWAP_LAMPORTS >
      solAfterClose
    ) {
      console.warn(
        paint(
          `  Remaining SOL ${formatBaseUnits(remainingLamports, 9)} is insufficient to re-wrap after fees; leaving as native SOL.`,
          "warn"
        )
      );
      remainingLamports = 0n;
    } else {
      console.log(
        paint(
          `  Re-wrapping ${formatBaseUnits(remainingLamports, 9)} SOL to preserve leftover wSOL balance.`,
          "muted"
        )
      );
      await ensureWrappedSolBalance(
        wrapConnection,
        wallet,
        remainingLamports,
        0n
      );
    }
  }

  const afterBalances = await getSolAndWrappedSolBalances(connection, wallet);
  console.log(
    paint(
      `    after  → SOL ${formatBaseUnits(afterBalances.solLamports, 9)} | wSOL ${formatBaseUnits(afterBalances.wsolLamports, 9)}`,
      "muted"
    )
  );
}

async function handleWalletCommand(args) {
  const subcommandRaw = args[0];
  if (!subcommandRaw) {
    console.log(
      "wallet usage: wallet <wrap|unwrap|list|info|sync|groups|transfer> [args...]"
    );
    return;
  }
  const subcommand = subcommandRaw.toLowerCase();
  const rest = args.slice(1);

  if (subcommand === "wrap") {
    await handleWalletWrap(rest);
    return;
  }
  if (subcommand === "unwrap") {
    await handleWalletUnwrap(rest);
    return;
  }
  if (subcommand === "list") {
    await walletListCommand();
    return;
  }
  if (subcommand === "info") {
    await walletInfoCommand(rest);
    return;
  }
  if (subcommand === "sync") {
    await walletSyncCommand();
    return;
  }
  if (subcommand === "groups") {
    await walletGroupsCommand();
    return;
  }
  if (subcommand === "transfer") {
    await walletTransferCommand(rest);
    return;
  }
  if (subcommand === "fund") {
    await walletFundCommand(rest);
    return;
  }
  if (subcommand === "redistribute") {
    await walletRedistributeCommand(rest);
    return;
  }
  if (subcommand === "aggregate") {
    await walletAggregateCommand(rest);
    return;
  }

  throw new Error(
    `Unknown wallet subcommand '${subcommandRaw}'. Expected 'wrap', 'unwrap', 'list', 'info', 'sync', 'groups', 'transfer', 'fund', 'redistribute', or 'aggregate'.`
  );
}

async function handleUnwrapWsolMenu() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const prompt = (question) =>
    new Promise((resolve) => rl.question(question, resolve));

  console.log(paint("\n=== Unwrap wSOL → SOL ===", "label"));

  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    rl.close();
    return;
  }

  // Show wallets with wSOL balances
  const connection = createRpcConnection("confirmed");
  const walletsWithWsol = [];

  for (const w of wallets) {
    try {
      const balances = await getSolAndWrappedSolBalances(connection, w);
      if (balances.wsolLamports > 0n) {
        walletsWithWsol.push({
          wallet: w,
          wsolAmount: formatBaseUnits(balances.wsolLamports, 9),
        });
      }
    } catch (err) {
      // Skip wallets with errors
    }
  }

  if (walletsWithWsol.length === 0) {
    console.log(paint("No wallets have wrapped SOL to unwrap.", "muted"));
    rl.close();
    return;
  }

  console.log(paint("\nWallets with wSOL:", "info"));
  for (let i = 0; i < walletsWithWsol.length; i++) {
    const { wallet, wsolAmount } = walletsWithWsol[i];
    console.log(paint(`  ${i + 1}. ${wallet.name} (${wsolAmount} wSOL)`, "muted"));
  }

  const walletInput = (await prompt("\nWallet name or number (or 'all'): "))?.trim();
  rl.close();

  if (!walletInput) {
    console.log(paint("Cancelled.", "muted"));
    return;
  }

  if (walletInput.toLowerCase() === "all") {
    // Unwrap all wallets
    console.log(paint(`\nUnwrapping wSOL from ${walletsWithWsol.length} wallet(s)...`, "info"));
    for (const { wallet } of walletsWithWsol) {
      try {
        await handleWalletUnwrap([wallet.name, "all"]);
      } catch (err) {
        console.error(paint(`  Failed to unwrap ${wallet.name}:`, "error"), err.message);
      }
    }
  } else {
    // Single wallet
    const walletNumber = parseInt(walletInput, 10);
    let targetWallet;

    if (!isNaN(walletNumber) && walletNumber >= 1 && walletNumber <= walletsWithWsol.length) {
      targetWallet = walletsWithWsol[walletNumber - 1].wallet.name;
    } else {
      targetWallet = walletInput;
    }

    try {
      await handleWalletUnwrap([targetWallet, "all"]);
    } catch (err) {
      console.error(paint(`  Failed to unwrap ${targetWallet}:`, "error"), err.message);
    }
  }

  console.log(paint("\nPress Enter to continue...", "muted"));
}

/* --- Wallet Registry Command Implementations --- */

async function walletListCommand() {
  const wallets = listWallets();

  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    return;
  }

  console.log(paint("\n=== Wallet Registry ===", "label"));
  console.log(paint(`Total: ${wallets.length} wallets\n`, "info"));

  // Get balances for all wallets
  const connection = createRpcConnection("confirmed");
  const walletsWithBalance = [];

  for (const w of wallets) {
    try {
      const balance = await getSolBalance(connection, w.kp.publicKey);
      walletsWithBalance.push({
        ...w,
        balance: BigInt(balance)
      });
    } catch (err) {
      walletsWithBalance.push({
        ...w,
        balance: 0n
      });
    }
  }

  // Print table header
  console.log(
    paint(
      `${"#".padEnd(4)} ${"Filename".padEnd(20)} ${"Role".padEnd(15)} ${"Master".padEnd(8)} ${"Group".padEnd(7)} ${"SOL Balance".padEnd(15)} ${"Status"}`,
      "label"
    )
  );
  console.log(paint("-".repeat(90), "muted"));

  // Print each wallet
  for (const w of walletsWithBalance) {
    const num = w.number ? `#${w.number}` : "-";
    const filename = w.name || "-";
    const role = w.role || "unknown";
    const master = w.master ? `#${w.master}` : "-";
    const group = w.group ? `${w.group}` : "-";
    const balance = formatBaseUnits(w.balance, 9);
    const status = w.balance < BigInt(GAS_RESERVE_LAMPORTS) ? paint("low", "warn") : paint("ok", "success");

    const roleColor = role === "master-master" ? "label" : role === "master" ? "info" : "muted";

    console.log(
      `${num.padEnd(4)} ${filename.padEnd(20)} ${paint(role.padEnd(15), roleColor)} ${master.padEnd(8)} ${group.padEnd(7)} ${balance.padEnd(15)} ${status}`
    );
  }

  console.log("");
}

async function walletInfoCommand(args) {
  const wallets = listWallets();

  if (args.length === 0) {
    console.error(paint("Usage: wallet info <number|filename>", "error"));
    return;
  }

  const identifier = args[0];
  let wallet;

  // Try to parse as number
  const num = parseInt(identifier, 10);
  if (!isNaN(num)) {
    wallet = wallets.find(w => w.number === num);
  } else {
    wallet = wallets.find(w => w.name === identifier);
  }

  if (!wallet) {
    console.error(paint(`Wallet '${identifier}' not found`, "error"));
    return;
  }

  console.log(paint("\n=== Wallet Info ===", "label"));
  console.log(paint(`Number:      `, "muted") + paint(`#${wallet.number}`, "info"));
  console.log(paint(`Filename:    `, "muted") + wallet.name);
  console.log(paint(`Role:        `, "muted") + paint(wallet.role, wallet.role === "master-master" ? "label" : "info"));
  console.log(paint(`Group:       `, "muted") + wallet.group);
  console.log(paint(`Master:      `, "muted") + (wallet.master ? `#${wallet.master}` : "none (this is master-master)"));
  console.log(paint(`Public Key:  `, "muted") + wallet.kp.publicKey.toBase58());

  // Get balance
  try {
    const connection = createRpcConnection("confirmed");
    const balance = await getSolBalance(connection, wallet.kp.publicKey);
    console.log(paint(`SOL Balance: `, "muted") + formatBaseUnits(BigInt(balance), 9) + " SOL");
  } catch (err) {
    console.log(paint(`SOL Balance: `, "muted") + paint("Error fetching balance", "error"));
  }

  // Show slaves if this is a master
  if (wallet.role === "master" || wallet.role === "master-master") {
    const slaves = walletRegistry.getSlaves(wallet.number);
    if (slaves.length > 0) {
      console.log(paint(`\nSlaves (${slaves.length}):`, "info"));
      for (const slave of slaves) {
        console.log(paint(`  #${slave.number}`, "muted") + ` (${slave.filename})`);
      }
    } else {
      console.log(paint(`\nSlaves: `, "muted") + "none");
    }
  }

  console.log("");
}

async function walletSyncCommand() {
  const wallets = listWallets();

  console.log(paint("Syncing wallet registry from filesystem...", "info"));
  const manifest = walletRegistry.syncWalletsFromFilesystem(wallets);

  console.log(paint(`✓ Synced ${manifest.wallets.length} wallets`, "success"));
  console.log(paint(`Manifest saved to: ${walletRegistry.MANIFEST_PATH}`, "muted"));
}

async function walletGroupsCommand() {
  const wallets = listWallets();

  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    return;
  }

  const hierarchy = walletRegistry.getHierarchySummary();
  const balanceConnection = createRpcConnection("confirmed");

  console.log(paint("\n=== Wallet Groups ===", "label"));
  console.log(paint(`Total: ${hierarchy.totalWallets} wallets in ${hierarchy.totalGroups} groups\n`, "info"));

  for (const group of hierarchy.groups) {
    if (!group.master) {
      console.log(paint(`Group ${group.groupNumber}: No master assigned`, "warn"));
      continue;
    }

    const masterWallet = wallets.find(w => w.number === group.master.number);
    const masterBalance = masterWallet
      ? formatBaseUnits(BigInt(await getSolBalance(balanceConnection, masterWallet.kp.publicKey)), 9)
      : "?";

    console.log(
      paint(
        `Group ${group.groupNumber}: `,
        "label"
      ) +
      paint(
        `Master #${group.master.number} (${group.master.filename})`,
        group.master.role === "master-master" ? "label" : "info"
      ) +
      paint(` - ${masterBalance} SOL`, "muted")
    );

    if (group.slaves.length > 0) {
      for (const slave of group.slaves) {
        const slaveWallet = wallets.find(w => w.number === slave.number);
        const slaveBalance = slaveWallet
          ? formatBaseUnits(BigInt(await getSolBalance(balanceConnection, slaveWallet.kp.publicKey)), 9)
          : "?";

        console.log(
          paint(`  └─ #${slave.number} `, "muted") +
          `(${slave.filename})` +
          paint(` - ${slaveBalance} SOL`, "muted")
        );
      }
    } else {
      console.log(paint(`  (no slaves)`, "muted"));
    }
    console.log("");
  }
}

async function walletTransferCommand(args) {
  if (args.length < 3) {
    console.error(paint("Usage: wallet transfer <from> <to> <amount> [token]", "error"));
    console.log(paint("Examples:", "muted"));
    console.log(paint("  wallet transfer 1 6 0.5", "muted"));
    console.log(paint("  wallet transfer crew_1.json crew_6.json 1.0 SOL", "muted"));
    console.log(paint("  wallet transfer 2 5 all", "muted"));
    return;
  }

  const [from, to, amount, token = 'SOL'] = args;
  await transferBetweenWallets(from, to, amount, token);
}

// fund all wallets from one source - same lamportsEach to each wallet
async function fundAll(fromWalletFile, lamportsEach) {
  const wallets = listWallets();
  const fromFp = path.join(KEYPAIR_DIR, fromWalletFile);
  if (!fs.existsSync(fromFp)) {
    console.error(paint("Source wallet not found:", "error"), fromWalletFile);
    return;
  }
  const fromKp = loadKeypairFromFile(fromFp);
  const connection = createRpcConnection("confirmed");

  // confirm from has funds
  const fromBal = await getSolBalance(connection, fromKp.publicKey);
  const required = BigInt(lamportsEach) * BigInt(wallets.length);
  if (BigInt(fromBal) < required) {
    console.warn(
      paint(
        `Source balance ${(fromBal/1e9).toFixed(6)} SOL is less than required ${(Number(required)/1e9).toFixed(6)} SOL`,
        "warn"
      )
    );
  }

  for (const w of wallets) {
    // skip if same as source
    if (w.kp.publicKey.equals(fromKp.publicKey)) {
      console.log(paint(`Skipping source wallet ${w.name}`, "muted"));
      continue;
    }
    try {
      const transferConnection = createRpcConnection("confirmed");
      const sig = await sendSolTransfer(transferConnection, fromKp, w.kp.publicKey, lamportsEach);
      console.log(
        paint(
          `Funded ${w.name} ${w.kp.publicKey.toBase58()} with ${lamportsEach} lamports — tx ${sig}`,
          "success"
        )
      );
    } catch (e) {
      console.error(paint(`Failed to fund ${w.name}: ${e.message}`, "error"));
    }
    await delay(DELAY_BETWEEN_CALLS_MS);
  }
}

// Evenly rebalance SOL across every wallet, keeping gas reserves in place and
// minimising transfer hops. Surplus wallets fund deficit wallets directly so
// we avoid routing every redistribution through a single “crew” address.
async function redistributeSol(fromWalletName) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.error(paint("No wallets found in keypairs folder.", "error"));
    return;
  }
  const source = wallets.find((w) => w.name === fromWalletName);
  if (!source) {
    console.error(paint("Source wallet not found:", "error"), fromWalletName);
    return;
  }

  console.log(
    paint(
      `Smart redistribution initiated — anchor wallet ${fromWalletName}.`,
      "label"
    )
  );

  const portfolio = [];
  for (const wallet of wallets) {
    const balanceConnection = createRpcConnection("confirmed");
    const balanceLamports = BigInt(
      await getSolBalance(balanceConnection, wallet.kp.publicKey)
    );
    let reserve = balanceLamports > GAS_RESERVE_LAMPORTS
      ? GAS_RESERVE_LAMPORTS
      : balanceLamports / 10n;
    if (reserve < 0n) reserve = 0n;
    if (reserve > balanceLamports) reserve = balanceLamports;
    const spendable = balanceLamports > reserve ? balanceLamports - reserve : 0n;
    portfolio.push({
      wallet,
      balance: balanceLamports,
      reserve,
      spendable,
    });
    await delay(DELAY_BETWEEN_CALLS_MS);
  }

  const totalSpendable = portfolio.reduce((acc, entry) => acc + entry.spendable, 0n);
  if (totalSpendable === 0n) {
    console.log(
      paint("No spendable SOL across wallets after reserves; redistribution skipped.", "muted")
    );
    return;
  }

  const walletCount = BigInt(portfolio.length);
  const baseShare = totalSpendable / walletCount;
  let remainder = totalSpendable % walletCount;

  const donors = [];
  const recipients = [];

  for (const entry of portfolio) {
    let desiredSpendable = baseShare;
    if (remainder > 0n) {
      desiredSpendable += 1n;
      remainder -= 1n;
    }
    const diff = entry.spendable - desiredSpendable;
    if (diff > 0n && diff >= MIN_TRANSFER_LAMPORTS) {
      donors.push({
        entry,
        surplus: diff,
      });
    } else if (diff < 0n) {
      const need = -diff;
      if (need >= MIN_TRANSFER_LAMPORTS) {
        recipients.push({
          entry,
          need,
        });
      }
    }
  }

  if (recipients.length === 0) {
    console.log(
      paint("All wallets already within target band after reserves; nothing to redistribute.", "muted")
    );
    return;
  }
  if (donors.length === 0) {
    console.log(
      paint("No wallets carry a surplus above the calculated target; redistribution skipped.", "muted")
    );
    return;
  }

  donors.sort((a, b) => (a.surplus === b.surplus ? 0 : a.surplus > b.surplus ? -1 : 1));
  recipients.sort((a, b) => (a.need === b.need ? 0 : a.need > b.need ? -1 : 1));

  console.log(
    paint(
      `Spendable pool ${formatBaseUnits(totalSpendable, 9)} SOL across ${wallets.length} wallet${wallets.length === 1 ? '' : 's'} (target ≈ ${formatBaseUnits(baseShare, 9)} SOL each, before reserves).`,
      "label"
    )
  );

  let totalTransferred = 0n;
  let transferCount = 0;
  let donorIdx = 0;
  let recipientIdx = 0;

  while (donorIdx < donors.length && recipientIdx < recipients.length) {
    const donor = donors[donorIdx];
    const recipient = recipients[recipientIdx];

    if (donor.surplus < MIN_TRANSFER_LAMPORTS) {
      donorIdx += 1;
      continue;
    }
    if (recipient.need < MIN_TRANSFER_LAMPORTS) {
      recipientIdx += 1;
      continue;
    }

    let amount = donor.surplus < recipient.need ? donor.surplus : recipient.need;
    if (amount < MIN_TRANSFER_LAMPORTS) {
      donorIdx += 1;
      continue;
    }

    const lamports = toSafeNumber(amount);
    let transferred = 0n;
    try {
      const transferConnection = createRpcConnection("confirmed");
      const sig = await sendSolTransfer(
        transferConnection,
        donor.entry.wallet.kp,
        recipient.entry.wallet.kp.publicKey,
        lamports
      );
      console.log(
        paint(
          `  ${donor.entry.wallet.name} → ${recipient.entry.wallet.name}: ${formatBaseUnits(amount, 9)} SOL — tx ${sig}`,
          "info"
        )
      );
      transferred = amount;
      totalTransferred += amount;
      transferCount += 1;
    } catch (err) {
      console.error(
        paint(
          `  Transfer ${donor.entry.wallet.name} → ${recipient.entry.wallet.name} failed: ${err.message}`,
          "error"
        )
      );
    }

    if (transferred === 0n) {
      donorIdx += 1;
      await balanceRpcDelay();
      continue;
    }

    donor.surplus -= transferred;
    recipient.need -= transferred;

    if (donor.surplus < MIN_TRANSFER_LAMPORTS) {
      donorIdx += 1;
    }
    if (recipient.need < MIN_TRANSFER_LAMPORTS) {
      recipientIdx += 1;
    }

    await balanceRpcDelay();
  }

  if (transferCount === 0) {
    console.log(
      paint(
        "Redistribution skipped: effective surplus remained below the minimum transfer threshold.",
        "muted"
      )
    );
    return;
  }

  console.log(
    paint(
      `Completed ${transferCount} transfer${transferCount === 1 ? "" : "s"} totalling ${formatBaseUnits(totalTransferred, 9)} SOL.`,
      "success"
    )
  );

  const summaryConnection = createRpcConnection("confirmed");
  console.log(paint("Post-redistribution SOL balances:", "label"));
  for (const entry of portfolio) {
    await balanceRpcDelay();
    const finalBalance = BigInt(
      await getSolBalance(summaryConnection, entry.wallet.kp.publicKey)
    );
    console.log(
      paint(
        `  ${entry.wallet.name}: ${formatBaseUnits(finalBalance, 9)} SOL`,
        "muted"
      )
    );
  }
}

// single send (explicit by file names)
async function sendSolBetween(fromWalletName, toWalletName, lamports) {
  const fromFp = path.join(KEYPAIR_DIR, fromWalletName);
  const toFp = path.join(KEYPAIR_DIR, toWalletName);
  if (!fs.existsSync(fromFp) || !fs.existsSync(toFp)) {
    console.error(paint("Invalid wallet names", "error"));
    return;
  }
  const fromKp = loadKeypairFromFile(fromFp);
  const toKp = loadKeypairFromFile(toFp);
  const connection = createRpcConnection("confirmed");
  const sig = await sendSolTransfer(connection, fromKp, toKp.publicKey, lamports);
  console.log(paint("Sent tx", "success"), sig);
}

// aggregate SOL from all wallets into one target wallet
async function aggregateSol(targetWalletName) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.error(paint("No wallets found in keypairs folder.", "error"));
    return;
  }

  let targetIndex = -1;
  try {
    const targetWallet = findWalletByName(targetWalletName);
    targetIndex = wallets.findIndex((w) => w.kp.publicKey.equals(targetWallet.kp.publicKey));
  } catch (err) {
    console.error(paint("Target wallet not found:", "error"), targetWalletName);
    return;
  }

  if (targetIndex === 0 && wallets.length === 1) {
    console.log(paint("Only one wallet present; nothing to aggregate.", "muted"));
    return;
  }

  console.log(
    paint(
      `Aggregating SOL backwards towards ${targetWalletName} — starting from wallet index ${wallets.length - 1}.`,
      "label"
    )
  );

  for (let idx = wallets.length - 1; idx > targetIndex; idx -= 1) {
    const donor = wallets[idx];
    const recipient = wallets[idx - 1];

    const connection = createRpcConnection("confirmed");
    const balance = BigInt(await getSolBalance(connection, donor.kp.publicKey));
    if (balance === 0n) {
      console.log(paint(`Skipping ${donor.name}: balance is zero.`, "muted"));
      continue;
    }
    let reserve = balance > GAS_RESERVE_LAMPORTS
      ? GAS_RESERVE_LAMPORTS
      : balance / 10n;
    if (reserve < MIN_TRANSFER_LAMPORTS && balance > MIN_TRANSFER_LAMPORTS) {
      reserve = MIN_TRANSFER_LAMPORTS;
    }
    const transferable = balance > reserve ? balance - reserve : 0n;
    if (transferable === 0n || transferable < MIN_TRANSFER_LAMPORTS) {
      console.log(
        paint(
          `Skipping ${donor.name}: transferable ${formatBaseUnits(transferable, 9)} SOL below minimum ${formatBaseUnits(MIN_TRANSFER_LAMPORTS, 9)} SOL.`,
          "muted"
        )
      );
      continue;
    }

    try {
      const lamports = toSafeNumber(transferable);
      const sig = await sendSolTransfer(
        connection,
        donor.kp,
        recipient.kp.publicKey,
        lamports
      );
      console.log(
        paint(
          `Moved ${formatBaseUnits(transferable, 9)} SOL from ${donor.name} -> ${recipient.name} — tx ${sig}`,
          "success"
        )
      );
    } catch (err) {
      console.error(paint(`Failed to move funds from ${donor.name}: ${err.message}`, "error"));
    }
    await delay(DELAY_BETWEEN_CALLS_MS);
  }

  const finalConnection = createRpcConnection("confirmed");
  const finalBalance = BigInt(
    await getSolBalance(finalConnection, wallets[targetIndex].kp.publicKey)
  );
  console.log(
    paint(
      `Aggregation complete. ${targetWalletName} now holds ${formatBaseUnits(finalBalance, 9)} SOL (pre-fee reserves remain in upstream wallets).`,
      "label"
    )
  );
}

/* --- Hierarchical Wallet Aggregation --- */

// aggregate SOL hierarchically: slaves send to their masters
async function aggregateHierarchical() {
  const wallets = listWallets();

  if (wallets.length === 0) {
    console.error(paint("No wallets found in keypairs folder.", "error"));
    return;
  }

  const hierarchy = walletRegistry.getHierarchySummary();
  console.log(
    paint(
      `Hierarchical aggregation: ${hierarchy.totalWallets} wallets in ${hierarchy.totalGroups} groups`,
      "label"
    )
  );

  let totalTransfers = 0;
  let totalAmount = 0n;

  // Process each group
  for (const group of hierarchy.groups) {
    if (!group.master || group.slaves.length === 0) {
      console.log(paint(`Group ${group.groupNumber}: No slaves to aggregate`, "muted"));
      continue;
    }

    const masterWallet = wallets.find(w => w.number === group.master.number);
    if (!masterWallet) {
      console.error(paint(`Master wallet #${group.master.number} not found`, "error"));
      continue;
    }

    console.log(
      paint(
        `\nGroup ${group.groupNumber}: Aggregating ${group.slaves.length} slaves → master #${group.master.number} (${group.master.filename})`,
        "info"
      )
    );

    // Transfer from each slave to master
    for (const slave of group.slaves) {
      const slaveWallet = wallets.find(w => w.number === slave.number);
      if (!slaveWallet) {
        console.error(paint(`  Slave wallet #${slave.number} not found`, "error"));
        continue;
      }

      const connection = createRpcConnection("confirmed");
      const balance = BigInt(await getSolBalance(connection, slaveWallet.kp.publicKey));

      if (balance === 0n) {
        console.log(paint(`  #${slave.number} (${slave.filename}): balance is zero`, "muted"));
        continue;
      }

      // Calculate reserve and transferable amount
      let reserve = balance > GAS_RESERVE_LAMPORTS
        ? GAS_RESERVE_LAMPORTS
        : balance / 10n;
      if (reserve < MIN_TRANSFER_LAMPORTS && balance > MIN_TRANSFER_LAMPORTS) {
        reserve = MIN_TRANSFER_LAMPORTS;
      }
      const transferable = balance > reserve ? balance - reserve : 0n;

      if (transferable === 0n || transferable < MIN_TRANSFER_LAMPORTS) {
        console.log(
          paint(
            `  #${slave.number} (${slave.filename}): transferable ${formatBaseUnits(transferable, 9)} SOL below minimum`,
            "muted"
          )
        );
        continue;
      }

      try {
        const lamports = toSafeNumber(transferable);
        const sig = await sendSolTransfer(
          connection,
          slaveWallet.kp,
          masterWallet.kp.publicKey,
          lamports
        );
        console.log(
          paint(
            `  ✓ #${slave.number} → #${group.master.number}: ${formatBaseUnits(transferable, 9)} SOL (tx: ${sig.slice(0, 8)}...)`,
            "success"
          )
        );
        totalTransfers++;
        totalAmount += transferable;
      } catch (err) {
        console.error(paint(`  ✗ Failed to transfer from #${slave.number}: ${err.message}`, "error"));
      }

      await delay(DELAY_BETWEEN_CALLS_MS);
    }
  }

  console.log(
    paint(
      `\nHierarchical aggregation complete: ${totalTransfers} transfers, ${formatBaseUnits(totalAmount, 9)} SOL total`,
      "label"
    )
  );
}

// aggregate masters to master-master (#1)
async function aggregateMasters() {
  const wallets = listWallets();

  if (wallets.length === 0) {
    console.error(paint("No wallets found in keypairs folder.", "error"));
    return;
  }

  const masterMaster = wallets.find(w => w.number === 1);
  if (!masterMaster) {
    console.error(paint("Master-master wallet #1 not found", "error"));
    return;
  }

  const groupMasters = walletRegistry.getGroupMasters();
  if (groupMasters.length === 0) {
    console.log(paint("No group masters to aggregate", "muted"));
    return;
  }

  console.log(
    paint(
      `Aggregating ${groupMasters.length} group masters → master-master #1 (${masterMaster.name})`,
      "label"
    )
  );

  let totalTransfers = 0;
  let totalAmount = 0n;

  for (const masterNum of groupMasters) {
    const masterWallet = wallets.find(w => w.number === masterNum);
    if (!masterWallet) {
      console.error(paint(`Master wallet #${masterNum} not found`, "error"));
      continue;
    }

    const connection = createRpcConnection("confirmed");
    const balance = BigInt(await getSolBalance(connection, masterWallet.kp.publicKey));

    if (balance === 0n) {
      console.log(paint(`  #${masterNum} (${masterWallet.name}): balance is zero`, "muted"));
      continue;
    }

    // Calculate reserve and transferable amount
    let reserve = balance > GAS_RESERVE_LAMPORTS
      ? GAS_RESERVE_LAMPORTS
      : balance / 10n;
    if (reserve < MIN_TRANSFER_LAMPORTS && balance > MIN_TRANSFER_LAMPORTS) {
      reserve = MIN_TRANSFER_LAMPORTS;
    }
    const transferable = balance > reserve ? balance - reserve : 0n;

    if (transferable === 0n || transferable < MIN_TRANSFER_LAMPORTS) {
      console.log(
        paint(
          `  #${masterNum} (${masterWallet.name}): transferable ${formatBaseUnits(transferable, 9)} SOL below minimum`,
          "muted"
        )
      );
      continue;
    }

    try {
      const lamports = toSafeNumber(transferable);
      const sig = await sendSolTransfer(
        connection,
        masterWallet.kp,
        masterMaster.kp.publicKey,
        lamports
      );
      console.log(
        paint(
          `  ✓ #${masterNum} → #1: ${formatBaseUnits(transferable, 9)} SOL (tx: ${sig.slice(0, 8)}...)`,
          "success"
        )
      );
      totalTransfers++;
      totalAmount += transferable;
    } catch (err) {
      console.error(paint(`  ✗ Failed to transfer from #${masterNum}: ${err.message}`, "error"));
    }

    await delay(DELAY_BETWEEN_CALLS_MS);
  }

  const finalConnection = createRpcConnection("confirmed");
  const finalBalance = BigInt(await getSolBalance(finalConnection, masterMaster.kp.publicKey));
  console.log(
    paint(
      `\nMaster aggregation complete: ${totalTransfers} transfers, ${formatBaseUnits(totalAmount, 9)} SOL total`,
      "label"
    )
  );
  console.log(
    paint(
      `Master-master #1 (${masterMaster.name}) now holds ${formatBaseUnits(finalBalance, 9)} SOL`,
      "success"
    )
  );
}

// direct transfer between wallets by number
async function transferBetweenWallets(fromIdentifier, toIdentifier, amount, token = 'SOL') {
  const wallets = listWallets();

  // Resolve from wallet
  let fromWallet;
  if (typeof fromIdentifier === 'number' || !isNaN(parseInt(fromIdentifier))) {
    const num = typeof fromIdentifier === 'number' ? fromIdentifier : parseInt(fromIdentifier);
    fromWallet = wallets.find(w => w.number === num);
    if (!fromWallet) {
      console.error(paint(`Source wallet #${num} not found`, "error"));
      return;
    }
  } else {
    fromWallet = wallets.find(w => w.name === fromIdentifier);
    if (!fromWallet) {
      console.error(paint(`Source wallet '${fromIdentifier}' not found`, "error"));
      return;
    }
  }

  // Resolve to wallet
  let toWallet;
  if (typeof toIdentifier === 'number' || !isNaN(parseInt(toIdentifier))) {
    const num = typeof toIdentifier === 'number' ? toIdentifier : parseInt(toIdentifier);
    toWallet = wallets.find(w => w.number === num);
    if (!toWallet) {
      console.error(paint(`Destination wallet #${num} not found`, "error"));
      return;
    }
  } else {
    toWallet = wallets.find(w => w.name === toIdentifier);
    if (!toWallet) {
      console.error(paint(`Destination wallet '${toIdentifier}' not found`, "error"));
      return;
    }
  }

  if (fromWallet.kp.publicKey.equals(toWallet.kp.publicKey)) {
    console.error(paint("Source and destination wallets are the same", "error"));
    return;
  }

  const connection = createRpcConnection("confirmed");

  if (token.toUpperCase() === 'SOL') {
    // Parse amount
    let lamports;
    if (amount === 'all') {
      const balance = BigInt(await getSolBalance(connection, fromWallet.kp.publicKey));
      const reserve = balance > GAS_RESERVE_LAMPORTS ? GAS_RESERVE_LAMPORTS : balance / 10n;
      lamports = toSafeNumber(balance > reserve ? balance - reserve : 0n);
    } else {
      lamports = Math.floor(parseFloat(amount) * 1e9);
    }

    if (lamports <= 0) {
      console.error(paint("Invalid transfer amount", "error"));
      return;
    }

    console.log(
      paint(
        `Transferring ${formatBaseUnits(BigInt(lamports), 9)} SOL: #${fromWallet.number} (${fromWallet.name}) → #${toWallet.number} (${toWallet.name})`,
        "info"
      )
    );

    try {
      const sig = await sendSolTransfer(
        connection,
        fromWallet.kp,
        toWallet.kp.publicKey,
        lamports
      );
      console.log(paint(`✓ Transfer successful (tx: ${sig})`, "success"));
    } catch (err) {
      console.error(paint(`✗ Transfer failed: ${err.message}`, "error"));
    }
  } else {
    console.error(paint("Token transfers not yet implemented. Only SOL is supported.", "error"));
  }
}

async function walletFundCommand(args = []) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    return;
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  const prompt = (question) =>
    new Promise((resolve) => rl.question(question, resolve));

  try {
    // Ask if user wants to see balances or go straight to transfer
    const modeInput = (await prompt(
      "\nTransfer mode: [1] Fast (skip balances) [2] Detailed (show balances) [default=1]: "
    )).trim();
    const showBalances = modeInput === "2";

    if (showBalances) {
      console.log(paint("\n=== Wallet Balances ===", "label"));
      const connection = createRpcConnection("confirmed");
      for (const wallet of wallets) {
        try {
          const solLamports = BigInt(await getSolBalance(connection, wallet.kp.publicKey));
          const label = wallet.number ? `#${wallet.number}` : "-";
          console.log(
            paint(
              `${label} ${wallet.name}: ${formatBaseUnits(solLamports, 9)} SOL`,
              "muted"
            )
          );
          const tokenBalances = await loadWalletTokenBalances(wallet);
          for (const entry of tokenBalances) {
            const amountRaw =
              typeof entry.amountRaw === "bigint"
                ? entry.amountRaw
                : BigInt(entry.amountRaw ?? 0);
            if (amountRaw <= 0n) continue;
            const symbol =
              entry.tokenRecord.symbol ||
              symbolForMint(entry.tokenRecord.mint) ||
              entry.tokenRecord.mint.slice(0, 4);
            console.log(
              paint(
                `    • ${symbol}: ${entry.amountDecimal ?? formatBaseUnits(amountRaw, entry.decimals)}`,
                "muted"
              )
            );
          }
        } catch (err) {
          console.warn(
            paint(
              `  Unable to fetch balances for ${wallet.name}: ${err.message || err}`,
              "warn"
            )
          );
        }
      }
    }

    // Prompt for transfer details
    const fromInput = (await prompt("\nFrom wallet (# or filename, blank to cancel): ")).trim();
    if (!fromInput) {
      console.log(paint("Cancelled.", "muted"));
      return;
    }
    const toInput = (await prompt("To wallet (# or filename): ")).trim();
    if (!toInput) {
      console.log(paint("Destination is required.", "warn"));
      return;
    }
    const amountInput = (await prompt("Amount (decimal or 'all', blank = all): ")).trim() || "all";

    let fromWallet;
    let toWallet;
    try {
      fromWallet = findWalletByName(fromInput);
    } catch (err) {
      console.error(paint(`Unknown source wallet '${fromInput}': ${err.message}`, "error"));
      return;
    }
    try {
      toWallet = findWalletByName(toInput);
    } catch (err) {
      console.error(paint(`Unknown destination wallet '${toInput}': ${err.message}`, "error"));
      return;
    }

    await transferBetweenWallets(fromWallet.name, toWallet.name, amountInput || "all", "SOL");
  } catch (err) {
    console.error(paint(`Funding aborted: ${err.message || err}`, "error"));
  } finally {
    rl.close();
  }
}

async function walletRedistributeCommand(args = []) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    return;
  }

  let anchorInput = args[0] || "";
  if (!anchorInput) {
    console.log(paint("\nRedistribute SOL from an anchor wallet across all wallets.", "label"));
    console.log(
      paint(
        "Enter the anchor wallet (number or filename). Leave blank to use wallet #1.",
        "muted"
      )
    );
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    anchorInput = (await new Promise((resolve) =>
      rl.question("Anchor wallet (# or filename): ", resolve)
    )).trim();
    rl.close();
  }

  let anchorWallet;
  try {
    anchorWallet = anchorInput
      ? findWalletByName(anchorInput)
      : wallets[0];
  } catch (err) {
    console.error(paint(`Unknown wallet '${anchorInput}': ${err.message}`, "error"));
    return;
  }

  console.log(
    paint(
      `\nRedistributing spendable SOL using anchor ${anchorWallet.name}...`,
      "info"
    )
  );
  await redistributeSol(anchorWallet.name);
}

async function walletAggregateCommand(args = []) {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found.", "muted"));
    return;
  }

  let targetInput = args[0] || "";
  if (!targetInput) {
    console.log(
      paint(
        "\nAggregate SOL backwards toward a target wallet (defaults to #1).",
        "label"
      )
    );
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    targetInput = (await new Promise((resolve) =>
      rl.question("Target wallet (# or filename): ", resolve)
    )).trim();
    rl.close();
  }

  let targetWallet;
  try {
    targetWallet = targetInput
      ? findWalletByName(targetInput)
      : wallets[0];
  } catch (err) {
    console.error(paint(`Unknown wallet '${targetInput}': ${err.message}`, "error"));
    return;
  }

  console.log(
    paint(
      `\nAggregating SOL toward ${targetWallet.name}...`,
      "info"
    )
  );
  await aggregateSol(targetWallet.name);
}

// airdrop single wallet (devnet)
async function airdrop(walletFile, lamports) {
  const fp = path.join(KEYPAIR_DIR, walletFile);
  if (!fs.existsSync(fp)) {
    console.error(paint("Wallet not found:", "error"), walletFile);
    return;
  }
  const kp = loadKeypairFromFile(fp);
  const connection = createRpcConnection("confirmed");
  console.log(paint("Requesting airdrop to", "info"), kp.publicKey.toBase58());
  const sig = await connection.requestAirdrop(kp.publicKey, lamports);
  await connection.confirmTransaction(sig, "confirmed");
  console.log(paint("Airdrop confirmed:", "success"), sig);
}

// airdrop all wallets
async function airdropAll(lamports) {
  const wallets = listWallets();
  for (const w of wallets) {
    const connection = createRpcConnection("confirmed");
    try {
      const sig = await connection.requestAirdrop(w.kp.publicKey, lamports);
      await connection.confirmTransaction(sig, "confirmed");
      console.log(
        paint(
          `Airdropped ${lamports} to ${w.name} ${w.kp.publicKey.toBase58()} tx ${sig}`,
          "success"
        )
      );
    } catch (e) {
      console.error(paint(`Airdrop failed for ${w.name}: ${e.message}`, "error"));
    }
    await delay(DELAY_BETWEEN_CALLS_MS);
  }
}

// ---- Balance display ----\n// Aggregates SOL + SPL holdings across every wallet. Explicit token arguments\n// ensure zero balances are surfaced (useful for monitoring positions).\n// show balances (optionally token)
async function showBalances(tokenArgs = []) {
  const wallets = listWallets();
  const walletMenuHotkey =
    formatHotkeyPrimaryKey("launcher", "wallet-tools") || "'w'";
  if (wallets.length === 0) {
    if (!fs.existsSync(KEYPAIR_DIR)) {
      console.log(paint(`No keypairs directory found at ${KEYPAIR_DIR}`, "warn"));
      console.log(
        paint(
          `Use wallet menu (hotkey ${walletMenuHotkey}) or 'generate <n> [prefix]' command to create wallets`,
          "info"
        )
      );
    } else {
      console.log(paint(`No wallets found in ${KEYPAIR_DIR}`, "muted"));
      console.log(
        paint(
          `Use wallet menu (hotkey ${walletMenuHotkey}) or 'generate <n> [prefix]' command to create wallets`,
          "info"
        )
      );
    }
    return;
  }

  const connection = createRpcConnection("confirmed");
  const solLamportsMap = await refreshWalletDisableStatus({
    connection,
    wallets,
    silent: true,
  });
  const hasExplicitTokens = Array.isArray(tokenArgs) && tokenArgs.length > 0;
  const requestedTokens = hasExplicitTokens
    ? (typeof tokenArgs[0] === "object" && tokenArgs[0] !== null && "mint" in tokenArgs[0]
        ? tokenArgs
        : parseTokenArgs(tokenArgs))
    : [];
  const globalTokenMeta = new Map();
  const walletTokenAmounts = new Map();
  const zeroBalanceDisplayMints = new Set();

  // Seed metadata with requested tokens so they appear even if balance is zero
  for (const token of requestedTokens) {
    const mint = token.mint;
    try {
      const meta = await resolveMintMetadata(connection, mint);
      const overrideSymbol = MINT_SYMBOL_OVERRIDES.get(mint) || null;
      const symbol = token.symbol || overrideSymbol || meta.symbol || KNOWN_MINTS.get(mint)?.symbol || mint.slice(0, 4);
      globalTokenMeta.set(mint, {
        mint,
        symbol,
        decimals: meta.decimals,
        programId: meta.programId,
      });
      zeroBalanceDisplayMints.add(mint);
    } catch (err) {
      console.warn(paint(`  warning: skipping token ${mint} — ${err.message || err}`, "warn"));
    }
  }

  for (const w of wallets) {
    const tokenMap = new Map();
    walletTokenAmounts.set(w.name, tokenMap);

    // populate requested tokens with zero amounts
    for (const [mint, meta] of globalTokenMeta.entries()) {
      tokenMap.set(mint, { amount: 0n, meta });
    }

    await balanceRpcDelay();
    const parsedAccounts = await getAllParsedTokenAccounts(
      connection,
      w.kp.publicKey
    );

    for (const { pubkey, account } of parsedAccounts) {
      const info = account.data.parsed.info;
      const mint = info.mint;
      if (SOL_LIKE_MINTS.has(mint)) continue;
      const amountRaw = BigInt(info.tokenAmount.amount);
      const decimals = info.tokenAmount.decimals;
      if (!hasExplicitTokens && amountRaw === 0n && !zeroBalanceDisplayMints.has(mint)) continue;

      let ownerProgramId = null;
      try {
        ownerProgramId = new PublicKey(account.owner);
      } catch (_) {}

      if (!globalTokenMeta.has(mint)) {
        const metaFallback = KNOWN_MINTS.get(mint) || {};
        const overrideSymbol = MINT_SYMBOL_OVERRIDES.get(mint) || null;
        const symbol = overrideSymbol || metaFallback.symbol || mint.slice(0, 4);
        globalTokenMeta.set(mint, {
          mint,
          symbol,
          decimals,
          programId: ownerProgramId || metaFallback.programId || TOKEN_PROGRAM_ID,
        });
      } else {
        const existing = globalTokenMeta.get(mint);
        if (!existing.programId && ownerProgramId) {
          existing.programId = ownerProgramId;
        }
        if (existing.programId?.equals && ownerProgramId && !existing.programId.equals(ownerProgramId)) {
          existing.programId = ownerProgramId;
        }
        if (existing.decimals === undefined && typeof decimals === "number") {
          existing.decimals = decimals;
        }
        globalTokenMeta.set(mint, existing);
      }

      const meta = globalTokenMeta.get(mint);
      tokenMap.set(mint, { amount: amountRaw, meta, accountPubkey: pubkey });
    }
  }

  for (const w of wallets) {
    let solLamports = solLamportsMap.get(w.name);
    if (solLamports === undefined) {
      await balanceRpcDelay();
      solLamports = BigInt(await getSolBalance(connection, w.kp.publicKey));
    }
    const solDisplay = formatBaseUnits(solLamports, 9);
    const isPrimaryCrew = w.name === "crew_1.json";
    const walletLine = `Wallet ${w.name}  ${w.kp.publicKey.toBase58()}`;
    console.log(paint(walletLine, isPrimaryCrew ? "success" : "label"));
    console.log(paint(`  SOL: ${solDisplay} SOL`, "info"));
    if (isWalletDisabledByGuard(w.name)) {
      console.log(
        paint(
          `  status: disabled for swaps (<${WALLET_DISABLE_THRESHOLD_LABEL} SOL). Fund and run balances or use force reset to re-enable.`,
          "warn"
        )
      );
    }

    const entries = Array.from(walletTokenAmounts.get(w.name)?.entries() || []);
    entries.sort((a, b) => {
      const symbolA = a[1].meta.symbol?.toUpperCase() || a[0];
      const symbolB = b[1].meta.symbol?.toUpperCase() || b[0];
      return symbolA.localeCompare(symbolB);
    });
    const displayEntries = entries.filter(([mint, { amount }]) =>
      amount > 0n ||
      (hasExplicitTokens && zeroBalanceDisplayMints.has(mint))
    );

    if (!hasExplicitTokens && displayEntries.length === 0) {
      console.log(paint("  No SPL token balances.", "muted"));
      continue;
    }

    for (const [mint, { amount, meta }] of displayEntries) {
      const formatted = formatBaseUnits(amount, meta.decimals);
      const zeroLabel =
        amount === 0n && hasExplicitTokens && zeroBalanceDisplayMints.has(mint)
          ? " (no balance)"
          : "";
      console.log(
        paint(
          `  ${meta.symbol || mint.slice(0, 4)}: ${formatted}${zeroLabel}  [${mint}]`,
          "muted"
        )
      );
    }
  }
}

function parseImportWalletArgs(rawArgs) {
  const opts = {
    prefix: "imported",
    path: null,
    passphrase: null,
    force: false,
    secret: null,
  };
  let positionalConsumed = false;
  for (let i = 0; i < rawArgs.length; i += 1) {
    const token = rawArgs[i];
    if (!token) continue;
    if (token === "--secret") {
      if (i + 1 >= rawArgs.length) throw new Error("import-wallet --secret requires a value");
      opts.secret = rawArgs[i + 1];
      i += 1;
      continue;
    }
    if (token === "--prefix" || token === "--name") {
      if (i + 1 >= rawArgs.length) throw new Error("import-wallet --prefix requires a value");
      opts.prefix = rawArgs[i + 1];
      i += 1;
      continue;
    }
    if (token === "--path") {
      if (i + 1 >= rawArgs.length) throw new Error("import-wallet --path requires a value");
      opts.path = rawArgs[i + 1];
      i += 1;
      continue;
    }
    if (token === "--passphrase") {
      if (i + 1 >= rawArgs.length) throw new Error("import-wallet --passphrase requires a value");
      opts.passphrase = rawArgs[i + 1];
      i += 1;
      continue;
    }
    if (token === "--force") {
      opts.force = true;
      continue;
    }
    if (!token.startsWith("--") && !positionalConsumed) {
      opts.secret = token;
      positionalConsumed = true;
      continue;
    }
    throw new Error(`Unknown argument for import-wallet: ${token}`);
  }
  if (!opts.secret || opts.secret.trim().length === 0) {
    throw new Error("import-wallet requires a secret key, JSON payload, or mnemonic phrase.");
  }
  if (opts.path && !/^m\//i.test(opts.path)) {
    throw new Error(`Invalid derivation path "${opts.path}". Paths must start with \"m/\".`);
  }
  return opts;
}

// ---- Auto-withdraw lend share tokens helper ----
// Detects if a token is a lend share token (jwSOL, etc.) and automatically
// withdraws it to the base asset before attempting swaps
async function autoWithdrawLendShareTokensForWallet(wallet, mint) {
  try {
    // Check if the mint is a lend share token by loading its metadata
    const tokenRecord = await resolveTokenRecord(mint);
    if (!tokenRecord) {
      return { withdrawn: false, reason: "token not found" };
    }

    if (!isLendShareToken(tokenRecord)) {
      return { withdrawn: false, reason: "not a share token" };
    }

    const symbol = tokenRecord.symbol || "unknown";
    console.log(
      paint(
        `  ${wallet.name}: detected lend share token ${symbol} (${mint}), attempting auto-withdrawal...`,
        "info"
      )
    );

    // Use the existing lendEarnTransferLike infrastructure to withdraw
    // We need to construct the args as if it were called from CLI
    const args = [wallet.name, mint, "*"];
    const options = { raw: false };

    try {
      // Call the withdraw action directly via lendEarnTransferLike
      await lendEarnTransferLike("withdraw", args, { valueField: "amount" });
      console.log(
        paint(
          `  ${wallet.name}: successfully withdrew ${symbol} from lend`,
          "success"
        )
      );
      return { withdrawn: true, symbol };
    } catch (withdrawErr) {
      console.warn(
        paint(
          `  ${wallet.name}: lend withdrawal failed for ${symbol}: ${withdrawErr.message || withdrawErr}`,
          "warn"
        )
      );
      return { withdrawn: false, reason: withdrawErr.message || "withdrawal failed" };
    }
  } catch (err) {
    return { withdrawn: false, reason: err.message || "detection failed" };
  }
}

// perform the swap across wallets (one swap each)
// ---- Core swap executor ----
// Performs a Jupiter swap across all wallets, handling per-wallet skips,
// retries, and amount selection. Optional arguments let callers reuse
// connections or silence skip logging (useful for multi-step chains).
async function doSwapAcross(inputMint, outputMint, amountInput, options = {}) {
  let walletList = Array.isArray(options.wallets) && options.wallets.length > 0
    ? options.wallets
    : listWallets();
  if (walletList.length === 0) {
    console.error(paint("No wallets found in keypairs folder.", "error"));
    return;
  }
  const quietSkips = options.quietSkips === true;
  const includeDisabled = options.includeDisabled === true;
  const failOnMinOutput = options.failOnMinOutput === true;
  if (!includeDisabled) {
    const disabledWithinScope = walletList.filter((w) =>
      isWalletDisabledByGuard(w.name)
    );
    if (disabledWithinScope.length > 0 && !quietSkips) {
      for (const disabledWallet of disabledWithinScope) {
        console.log(
          paint(
            `Skipping ${disabledWallet.name}: disabled for swaps (<${WALLET_DISABLE_THRESHOLD_LABEL} SOL).`,
            "muted"
          )
        );
      }
    }
    walletList = walletList.filter((w) => !isWalletDisabledByGuard(w.name));
    if (walletList.length === 0) {
      if (!quietSkips) {
        console.warn(
          paint(
            "All wallets disabled for swaps; run balances after funding or use force reset to re-enable.",
            "warn"
          )
        );
      }
      return;
    }
  }
  const explicitWalletDelayMs =
    options.walletDelayMs !== undefined
      ? Math.max(0, Number(options.walletDelayMs) || 0)
      : null;
  const resolveWalletDelayMs = (mode) =>
    explicitWalletDelayMs ?? (mode === "ultra" ? ULTRA_WALLET_DELAY_MS : DELAY_BETWEEN_CALLS_MS);
  const suppressMetadataLog = options.suppressMetadata === true;
  const walletSkipRegistryMap = options.walletSkipRegistryMap instanceof Map ? options.walletSkipRegistryMap : null;
  const sharedSkipRegistry = options.walletSkipRegistry instanceof Set ? options.walletSkipRegistry : null;
  const maxSlippageRetries = typeof options.maxSlippageRetries === "number"
    ? Math.max(0, options.maxSlippageRetries)
    : MAX_SLIPPAGE_RETRIES;
  const slippageBoostAfter = typeof options.slippageBoostAfter === "number"
    ? Math.max(1, options.slippageBoostAfter)
    : 2;
  const slippageBoostStrategy = typeof options.slippageBoostStrategy === "string"
    ? options.slippageBoostStrategy
    : "double";
  const slippageBoostIncrementBps = typeof options.slippageBoostIncrementBps === "number"
    ? Math.max(0, options.slippageBoostIncrementBps)
    : SLIPPAGE_BPS;
  const forcedRpcEndpoint =
    typeof options.forcedRpcEndpoint === "string" && options.forcedRpcEndpoint.trim().length > 0
      ? options.forcedRpcEndpoint.trim()
      : null;
  let swapEngineMode = USE_ULTRA_ENGINE ? "ultra" : "lite";
  let ultraUnavailableLogged = false;
  const metadataConnection = createRpcConnection("confirmed", forcedRpcEndpoint);
  let inputMeta;
  let outputMeta;
  try {
    inputMeta = await resolveMintMetadata(metadataConnection, inputMint);
  } catch (err) {
    throw new Error(`Failed to resolve input mint ${inputMint}: ${err.message || err}`);
  }
  try {
    outputMeta = await resolveMintMetadata(metadataConnection, outputMint);
  } catch (err) {
    throw new Error(`Failed to resolve output mint ${outputMint}: ${err.message || err}`);
  }
  const inputDecimals = inputMeta.decimals;

  let amountMode = DEFAULT_SWAP_AMOUNT_MODE;
  let desiredAmount = null;
  if (amountInput !== null && amountInput !== undefined) {
    const rawInput = amountInput.toString().trim();
    const lowered = rawInput.toLowerCase();
    if (lowered === "all") {
      amountMode = "all";
    } else if (lowered === "random") {
      amountMode = "random";
    } else if (rawInput.length > 0) {
      amountMode = "explicit";
      desiredAmount = decimalToBaseUnits(rawInput, inputDecimals);
    }
  }

  const randomizeAmounts = amountMode === "random";
  const useAllBalance = amountMode === "all";
  const useExplicitAmount = amountMode === "explicit";
  const inputMintPubkey = new PublicKey(inputMint);
  const outputMintPubkey = new PublicKey(outputMint);
  if (cachedAtaRentLamports === null) {
    const rent = await metadataConnection.getMinimumBalanceForRentExemption(165);
    cachedAtaRentLamports = BigInt(rent);
  }
  const ataRent = cachedAtaRentLamports;
  const wrapAccountRent = cachedAtaRentLamports;

  if (!suppressMetadataLog) {
    const engineLabel = swapEngineMode === "ultra" ? "[ULTRA]" : "[LITE]";
    console.log(
      paint(
        `Swap path ${engineLabel}: ${describeMintLabel(inputMint)} → ${describeMintLabel(outputMint)}`,
        "label"
      )
    );
    console.log(
      paint(
        `  input program ${inputMeta.programId?.toBase58?.() ?? "native"}, decimals ${inputMeta.decimals}; output program ${outputMeta.programId?.toBase58?.() ?? "native"}, decimals ${outputMeta.decimals}`,
        "muted"
      )
    );
  }

  for (const w of walletList) {
    let engine = swapEngineMode;
    let connection = createRpcConnection("confirmed", forcedRpcEndpoint);
    const getCurrentRpcEndpoint = () =>
      connection.__rpcEndpoint || connection._rpcEndpoint || "unknown";
    const markEndpoint = (reason, options = {}) => {
      const delegate = connection.__markUnhealthy;
      if (typeof delegate === "function") {
        delegate(reason, options);
      }
    };
    const rotateConnection = (reason, label) => {
      const previous = getCurrentRpcEndpoint();
      const isRateLimit = /rate[-\s]?limit/i.test(reason);
      markEndpoint(reason, {
        cooldownMs: isRateLimit ? RPC_RATE_LIMIT_COOLDOWN_MS : RPC_GENERAL_COOLDOWN_MS,
      });
      connection = createRpcConnection("confirmed", forcedRpcEndpoint);
      const next = getCurrentRpcEndpoint();
      const reasonLabel = reason || "rotation";
      const sameEndpoint = next === previous;
      const message = sameEndpoint
        ? `RPC ${previous} (${reasonLabel}) during ${label}; no healthy alternatives, retrying same endpoint`
        : `RPC ${previous} (${reasonLabel}) during ${label}; switched to ${next}`;
      if (quietSkips) {
        console.warn(paint(message, "warn"));
      } else {
        logWarn(`  ${message}`);
      }
      rpcLogged = false;
      return { previous, next };
    };
    const walletKey = w.kp.publicKey.toBase58();
    let walletSkipRegistry = null;
    if (sharedSkipRegistry) {
      walletSkipRegistry = sharedSkipRegistry;
    } else if (walletSkipRegistryMap) {
      walletSkipRegistry = walletSkipRegistryMap.get(walletKey);
      if (!walletSkipRegistry) {
        walletSkipRegistry = new Set();
        walletSkipRegistryMap.set(walletKey, walletSkipRegistry);
      }
    }
    let headerLogged = false;
    const ensureHeader = () => {
      if (!headerLogged) {
        console.log(paint(`\n>> wallet ${w.name} (${w.kp.publicKey.toBase58()})`, "label"));
        headerLogged = true;
      }
    };
    const logWarn = (message) => {
      ensureHeader();
      console.warn(paint(message, "warn"));
    };
    let ultraFallbackCount = 0;
    let liteFallbackActive = false;
    const logInfo = (message) => {
      ensureHeader();
      console.log(paint(message, "info"));
    };
    const logMuted = (message, value) => {
      // Suppress verbose logs in QUIET_MODE
      if (QUIET_MODE) {
        // Only log critical info in quiet mode - skip verbose debug logs
        const criticalKeywords = ['confirmed', 'error', 'failed', 'success', 'completed'];
        const msgLower = String(message).toLowerCase();
        const isCritical = criticalKeywords.some(keyword => msgLower.includes(keyword));
        if (!isCritical) {
          return;  // Skip non-critical logs in quiet mode
        }
      }
      ensureHeader();
      if (value === undefined) {
        console.log(paint(message, "muted"));
      } else {
        console.log(paint(message, "muted"), value);
      }
    };
    const logSuccess = (message, value) => {
      ensureHeader();
      if (value === undefined) {
        console.log(paint(message, "success"));
      } else {
        console.log(paint(message, "success"), value);
      }
    };
    const logError = (message, value) => {
      ensureHeader();
      if (value === undefined) {
        console.error(paint(message, "error"));
      } else {
        console.error(paint(message, "error"), value);
      }
    };
    let rpcLogged = false;
    const logRpcEndpoint = () => {
      if (rpcLogged) return;
      ensureHeader();
      console.log(paint(`  RPC endpoint: ${getCurrentRpcEndpoint()}`, "muted"));
      rpcLogged = true;
    };
    const runWithRpcRetry = async (label, fn) => {
      while (true) {
        try {
          return await fn();
        } catch (err) {
          if (isRateLimitError(err)) {
            rotateConnection(`rate-limit ${label}`, label);
            await delay(500);
            continue;
          }
          throw err;
        }
      }
    };
    if (walletSkipRegistry?.has(inputMint)) {
      if (!quietSkips) {
        logWarn(`  skipping: ${symbolForMint(inputMint)} previously marked empty for this wallet`);
      }
      continue;
    }
    try {
      let effectiveAmount = useExplicitAmount ? desiredAmount : 0n;
      let solBalanceLamports = await runWithRpcRetry("getBalance", async () =>
        BigInt(await getSolBalance(connection, w.kp.publicKey))
      );
      let tokenBalanceBefore = null;
      let requiresAtaCreation = false;
      if (!SOL_LIKE_MINTS.has(outputMint)) {
        const ataAddr = await getAssociatedTokenAddress(
          outputMintPubkey,
          w.kp.publicKey,
          false,
          outputMeta.programId || TOKEN_PROGRAM_ID,
          ASSOCIATED_TOKEN_PROGRAM_ID
        );
        const ataInfo = await runWithRpcRetry("getAccountInfo", () =>
          connection.getAccountInfo(ataAddr)
        );
        requiresAtaCreation = ataInfo === null;
        if (requiresAtaCreation && solBalanceLamports < ataRent + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS) {
          if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
          if (!quietSkips) {
            logWarn(
              `  skipping: insufficient SOL to create ATA (need ${(Number(ataRent + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS)/1e9).toFixed(6)} SOL, have ${(Number(solBalanceLamports)/1e9).toFixed(6)} SOL)`
            );
          }
          continue;
        }
      }

      if (SOL_LIKE_MINTS.has(inputMint)) {
        if (solBalanceLamports <= MIN_SOL_PER_SWAP_LAMPORTS) {
          if (!quietSkips) {
            logWarn("  skipping: SOL balance below minimum reserve");
          }
          continue;
        }
        let reserve = solBalanceLamports > GAS_RESERVE_LAMPORTS
          ? GAS_RESERVE_LAMPORTS
          : solBalanceLamports / 10n; // leave at least 10% for fees if balance is tiny
        if (requiresAtaCreation) {
          reserve += ataRent;
        }
        reserve += wrapAccountRent;
        reserve += JUPITER_SOL_BUFFER_LAMPORTS;
        if (reserve < MIN_SOL_PER_SWAP_LAMPORTS) {
          reserve = MIN_SOL_PER_SWAP_LAMPORTS;
        }
        if (!quietSkips) {
          logMuted(`  solBalance=${solBalanceLamports} rent=${ataRent} reserve=${reserve}`);
        }
        const maxSpendable = solBalanceLamports > reserve ? solBalanceLamports - reserve : 0n;
        if (!quietSkips) {
          logMuted(`  maxSpendable=${maxSpendable} (≈${formatBaseUnits(maxSpendable, 9)} SOL)`);
        }
        if (maxSpendable === 0n) {
          if (!quietSkips) {
            logWarn(
              `  skipping: balance ${formatBaseUnits(solBalanceLamports, 9)} SOL too small after reserving fees`
            );
          }
          continue;
        }
        if (randomizeAmounts) {
          let candidate = pickRandomPortion(maxSpendable);
          if (candidate >= maxSpendable && maxSpendable > 1n) {
            candidate = maxSpendable - 1n;
          }
          if (candidate <= 0n) {
            candidate = maxSpendable;
          }
          effectiveAmount = candidate;
        } else if (useAllBalance) {
          effectiveAmount = maxSpendable;
        }
        if (effectiveAmount > maxSpendable) {
          effectiveAmount = maxSpendable;
          console.log(
            paint(
              `  adjusted amount to ${formatBaseUnits(effectiveAmount, 9)} SOL to leave gas`,
              "muted"
            )
          );
        }
      } else {
        const tokenBalance = await runWithRpcRetry("getTokenBalance", () =>
          getTokenBalanceBaseUnits(
            connection,
            w.kp.publicKey,
            inputMintPubkey,
            inputMeta.programId
          )
        );
        if (tokenBalance === 0n) {
          if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
          if (!quietSkips) {
            logWarn("  skipping: no balance for input token");
          }
          continue;
        }
        tokenBalanceBefore = tokenBalance;
        if (requiresAtaCreation && solBalanceLamports < ataRent + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS) {
          if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
          if (!quietSkips) {
            logWarn(
              `  skipping: insufficient SOL to create ATA (need ${(Number(ataRent + GAS_RESERVE_LAMPORTS + ATA_CREATION_FEE_LAMPORTS)/1e9).toFixed(6)} SOL, have ${(Number(solBalanceLamports)/1e9).toFixed(6)} SOL)`
            );
          }
          continue;
        }
        if (randomizeAmounts) {
          let candidate = pickRandomPortion(tokenBalance);
          if (candidate >= tokenBalance && tokenBalance > 1n) {
            candidate = tokenBalance - 1n;
          }
          if (candidate <= 0n) {
            candidate = tokenBalance;
          }
          effectiveAmount = candidate;
        } else if (useAllBalance) {
          effectiveAmount = tokenBalance;
        }
        if (effectiveAmount > tokenBalance) {
          effectiveAmount = tokenBalance;
          console.log(
            paint(
              `  adjusted amount to available balance ${formatBaseUnits(effectiveAmount, inputDecimals)}`,
              "muted"
            )
          );
        }
      }
      if (effectiveAmount <= 0n) {
        if (!quietSkips) {
          logWarn("  skipping: effective amount is <= 0 after reserve adjustment");
        }
        continue;
      }
      // ensure ATA for outputMint
      if (!SOL_LIKE_MINTS.has(outputMint)) {
        try {
          await runWithRpcRetry("ensureAta", () =>
            ensureAta(
              connection,
              w.kp.publicKey,
              outputMintPubkey,
              w.kp,
              outputMeta.programId
            )
          );
        } catch (ataErr) {
          if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
          if (!quietSkips) {
            logWarn(`  skipping: cannot create ATA for output mint — ${ataErr.message}`);
          }
          continue;
        }
      }
      let amountRetry = 0;
      let slippageRetry = 0;
      let generalRetry = 0;
      let swapComplete = false;
      let currentSlippageBps = SLIPPAGE_BPS;
      let slippageBoosted = false;
      const trackSolChange = SOL_LIKE_MINTS.has(inputMint) || SOL_LIKE_MINTS.has(outputMint);
      while (!swapComplete) {
        try {
          const amountLabel = formatBaseUnits(effectiveAmount, inputDecimals);
          const amountPrefix = randomizeAmounts ? ' (random)' : '';
          logRpcEndpoint();
          logInfo(`  using amount${amountPrefix}: ${amountLabel}`);
          let networkSignature = null;

          if (engine === "ultra") {
            let orderInfo = null;
            for (let orderRetry = 0; ; orderRetry += 1) {
              try {
                orderInfo = await createUltraOrder({
                  inputMint,
                  outputMint,
                amountLamports: effectiveAmount,
                userPublicKey: w.kp.publicKey.toBase58(),
                slippageBps: currentSlippageBps,
                wrapAndUnwrapSol: SOL_LIKE_MINTS.has(inputMint) || SOL_LIKE_MINTS.has(outputMint),
                });
                break;
              } catch (orderErr) {
                const message = orderErr?.message || "";
                if (/(?:^|\D)(403|401)(?:\D|$)/.test(message)) {
                  markEndpoint(message);
                  logWarn("  Ultra API returned 401/403; verify JUPITER_ULTRA_API_KEY or JUPITER_ULTRA_API_BASE.");
                }
                if (/rate limit/i.test(message) && orderRetry < 4) {
                  logWarn(`  Ultra rate limit, retry ${orderRetry + 1}/4 after backoff`);
                  await delay(500 * (orderRetry + 1));
                  continue;
                }
                const status = orderErr?.status ?? null;
                const errorMessage = orderErr?.message || String(orderErr);

                // Fall back to Lite API for various Ultra API issues
                const shouldFallbackToLite =
                  engine === "ultra" && (
                    (status !== null && (status === 404 || status === 401 || status === 403)) ||
                    /missing swap transaction payload/i.test(errorMessage) ||
                    /failed to get quotes/i.test(errorMessage) ||
                    /no routes found/i.test(errorMessage)
                  );

                if (shouldFallbackToLite) {
                  const fallbackReason = (() => {
                    if (status === 404) return "order endpoint returned 404";
                    if (status === 401 || status === 403) return "order endpoint returned 401/403";
                    if (/missing swap transaction payload/i.test(errorMessage)) return "incomplete response";
                    if (/failed to get quotes/i.test(errorMessage)) return "failed to get quotes";
                    if (/no routes found/i.test(errorMessage)) return "no routes found";
                    return errorMessage.substring(0, 60) || "unknown Ultra error";
                  })();

                  if (ultraFallbackCount < MAX_ULTRA_FALLBACK_RETRIES) {
                    ultraFallbackCount += 1;
                    const backoffMs = Math.min(
                      2000,
                      ULTRA_RETRY_BACKOFF_BASE_MS * Math.pow(2, ultraFallbackCount - 1)
                    );
                    logWarn(
                      `  Ultra issue (${fallbackReason}); retrying Ultra ${ultraFallbackCount}/${MAX_ULTRA_FALLBACK_RETRIES} before falling back.`
                    );
                    await delay(backoffMs);
                    continue;
                  }

                  if (!ultraUnavailableLogged) {
                    logWarn(`  Ultra issue (${fallbackReason}); falling back to legacy Lite API.`);
                    ultraUnavailableLogged = true;
                  }
                  liteFallbackActive = true;
                  engine = "lite";
                  swapEngineMode = "lite";
                  orderInfo = null;
                  break;
                }
                throw orderErr;
              }
            }
            if (engine === "lite" && orderInfo === null) {
              continue;
            }

            if (engine === "ultra") {
              ultraFallbackCount = 0;
            }
            if (orderInfo.outAmount) {
              logMuted("  expected outAmount:", orderInfo.outAmount);
            }
            if (orderInfo.clientOrderId) {
              logMuted("  clientOrderId:", orderInfo.clientOrderId);
            }
            if (orderInfo.payload) {
              try {
                const preview = JSON.stringify(orderInfo.payload);
                const snippet = preview.length > 400 ? `${preview.slice(0, 400)}…` : preview;
                logMuted("  ultra order payload:", snippet);
              } catch (_) {}
            }

            const txbuf = Buffer.from(orderInfo.swapTransaction, "base64");
            const vtx = VersionedTransaction.deserialize(txbuf);
            vtx.sign([w.kp]);
            const rawSigned = vtx.serialize();
            const signedBase64 = Buffer.from(rawSigned).toString("base64");
            const derivedSignature = bs58.encode(vtx.signatures[0]);

            const executeDelayMs = explicitWalletDelayMs ?? ULTRA_EXECUTE_DELAY_MS;
            if (executeDelayMs > 0) {
              await delay(executeDelayMs);
            }
            const executeResult = await executeUltraSwap({
              signedTransaction: signedBase64,
              clientOrderId: orderInfo.clientOrderId,
              signatureHint: derivedSignature,
            });
            networkSignature = executeResult.signature || derivedSignature;
            logSuccess("  executed tx:", networkSignature);
            if (executeResult.payload) {
              try {
                const preview = JSON.stringify(executeResult.payload);
                const snippet = preview.length > 400 ? `${preview.slice(0, 400)}…` : preview;
                logMuted("  ultra execute payload:", snippet);
              } catch (_) {}
            }
          } else {
            let quote;
            for (let quoteRetry = 0; ; quoteRetry += 1) {
              try {
                quote = await fetchLegacyQuote(
                  inputMint,
                  outputMint,
                  effectiveAmount,
                  w.kp.publicKey.toBase58(),
                  currentSlippageBps
                );
                break;
              } catch (quoteErr) {
                const qMsg = quoteErr?.message || '';
                if (/(?:^|\D)(403|401)(?:\D|$)/.test(qMsg)) {
                  markEndpoint(qMsg);
                }
                if (/rate limit/i.test(qMsg) && quoteRetry < 4) {
                  logWarn(`  Jupiter rate limit, retry ${quoteRetry + 1}/4 after backoff`);
                  await delay(500 * (quoteRetry + 1));
                  continue;
                }
                throw quoteErr;
              }
            }

            if (quote.outAmount) {
              logMuted("  expected outAmount:", quote.outAmount);
            }

            const liteSubmitDelayMs = explicitWalletDelayMs ?? DELAY_BETWEEN_CALLS_MS;
            if (liteSubmitDelayMs > 0) {
              await delay(liteSubmitDelayMs);
            }
            const swapResp = await fetchLegacySwap(
              quote,
              w.kp.publicKey.toBase58(),
              SOL_LIKE_MINTS.has(inputMint) || SOL_LIKE_MINTS.has(outputMint)
            );
            const txbase64 = swapResp.swapTransaction;
            const txbuf = Buffer.from(txbase64, "base64");
            const vtx = VersionedTransaction.deserialize(txbuf);
            vtx.sign([w.kp]);
            const raw = vtx.serialize();
            const sig = await runWithRpcRetry("sendRawTransaction", () =>
              connection.sendRawTransaction(raw)
            );
            networkSignature = sig;
            logSuccess("  sent tx:", sig);
          }

          // Enhanced confirmation with better error handling
          let confirmed = false;
          const confirmRetryManager = new SwapRetryManager({ maxRetries: 3, baseDelay: 2000 });
          let confirmRetryCount = 0;
          
          while (!confirmed && confirmRetryCount <= 3) {
            try {
              await runWithRpcRetry("confirmTransaction", () =>
                connection.confirmTransaction(networkSignature, "confirmed")
              );
              confirmed = true;
              logSuccess("  confirmed");
            } catch (confirmErr) {
              const errorInfo = classifySwapError(confirmErr);
              
              if (errorInfo.type === 'confirmation' && confirmRetryManager.shouldRetry(errorInfo.type, confirmRetryCount)) {
                confirmRetryCount++;
                const retryDelayMs = confirmRetryManager.getRetryDelay(confirmRetryCount - 1);
                logWarn(`  ${confirmRetryManager.formatRetryMessage(errorInfo.type, confirmRetryCount, 3, errorInfo)}`);
                
                // Try alternative confirmation method
                try {
                  const statusResp = await runWithRpcRetry("getSignatureStatuses", () =>
                    connection.getSignatureStatuses([networkSignature])
                  );
                  const statusInfo = statusResp?.value?.[0] || null;
                  const confirmedStatus = statusInfo && (
                    statusInfo.confirmationStatus === "confirmed" ||
                    statusInfo.confirmationStatus === "finalized" ||
                    (typeof statusInfo.confirmations === "number" && statusInfo.confirmations >= 1)
                  );
                  if (confirmedStatus) {
                    logSuccess("  confirmed (via status check)");
                    confirmed = true;
                    break;
                  }
                } catch (statusErr) {
                  logWarn(`  status check failed: ${statusErr.message || statusErr}`);
                }
                
                await delay(retryDelayMs);
                continue;
              }
              
              // If not retryable or max retries reached, throw
              throw confirmErr;
            }
          }
          
          if (!confirmed) {
            throw new Error(`Transaction ${networkSignature} not confirmed after ${confirmRetryCount} attempts`);
          }
          if (trackSolChange) {
            const solAfterSwap = await runWithRpcRetry("getBalance", async () =>
              BigInt(await getSolBalance(connection, w.kp.publicKey))
            );
            const solDelta = solAfterSwap - solBalanceLamports;
            logMuted(
              `  SOL balance: ${formatBaseUnits(solBalanceLamports, 9)} → ${formatBaseUnits(solAfterSwap, 9)} (Δ ${formatLamportsDelta(solDelta)})`
            );
            solBalanceLamports = solAfterSwap;
          }
          if (!SOL_LIKE_MINTS.has(inputMint)) {
            const tokenBalanceAfter = await runWithRpcRetry("getTokenBalance", () =>
              getTokenBalanceBaseUnits(
                connection,
                w.kp.publicKey,
                inputMintPubkey,
                inputMeta.programId
              )
            );
            const tokenDelta = tokenBalanceBefore - tokenBalanceAfter;
            if (tokenDelta <= 0n) {
              walletSkipRegistry?.add(inputMint);
              logWarn("  balance unchanged after swap; marking token as skipped");
              break;
            }
            tokenBalanceBefore = tokenBalanceAfter;
          }
          swapComplete = true;
          if (liteFallbackActive && USE_ULTRA_ENGINE) {
            swapEngineMode = "ultra";
            liteFallbackActive = false;
          }
        } catch (innerErr) {
          const errorInfo = classifySwapError(innerErr);

          if (errorInfo.type === 'min_output') {
            if (failOnMinOutput) {
              throw innerErr;
            }
            if (!quietSkips) {
              logWarn(`  skipping: ${errorInfo.message || "minimum output not satisfied"}`);
            }
            if (!SOL_LIKE_MINTS.has(inputMint)) {
              walletSkipRegistry?.add(inputMint);
            }
            break;
          }

          if (engine === "ultra") {
            const status = innerErr?.status ?? null;
            const message = innerErr?.message || "";
            if (status === 404 || status === 401 || status === 403) {
              const fallbackReason =
                status === 404
                  ? "execute endpoint returned 404"
                  : "execute endpoint returned 401/403";
              if (ultraFallbackCount < MAX_ULTRA_FALLBACK_RETRIES) {
                ultraFallbackCount += 1;
                const backoffMs = Math.min(
                  2000,
                  ULTRA_RETRY_BACKOFF_BASE_MS * Math.pow(2, ultraFallbackCount - 1)
                );
                logWarn(
                  `  Ultra issue (${fallbackReason}); retrying Ultra ${ultraFallbackCount}/${MAX_ULTRA_FALLBACK_RETRIES} before falling back.`
                );
                await delay(backoffMs);
                continue;
              }
              if (!ultraUnavailableLogged) {
                logWarn(`  Ultra issue (${fallbackReason}); falling back to legacy Lite API.`);
                ultraUnavailableLogged = true;
              }
              liteFallbackActive = true;
              engine = "lite";
              swapEngineMode = "lite";
              continue;
            }
          }
          
          // Handle RPC authentication issues
          if (errorInfo.type === 'rpc_auth') {
            markEndpoint(errorInfo.message);
            rotateConnection("rpc-auth", "swap attempt");
            await delay(500);
            continue;
          }
          
          // Handle rate limiting
          if (errorInfo.type === 'rate_limit') {
            rotateConnection("rate-limit swap", "swap attempt");
            await delay(500);
            continue;
          }
          
          // Handle insufficient funds with amount reduction
          if (errorInfo.type === 'insufficient_funds' && SOL_LIKE_MINTS.has(inputMint)) {
            if (amountRetry < JUPITER_SOL_MAX_RETRIES && effectiveAmount > JUPITER_SOL_RETRY_DELTA_LAMPORTS) {
              const reduction = JUPITER_SOL_RETRY_DELTA_LAMPORTS < effectiveAmount
                ? JUPITER_SOL_RETRY_DELTA_LAMPORTS
                : effectiveAmount / 2n;
              effectiveAmount = effectiveAmount - reduction;
              
              if (effectiveAmount <= 0n) {
                if (!quietSkips) {
                  logWarn("  skipping: unable to retain enough SOL after retry adjustments");
                }
                throw innerErr;
              }
              
              amountRetry += 1;
              logWarn(
                `  retry ${amountRetry}/${JUPITER_SOL_MAX_RETRIES}: reducing SOL spend to ${formatBaseUnits(effectiveAmount, inputDecimals)} to satisfy lamport reserve`
              );
              await delay(DELAY_BETWEEN_CALLS_MS);
              continue;
            }
          }
          
          // Handle simulation failures
          if (errorInfo.type === 'simulation') {
            generalRetry += 1;
            if (generalRetry <= GENERAL_SIMULATION_RETRY_LIMIT) {
              logWarn(
                `  retry ${generalRetry}/${GENERAL_SIMULATION_RETRY_LIMIT}: simulation error; reattempting same amount`
              );
              await delay(DELAY_BETWEEN_CALLS_MS);
              continue;
            }
            
            // Reduce amount after max simulation retries
            const reductionBps = BigInt(10000 - GENERAL_SIMULATION_REDUCTION_BPS);
            let reduced = (effectiveAmount * reductionBps) / 10000n;
            if (reduced >= effectiveAmount && effectiveAmount > 1n) {
              reduced = effectiveAmount - 1n;
            }
            if (reduced <= 0n) {
              if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
              logWarn('  skipping: amount became too small after simulation retries');
              throw innerErr;
            }
            logWarn(
              `  reducing amount after ${generalRetry} simulation failures; new amount ${formatBaseUnits(reduced, inputDecimals)}`
            );
            effectiveAmount = reduced;
            generalRetry = 0;
            await delay(DELAY_BETWEEN_CALLS_MS);
            continue;
          }
          
          // Handle slippage issues
          if (errorInfo.type === 'slippage' && slippageRetry < maxSlippageRetries) {
            slippageRetry += 1;
            if (!slippageBoosted && slippageRetry >= slippageBoostAfter) {
              slippageBoosted = true;
              if (slippageBoostStrategy === "add") {
                currentSlippageBps = Math.max(1, currentSlippageBps + slippageBoostIncrementBps);
              } else if (slippageBoostStrategy === "set") {
                currentSlippageBps = Math.max(1, slippageBoostIncrementBps);
              } else {
                currentSlippageBps = Math.max(currentSlippageBps * 2, SLIPPAGE_BPS * 2);
              }
              logWarn(
                `  retry ${slippageRetry}/${maxSlippageRetries}: boosting slippage to ${currentSlippageBps} bps`
              );
            } else {
              logWarn(
                `  retry ${slippageRetry}/${maxSlippageRetries}: Jupiter reported slippage tolerance exceeded; refetching route`
              );
            }
            await delay(DELAY_BETWEEN_CALLS_MS);
            continue;
          }
          
          // Handle non-retryable errors
          if (errorInfo.type === 'account_issue' || errorInfo.type === 'no_route') {
            if (!quietSkips) {
              logWarn(`  skipping: ${errorInfo.message}`);
            }
            if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
            break;
          }
          
          // If we get here, it's an unhandled error - throw it
          throw innerErr;
        }
      }
    } catch (e) {
      const errorInfo = classifySwapError(e);

      // Check if the error is about a non-tradable token (lend share token)
      const errorMsg = (errorInfo.message || e.message || "").toLowerCase();
      if (errorMsg.includes("not tradable") || errorMsg.includes("not tradeable")) {
        // Skip lend share tokens - user can manually withdraw via 'lend withdraw' command
        logWarn(`  ${w.name}: skipping lend share token (not tradable via swap API). Use 'lend withdraw' to redeem.`);
        if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
        break; // Skip this wallet for this token
      }

      if (errorInfo.type === 'min_output') {
        if (failOnMinOutput) {
          throw e;
        }
        if (!quietSkips) {
          logWarn(`  skipping: ${errorInfo.message || "minimum output not satisfied"}`);
        }
        if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
        continue;
      }

      // Handle rate limiting at outer level
      if (errorInfo.type === 'rate_limit') {
        rotateConnection("rate-limit outer", "outer swap stage");
        await delay(500);
        continue;
      }
      
      // Handle RPC authentication issues
      if (errorInfo.type === 'rpc_auth') {
        markEndpoint(errorInfo.message);
        rotateConnection("rpc-auth outer", "outer swap stage");
        await delay(500);
        continue;
      }
      
      // Handle route not found
      if (errorInfo.type === 'no_route') {
        if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
        logWarn(`  skipping: ${errorInfo.message}`);
        continue;
      }
      
      // Log error details with better formatting
      const sanitised = typeof errorInfo.message === "string"
        ? errorInfo.message.replace(/Catch the `SendTransactionError`.*$/s, "").trim()
        : errorInfo.message;
      
      logError(`  swap error (${errorInfo.type}):`, sanitised);
      
      // Log additional context if available
      if (e.logs && Array.isArray(e.logs) && e.logs.length > 0) {
        logMuted("  transaction logs:", e.logs.slice(0, 3).join(" | "));
        if (e.logs.length > 3) {
          logMuted(`  ... and ${e.logs.length - 3} more log entries`);
        }
      } else if (e.response?.data) {
        logMuted("  API response:", JSON.stringify(e.response.data, null, 2));
      } else if (e.stack) {
        const stackLines = e.stack.split("\n").slice(0, 3);
        logMuted("  stack trace:", stackLines.join("\n"));
      }
      
      // Mark token as problematic if it's not SOL
      if (!SOL_LIKE_MINTS.has(inputMint)) walletSkipRegistry?.add(inputMint);
    }
    const postWalletDelay = resolveWalletDelayMs(engine);
    if (postWalletDelay > 0) {
      await delay(postWalletDelay);
    }
  }
}

function describeEnumVariant(enumObject) {
  if (!enumObject || typeof enumObject !== "object") return "unknown";
  for (const [key, value] of Object.entries(enumObject)) {
    if (value === null || value === undefined) return key.toLowerCase();
    if (value === true) return key.toLowerCase();
    if (typeof value === "object") return key.toLowerCase();
  }
  const keys = Object.keys(enumObject);
  return keys.length ? keys[0].toLowerCase() : "unknown";
}

function printPerpsUsage() {
  console.log(paint("Perpetuals command usage:", "label"));
  console.log(
    paint(
      "  perps positions [walletName ... | --wallet <walletName>]",
      "muted"
    )
  );
  console.log(paint("  perps funding", "muted"));
  console.log(
    paint(
      "  perps increase <walletName> --custody <symbol|address> --collateral <symbol|address> --side <long|short> --size-usd <amount> --collateral-amount <amount> --price-slippage <amount> [--min-out <amount>] [--input-mint <mint>] [--compute-price <microLamports>] [--compute-units <units>] [--referral <pubkey>] [--skip-sim]",
      "muted"
    )
  );
  console.log(
    paint(
      "  perps decrease <walletName> --position <pubkey> --price-slippage <amount> [--collateral-usd <amount>] [--size-usd <amount>] [--desired <mint>] [--min-out <amount>] [--entire] [--compute-price <microLamports>] [--compute-units <units>] [--referral <pubkey>] [--skip-sim]",
      "muted"
    )
  );
  console.log(
    paint(
      `  Known custody symbols: ${perpsKnownCustodyLabels()}`,
      "muted"
    )
  );
}

function ensurePerpsProgramMatchesConfig(connection) {
  const program = getPerpsProgram(connection);
  const resolvedProgramId = program?.programId;
  if (!resolvedProgramId) {
    throw new Error(
      "Unable to resolve the Jupiter perps program ID from the Anchor program"
    );
  }
  const expectedProgramId = getPerpsProgramId();
  if (!resolvedProgramId.equals(expectedProgramId)) {
    throw new Error(
      `Perps program mismatch: Anchor loaded ${resolvedProgramId.toBase58()} but resolver returned ${expectedProgramId.toBase58()}`
    );
  }
  const configuredProgramId = JUPITER_PERPS_PROGRAM_ID;
  if (
    configuredProgramId &&
    resolvedProgramId.toBase58() !== configuredProgramId
  ) {
    throw new Error(
      `Perps program mismatch: configured ${configuredProgramId} but loaded ${resolvedProgramId.toBase58()}`
    );
  }
  return program;
}

async function perpsPositionsCommand(rawArgs) {
  const wallets = listWallets();
  if (!wallets.length) {
    console.log(
      paint(
        "Perps positions aborted: no wallets found in keypairs directory.",
        "warn"
      )
    );
    return;
  }
  const selectedNames = [];
  for (let i = 0; i < rawArgs.length; i += 1) {
    const token = rawArgs[i];
    if (!token) continue;
    if (token === "--wallet" || token === "-w") {
      if (i + 1 >= rawArgs.length) {
        throw new Error("perps positions --wallet requires a wallet name");
      }
      selectedNames.push(rawArgs[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--help" || token === "-h") {
      printPerpsUsage();
      return;
    }
    if (token.startsWith("--")) {
      throw new Error(`perps positions: unknown flag ${token}`);
    }
    selectedNames.push(token);
  }
  const targetWallets = selectedNames.length
    ? selectedNames.map((name) => {
        const match = wallets.find((entry) => entry.name === name);
        if (!match) {
          throw new Error(`perps positions: wallet ${name} not found`);
        }
        return match;
      })
    : wallets;
  if (!targetWallets.length) {
    console.log(paint("Perps positions aborted: no wallets selected.", "warn"));
    return;
  }
  let connection = createRpcConnection("confirmed");
  const getEndpoint = () =>
    connection.__rpcEndpoint || connection._rpcEndpoint || DEFAULT_RPC_URL;
  const rotateConnection = (reason) => {
    const previous = getEndpoint();
    if (typeof connection.__markUnhealthy === "function") {
      connection.__markUnhealthy(reason);
    }
    connection = createRpcConnection("confirmed");
    const next = getEndpoint();
    console.warn(paint(`RPC ${previous} ${reason}; switched to ${next}`, "warn"));
  };
  const runWithRpcRetry = async (label, fn) => {
    while (true) {
      try {
        return await fn();
      } catch (err) {
        if (isRateLimitError(err)) {
          rotateConnection(`rate-limit ${label}`);
          await delay(500);
          continue;
        }
        throw err;
      }
    }
  };
  ensurePerpsProgramMatchesConfiguration(connection);
  const ownerPubkeys = targetWallets.map((wallet) => wallet.kp.publicKey);
  const ownerResults = await runWithRpcRetry("fetch positions", () =>
    fetchPositionsForOwners(connection, ownerPubkeys)
  );
  const custodySet = new Set();
  for (const { positions } of ownerResults) {
    for (const entry of positions) {
      custodySet.add(entry.account.custody.toBase58());
      custodySet.add(entry.account.collateralCustody.toBase58());
    }
  }
  const custodyMap = await runWithRpcRetry("fetch custodies", () =>
    fetchCustodyAccounts(connection, Array.from(custodySet))
  );
  const ownerPositionMap = new Map();
  for (const entry of ownerResults) {
    ownerPositionMap.set(entry.owner.toBase58(), entry.positions);
  }
  console.log(
    paint(
      `Perps positions across ${targetWallets.length} wallet(s).`,
      "label"
    )
  );
  for (const wallet of targetWallets) {
    const positions = ownerPositionMap.get(wallet.kp.publicKey.toBase58()) || [];
    console.log(
      paint(
        `\nWallet ${wallet.name} (${wallet.kp.publicKey.toBase58()})`,
        "info"
      )
    );
    if (!positions.length) {
      console.log(paint("  No open perps positions.", "muted"));
      continue;
    }
    positions.sort((a, b) => {
      const timeA = Number(bnToBigInt(a.account.updateTime));
      const timeB = Number(bnToBigInt(b.account.updateTime));
      return timeB - timeA;
    });
    for (const position of positions) {
      const custodyKey = position.account.custody.toBase58();
      const collateralKey = position.account.collateralCustody.toBase58();
      const custodyInfo = custodyMap.get(custodyKey);
      const collateralInfo = custodyMap.get(collateralKey);
      const marketMint = custodyInfo?.account?.mint?.toBase58() || custodyKey;
      const collateralMint =
        collateralInfo?.account?.mint?.toBase58() || collateralKey;
      const marketSymbol = symbolForMint(marketMint);
      const collateralSymbol = symbolForMint(collateralMint);
      const sideLabel = extractSideLabel(position.account.side).toUpperCase();
      const sizeUsd = formatBaseUnits(bnToBigInt(position.account.sizeUsd), 6);
      const collateralUsd = formatBaseUnits(
        bnToBigInt(position.account.collateralUsd),
        6
      );
      const entryPrice = formatBaseUnits(bnToBigInt(position.account.price), 6);
      const marketDecimals = custodyInfo?.account?.decimals ?? 9;
      const lockedAmount = formatBaseUnits(
        bnToBigInt(position.account.lockedAmount),
        marketDecimals
      );
      const realisedPnl = formatSignedBaseUnits(
        bnToBigInt(position.account.realisedPnlUsd),
        6
      );
      console.log(
        paint(
          `  Position ${position.publicKey.toBase58()} — ${marketSymbol} ${sideLabel}`,
          sideLabel === "LONG" ? "success" : "warn"
        )
      );
      console.log(
        paint(
          `    size: ${sizeUsd} USD | collateral: ${collateralUsd} USD`,
          "muted"
        )
      );
      console.log(
        paint(
          `    entry price: ${entryPrice} USD | locked: ${lockedAmount} ${marketSymbol}`,
          "muted"
        )
      );
      console.log(
        paint(
          `    realised PnL: ${realisedPnl} USD | updated: ${formatTimestampSeconds(position.account.updateTime)}`,
          "muted"
        )
      );
      console.log(
        paint(
          `    custody: ${marketSymbol} [${marketMint}] | collateral: ${collateralSymbol} [${collateralMint}]`,
          "muted"
        )
      );
    }
  }
}

async function perpsFundingCommand(rawArgs) {
  for (const token of rawArgs) {
    if (!token) continue;
    if (token === "--help" || token === "-h") {
      printPerpsUsage();
      return;
    }
    if (token.startsWith("--")) {
      throw new Error(`perps funding: unknown flag ${token}`);
    }
  }
  let connection = createRpcConnection("confirmed");
  const getEndpoint = () =>
    connection.__rpcEndpoint || connection._rpcEndpoint || DEFAULT_RPC_URL;
  const rotateConnection = (reason) => {
    const previous = getEndpoint();
    if (typeof connection.__markUnhealthy === "function") {
      connection.__markUnhealthy(reason);
    }
    connection = createRpcConnection("confirmed");
    const next = getEndpoint();
    console.warn(paint(`RPC ${previous} ${reason}; switched to ${next}`, "warn"));
  };
  const runWithRpcRetry = async (label, fn) => {
    while (true) {
      try {
        return await fn();
      } catch (err) {
        if (isRateLimitError(err)) {
          rotateConnection(`rate-limit ${label}`);
          await delay(500);
          continue;
        }
        throw err;
      }
    }
  };
  ensurePerpsProgramMatchesConfiguration(connection);
  const { account: pool } = await runWithRpcRetry("fetch pool", () =>
    fetchPoolAccount(connection)
  );
  const custodyPubkeys = pool.custodies.map((pk) => pk.toBase58());
  const custodyMap = await runWithRpcRetry("fetch custodies", () =>
    fetchCustodyAccounts(connection, custodyPubkeys)
  );
  console.log(
    paint(
      `Perps funding overview for ${custodyMap.size} custody account(s).`,
      "label"
    )
  );
  for (const entry of custodyMap.values()) {
    const account = entry.account;
    const mint = account.mint.toBase58();
    const symbol = symbolForMint(mint);
    const decimals = account.decimals;
    const locked = formatBaseUnits(bnToBigInt(account.assets.locked), decimals);
    const owned = formatBaseUnits(bnToBigInt(account.assets.owned), decimals);
    const fees = formatBaseUnits(bnToBigInt(account.assets.feesReserves), decimals);
    const hourlyDbps = bnToBigInt(account.fundingRateState.hourlyFundingDbps);
    const hourlyPercent = convertDbpsToHourlyRate(hourlyDbps) * 100;
    const dailyPercent = hourlyPercent * 24;
    const annualPercent = dailyPercent * 365;
    const increaseBps = Number(bnToBigInt(account.increasePositionBps));
    const decreaseBps = Number(bnToBigInt(account.decreasePositionBps));
    const targetRatioBps = Number(bnToBigInt(account.targetRatioBps));
    console.log(paint(`\n${symbol} custody [${entry.pubkey.toBase58()}]`, "info"));
    console.log(
      paint(
        `  mint: ${mint} (decimals ${decimals}) | stable: ${account.isStable ? "yes" : "no"}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  assets — owned: ${owned} ${symbol}, locked: ${locked} ${symbol}, fees: ${fees} ${symbol}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  target ratio: ${(targetRatioBps / 100).toFixed(2)}% | increase fee: ${(increaseBps / 100).toFixed(2)}% | decrease fee: ${(decreaseBps / 100).toFixed(2)}%`,
        "muted"
      )
    );
    console.log(
      paint(
        `  funding/hour: ${hourlyPercent.toFixed(6)}% (${Number(hourlyDbps) / 1000} dbps) | funding/day: ${dailyPercent.toFixed(4)}% | funding/year: ${annualPercent.toFixed(2)}%`,
        "muted"
      )
    );
  }
}

async function perpsIncreaseCommand(rawArgs) {
  if (!rawArgs.length || rawArgs.includes("--help") || rawArgs.includes("-h")) {
    printPerpsUsage();
    return;
  }
  const opts = {
    computePrice: PERPS_COMPUTE_BUDGET.priceMicrolamports,
    simulate: true,
  };
  const positional = [];
  for (let i = 0; i < rawArgs.length; i += 1) {
    const token = rawArgs[i];
    if (!token) continue;
    if (!token.startsWith("--")) {
      positional.push(token);
      continue;
    }
    const flag = token.toLowerCase();
    const requireValue = () => {
      if (i + 1 >= rawArgs.length) {
        throw new Error(`perps increase ${flag} requires a value`);
      }
      return rawArgs[++i];
    };
    if (flag === "--custody") {
      opts.custody = requireValue();
    } else if (flag === "--collateral") {
      opts.collateral = requireValue();
    } else if (flag === "--input-mint") {
      opts.inputMint = requireValue();
    } else if (flag === "--side") {
      opts.side = requireValue();
    } else if (flag === "--size-usd" || flag === "--size") {
      opts.sizeUsd = requireValue();
    } else if (flag === "--collateral-amount" || flag === "--collateral-size") {
      opts.collateralAmount = requireValue();
    } else if (flag === "--price-slippage") {
      opts.priceSlippage = requireValue();
    } else if (flag === "--min-out" || flag === "--jupiter-min-out") {
      opts.minOut = requireValue();
    } else if (flag === "--counter") {
      opts.counter = requireValue();
    } else if (flag === "--compute-price") {
      const value = parseInt(requireValue(), 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("perps increase --compute-price must be a positive integer");
      }
      opts.computePrice = value;
    } else if (flag === "--compute-units") {
      const value = parseInt(requireValue(), 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("perps increase --compute-units must be a positive integer");
      }
      opts.computeUnits = value;
    } else if (flag === "--referral") {
      opts.referral = requireValue();
    } else if (flag === "--skip-sim" || flag === "--no-sim") {
      opts.simulate = false;
    } else if (flag === "--simulate") {
      opts.simulate = true;
    } else {
      throw new Error(`perps increase: unknown flag ${token}`);
    }
  }
  if (!positional.length) {
    throw new Error(
      "perps increase usage: perps increase <walletName> --custody <symbol|address> --collateral <symbol|address> --side <long|short> --size-usd <amount> --collateral-amount <amount> --price-slippage <amount>"
    );
  }
  const walletName = positional[0];
  const wallet = listWallets().find((entry) => entry.name === walletName);
  if (!wallet) {
    throw new Error(`perps increase: wallet ${walletName} not found`);
  }
  if (!opts.custody || !opts.collateral || !opts.side || !opts.sizeUsd || !opts.collateralAmount || !opts.priceSlippage) {
    throw new Error(
      "perps increase requires --custody, --collateral, --side, --size-usd, --collateral-amount, and --price-slippage"
    );
  }
  const sideNormalized = opts.side.toLowerCase();
  if (sideNormalized !== "long" && sideNormalized !== "short") {
    throw new Error("perps increase --side must be 'long' or 'short'");
  }
  const custodyResolved = resolveCustodyIdentifier(opts.custody);
  if (!custodyResolved) {
    throw new Error(`perps increase: unknown custody ${opts.custody}`);
  }
  const collateralResolved = resolveCustodyIdentifier(opts.collateral);
  if (!collateralResolved) {
    throw new Error(`perps increase: unknown collateral ${opts.collateral}`);
  }
  let connection = createRpcConnection("confirmed");
  const getEndpoint = () =>
    connection.__rpcEndpoint || connection._rpcEndpoint || DEFAULT_RPC_URL;
  const rotateConnection = (reason) => {
    const previous = getEndpoint();
    if (typeof connection.__markUnhealthy === "function") {
      connection.__markUnhealthy(reason);
    }
    connection = createRpcConnection("confirmed");
    const next = getEndpoint();
    console.warn(paint(`RPC ${previous} ${reason}; switched to ${next}`, "warn"));
  };
  const runWithRpcRetry = async (label, fn) => {
    while (true) {
      try {
        return await fn();
      } catch (err) {
        if (isRateLimitError(err)) {
          rotateConnection(`rate-limit ${label}`);
          await delay(500);
          continue;
        }
        throw err;
      }
    }
  };
  ensurePerpsProgramMatchesConfiguration(connection);
  const custodyMap = await runWithRpcRetry("fetch custodies", () =>
    fetchCustodyAccounts(connection, [
      custodyResolved.custody,
      collateralResolved.custody,
    ])
  );
  const custodyEntry = custodyMap.get(custodyResolved.custody.toBase58());
  const collateralEntry = custodyMap.get(collateralResolved.custody.toBase58());
  if (!custodyEntry || !collateralEntry) {
    throw new Error("perps increase: failed to fetch custody metadata");
  }
  const custodyAccount = custodyEntry.account;
  const collateralAccount = collateralEntry.account;
  const collateralDecimals = collateralAccount.decimals;
  const custodyDecimals = custodyAccount.decimals;
  const sizeUsdDelta = decimalToBaseUnits(opts.sizeUsd, 6);
  const collateralTokenDelta = decimalToBaseUnits(
    opts.collateralAmount,
    collateralDecimals
  );
  const priceSlippageDelta = decimalToBaseUnits(opts.priceSlippage, 6);
  const jupiterMinOut = opts.minOut
    ? decimalToBaseUnits(opts.minOut, custodyDecimals)
    : null;
  const counterValue = opts.counter ? BigInt(opts.counter) : null;
  const referralPubkey = opts.referral
    ? parsePublicKeyStrict(opts.referral, "referral public key")
    : null;
  const inputMintPk = opts.inputMint
    ? parsePublicKeyStrict(opts.inputMint, "input mint")
    : collateralAccount.mint;
  const marketSymbol = symbolForMint(custodyAccount.mint.toBase58());
  const collateralSymbol = symbolForMint(collateralAccount.mint.toBase58());
  if (inputMintPk.equals(NATIVE_MINT)) {
    await ensureWrappedSolBalance(connection, wallet, collateralTokenDelta);
  } else {
    const inputMintTokenProgram = await runWithRpcRetry(
      "resolve input mint owner",
      () => resolveTokenProgramForMint(connection, inputMintPk)
    );
    await ensureAtaForMint(
      connection,
      wallet,
      inputMintPk,
      inputMintTokenProgram,
      {
        label: "perps",
      }
    );
  }
  const increaseResult = await buildIncreaseRequestInstruction({
    connection,
    owner: wallet.kp.publicKey,
    custody: custodyResolved.custody,
    collateralCustody: collateralResolved.custody,
    inputMint: inputMintPk,
    sizeUsdDelta,
    collateralTokenDelta,
    side: sideNormalized,
    priceSlippage: priceSlippageDelta,
    jupiterMinimumOut: jupiterMinOut,
    counter: counterValue,
    referral: referralPubkey,
  });
  const defaultComputePrice =
    PERPS_COMPUTE_BUDGET.priceMicrolamports || 100000;
  const computePrice = opts.computePrice || defaultComputePrice;
  let computeUnits = opts.computeUnits || null;
  const previewInstructions = [
    ...buildComputeBudgetInstructions({ microLamports: computePrice }),
    increaseResult.instruction,
  ];
  let simulation = null;
  let simUnits = null;
  if (opts.simulate) {
    simulation = await runWithRpcRetry("simulate", () =>
      simulatePerpsInstructions({
        connection,
        payer: wallet.kp.publicKey,
        instructions: previewInstructions,
      })
    );
    if (simulation.value?.err) {
      console.error(
        paint(
          `Simulation failed: ${JSON.stringify(simulation.value.err)}`,
          "error"
        )
      );
      const logs = simulation.value?.logs || [];
      logs.slice(0, 5).forEach((log) =>
        console.error(paint(`  log: ${log}`, "error"))
      );
      throw new Error("perps increase simulation failed");
    }
    simUnits = simulation.value?.unitsConsumed || null;
    if (simUnits) {
      console.log(
        paint(
          `  simulation consumed ${simUnits} compute units`,
          "muted"
        )
      );
    }
    const logs = simulation.value?.logs || [];
    if (logs.length) {
      logs.slice(0, 4).forEach((log) =>
        console.log(paint(`    sim log: ${log}`, "muted"))
      );
      if (logs.length > 4) {
        console.log(
          paint(
            `    sim logs truncated (${logs.length - 4} more)`,
            "muted"
          )
        );
      }
    }
  } else {
    console.log(paint("  simulation skipped (--skip-sim)", "warn"));
  }
  const DEFAULT_COMPUTE_UNITS =
    PERPS_COMPUTE_BUDGET.unitLimit || 1_400_000;
  if (!computeUnits) {
    if (simUnits) {
      computeUnits = Math.min(
        DEFAULT_COMPUTE_UNITS,
        Math.max(simUnits + 50_000, 600_000)
      );
    } else {
      computeUnits = DEFAULT_COMPUTE_UNITS;
    }
  }
  const finalInstructions = [
    ...buildComputeBudgetInstructions({
      units: computeUnits,
      microLamports: computePrice,
    }),
    increaseResult.instruction,
  ];
  const previewBase64 = preparePreviewTransaction({
    instructions: finalInstructions,
    payer: wallet.kp.publicKey,
  });
  const createdAtas = await ensureAtasForTransaction({
    connection,
    wallet,
    txBase64: previewBase64,
    label: "perps increase request",
  });
  if (createdAtas > 0) {
    console.log(
      paint(
        `  Prepared ${createdAtas} associated token account(s) for request`,
        "muted"
      )
    );
  }
  const latestBlockhash = await runWithRpcRetry("getLatestBlockhash", () =>
    connection.getLatestBlockhash()
  );
  const message = new TransactionMessage({
    payerKey: wallet.kp.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    instructions: finalInstructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  tx.sign([wallet.kp]);
  const rawTx = tx.serialize();
  console.log(
    paint(
      `Submitting increase request on RPC ${getEndpoint()}`,
      "info"
    )
  );
  console.log(
    paint(
      `  market ${marketSymbol} | collateral ${collateralSymbol} | side ${sideNormalized}`,
      "muted"
    )
  );
  console.log(
    paint(
      `  size: ${formatBaseUnits(sizeUsdDelta, 6)} USD | collateral: ${formatBaseUnits(collateralTokenDelta, collateralDecimals)} ${collateralSymbol}`,
      "muted"
    )
  );
  console.log(
    paint(
      `  position: ${increaseResult.position.toBase58()} | request: ${increaseResult.positionRequest.toBase58()} (counter ${increaseResult.counter.toString()})`,
      "muted"
    )
  );
  const signature = await runWithRpcRetry("sendRawTransaction", () =>
    connection.sendRawTransaction(rawTx)
  );
  console.log(paint(`  submitted: ${signature}`, "success"));
  await runWithRpcRetry("confirmTransaction", () =>
    connection.confirmTransaction(
      {
        signature,
        blockhash: latestBlockhash.blockhash,
        lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
      },
      "confirmed"
    )
  );
  console.log(paint("  confirmed", "success"));
  try {
    const program = ensurePerpsProgramMatchesConfiguration(connection);
    const requestAccount = await runWithRpcRetry("fetch request", () =>
      program.account.positionRequest.fetch(increaseResult.positionRequest)
    );
    const requestChange = describeEnumVariant(requestAccount.requestChange);
    const requestType = describeEnumVariant(requestAccount.requestType);
    console.log(
      paint(
        `  keeper pending: request ${increaseResult.positionRequest.toBase58()} (${requestChange}/${requestType}) executed=${requestAccount.executed ? "yes" : "no"}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  request mint: ${requestAccount.mint.toBase58()} | update time: ${formatTimestampSeconds(requestAccount.updateTime)}`,
        "muted"
      )
    );
  } catch (err) {
    console.warn(
      paint(
        `  warning: failed to fetch position request account — ${err.message || err}`,
        "warn"
      )
    );
  }
}

async function perpsDecreaseCommand(rawArgs) {
  if (!rawArgs.length || rawArgs.includes("--help") || rawArgs.includes("-h")) {
    printPerpsUsage();
    return;
  }
  const opts = {
    computePrice: PERPS_COMPUTE_BUDGET.priceMicrolamports,
    simulate: true,
  };
  const positional = [];
  for (let i = 0; i < rawArgs.length; i += 1) {
    const token = rawArgs[i];
    if (!token) continue;
    if (!token.startsWith("--")) {
      positional.push(token);
      continue;
    }
    const flag = token.toLowerCase();
    const requireValue = () => {
      if (i + 1 >= rawArgs.length) {
        throw new Error(`perps decrease ${flag} requires a value`);
      }
      return rawArgs[++i];
    };
    if (flag === "--position") {
      opts.position = requireValue();
    } else if (flag === "--price-slippage") {
      opts.priceSlippage = requireValue();
    } else if (flag === "--collateral-usd") {
      opts.collateralUsd = requireValue();
    } else if (flag === "--size-usd" || flag === "--size") {
      opts.sizeUsd = requireValue();
    } else if (flag === "--desired" || flag === "--desired-mint") {
      opts.desiredMint = requireValue();
    } else if (flag === "--min-out" || flag === "--jupiter-min-out") {
      opts.minOut = requireValue();
    } else if (flag === "--entire") {
      opts.entire = true;
    } else if (flag === "--no-entire") {
      opts.entire = false;
    } else if (flag === "--counter") {
      opts.counter = requireValue();
    } else if (flag === "--compute-price") {
      const value = parseInt(requireValue(), 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("perps decrease --compute-price must be a positive integer");
      }
      opts.computePrice = value;
    } else if (flag === "--compute-units") {
      const value = parseInt(requireValue(), 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("perps decrease --compute-units must be a positive integer");
      }
      opts.computeUnits = value;
    } else if (flag === "--referral") {
      opts.referral = requireValue();
    } else if (flag === "--skip-sim" || flag === "--no-sim") {
      opts.simulate = false;
    } else if (flag === "--simulate") {
      opts.simulate = true;
    } else {
      throw new Error(`perps decrease: unknown flag ${token}`);
    }
  }
  if (!positional.length) {
    throw new Error(
      "perps decrease usage: perps decrease <walletName> --position <pubkey> --price-slippage <amount> [--collateral-usd <amount>] [--size-usd <amount>] [--desired <mint>] [--min-out <amount>] [--entire]"
    );
  }
  const walletName = positional[0];
  const wallet = listWallets().find((entry) => entry.name === walletName);
  if (!wallet) {
    throw new Error(`perps decrease: wallet ${walletName} not found`);
  }
  if (!opts.position || !opts.priceSlippage) {
    throw new Error(
      "perps decrease requires --position and --price-slippage"
    );
  }
  if (!opts.entire && !opts.collateralUsd && !opts.sizeUsd) {
    throw new Error(
      "perps decrease requires --entire or at least one of --collateral-usd/--size-usd"
    );
  }
  const positionPubkey = parsePublicKeyStrict(
    opts.position,
    "position public key"
  );
  let connection = createRpcConnection("confirmed");
  const getEndpoint = () =>
    connection.__rpcEndpoint || connection._rpcEndpoint || DEFAULT_RPC_URL;
  const rotateConnection = (reason) => {
    const previous = getEndpoint();
    if (typeof connection.__markUnhealthy === "function") {
      connection.__markUnhealthy(reason);
    }
    connection = createRpcConnection("confirmed");
    const next = getEndpoint();
    console.warn(paint(`RPC ${previous} ${reason}; switched to ${next}`, "warn"));
  };
  const runWithRpcRetry = async (label, fn) => {
    while (true) {
      try {
        return await fn();
      } catch (err) {
        if (isRateLimitError(err)) {
          rotateConnection(`rate-limit ${label}`);
          await delay(500);
          continue;
        }
        throw err;
      }
    }
  };
  const program = ensurePerpsProgramMatchesConfiguration(connection);
  const positionAccount = await runWithRpcRetry("fetch position", () =>
    program.account.position.fetch(positionPubkey)
  );
  const custodyMap = await runWithRpcRetry("fetch custodies", () =>
    fetchCustodyAccounts(connection, [
      positionAccount.custody,
      positionAccount.collateralCustody,
    ])
  );
  const custodyEntry = custodyMap.get(positionAccount.custody.toBase58());
  const collateralEntry = custodyMap.get(
    positionAccount.collateralCustody.toBase58()
  );
  if (!custodyEntry || !collateralEntry) {
    throw new Error("perps decrease: failed to load custody metadata");
  }
  const custodyAccount = custodyEntry.account;
  const collateralAccount = collateralEntry.account;
  const collateralDecimals = collateralAccount.decimals;
  let desiredMintPk = opts.desiredMint
    ? parsePublicKeyStrict(opts.desiredMint, "desired mint")
    : collateralAccount.mint;
  let desiredDecimals = collateralAccount.decimals;
  if (opts.desiredMint) {
    const desiredMintStr = desiredMintPk.toBase58();
    const matchingCustody = Array.from(custodyMap.values()).find((entry) =>
      entry.account.mint.toBase58() === desiredMintStr
    );
    if (matchingCustody) {
      desiredDecimals = matchingCustody.account.decimals;
    } else {
      try {
        const mintInfo = await runWithRpcRetry("getMint", () =>
          getMint(connection, desiredMintPk)
        );
        if (typeof mintInfo?.decimals === "number") {
          desiredDecimals = mintInfo.decimals;
        }
      } catch (err) {
        console.warn(
          paint(
            `  warning: failed to fetch mint info for ${desiredMintStr}; using decimals ${desiredDecimals}`,
            "warn"
          )
        );
      }
    }
  }
  const collateralUsdDelta = opts.collateralUsd
    ? decimalToBaseUnits(opts.collateralUsd, 6)
    : 0n;
  const sizeUsdDelta = opts.sizeUsd
    ? decimalToBaseUnits(opts.sizeUsd, 6)
    : 0n;
  const priceSlippageDelta = decimalToBaseUnits(opts.priceSlippage, 6);
  const jupiterMinOut = opts.minOut
    ? decimalToBaseUnits(opts.minOut, desiredDecimals)
    : null;
  const counterValue = opts.counter ? BigInt(opts.counter) : null;
  const referralPubkey = opts.referral
    ? parsePublicKeyStrict(opts.referral, "referral public key")
    : null;
  const marketSymbol = symbolForMint(custodyAccount.mint.toBase58());
  const collateralSymbol = symbolForMint(collateralAccount.mint.toBase58());
  const desiredSymbol = symbolForMint(desiredMintPk.toBase58());
  const desiredMintTokenProgram = await runWithRpcRetry(
    "resolve desired mint owner",
    () => resolveTokenProgramForMint(connection, desiredMintPk)
  );
  await ensureAtaForMint(
    connection,
    wallet,
    desiredMintPk,
    desiredMintTokenProgram,
    {
      label: "perps",
    }
  );
  const decreaseResult = await buildDecreaseRequestInstruction({
    connection,
    owner: wallet.kp.publicKey,
    position: positionPubkey,
    desiredMint: desiredMintPk,
    collateralUsdDelta,
    sizeUsdDelta,
    priceSlippage: priceSlippageDelta,
    jupiterMinimumOut: jupiterMinOut,
    entirePosition: opts.entire === true,
    counter: counterValue,
    referral: referralPubkey,
  });
  const defaultComputePrice =
    PERPS_COMPUTE_BUDGET.priceMicrolamports || 100000;
  const computePrice = opts.computePrice || defaultComputePrice;
  let computeUnits = opts.computeUnits || null;
  const previewInstructions = [
    ...buildComputeBudgetInstructions({ microLamports: computePrice }),
    decreaseResult.instruction,
  ];
  let simulation = null;
  let simUnits = null;
  if (opts.simulate) {
    simulation = await runWithRpcRetry("simulate", () =>
      simulatePerpsInstructions({
        connection,
        payer: wallet.kp.publicKey,
        instructions: previewInstructions,
      })
    );
    if (simulation.value?.err) {
      console.error(
        paint(
          `Simulation failed: ${JSON.stringify(simulation.value.err)}`,
          "error"
        )
      );
      const logs = simulation.value?.logs || [];
      logs.slice(0, 5).forEach((log) =>
        console.error(paint(`  log: ${log}`, "error"))
      );
      throw new Error("perps decrease simulation failed");
    }
    simUnits = simulation.value?.unitsConsumed || null;
    if (simUnits) {
      console.log(
        paint(
          `  simulation consumed ${simUnits} compute units`,
          "muted"
        )
      );
    }
    const logs = simulation.value?.logs || [];
    if (logs.length) {
      logs.slice(0, 4).forEach((log) =>
        console.log(paint(`    sim log: ${log}`, "muted"))
      );
      if (logs.length > 4) {
        console.log(
          paint(
            `    sim logs truncated (${logs.length - 4} more)`,
            "muted"
          )
        );
      }
    }
  } else {
    console.log(paint("  simulation skipped (--skip-sim)", "warn"));
  }
  const DEFAULT_COMPUTE_UNITS =
    PERPS_COMPUTE_BUDGET.unitLimit || 1_400_000;
  if (!computeUnits) {
    if (simUnits) {
      computeUnits = Math.min(
        DEFAULT_COMPUTE_UNITS,
        Math.max(simUnits + 50_000, 600_000)
      );
    } else {
      computeUnits = DEFAULT_COMPUTE_UNITS;
    }
  }
  const finalInstructions = [
    ...buildComputeBudgetInstructions({
      units: computeUnits,
      microLamports: computePrice,
    }),
    decreaseResult.instruction,
  ];
  const previewBase64 = preparePreviewTransaction({
    instructions: finalInstructions,
    payer: wallet.kp.publicKey,
  });
  const createdAtas = await ensureAtasForTransaction({
    connection,
    wallet,
    txBase64: previewBase64,
    label: "perps decrease request",
  });
  if (createdAtas > 0) {
    console.log(
      paint(
        `  Prepared ${createdAtas} associated token account(s) for request`,
        "muted"
      )
    );
  }
  const latestBlockhash = await runWithRpcRetry("getLatestBlockhash", () =>
    connection.getLatestBlockhash()
  );
  const message = new TransactionMessage({
    payerKey: wallet.kp.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    instructions: finalInstructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  tx.sign([wallet.kp]);
  const rawTx = tx.serialize();
  console.log(
    paint(
      `Submitting decrease request on RPC ${getEndpoint()}`,
      "info"
    )
  );
  console.log(
    paint(
      `  market ${marketSymbol} | collateral ${collateralSymbol} | desired ${desiredSymbol}`,
      "muted"
    )
  );
  console.log(
    paint(
      `  collateral delta: ${formatBaseUnits(collateralUsdDelta, 6)} USD | size delta: ${formatBaseUnits(sizeUsdDelta, 6)} USD | entire=${opts.entire ? "yes" : "no"}`,
      "muted"
    )
  );
  console.log(
    paint(
      `  request: ${decreaseResult.positionRequest.toBase58()} (counter ${decreaseResult.counter.toString()})`,
      "muted"
    )
  );
  const signature = await runWithRpcRetry("sendRawTransaction", () =>
    connection.sendRawTransaction(rawTx)
  );
  console.log(paint(`  submitted: ${signature}`, "success"));
  await runWithRpcRetry("confirmTransaction", () =>
    connection.confirmTransaction(
      {
        signature,
        blockhash: latestBlockhash.blockhash,
        lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
      },
      "confirmed"
    )
  );
  console.log(paint("  confirmed", "success"));
  try {
    const programLatest = ensurePerpsProgramMatchesConfiguration(connection);
    const requestAccount = await runWithRpcRetry("fetch request", () =>
      programLatest.account.positionRequest.fetch(
        decreaseResult.positionRequest
      )
    );
    const requestChange = describeEnumVariant(requestAccount.requestChange);
    const requestType = describeEnumVariant(requestAccount.requestType);
    console.log(
      paint(
        `  keeper pending: request ${decreaseResult.positionRequest.toBase58()} (${requestChange}/${requestType}) executed=${requestAccount.executed ? "yes" : "no"}`,
        "muted"
      )
    );
    console.log(
      paint(
        `  request mint: ${requestAccount.mint.toBase58()} | update time: ${formatTimestampSeconds(requestAccount.updateTime)}`,
        "muted"
      )
    );
  } catch (err) {
    console.warn(
      paint(
        `  warning: failed to fetch position request account — ${err.message || err}`,
        "warn"
      )
    );
  }
}

// Helper function to parse flow command-line options
function parseFlowOptions(args) {
  const options = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg || typeof arg !== 'string') continue;

    const lower = arg.toLowerCase();
    if (lower === '--infinite' || lower === '--loop') {
      options.infinite = true;
    } else if (lower === '--loop-cooldown' || lower === '--cooldown') {
      if (i + 1 < args.length) {
        const value = parseInt(args[i + 1], 10);
        if (Number.isFinite(value) && value >= 0) {
          options.loopCooldown = value;
          i++; // Skip next arg
        }
      }
    }
  }
  return options;
}

// Helper function to run flows with optional indefinite looping
async function runFlowWithLoopOption(flowKey, options = {}) {
  // Interactive prompts for flow configuration (if not provided via CLI flags)
  let infiniteMode = options.infinite || options.loop || false;
  let loopCooldownMs = options.loopCooldown ?? null;
  let useQuietMode = QUIET_MODE;

  // Only prompt if options weren't passed via command line
  const shouldPrompt = !options.infinite && !options.loop && options.loopCooldown === undefined;

  if (shouldPrompt) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    console.log(paint(`\n━━━ Flow Configuration ━━━`, "label"));

    // Prompt for quiet mode
    const quietAnswer = await new Promise((resolve) => {
      rl.question(
        paint(`Enable quiet mode (less console spam)? (Y/n): `, "info"),
        resolve
      );
    });
    const quietNormalized = (quietAnswer || "").trim().toLowerCase();
    useQuietMode = quietNormalized === "" || quietNormalized === "y" || quietNormalized === "yes";

    // Prompt for infinite loop
    const infiniteAnswer = await new Promise((resolve) => {
      rl.question(
        paint(`Run flow infinitely (no manual prompts)? (Y/n): `, "info"),
        resolve
      );
    });
    const infiniteNormalized = (infiniteAnswer || "").trim().toLowerCase();
    infiniteMode = infiniteNormalized === "" || infiniteNormalized === "y" || infiniteNormalized === "yes";

    // Prompt for loop cooldown (only if infinite mode enabled)
    if (infiniteMode) {
      const cooldownAnswer = await new Promise((resolve) => {
        rl.question(
          paint(`Loop cooldown in seconds (default: 60): `, "info"),
          resolve
        );
      });
      const cooldownValue = parseInt(cooldownAnswer, 10);
      loopCooldownMs = Number.isFinite(cooldownValue) && cooldownValue >= 0
        ? cooldownValue * 1000
        : FLOW_LOOP_COOLDOWN_MS;
    }

    rl.close();

    // Display configuration summary
    console.log(paint(`\n📋 Configuration:`, "label"));
    console.log(paint(`  • Quiet mode: ${useQuietMode ? "✓ Enabled" : "✗ Disabled"}`, "info"));
    console.log(paint(`  • Infinite loop: ${infiniteMode ? "✓ Enabled" : "✗ Disabled"}`, "info"));
    if (infiniteMode) {
      const cooldownSec = (loopCooldownMs / 1000).toFixed(0);
      console.log(paint(`  • Loop cooldown: ${cooldownSec}s`, "info"));
    }
    console.log(paint(`\nStarting ${flowKey} flow...\n`, "success"));
  }

  // Apply quiet mode setting (override global QUIET_MODE for this session)
  if (shouldPrompt && useQuietMode !== QUIET_MODE) {
    // Store original logMuted function
    const originalLogMuted = global.logMuted || logMuted;

    // Override logMuted temporarily if quiet mode differs from global
    if (useQuietMode && !QUIET_MODE) {
      global.logMuted = (message, value) => {
        const criticalKeywords = ['confirmed', 'error', 'failed', 'success', 'completed'];
        const msgLower = String(message).toLowerCase();
        const isCritical = criticalKeywords.some(keyword => msgLower.includes(keyword));
        if (!isCritical) return;
        originalLogMuted(message, value);
      };
    }
  }

  // Default loop cooldown if still not set
  if (loopCooldownMs === null) {
    loopCooldownMs = FLOW_LOOP_COOLDOWN_MS;
  }

  let loopCount = 0;

  // Set up graceful shutdown handler
  let shutdownRequested = false;
  const handleShutdown = () => {
    if (!shutdownRequested) {
      shutdownRequested = true;
      console.log(paint(`\n\n⚠️  Shutdown requested. Finishing current loop...`, "warn"));
    }
  };
  process.on('SIGINT', handleShutdown);
  process.on('SIGTERM', handleShutdown);

  try {
    while (true) {
      loopCount++;

      if (infiniteMode && loopCount > 1) {
        console.log(paint(`\n━━━ Loop #${loopCount} starting ━━━`, "label"));
      }

      // Wrap flow execution in try-catch to prevent crashes
      try {
        await runPrewrittenFlowPlan(flowKey, {});
      } catch (err) {
        // Check if this is an RPC rate limit error
        const isRateLimit = err.message && (
          err.message.includes('429') ||
          err.message.includes('Too Many Requests') ||
          err.message.includes('rate limit')
        );

        if (isRateLimit) {
          console.error(paint(`\n⚠️  RPC rate limit hit during flow execution`, "warn"));
          console.log(paint(`  Error: ${err.message}`, "error"));
          if (infiniteMode) {
            console.log(paint(`  Increasing cooldown and continuing to next loop...`, "warn"));
            // Add extra delay for rate limit recovery
            await new Promise(resolve => setTimeout(resolve, 10000));  // 10s extra delay
          }
        } else {
          console.error(paint(`\n❌ Flow execution error: ${err.message}`, "error"));
        }

        if (!infiniteMode) {
          throw err;  // Re-throw in non-infinite mode
        }
        // In infinite mode, log error and continue to next loop
        console.log(paint(`  Continuing to next loop...`, "info"));
      }

      // Check if shutdown was requested
      if (shutdownRequested) {
        console.log(paint(`\n✅ ${flowKey} shut down gracefully after ${loopCount} loop(s).`, "success"));
        break;
      }

      if (infiniteMode) {
        // Infinite mode: automatic restart with cooldown
        console.log(paint(`\n✅ Loop #${loopCount} completed.`, "success"));
        if (loopCooldownMs > 0) {
          const cooldownSec = (loopCooldownMs / 1000).toFixed(1);
          console.log(paint(`  Cooldown: ${cooldownSec}s before next loop...`, "muted"));
          await new Promise(resolve => setTimeout(resolve, loopCooldownMs));
        }
        console.log(paint(`\n♻️  Starting Loop #${loopCount + 1}...\n`, "info"));
        continue;
      }

      // Manual mode: prompt user
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
      });

      const answer = await new Promise((resolve) => {
        rl.question(
          paint(`\n🔁 Run ${flowKey} again? (y/n): `, "label"),
          resolve
        );
      });

      rl.close();

      const normalized = (answer || "").trim().toLowerCase();
      if (normalized !== "y" && normalized !== "yes") {
        console.log(paint(`✅ ${flowKey} completed after ${loopCount} loop(s).`, "success"));
        break;
      }

      console.log(paint(`\n♻️  Restarting ${flowKey}...\n`, "info"));
    }
  } finally {
    // Clean up signal handlers
    process.off('SIGINT', handleShutdown);
    process.off('SIGTERM', handleShutdown);
  }
}

// CLI dispatch
// ---- CLI entry point ----
// Parses CLI arguments, performs RPC health checks, and dispatches to the
// appropriate command implementation. New commands should be registered here.
async function main() {
  const startupResources = ensureStartupResources();
  const args = process.argv.slice(2);
  const cmd = args[0];
  const normalizedCmd =
    typeof cmd === "string" ? cmd.trim().toLowerCase() : "";
  const skipBanner =
    process.env.JUPITER_SWAP_TOOL_NO_BANNER === "1" ||
    process.env.JUPITER_NO_BANNER === "1" ||
    normalizedCmd === "help" ||
    normalizedCmd === "--help" ||
    normalizedCmd === "-h" ||
    normalizedCmd === "version" ||
    normalizedCmd === "--version" ||
    normalizedCmd === "-v";
  if (!skipBanner) {
    printStartupBanner();
  }
  announceStartupResources(startupResources, { skipBanner });
  if (!cmd || normalizedCmd === "help" || normalizedCmd === "--help" || normalizedCmd === "-h") {
    printGeneralUsage();
    process.exit(0);
  }
  if (
    normalizedCmd === "version" ||
    normalizedCmd === "--version" ||
    normalizedCmd === "-v"
  ) {
    console.log(`Jupiter Swap Tool v${TOOL_VERSION}`);
    process.exit(0);
  }

  try {
    const skipInit = process.env.JUPITER_SWAP_TOOL_SKIP_INIT === "1";
    const healthUrl = (process.env.RPC_HEALTH_URL || process.env.RPC_HEALTH_ENDPOINT || "").trim();
    const healthIndexRaw = (process.env.RPC_HEALTH_INDEX || "").trim();
    const healthOptions = {};
    if (cmd === "hotkeys") {
      handleHotkeysCommand(args.slice(1));
      process.exit(0);
    }
    if (cmd === "tokens") {
      let verbose = false;
      let refreshRequested = false;
      for (const flag of args.slice(1)) {
        const lowered = flag.toLowerCase();
        if (lowered === "--verbose" || lowered === "-v" || lowered === "--full") {
          verbose = true;
        } else if (lowered === "--refresh") {
          refreshRequested = true;
        } else {
          throw new Error("tokens usage: tokens [--verbose] [--refresh]");
        }
      }
      if (refreshRequested) {
        await refreshTokenCatalogFromApi({ query: " ", limit: 2000 });
      }
      listTokenCatalog({ verbose });
      process.exit(0);
    }
    if (cmd === "lend") {
      await handleLendCommand(args.slice(1));
      process.exit(0);
    }
    if (cmd === "perps") {
      await handlePerpsCommand(args.slice(1));
      process.exit(0);
    }
    if (cmd === "wallet") {
      await handleWalletCommand(args.slice(1));
      process.exit(0);
    }
    if (cmd === "perps") {
      const sub = args[1];
      const subArgs = args.slice(2);
      if (!sub || sub === "--help" || sub === "-h") {
        printPerpsUsage();
        process.exit(0);
      }
      const lowered = sub.toLowerCase();
      if (lowered === "positions") {
        await perpsPositionsCommand(subArgs);
        process.exit(0);
      }
      if (lowered === "funding") {
        await perpsFundingCommand(subArgs);
        process.exit(0);
      }
      if (lowered === "increase" || lowered === "open") {
        await perpsIncreaseCommand(subArgs);
        process.exit(0);
      }
      if (lowered === "decrease" || lowered === "close") {
        await perpsDecreaseCommand(subArgs);
        process.exit(0);
      }
      throw new Error(`perps: unknown subcommand ${sub}`);
    }
    if (cmd === "launcher-bootstrap") {
      let forceRefresh = false;
      for (const flag of args.slice(1)) {
        const lowered = flag.toLowerCase();
        if (lowered === "--refresh" || lowered === "--refresh=true") {
          forceRefresh = true;
        } else if (lowered === "--refresh=false") {
          forceRefresh = false;
        } else {
          throw new Error("launcher-bootstrap usage: launcher-bootstrap [--refresh]");
        }
      }
      const wallets = listWallets();
      const walletCount = wallets.length;
      const summaryBefore = getWalletGuardSummary({ wallets });
      const guardSuspended = summaryBefore.guardSuspended === true;
      const lastComputedAt =
        typeof disableState.lastComputedAt === "number"
          ? disableState.lastComputedAt
          : null;
      const now = Date.now();
      let needsRefresh =
        !guardSuspended &&
        (!Number.isFinite(lastComputedAt) ||
          (LAUNCHER_GUARD_MAX_AGE_MS > 0 &&
            now - lastComputedAt > LAUNCHER_GUARD_MAX_AGE_MS));
      if (forceRefresh && !guardSuspended) {
        await refreshWalletDisableStatus({ silent: true, wallets });
        needsRefresh = false;
      }
      const summary = getWalletGuardSummary({ wallets });
      const mode = summary.guardSuspended ? "force-reset" : "auto";
      const disabledSample = summary.disabledNames.slice(0, 8).join(",");
      const computedAt =
        typeof disableState.lastComputedAt === "number"
          ? disableState.lastComputedAt
          : "";
      console.log(`walletCount=${walletCount}`);
      console.log(`guardMode=${mode}`);
      console.log(`guardActive=${summary.active}`);
      console.log(`guardTotal=${summary.total}`);
      console.log(`guardDisabled=${summary.disabled}`);
      console.log(`guardDisabledSample=${disabledSample}`);
      console.log(`guardLastComputedAt=${computedAt}`);
      console.log(`needsRefresh=${needsRefresh ? "true" : "false"}`);
      console.log(`refreshRan=${forceRefresh && !guardSuspended ? "true" : "false"}`);
      process.exit(0);
    }
    if (!skipInit) {
      if (healthUrl.length > 0) {
        healthOptions.endpointOverride = healthUrl;
      } else if (healthIndexRaw.length > 0) {
        const parsedValue = parseInt(healthIndexRaw, 10);
        if (!Number.isFinite(parsedValue)) {
          throw new Error(`Invalid RPC_HEALTH_INDEX value: ${healthIndexRaw}`);
        }
        const zeroBased = parsedValue - 1;
        if (zeroBased < 0) {
          throw new Error(`RPC_HEALTH_INDEX must be >= 1 (received ${parsedValue})`);
        }
        healthOptions.endpointIndex = zeroBased;
      }

      await checkRpcHealth(healthOptions);
      await autoRefreshWalletDisables();
    }
    if (cmd === "generate") {
      const n = parseInt(args[1]);
      if (!n || n <= 0) throw new Error("Invalid number for generate");
      const prefix = args[2] || "wallet";
      const created = generateWallets(n, prefix);
      console.log(paint("Generated wallets:", "label"));
      for (const w of created) {
        const secretLine = PRINT_SECRET_KEYS && w.secretKeyBase58
          ? `  secretKey(base58): ${w.secretKeyBase58}`
          : null;
        console.log(paint(`  ${w.name}`, "info"));
        console.log(`    publicKey: ${w.publicKey}`);
        if (secretLine) console.log(secretLine);
      }
    } else if (cmd === "import-wallet") {
      const opts = parseImportWalletArgs(args.slice(1));
      const looksMnemonic = bip39.validateMnemonic(normalizeMnemonicInput(opts.secret));
      const importResult = importWalletFromSecret(opts.secret, {
        prefix: opts.prefix,
        path: opts.path,
        passphrase: opts.passphrase,
        force: opts.force,
        source: looksMnemonic ? "manual-mnemonic" : "manual-secret",
      });
      const targetPath = path.join(KEYPAIR_DIR, importResult.filename);
      const statusLabel = importResult.overwritten ? "Updated" : "Imported";
      console.log(
        paint(
          `${statusLabel} wallet ${importResult.publicKey} -> ${targetPath}`,
          "success"
        )
      );
      if (PRINT_SECRET_KEYS) {
        console.log(paint("  secretKey(base58):", "muted"), importResult.secretKeyBase58);
      }
    } else if (cmd === "list") {
      await listWalletAddresses();
    } else if (cmd === "balances") {
      await showBalances(args.slice(1));
    } else if (cmd === "fund-all") {
      const [ , fromName, lamStr ] = args;
      const lam = parseInt(lamStr);
      if (!fromName || isNaN(lam)) throw new Error("fund-all usage: fund-all <fromWalletFile> <lamportsEach>");
      await fundAll(fromName, lam);
    } else if (cmd === "redistribute") {
      const fromName = args[1];
      if (!fromName) throw new Error("redistribute usage: redistribute <fromWalletFile>");
      await redistributeSol(fromName);
    } else if (cmd === "campaign") {
      await handleCampaignCommand(args.slice(1));
    } else if (cmd === "fund" || cmd === "send") {
      const [ , fromName, toName, lamStr ] = args;
      const lam = parseInt(lamStr);
      if (!fromName || !toName || isNaN(lam)) throw new Error("send/fund usage: send <fromWalletFile> <toWalletFile> <lamports>");
      await sendSolBetween(fromName, toName, lam);
    } else if (cmd === "aggregate") {
      const targetName = args[1];
      if (!targetName) throw new Error("aggregate usage: aggregate <wallet #|filename>");
      await aggregateSol(targetName);
    } else if (cmd === "aggregate-hierarchical") {
      await aggregateHierarchical();
    } else if (cmd === "aggregate-masters") {
      await aggregateMasters();
    } else if (cmd === "airdrop") {
      const [ , walletFile, lamStr ] = args;
      const lam = parseInt(lamStr);
      if (!walletFile || isNaN(lam)) throw new Error("airdrop usage: airdrop <walletFile> <lamports>");
      await airdrop(walletFile, lam);
    } else if (cmd === "airdrop-all") {
      const lam = parseInt(args[1]);
      if (isNaN(lam)) throw new Error("airdrop-all usage: airdrop-all <lamports>");
      await airdropAll(lam);
    } else if (cmd === "wallet-guard-status") {
      const flagSet = new Set(args.slice(1));
      const wantsRefresh = flagSet.has("--refresh");
    if (wantsRefresh) {
      await refreshWalletDisableStatus({ silent: true });
    }
    const summary = getWalletGuardSummary();
      const mode = summary.guardSuspended ? "force-reset" : "auto";
      if (flagSet.has("--summary")) {
        console.log(
          `ACTIVE=${summary.active} TOTAL=${summary.total} DISABLED=${summary.disabled} MODE=${mode}`
        );
      } else {
        const modeLabel = summary.guardSuspended
          ? "force reset active (all wallets enabled)"
          : "auto guard";
        console.log(
          paint(
            `Wallet guard: ${summary.active}/${summary.total} active — ${modeLabel}`,
            "info"
          )
        );
        if (!summary.guardSuspended && summary.disabled > 0) {
          const truncated = summary.disabledNames.slice(0, 8);
          const suffix = summary.disabledNames.length > truncated.length
            ? ", …"
            : "";
          console.log(
            paint(
              `  Disabled wallets (${summary.disabled}): ${truncated.join(", ")}${suffix}`,
              "muted"
            )
          );
        }
      }
    } else if (cmd === "test-rpcs") {
      let selector = "all";
      let swapTest = false;
      let swapLoops = 10;
      let swapDelayMs = 1000;
      let swapAmount = null;
      let swapConfirm = false;
      let index = 1;
      while (index < args.length) {
        const token = args[index];
        if (!token.startsWith("--")) {
          if (selector !== "all") {
            throw new Error("test-rpcs usage: only one selector argument is permitted");
          }
          selector = token;
          index += 1;
          continue;
        }
        const flag = token.toLowerCase();
        if (flag === "--swap") {
          swapTest = true;
          index += 1;
        } else if (flag === "--loops") {
          if (index + 1 >= args.length) {
            throw new Error("test-rpcs --loops requires a numeric value");
          }
          const value = parseInt(args[index + 1], 10);
          if (!Number.isFinite(value) || value <= 0) {
            throw new Error("test-rpcs --loops must be a positive integer");
          }
          swapLoops = value;
          index += 2;
        } else if (flag === "--delay" || flag === "--delay-ms") {
          if (index + 1 >= args.length) {
            throw new Error("test-rpcs --delay requires a numeric value (milliseconds)");
          }
          const value = parseInt(args[index + 1], 10);
          if (!Number.isFinite(value) || value < 0) {
            throw new Error("test-rpcs --delay must be >= 0");
          }
          swapDelayMs = value;
          index += 2;
        } else if (flag === "--amount") {
          if (index + 1 >= args.length) {
            throw new Error("test-rpcs --amount requires a decimal value in SOL");
          }
          swapAmount = args[index + 1].trim();
          if (!swapAmount) {
            throw new Error("test-rpcs --amount cannot be empty");
          }
          index += 2;
        } else if (flag === "--confirm") {
          swapConfirm = true;
          index += 1;
        } else {
          throw new Error(`Unknown flag for test-rpcs: ${token}`);
        }
      }
      await testRpcEndpoints(selector, {
        swapTest,
        swapLoops,
        swapDelayMs,
        swapAmount,
        swapConfirm,
      });
    } else if (cmd === "test-ultra") {
      const { options, rest } = parseCliOptions(args.slice(1));
      const inputMint =
        options.input ||
        options["input-mint"] ||
        rest[0] ||
        SOL_MINT;
      const outputMint =
        options.output ||
        options["output-mint"] ||
        rest[1] ||
        DEFAULT_USDC_MINT;
      const amount =
        options.amount ||
        options.size ||
        rest[2] ||
        "0.001";
      const walletOption =
        options.wallet ||
        options.from ||
        options["wallet-file"] ||
        null;
      const slippageOption =
        options.slippage ||
        options["slippage-bps"] ||
        options["slippagebps"] ||
        null;
      const submitOption = options.submit;
      const submit =
        submitOption === true ||
        (typeof submitOption === "string" &&
          submitOption.trim().toLowerCase() === "true");
      await testJupiterUltraOrder({
        inputMint,
        outputMint,
        amount,
        walletName: walletOption,
        submit,
        slippageBps: slippageOption,
      });
    } else if (cmd === "swap") {
      const inMint = (args[1] || "").trim();
      const outMint = (args[2] || "").trim();
      const amtStr = args[3];
      if (!inMint || !outMint) {
        throw new Error("swap usage: swap <inputMint> <outputMint> [amountInTokens|all|random]");
      }
      await ensureMintInfo(inMint);
      await ensureMintInfo(outMint);
      const inLabel = describeMintLabel(inMint);
      const outLabel = describeMintLabel(outMint);
      const amountPreview = amtStr ? amtStr : "default";
      console.log(
        paint(
          `swap plan: ${inLabel} → ${outLabel} amount=${amountPreview}`,
          "info"
        )
      );
      const amountArg = typeof amtStr === "string" && amtStr.trim().length > 0 ? amtStr : null;
      await doSwapAcross(inMint, outMint, amountArg);
    } else if (cmd === "swap-all") {
      const inMint = (args[1] || "").trim();
      const outMint = (args[2] || "").trim();
      if (!inMint || !outMint) {
        throw new Error("swap-all usage: swap-all <inputMint> <outputMint>");
      }
      await ensureMintInfo(inMint);
      await ensureMintInfo(outMint);
      console.log(
        paint(
          `swap-all plan: ${describeMintLabel(inMint)} → ${describeMintLabel(outMint)}`,
          "info"
        )
      );
      await doSwapAcross(inMint, outMint, "all");
    } else if (cmd === "swap-sol-to") {
      const targetMint = (args[1] || "").trim();
      const amountArg = args[2] || "all";
      if (!targetMint) throw new Error("swap-sol-to usage: swap-sol-to <targetMint> [amount|all]");
      await ensureMintInfo(targetMint);
      console.log(
        paint(
          `swap-sol-to plan: ${describeMintLabel(SOL_MINT)} → ${describeMintLabel(targetMint)} amount=${amountArg}`,
          "info"
        )
      );
      await doSwapAcross(SOL_MINT, targetMint, amountArg);
    } else if (cmd === "buckshot") {
      await runBuckshot();
    } else if (cmd === "target-loop") {
      const startMint = args[1] || null;
      await runInteractiveTargetLoop(startMint);
    } else if (cmd === "force-reset-wallets" || cmd === "force-reset-wallet") {
      forceResetWalletDisableState();
      console.log(
        paint(
          "Wallet swap guard reset: all wallets re-enabled until balances command runs.",
          "success"
        )
      );
    } else if (cmd === "sol-usdc-popcat") {
      await swapSolToUsdcThenPopcat();
    } else if (cmd === "interval-cycle" || cmd === "crew1-cycle") {
      await runWalletIntervalCycle();
    } else if (cmd === "long-circle") {
      const extraFlag = (args[1] || "").toLowerCase();
      const enableSecondary = extraFlag === "extra" || extraFlag === "--extra" || extraFlag === "secondary" || extraFlag === "--secondary";
      await runLongCircle({ enableSecondary });
    } else if (cmd === "sweep-defaults") {
      await sweepTokensToSol(DEFAULT_SWEEP_MINTS, "default tokens");
    } else if (cmd === "sweep-all") {
      await sweepAllTokensToSol();
    } else if (cmd === "sweep-to-btc-eth") {
      await sweepTokensToBtcEthTargets();
    } else if (
      cmd === "reclaim-sol" ||
      cmd === "close-token-accounts"
    ) {
      await closeEmptyTokenAccounts();
    } else if (cmd === "arpeggio") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("arpeggio", flowOpts);
    } else if (cmd === "horizon") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("horizon", flowOpts);
    } else if (cmd === "echo") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("echo", flowOpts);
    } else if (cmd === "icarus") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("icarus", flowOpts);
    } else if (cmd === "zenith") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("zenith", flowOpts);
    } else if (cmd === "aurora") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("aurora", flowOpts);
    } else if (cmd === "titan") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("titan", flowOpts);
    } else if (cmd === "odyssey") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("odyssey", flowOpts);
    } else if (cmd === "sovereign") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("sovereign", flowOpts);
    } else if (cmd === "nova") {
      const flowOpts = parseFlowOptions(args.slice(1));
      await runFlowWithLoopOption("nova", flowOpts);
    } else {
      throw new Error("Unknown command: " + cmd);
    }
  } catch (e) {
    console.error(paint("Error:", "error"), e.message);
    process.exit(1);
  }

  process.exit(0);
}

if (IS_MAIN_EXECUTION) {
  main();
}

export {
  SOL_MINT,
  listWallets,
  ensureAtaForMint,
  ensureWrappedSolBalance,
  resolveTokenProgramForMint,
  resolveRandomCatalogMint,
  pickRandomCatalogMint,
  createDeterministicRng,
  stepsFromMints,
  snapshotTokenCatalog,
  handleUnwrapWsolMenu,
};
