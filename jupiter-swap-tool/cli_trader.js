#!/usr/bin/env node

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";
import bs58 from "bs58";
import readline from "readline";
import * as bip39 from "bip39";
import { derivePath } from "ed25519-hd-key";
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
  getPerpsProgramId,
} from "./perps.js";
import { getPerpsProgramId } from "./perps/client.js";
import {
  instantiateCampaignForWallets,
  executeTimedPlansAcrossWallets,
  registerHooks as registerCampaignHooks,
  truncatePlanToBudget,
  CAMPAIGNS,
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
// version 1.1.2
// --------------------------------------------------

const TOOL_VERSION = "1.1.2";
const GENERAL_USAGE_MESSAGE =
  "Commands: tokens [--verbose|--refresh] | lend <earn|borrow> ... | lend overview | perps <markets|positions|open|close> [...options] | wallet <wrap|unwrap> <wallet> [amount|all] [--raw] | list | generate <n> [prefix] | import-wallet --secret <secret> [--prefix name] [--path path] [--force] | balances [tokenMint[:symbol] ...] | fund-all <from> <lamportsEach> | redistribute <wallet> | fund <from> <to> <lamports> | send <from> <to> <lamports> | aggregate <wallet> | airdrop <wallet> <lamports> | airdrop-all <lamports> | campaign <meme-carousel|scatter-then-converge|btc-eth-circuit> <30m|1h|2h|6h> [--batch <1|2|all>] [--dry-run] | swap <inputMint> <outputMint> [amount|all|random] | swap-all <inputMint> <outputMint> | swap-sol-to <mint> [amount|all|random] | buckshot | wallet-guard-status [--summary|--refresh] | test-rpcs [all|index|match|url] | test-ultra [inputMint] [outputMint] [amount] [--wallet name] [--submit] | sol-usdc-popcat | long-circle | crew1-cycle | sweep-defaults | sweep-all | sweep-to-btc-eth | reclaim-sol | target-loop [startMint] | force-reset-wallets";

function printGeneralUsage() {
  console.log(GENERAL_USAGE_MESSAGE);
}

// ---------------- Config ----------------
// All of the CLI's tunable parameters live in this block so the rest of the
// code has a single source of truth. Most values can be overridden via
// environment variables; RPC endpoints can also be provided via a file next
// to the script.
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

const SCRIPT_FILE_PATH = fileURLToPath(import.meta.url);
const SCRIPT_COMPARABLE_PATH =
  toComparablePath(SCRIPT_FILE_PATH) ?? path.normalize(SCRIPT_FILE_PATH);

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
const DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com";
const SCRIPT_DIR = path.dirname(SCRIPT_FILE_PATH);
const PERPS_COMPUTE_UNIT_LIMIT = process.env.PERPS_COMPUTE_UNIT_LIMIT
  ? Math.max(1, parseInt(process.env.PERPS_COMPUTE_UNIT_LIMIT, 10) || 0)
  : 1_200_000;
const PERPS_COMPUTE_UNIT_PRICE_MICROLAMPORTS =
  process.env.PERPS_COMPUTE_UNIT_PRICE_MICROLAMPORTS
    ? Math.max(
        0,
        parseInt(process.env.PERPS_COMPUTE_UNIT_PRICE_MICROLAMPORTS, 10) || 0
      )
    : 10_000;
const PERPS_MARKET_CACHE_PATH =
  process.env.PERPS_MARKET_CACHE_PATH ||
  path.resolve(SCRIPT_DIR, "perps/market_cache.json");
const RPC_LIST_FILE =
  process.env.RPC_LIST_FILE || path.resolve(SCRIPT_DIR, "rpc_endpoints.txt");
let RPC_ENDPOINTS_FILE_USED = null;
const UNHEALTHY_RPC_ENDPOINTS = new Map(); // endpoint -> unhealthyUntil timestamp
const DEFAULT_ULTRA_API_KEY = "91233f8d-d064-48c7-a97a-87b5d4d8a511";
const JUPITER_ULTRA_API_KEY = process.env.JUPITER_ULTRA_API_KEY || DEFAULT_ULTRA_API_KEY;
const JUPITER_SWAP_ENGINE = (process.env.JUPITER_SWAP_ENGINE || "ultra").toLowerCase();
const JUPITER_SWAP_API_BASE =
  process.env.JUPITER_SWAP_API_BASE || "https://lite-api.jup.ag";
const JUPITER_SWAP_QUOTE_URL = `${JUPITER_SWAP_API_BASE.replace(/\/$/, "")}/swap/v1/quote`;
const JUPITER_SWAP_URL = `${JUPITER_SWAP_API_BASE.replace(/\/$/, "")}/swap/v1/swap`;
const JUPITER_ULTRA_DEFAULT_BASE = (() => {
  if (JUPITER_ULTRA_API_KEY) {
    return `https://api.jup.ag/ultra/${JUPITER_ULTRA_API_KEY}`;
  }
  return "https://api.jup.ag/ultra/v1";
})();
const JUPITER_ULTRA_API_BASE_RAW =
  process.env.JUPITER_ULTRA_API_BASE || JUPITER_ULTRA_DEFAULT_BASE;
const JUPITER_ULTRA_API_BASE = JUPITER_ULTRA_API_BASE_RAW.replace(/\/$/, "");
const SHOULD_SEND_ULTRA_HEADER = !!JUPITER_ULTRA_API_KEY;
const JUPITER_ULTRA_ORDER_URL = `${JUPITER_ULTRA_API_BASE}/order`;
const JUPITER_ULTRA_EXECUTE_URL = `${JUPITER_ULTRA_API_BASE}/execute`;
const JUPITER_ULTRA_HOLDINGS_URL = `${JUPITER_ULTRA_API_BASE}/holdings`;
const JUPITER_ULTRA_SHIELD_URL = `${JUPITER_ULTRA_API_BASE}/shield`;
const JUPITER_ULTRA_SEARCH_URL = `${JUPITER_ULTRA_API_BASE}/search`;
const JUPITER_ULTRA_ROUTERS_URL = `${JUPITER_ULTRA_API_BASE}/routers`;
const SOL_MINT = "So11111111111111111111111111111111111111112";
const RAW_SWAP_AMOUNT_MODE = (process.env.SWAP_AMOUNT_MODE || "all").toLowerCase();
const DEFAULT_SWAP_AMOUNT_MODE = RAW_SWAP_AMOUNT_MODE === "random" ? "random" : "all";
const SLIPPAGE_BPS = process.env.SLIPPAGE_BPS ? Math.max(1, parseInt(process.env.SLIPPAGE_BPS, 10) || 1) : 20;
const DELAY_BETWEEN_CALLS_MS = 500;
const BALANCE_RPC_DELAY_MS = process.env.BALANCE_RPC_DELAY_MS
  ? Math.max(0, parseInt(process.env.BALANCE_RPC_DELAY_MS, 10) || 0)
  : 250;
const PRINT_SECRET_KEYS = process.env.PRINT_SECRET_KEYS === "1";
const PASSIVE_STEP_DELAY_MS = process.env.PASSIVE_STEP_DELAY_MS
  ? Math.max(0, parseInt(process.env.PASSIVE_STEP_DELAY_MS, 10) || 0)
  : 1500;
const PASSIVE_STEP_JITTER_MS = process.env.PASSIVE_STEP_DELAY_JITTER_MS
  ? Math.max(0, parseInt(process.env.PASSIVE_STEP_DELAY_JITTER_MS, 10) || 0)
  : 800;
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
const WALLET_DISABLE_THRESHOLD_LAMPORTS = BigInt(10_000_000); // 0.01 SOL guardrail
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
  process.env.JUPITER_PRICE_API_BASE || "https://api.jup.ag/price/v3";
const JUPITER_PRICE_ENDPOINT = `${JUPITER_PRICE_API_BASE}/price`;
const JUPITER_TOKENS_API_BASE =
  process.env.JUPITER_TOKENS_API_BASE || "https://api.jup.ag/tokens/v2";
const JUPITER_TOKENS_SEARCH_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/search`;
const JUPITER_TOKENS_TAG_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/tag`;
const JUPITER_TOKENS_CATEGORY_ENDPOINT = `${JUPITER_TOKENS_API_BASE}/category`;
const DEFAULT_LEND_EARN_BASE =
  process.env.JUPITER_LEND_API_BASE || "https://lite-api.jup.ag/lend/v1/earn";
const DEFAULT_LEND_BORROW_BASE =
  process.env.JUPITER_LEND_BORROW_API_BASE || "https://lite-api.jup.ag/lend/v1/borrow";
const DEFAULT_PERPS_API_BASE =
  process.env.JUPITER_PERPS_API_BASE || "https://lite-api.jup.ag/perps/v1";
const FALLBACK_LEND_EARN_BASES = [
  "https://api.jup.ag/lend/v1/earn",
  "https://api.jup.ag/lend/earn",
  "https://api.jup.ag/jup-integrators/earn",
];
const FALLBACK_LEND_BORROW_BASES = [
  "https://api.jup.ag/lend/v1/borrow",
  "https://api.jup.ag/lend/borrow",
  "https://api.jup.ag/jup-integrators/borrow",
];
const FALLBACK_PERPS_BASES = [
  "https://api.jup.ag/perps/v1",
  "https://api.jup.ag/perps",
];
const LEND_EARN_BASES = Array.from(
  new Set([DEFAULT_LEND_EARN_BASE, ...FALLBACK_LEND_EARN_BASES])
);
const LEND_BORROW_BASES = Array.from(
  new Set([DEFAULT_LEND_BORROW_BASE, ...FALLBACK_LEND_BORROW_BASES])
);
const PERPS_API_BASES = Array.from(
  new Set([DEFAULT_PERPS_API_BASE, ...FALLBACK_PERPS_BASES])
);
const USE_ULTRA_ENGINE = JUPITER_SWAP_ENGINE !== "lite";
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
    tags: ["default-sweep", "long-circle", "secondary-pool", "secondary-terminal", "crew-cycle"],
  },
  {
    symbol: "USDT",
    mint: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    decimals: 6,
    program: "spl",
    tags: [],
  },
  {
    symbol: "POPCAT",
    mint: "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
    decimals: 9,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "PUMP",
    mint: "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn",
    decimals: 6,
    program: "token-2022",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "PENGU",
    mint: "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
    decimals: 6,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "FART",
    mint: "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
    decimals: 6,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "USELESS",
    mint: "Dz9mQ9NzkBcCsuGPFJ3r1bS4wgqKMHBPiVuniW8Mbonk",
    decimals: 6,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "WIF",
    mint: "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    decimals: 6,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "PFP",
    mint: "5TfqNKZbn9AnNtzq8bbkyhKgcPGTfNDc9wNzFrTBpump",
    decimals: 6,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool"],
  },
  {
    symbol: "wBTC",
    mint: "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",
    decimals: 8,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool", "secondary-terminal"],
  },
  {
    symbol: "cbBTC",
    mint: "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij",
    decimals: 8,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool", "secondary-terminal"],
  },
  {
    symbol: "wETH",
    mint: "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",
    decimals: 8,
    program: "spl",
    tags: ["default-sweep", "crew-cycle", "long-circle", "secondary-pool", "secondary-terminal"],
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
const USELESS_MINT = mintBySymbol(
  "USELESS",
  "Dz9mQ9NzkBcCsuGPFJ3r1bS4wgqKMHBPiVuniW8Mbonk"
);
const WIF_MINT = mintBySymbol(
  "WIF",
  "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
);
const PFP_MINT = mintBySymbol(
  "PFP",
  "5TfqNKZbn9AnNtzq8bbkyhKgcPGTfNDc9wNzFrTBpump"
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
function markRpcEndpointUnhealthy(url, reason = "") {
  if (!url) return;
  const cooldownMs = 5 * 60 * 1000; // 5 minutes cooldown
  if (!UNHEALTHY_RPC_ENDPOINTS.has(url)) {
    const note = reason ? ` (${reason})` : "";
    console.warn(paint(`RPC endpoint ${url} marked unhealthy${note}`, "warn"));
  }
  UNHEALTHY_RPC_ENDPOINTS.set(url, Date.now() + cooldownMs);
}

function nextRpcEndpoint() {
  if (RPC_ENDPOINTS.length === 0) return DEFAULT_RPC_URL;
  const total = RPC_ENDPOINTS.length;
  for (let i = 0; i < total; i += 1) {
    const url = RPC_ENDPOINTS[rpcEndpointCursor];
    rpcEndpointCursor = (rpcEndpointCursor + 1) % total;
    const unhealthyUntil = UNHEALTHY_RPC_ENDPOINTS.get(url);
    if (unhealthyUntil && unhealthyUntil > Date.now()) {
      continue;
    }
    UNHEALTHY_RPC_ENDPOINTS.delete(url);
    return url;
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
  connection.__markUnhealthy = (reason) => markRpcEndpointUnhealthy(url, reason);
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
const LEND_BASE_ASSET_MINTS = new Set([
  SOL_MINT,
  DEFAULT_USDC_MINT,
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
  WBTC_MINT,
  CBBTC_MINT,
  WETH_MINT,
]);
function isLendShareToken(tokenRecord) {
  const symbol = tokenRecord?.symbol || "";
  return typeof symbol === "string" && symbol.toUpperCase().startsWith("JL");
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
  const baseHeaders = buildJsonApiHeaders(headers, { includeUltraKey: true });
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

const lendBorrowClient = createNamespaceClient({
  name: "lend borrow",
  bases: LEND_BORROW_BASES,
});

const perpsClient = createNamespaceClient({
  name: "perps",
  bases: PERPS_API_BASES,
});

async function lendEarnRequest(options) {
  return lendEarnClient.request(options);
}

async function lendBorrowRequest(options) {
  return lendBorrowClient.request(options);
}

async function perpsApiRequest(options) {
  return perpsClient.request(options);
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

function findWalletByName(name) {
  const wallet = listWallets().find((w) => w.name === name);
  if (!wallet) {
    throw new Error(`Wallet ${name} not found in keypairs directory`);
  }
  return wallet;
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
        "  Tip: the default Lend endpoint may be gated. Set JUPITER_LEND_API_BASE / JUPITER_LEND_BORROW_API_BASE to the correct integrator URL if you have one.",
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
  return expanded.map((item) => {
    const trimmed = item.trim();
    const match = existing.find((w) => w.name === trimmed);
    if (match) {
      return { name: match.name, pubkey: match.kp.publicKey.toBase58() };
    }
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
      "lend usage: lend <earn|borrow> <action> [...options] | lend overview"
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
  const rest = args.slice(1);
  if (category === "earn") {
    await handleLendEarnCommand(rest);
  } else if (category === "borrow") {
    await handleLendBorrowCommand(rest);
  } else {
    throw new Error(`Unknown lend category '${categoryRaw}'. Expected 'earn' or 'borrow'.`);
  }
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
  const { options, rest } = parseCliOptions(args);
  const filters = {};
  const search = options.query ?? options.search ?? rest[0];
  const status = options.status ?? options.state;
  const category = options.category ?? options.tag ?? options.type;
  const limitRaw = options.limit ?? options.max;
  if (search) filters.query = search;
  if (status) filters.status = status;
  if (category) filters.category = category;
  if (limitRaw !== undefined) {
    const parsedLimit = parseInt(limitRaw, 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      throw new Error("perps markets: --limit must be a positive integer");
    }
    filters.limit = parsedLimit;
  }
  const payload = Object.keys(filters).length > 0 ? filters : undefined;
  const request = buildPerpsRequest(
    "markets",
    payload,
    options,
    { defaultPath: "markets", defaultMethod: "GET" }
  );
  try {
    const result = await perpsApiRequest(request);
    logPerpsApiResult("markets", result);
    const markets =
      extractIterableFromLendData(result.data) ||
      (Array.isArray(result.data?.markets) ? result.data.markets : []);
    if (!Array.isArray(markets) || markets.length === 0) {
      console.log(paint("  No markets returned for the selected filters.", "muted"));
      return;
    }
    const sample = markets.slice(0, Math.min(markets.length, 8));
    console.log(
      paint(
        `  Showing ${sample.length}/${markets.length} market(s).`,
        "muted"
      )
    );
    for (const entry of sample) {
      console.log(paint(`    ${formatPerpsMarketSnippet(entry)}`, "info"));
    }
    if (markets.length > sample.length) {
      console.log(
        paint(`    ... ${markets.length - sample.length} more market(s).`, "muted")
      );
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
  const payload = {
    wallets: identifiers.map((entry) => entry.pubkey).join(","),
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
    logPerpsApiResult("positions", result);
    const entries = extractIterableFromLendData(result.data) || [];
    if (!Array.isArray(entries) || entries.length === 0) {
      console.log(
        paint(
          "  No perps positions found for the requested wallet scope.",
          "muted"
        )
      );
      return;
    }
    const owners = new Map();
    for (const entry of entries) {
      const ownerKey =
        normalizeWalletIdentifier(entry.owner) ||
        normalizeWalletIdentifier(entry.wallet) ||
        normalizeWalletIdentifier(entry.account);
      if (!ownerKey) continue;
      const label = walletNameMap.get(ownerKey) || ownerKey;
      owners.set(label, (owners.get(label) || 0) + 1);
    }
    console.log(
      paint(
        `  Found ${entries.length} open position(s) across ${owners.size || 1} wallet(s).`,
        "info"
      )
    );
    const sample = entries.slice(0, Math.min(entries.length, 6));
    for (const entry of sample) {
      const ownerKey =
        normalizeWalletIdentifier(entry.owner) ||
        normalizeWalletIdentifier(entry.wallet) ||
        normalizeWalletIdentifier(entry.account);
      const label = ownerKey && walletNameMap.get(ownerKey)
        ? walletNameMap.get(ownerKey)
        : ownerKey || "unknown";
      console.log(
        paint(`    ${label}: ${formatPerpsPositionSnippet(entry)}`, "muted")
      );
    }
    if (entries.length > sample.length) {
      console.log(
        paint(`    ... ${entries.length - sample.length} more position(s).`, "muted")
      );
    }
  } catch (err) {
    console.error(paint("Perps positions request failed:", "error"), err.message);
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
      "perps open usage: perps open <walletFile> <market> <side> <size> [price] [--options ...]"
    );
  }
  const wallet = findWalletByName(walletName);
  const payload = {
    wallet: wallet.kp.publicKey.toBase58(),
    market,
    side: normalizePerpsSide(sideRaw),
    size: sizeValue,
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
  console.log(
    paint(
      `  Wallet ${wallet.name}: ${wallet.kp.publicKey.toBase58()} — ${payload.side} ${payload.size} ${payload.market}`,
      "muted"
    )
  );
  const dryRun =
    coerceCliBoolean(options["dry-run"]) || coerceCliBoolean(options.dryRun);
  if (dryRun) {
    console.log(paint("  Dry-run enabled — request payload:", "warn"));
    console.log(JSON.stringify(payload, null, 2));
    return;
  }
  const request = buildPerpsRequest(
    "open",
    payload,
    options,
    { defaultPath: "orders/open", defaultMethod: "POST" }
  );
  try {
    const result = await perpsApiRequest(request);
    logPerpsApiResult("open", result);
  } catch (err) {
    console.error(paint("Perps open request failed:", "error"), err.message);
  }
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
  const request = buildPerpsRequest(
    "close",
    payload,
    options,
    { defaultPath: "positions/close", defaultMethod: "POST" }
  );
  try {
    const result = await perpsApiRequest(request);
    logPerpsApiResult("close", result);
  } catch (err) {
    console.error(paint("Perps close request failed:", "error"), err.message);
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
    case "redeem-instructions":
      await lendEarnTransferLike(action, rest, { valueField: "amount" });
      return;
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
    const result = await lendEarnRequest({
      path: "positions",
      method: "GET",
      query: { wallets: identifiers.map((entry) => entry.pubkey).join(",") },
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
  try {
    const result = await lendEarnRequest({
      path: "earnings",
      method: "GET",
      query: { wallets: identifiers.map((entry) => entry.pubkey).join(",") },
    });
    logLendApiResult("earn earnings", result);
  } catch (err) {
    console.error(paint("Lend earn earnings request failed:", "error"), err.message);
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

async function ensureAtaForMint(connection, wallet, mintPubkey, tokenProgram, options = {}) {
  return sharedEnsureAtaForMint(connection, wallet, mintPubkey, tokenProgram, options);
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
  const sections = [
    {
      label: "earn positions",
      runner: () =>
        lendEarnRequest({
          path: "positions",
          method: "GET",
          query: { wallets: walletCsv },
        }),
      summaryLabel: "earn positions",
    },
    {
      label: "earn earnings",
      runner: () =>
        lendEarnRequest({
          path: "earnings",
          method: "GET",
          query: { wallets: walletCsv },
        }),
      summaryLabel: "earn earnings",
    },
    {
      label: "borrow positions",
      runner: () =>
        lendBorrowRequest({
          path: "positions",
          method: "GET",
          query: { wallets: walletCsv },
        }),
      summaryLabel: "borrow positions",
    },
  ];
  for (const section of sections) {
    console.log(paint(`\n=== ${section.label} ===`, "label"));
    try {
      const result = await section.runner();
      logLendApiResult(section.label, result);
      summariseLendEntries(result, walletNameMap, {
        label: section.summaryLabel,
      });
    } catch (err) {
      console.error(
        paint(`${section.label} request failed:`, "error"),
        err.message || err
      );
    }
  }
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
  if (mintArg !== "*" && !tokenFilterRecord) {
    throw new Error(`Token ${mintArg} not found in catalog (try running 'tokens --refresh')`);
  }
  const tokenFilterLabel = (() => {
    if (mintArg === "*") {
      if (usesBaseAssets) {
        const baseSymbols = Array.from(LEND_BASE_ASSET_MINTS).map((mint) =>
          symbolForMint(mint)
        );
        const uniqueSymbols = [...new Set(baseSymbols.filter(Boolean))];
        return uniqueSymbols.length
          ? `eligible base assets (${uniqueSymbols.join("/")})`
          : "eligible base assets";
      }
      if (usesShareTokens) {
        return "all Jupiter lend share tokens (JL-*)";
      }
      return "all tokens with balances";
    }
    if (tokenFilterRecord?.symbol) {
      return `${tokenFilterRecord.symbol}`;
    }
    return mintArg;
  })();
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

    const filteredBalances = tokenFilterRecord
      ? eligibleBalances.filter((entry) => entry.tokenRecord.mint === tokenFilterRecord.mint)
      : eligibleBalances;

    if (!filteredBalances.length) {
      console.log(
        paint(
          `  Wallet ${wallet.name} has no balance for ${mintArg === "*" ? "selected tokens" : tokenFilterRecord.symbol}.`,
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

        const body = {
          wallet: wallet.kp.publicKey.toBase58(),
          tokenMint: currentBalance.tokenRecord.mint,
          tokenSymbol: currentBalance.tokenRecord.symbol,
          amountInput: displayAmount,
          amountDecimals: currentBalance.decimals,
          [field]: baseAmount,
          ...extra,
        };
        for (const [key, value] of Object.entries(options)) {
          if (ignoreKeys.has(key)) continue;
          body[key] = value;
        }

        if (body.asset === undefined) {
          body.asset = currentBalance.tokenRecord.mint;
        }
        if (body.assetSymbol === undefined && currentBalance.tokenRecord.symbol) {
          body.assetSymbol = currentBalance.tokenRecord.symbol;
        }
        if (body.signer === undefined) {
          body.signer = wallet.kp.publicKey.toBase58();
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

        let ataRetryNeeded = false;
        try {
          const result = await lendEarnRequest({
            path: action,
            method: "POST",
            body,
          });
          logLendApiResult(`earn ${action}`, result);
          if (!skipSendFlag && !isInstructionRequest) {
            const transactions = extractTransactionsFromLendResponse(result.data);
            if (transactions.length > 0) {
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

async function handleLendBorrowCommand(args) {
  const actionRaw = args[0];
  if (!actionRaw) {
    console.log(
      "lend borrow usage: lend borrow <pairs|positions|open|repay|close> [...options]"
    );
    return;
  }
  const action = actionRaw.toLowerCase();
  const rest = args.slice(1);
  switch (action) {
    case "pairs":
      await lendBorrowPairs(rest);
      return;
    case "positions":
      await lendBorrowPositions(rest);
      return;
    case "open":
      await lendBorrowOpen(rest);
      return;
    case "repay":
      await lendBorrowRepay(rest);
      return;
    case "close":
      await lendBorrowClose(rest);
      return;
    default:
      throw new Error(`Unknown lend borrow action '${actionRaw}'.`);
  }
}

async function lendBorrowPairs(args) {
  const { options } = parseCliOptions(args);
  const query = {};
  if (options.collateral) query.collateralMint = options.collateral;
  if (options.borrow) query.borrowMint = options.borrow;
  try {
    const result = await lendBorrowRequest({
      path: "pairs",
      method: "GET",
      query,
    });
    logLendApiResult("borrow pairs", result);
  } catch (err) {
    console.error(paint("Lend borrow pairs request failed:", "error"), err.message);
  }
}

async function lendBorrowPositions(args) {
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
    throw new Error("lend borrow positions usage: lend borrow positions <walletName|pubkey>[,...]");
  }
  const identifiers = resolveWalletIdentifiers(input);
  if (needsAllWallets) {
    console.log(
      paint(
        `  Targeting all ${identifiers.length} wallet(s) for borrow positions.`,
        "muted"
      )
    );
  } else if (identifiers.length > 1) {
    console.log(
      paint(
        `  Targeting ${identifiers.length} wallet(s) for borrow positions.`,
        "muted"
      )
    );
  }
  const borrowScope = identifiers
    .map((entry) => entry.name || entry.pubkey)
    .filter(Boolean);
  if (borrowScope.length > 0 && borrowScope.length <= 5) {
    console.log(paint(`  Wallet scope: ${borrowScope.join(", ")}`, "muted"));
  }
  try {
    const result = await lendBorrowRequest({
      path: "positions",
      method: "GET",
      query: { wallets: identifiers.map((entry) => entry.pubkey).join(",") },
    });
    logLendApiResult("borrow positions", result);
  } catch (err) {
    console.error(paint("Lend borrow positions request failed:", "error"), err.message);
  }
}

async function lendBorrowOpen(args) {
  const walletName = args[0];
  const collateralInput = args[1];
  const borrowInput = args[2];
  const collateralAmountInput = args[3];
  const borrowAmountInput = args[4];
  if (!walletName || !collateralInput || !borrowInput || !collateralAmountInput || !borrowAmountInput) {
    throw new Error("lend borrow open usage: lend borrow open <walletFile> <collateralMint|symbol> <borrowMint|symbol> <collateralAmount> <borrowAmount> [--extra '{...}']");
  }
  const wallet = findWalletByName(walletName);
  const collateralToken = await resolveTokenRecord(collateralInput);
  const borrowToken = await resolveTokenRecord(borrowInput);
  if (!collateralToken) {
    throw new Error(`Collateral token ${collateralInput} not found`);
  }
  if (!borrowToken) {
    throw new Error(`Borrow token ${borrowInput} not found`);
  }
  const tail = args.slice(5);
  const { options } = parseCliOptions(tail);
  const extra = parseJsonOption(options.extra, "--extra");
  const rawCollateral = options.rawCollateral === "true" || options.rawCollateral === true;
  const rawBorrow = options.rawBorrow === "true" || options.rawBorrow === true;
  const collateralAmount = rawCollateral
    ? collateralAmountInput
    : decimalToBaseUnits(collateralAmountInput, collateralToken.decimals).toString();
  const borrowAmount = rawBorrow
    ? borrowAmountInput
    : decimalToBaseUnits(borrowAmountInput, borrowToken.decimals).toString();
  const body = {
    wallet: wallet.kp.publicKey.toBase58(),
    collateralMint: collateralToken.mint,
    borrowMint: borrowToken.mint,
    collateralInput,
    borrowInput,
    collateralDecimals: collateralToken.decimals,
    borrowDecimals: borrowToken.decimals,
    collateralAmount,
    borrowAmount,
    ...extra,
  };
  const ignore = new Set(["extra", "rawCollateral", "rawBorrow"]);
  for (const [key, value] of Object.entries(options)) {
    if (ignore.has(key)) continue;
    body[key] = value;
  }
  console.log(
    paint("  request payload (borrow open)", "muted"),
    JSON.stringify(body, null, 2)
  );
  try {
    const result = await lendBorrowRequest({
      path: "open",
      method: "POST",
      body,
    });
    logLendApiResult("borrow open", result);
  } catch (err) {
    console.error(paint("Lend borrow open request failed:", "error"), err.message);
  }
}

async function lendBorrowRepay(args) {
  const walletName = args[0];
  const borrowMintInput = args[1];
  const repayAmountInput = args[2];
  if (!walletName || !borrowMintInput || !repayAmountInput) {
    throw new Error("lend borrow repay usage: lend borrow repay <walletFile> <borrowMint|symbol> <amount> [--position id] [--extra '{...}']");
  }
  const wallet = findWalletByName(walletName);
  const borrowToken = await resolveTokenRecord(borrowMintInput);
  if (!borrowToken) {
    throw new Error(`Borrow token ${borrowMintInput} not found`);
  }
  const tail = args.slice(3);
  const { options } = parseCliOptions(tail);
  const extra = parseJsonOption(options.extra, "--extra");
  const raw = options.raw === "true" || options.raw === true;
  const repayAmount = raw
    ? repayAmountInput
    : decimalToBaseUnits(repayAmountInput, borrowToken.decimals).toString();
  const body = {
    wallet: wallet.kp.publicKey.toBase58(),
    borrowMint: borrowToken.mint,
    borrowDecimals: borrowToken.decimals,
    borrowAmount: repayAmount,
    amountInput: repayAmountInput,
    ...extra,
  };
  if (options.position) body.positionId = options.position;
  const ignore = new Set(["extra", "raw", "position"]);
  for (const [key, value] of Object.entries(options)) {
    if (ignore.has(key)) continue;
    body[key] = value;
  }
  console.log(
    paint("  request payload (borrow repay)", "muted"),
    JSON.stringify(body, null, 2)
  );
  try {
    const result = await lendBorrowRequest({
      path: "repay",
      method: "POST",
      body,
    });
    logLendApiResult("borrow repay", result);
  } catch (err) {
    console.error(paint("Lend borrow repay request failed:", "error"), err.message);
  }
}

async function lendBorrowClose(args) {
  const walletName = args[0];
  if (!walletName) {
    throw new Error("lend borrow close usage: lend borrow close <walletFile> [positionId|*] [--all] [--extra '{...}']");
  }
  const wallet = findWalletByName(walletName);
  const { options, rest } = parseCliOptions(args.slice(1));
  const extra = parseJsonOption(options.extra, "--extra");
  const explicitIds = rest
    .map((item) => item.trim())
    .filter((item) => item.length > 0 && item !== "*");
  const wildcardRequested =
    rest.length === 0 ||
    rest.some((item) => {
      const trimmed = item.trim().toLowerCase();
      return trimmed.length === 0 || trimmed === "*" || trimmed === "all";
    }) ||
    options.all === true ||
    (typeof options.all === "string" &&
      options.all.trim().toLowerCase() === "true");
  if (!wildcardRequested && options.position && typeof options.position === "string") {
    explicitIds.push(options.position.trim());
  }
  let positionIds = Array.from(new Set(explicitIds.filter(Boolean)));
  const walletPubkey = wallet.kp.publicKey.toBase58();
  if (wildcardRequested) {
    console.log(
      paint(
        `  Fetching borrow positions for ${wallet.name} to close all.`,
        "muted"
      )
    );
    try {
      const result = await lendBorrowRequest({
        path: "positions",
        method: "GET",
        query: { wallets: walletPubkey },
      });
      if (!result.ok) {
        console.error(
          paint(
            `Unable to enumerate borrow positions for ${wallet.name}:`,
            "error"
          ),
          result.status,
          result.data?.error || result.raw || ""
        );
        return;
      }
      const entries = extractIterableFromLendData(result.data) || [];
      const ids = collectBorrowPositionIds(entries, walletPubkey);
      positionIds = ids;
      if (!ids.length) {
        console.log(
          paint(
            `  No open borrow positions found for ${wallet.name}.`,
            "muted"
          )
        );
        return;
      }
      console.log(
        paint(
          `  Closing ${ids.length} borrow position(s) for ${wallet.name}.`,
          "info"
        )
      );
    } catch (err) {
      console.error(
        paint(
          `Unable to enumerate borrow positions for ${wallet.name}:`,
          "error"
        ),
        err.message || err
      );
      return;
    }
  }
  if (!positionIds.length) {
    throw new Error("lend borrow close usage: supply a position id or request --all/\"*\".");
  }
  const ignore = new Set(["extra", "all", "position"]);
  const additionalFields = {};
  for (const [key, value] of Object.entries(options)) {
    if (ignore.has(key)) continue;
    additionalFields[key] = value;
  }
  for (const positionId of positionIds) {
    const body = {
      wallet: walletPubkey,
      positionId,
      ...extra,
      ...additionalFields,
    };
    console.log(
      paint(
        `  request payload (borrow close) → ${wallet.name}`,
        "muted"
      ),
      JSON.stringify(body, null, 2)
    );
    try {
      const result = await lendBorrowRequest({
        path: "close",
        method: "POST",
        body,
      });
      logLendApiResult("borrow close", result);
    } catch (err) {
      console.error(
        paint(
          `Lend borrow close request failed for ${wallet.name} (position ${positionId}):`,
          "error"
        ),
        err.message || err
      );
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
  if (USE_ULTRA_ENGINE) {
    const engineLabel = JUPITER_ULTRA_API_KEY ? "Ultra API (authenticated)" : "Ultra API (no key)";
    console.log(paint(`Swap engine: ${engineLabel}`, JUPITER_ULTRA_API_KEY ? "info" : "warn"));
    if (!JUPITER_ULTRA_API_KEY) {
      console.log(paint("  Tip: set JUPITER_ULTRA_API_KEY for higher limits.", "warn"));
    }
  } else {
    console.log(paint("Swap engine: Legacy Lite API", "muted"));
  }
  console.log(
    paint(
      "Tip: run `tokens --verbose` to inspect the catalog used by automated flows.",
      "muted"
    )
  );
}


const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

function shuffleArray(array) {
  const result = [...array];
  for (let i = result.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
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
    splToLamports: async (pubkeyBase58, mint, uiAmount) => {
      return campaignSplToLamports(pubkeyBase58, mint, uiAmount);
    },
    findSplHoldingForMint: async (pubkeyBase58, mint) => {
      return campaignFindHoldingForMint(pubkeyBase58, mint);
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

async function campaignFindHoldingForMint(pubkeyBase58, mint) {
  if (!mint) {
    return null;
  }
  const entry = getCampaignWallet(pubkeyBase58);
  const owner = entry.wallet.kp.publicKey;
  const connection = createRpcConnection("confirmed");
  try {
    const parsed = await getAllParsedTokenAccounts(connection, owner);
    for (const { account } of parsed) {
      const info = account?.data?.parsed?.info;
      if (!info) continue;
      if (info.mint !== mint) continue;
      const rawAmount = info.tokenAmount?.amount ?? "0";
      let amount;
      try {
        amount = BigInt(rawAmount);
      } catch (_) {
        amount = 0n;
      }
      if (amount <= 0n) {
        const fallbackUi = info.tokenAmount?.uiAmount ?? info.tokenAmount?.uiAmountString;
        if (!fallbackUi) {
          continue;
        }
        return {
          mint,
          uiAmount: fallbackUi,
          decimals: info.tokenAmount?.decimals ?? 0,
        };
      }
      return {
        mint,
        uiAmount: rawAmount,
        decimals: info.tokenAmount?.decimals ?? 0,
      };
    }
    return null;
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
  if (!campaignKeyRaw || !durationKeyRaw) {
    throw new Error(
      "campaign usage: campaign <meme-carousel|scatter-then-converge|btc-eth-circuit> <30m|1h|2h|6h> [--batch <1|2|all>] [--dry-run]"
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

  const pubkeys = wallets.map((wallet) => wallet.kp.publicKey.toBase58());
  const { plansByWallet } = instantiateCampaignForWallets({
    campaignKey,
    durationKey,
    walletPubkeys: pubkeys,
  });

  const connection = createRpcConnection("confirmed");
  const preparedPlans = new Map();
  for (const wallet of wallets) {
    const pubkey = wallet.kp.publicKey.toBase58();
    const plan = plansByWallet.get(pubkey);
    if (!plan) continue;
    let balance = 0n;
    try {
      balance = BigInt(await connection.getBalance(wallet.kp.publicKey));
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
    preparedPlans.set(pubkey, { schedule: truncated, rng: plan.rng });
  }

  if (preparedPlans.size === 0) {
    console.log(paint("No wallets have sufficient balance to participate.", "warn"));
    return;
  }

  campaignDryRun = dryRun;
  const swapCounts = [];
  for (const [pubkey, { schedule }] of preparedPlans.entries()) {
    const swapSteps = schedule.filter((step) => step.kind === "swapHop").length;
    const checkpointSteps = schedule.filter((step) => step.kind === "checkpointToSOL").length;
    const label = campaignWalletRegistry.get(pubkey)?.name || pubkey;
    swapCounts.push({ label, swapSteps, checkpointSteps });
  }

  console.log(
    paint(
      `Starting campaign ${campaignKey} (${durationKey}) across ${preparedPlans.size} wallet(s) — dryRun=${dryRun ? "yes" : "no"}.`,
      "info"
    )
  );
  swapCounts.forEach(({ label, swapSteps, checkpointSteps }) => {
    console.log(
      paint(
        `  ${label}: ${swapSteps} swap(s) + ${checkpointSteps} checkpoint(s) scheduled.`,
        "muted"
      )
    );
  });

  await executeTimedPlansAcrossWallets({ plansByWallet: preparedPlans });
  console.log(paint("Campaign complete.", "success"));
}

function decimalToBaseUnits(amountStr, decimals) {
  if (typeof amountStr !== "string") amountStr = String(amountStr);
  const normalized = amountStr.trim();
  if (!/^[0-9]+(\.[0-9]+)?$/.test(normalized)) {
    throw new Error(`Invalid decimal amount: ${amountStr}`);
  }
  const [wholePart, fractionalPart = ""] = normalized.split(".");
  if (fractionalPart.length > decimals) {
    throw new Error(
      `Amount ${amountStr} has more fractional digits than supported (${decimals})`
    );
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
async function calculateTotalGasRequirements(steps, connection, walletPublicKey) {
  if (!steps || steps.length === 0) return { totalGas: 0n, ataCreations: 0, breakdown: [] };
  
  let totalGas = 0n;
  let ataCreations = 0;
  const breakdown = [];
  const uniqueTokens = new Set();
  
  // Collect all unique tokens that will need ATAs
  for (const step of steps) {
    if (!SOL_LIKE_MINTS.has(step.to)) {
      uniqueTokens.add(step.to);
    }
  }
  
  // Estimate ATA creation costs
  for (const tokenMint of uniqueTokens) {
    try {
      const ataAddr = await getAssociatedTokenAddress(
        new PublicKey(tokenMint),
        walletPublicKey,
        false,
        TOKEN_PROGRAM_ID,
        ASSOCIATED_TOKEN_PROGRAM_ID
      );
      const ataInfo = await connection.getAccountInfo(ataAddr);
      if (ataInfo === null) {
        ataCreations++;
        totalGas += ESTIMATED_ATA_CREATION_LAMPORTS;
        breakdown.push({
          type: 'ata_creation',
          token: symbolForMint(tokenMint),
          cost: ESTIMATED_ATA_CREATION_LAMPORTS
        });
      }
    } catch (err) {
      // If we can't check, assume we need to create it (conservative estimate)
      ataCreations++;
      totalGas += ESTIMATED_ATA_CREATION_LAMPORTS;
      breakdown.push({
        type: 'ata_creation',
        token: symbolForMint(tokenMint),
        cost: ESTIMATED_ATA_CREATION_LAMPORTS
      });
    }
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
    } catch (_) {}
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

  console.log(
    paint(
      `Sweeping ${uniqueMints.length} token(s) back to SOL${label ? ` (${label})` : ""}...`,
      "label"
    )
  );

  for (const mint of uniqueMints) {
    try {
      await doSwapAcross(mint, SOL_MINT, "all");
    } catch (err) {
      logDetailedError(`  sweep ${mint} failed`, err);
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

  for (const w of wallets) {
    const connection = createRpcConnection("confirmed");
    const parsedAccounts = await getAllParsedTokenAccounts(connection, w.kp.publicKey);
    for (const { account } of parsedAccounts) {
      const info = account.data.parsed.info;
      const mint = info.mint;
      const amount = BigInt(info.tokenAmount.amount);
      if (amount > 0n && !SOL_LIKE_MINTS.has(mint)) {
        mintSet.add(mint);
      }
    }
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

  const targets = [
    { mint: WBTC_MINT, label: "wBTC" },
    { mint: CBBTC_MINT, label: "cbBTC" },
    { mint: WETH_MINT, label: "wETH" },
  ];

  const randomMode = DEFAULT_SWAP_AMOUNT_MODE === "random";
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
      reserve += cachedAtaRentLamports * BigInt(targets.length);
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

      const weights = targets.map(() => randomMode ? BigInt(randomIntInclusive(1, 100)) : 1n);
      const totalWeight = weights.reduce((acc, w) => acc + w, 0n);
      if (totalWeight === 0n) {
        console.log(paint(`Skipping ${wallet.name}: unable to derive allocation weights`, "muted"));
        continue;
      }

      const summary = `Allocating ${formatBaseUnits(spendable, 9)} SOL from ${wallet.name} across ${targets.length} targets (${randomMode ? "random" : "even"} split)`;
      const allocations = [];
      let remaining = spendable;
      for (let i = 0; i < targets.length; i += 1) {
        const target = targets[i];
        let share;
        if (i === targets.length - 1) {
          share = remaining;
        } else {
          share = (spendable * weights[i]) / totalWeight;
          if (share > remaining) share = remaining;
        }
        if (share <= 0n) {
          continue;
        }
        const shareDecimal = formatBaseUnits(share, 9);
        allocations.push({
          mint: target.mint,
          label: target.label,
          amountDecimal: shareDecimal,
        });
        remaining -= share;
        if (remaining <= 0n) break;
      }

      if (allocations.length > 0) {
        entries.push({
          wallet,
          summary,
          allocations,
        });
      }

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
  for (const step of steps) {
    index += 1;
    const fromSymbol = MINT_SYMBOL_OVERRIDES.get(step.from) || step.from.slice(0, 4);
    const toSymbol = MINT_SYMBOL_OVERRIDES.get(step.to) || step.to.slice(0, 4);
    const descriptor = step.description || `${fromSymbol} -> ${toSymbol}`;
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

const CREW1_CYCLE_TOKENS = [
  { mint: POPCAT_MINT, symbol: 'POPCAT' },
  { mint: PUMP_MINT, symbol: 'PUMP' },
  { mint: PENGU_MINT, symbol: 'PENGU' },
  { mint: FARTCOIN_MINT, symbol: 'FART' },
  { mint: USELESS_MINT, symbol: 'USELESS' },
  { mint: WIF_MINT, symbol: 'WIF' },
  { mint: PFP_MINT, symbol: 'PFP' },
  { mint: WBTC_MINT, symbol: 'wBTC' },
  { mint: CBBTC_MINT, symbol: 'cbBTC' },
  { mint: WETH_MINT, symbol: 'wETH' },
];

const LONG_CHAIN_SEGMENTS_BASE = [
  { name: 'sol-usdc-popcat', mints: [SOL_MINT, DEFAULT_USDC_MINT, POPCAT_MINT, SOL_MINT] },
  { name: 'sol-pump', mints: [SOL_MINT, PUMP_MINT, SOL_MINT] },
  { name: 'sol-pengu-fart', mints: [SOL_MINT, PENGU_MINT, FARTCOIN_MINT, SOL_MINT] },
  { name: 'sol-usdc-useless', mints: [SOL_MINT, DEFAULT_USDC_MINT, USELESS_MINT, SOL_MINT] },
  { name: 'sol-wif', mints: [SOL_MINT, WIF_MINT, SOL_MINT] },
  { name: 'sol-pfp-loop', mints: [SOL_MINT, DEFAULT_USDC_MINT, PFP_MINT, POPCAT_MINT, PENGU_MINT, DEFAULT_USDC_MINT, SOL_MINT] },
  { name: 'sol-wbtc', mints: [SOL_MINT, WBTC_MINT, SOL_MINT] },
  { name: 'sol-cbbtc', mints: [SOL_MINT, CBBTC_MINT, SOL_MINT] },
  { name: 'sol-weth', mints: [SOL_MINT, WETH_MINT, SOL_MINT] },
];

const SECONDARY_RANDOM_POOL = [
  DEFAULT_USDC_MINT,
  POPCAT_MINT,
  PUMP_MINT,
  PENGU_MINT,
  FARTCOIN_MINT,
  USELESS_MINT,
  WIF_MINT,
  PFP_MINT,
  WBTC_MINT,
  CBBTC_MINT,
  WETH_MINT,
];

const SECONDARY_TERMINALS = [
  DEFAULT_USDC_MINT,
  WBTC_MINT,
  CBBTC_MINT,
  WETH_MINT,
  SOL_MINT,
];

const BUCKSHOT_TOKEN_MINTS = Array.from(
  new Set(
    LONG_CHAIN_SEGMENTS_BASE.flatMap((segment) =>
      segment.mints
        .map((mint) => normaliseSolMint(mint))
        .filter((mint) => !SOL_LIKE_MINTS.has(mint))
    )
  )
);

function stepsFromMints(mints, options = {}) {
  const steps = [];
  if (!Array.isArray(mints) || mints.length < 2) return steps;
  for (let i = 0; i < mints.length - 1; i += 1) {
    const from = normaliseSolMint(mints[i]);
    const to = normaliseSolMint(mints[i + 1]);
    if (from === to) continue;
    steps.push({
      from,
      to,
      description: `${symbolForMint(from)} -> ${symbolForMint(to)}`,
      forceAll: options.forceAll === true,
    });
  }
  return steps;
}

function flattenSegmentsToSteps(segments) {
  const steps = [];
  for (const segment of segments) {
    steps.push(
      ...stepsFromMints(segment.mints, { forceAll: segment.forceAll })
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
function selectSegmentsForWallet(randomMode) {
  if (!randomMode) return LONG_CHAIN_SEGMENTS_BASE;
  const shuffled = shuffleArray(LONG_CHAIN_SEGMENTS_BASE);
  const maxSegments = LONG_CHAIN_SEGMENTS_BASE.length;
  const minSegments = Math.min(2, maxSegments);
  for (let attempt = 0; attempt < maxSegments; attempt += 1) {
    const chosenCount = randomIntInclusive(minSegments, maxSegments);
    const candidate = shuffled.slice(0, chosenCount);
    if (flattenSegmentsToSteps(candidate).length >= 3) {
      return candidate;
    }
  }
  return shuffled;
}

// Generates the optional post-chain random sweep path. Ensures at least
// three hops so the run is meaningful, falling back to the full token list
// if random selection still ends up too short.
function buildSecondaryPathMints(randomMode) {
  if (!randomMode) {
    return [SOL_MINT, DEFAULT_USDC_MINT];
  }
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const terminal = SECONDARY_TERMINALS[randomIntInclusive(0, SECONDARY_TERMINALS.length - 1)];
    const pool = shuffleArray(
      SECONDARY_RANDOM_POOL.filter((mint) => mint !== terminal)
    );
    const maxIntermediates = Math.min(SECONDARY_RANDOM_POOL.length, pool.length);
    let intermediateCount = randomIntInclusive(1, Math.max(1, maxIntermediates));
    const intermediates = pool.slice(0, intermediateCount);
    const path = [SOL_MINT, ...intermediates, terminal];
    const deduped = [];
    for (const mint of path) {
      if (deduped.length === 0 || deduped[deduped.length - 1] !== mint) {
        deduped.push(mint);
      }
    }
    if (deduped.length - 1 >= 3) {
      return deduped;
    }
  }
  const fallback = [SOL_MINT, ...SECONDARY_RANDOM_POOL, DEFAULT_USDC_MINT];
  const uniqueFallback = [];
  const seen = new Set();
  for (const mint of fallback) {
    if (seen.has(mint)) continue;
    seen.add(mint);
    uniqueFallback.push(mint);
  }
  return uniqueFallback;
}

async function executeSwapPlanForWallet(wallet, steps, label, options = {}) {
  if (!steps || steps.length === 0) return;
  if (isWalletDisabledByGuard(wallet.name)) {
    console.log(
      paint(
        `Skipping ${wallet.name}: disabled for swaps (<0.01 SOL).`,
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
async function runCrew1Cycle() {
  const wallet = listWallets().find((w) => w.name === 'crew_1.json');
  if (!wallet) {
    console.log(paint('crew_1.json not found; cannot run crew1-cycle.', 'warn'));
    return;
  }

  const SWAP_DELAY_MS = 60_000;
  const LAP_RESTS_MS = [120_000, 180_000];
  const laps = 3;
  const options = {
    wallets: [wallet],
    quietSkips: true,
    suppressMetadata: false,
    walletDelayMs: 0,
  };

  const address = wallet.kp.publicKey.toBase58();
  console.log(paint(`Crew1 cycle starting for ${wallet.name} (${address})`, 'label'));

  for (let lap = 0; lap < laps; lap += 1) {
    console.log(paint(`
-- crew1 cycle lap ${lap + 1}/${laps} --`, 'label'));
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

  console.log(paint('Crew1 cycle completed.', 'success'));
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
      let segments = selectSegmentsForWallet(randomMode);
      let steps = flattenSegmentsToSteps(segments);
      if (randomMode && steps.length < 3) {
        const extended = new Set(segments);
        for (const segment of LONG_CHAIN_SEGMENTS_BASE) {
          if (extended.has(segment)) continue;
          extended.add(segment);
          const candidateSegments = Array.from(extended);
          const candidateSteps = flattenSegmentsToSteps(candidateSegments);
          if (candidateSteps.length >= 3) {
            segments = candidateSegments;
            steps = candidateSteps;
            break;
          }
        }
      }
      return {
        wallet,
        steps,
        summary: describeStepSequence(steps),
        skipRegistry: new Set(),
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
        const secondaryPath = buildSecondaryPathMints(randomMode);
        const secondarySteps = stepsFromMints(secondaryPath);
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

async function runBuckshot() {
  const wallets = listWallets();
  if (wallets.length === 0) {
    console.log(paint("No wallets found", "muted"));
    return;
  }
  if (BUCKSHOT_TOKEN_MINTS.length === 0) {
    console.log(paint("Buckshot token list empty; nothing to do.", "muted"));
    return;
  }

  console.log(
    paint(
      `Buckshot mode — targeting ${BUCKSHOT_TOKEN_MINTS.length} token${BUCKSHOT_TOKEN_MINTS.length === 1 ? '' : 's'} from round-robin set`,
      "label"
    )
  );

  const walletHoldings = new Map();
  const tokenCount = BigInt(BUCKSHOT_TOKEN_MINTS.length);

  const planEntries = await measureAsync("buckshot:plan-wallets", async () => {
    const entries = [];
    for (const wallet of wallets) {
      if (isWalletDisabledByGuard(wallet.name)) {
        console.log(
          paint(
            `Skipping ${wallet.name}: disabled for swaps (<0.01 SOL).`,
            "muted"
          )
        );
        continue;
      }

      const connection = createRpcConnection("confirmed");
      const ataRent = await ensureCachedAtaRentLamports(connection);
      const solLamports = BigInt(await getSolBalance(connection, wallet.kp.publicKey));
      const spendable = computeBuckshotSpendable(solLamports, ataRent);
      if (spendable <= 0n) {
        console.log(
          paint(
            `Skipping ${wallet.name}: SOL balance ${formatBaseUnits(solLamports, 9)} below reserve requirement for buckshot.`,
            "muted"
          )
        );
        continue;
      }
      if (tokenCount === 0n) break;
      const perToken = spendable / tokenCount;
      if (perToken <= 0n) {
        console.log(
          paint(
            `Skipping ${wallet.name}: spendable SOL too small to distribute across ${BUCKSHOT_TOKEN_MINTS.length} tokens.`,
            "muted"
          )
        );
        continue;
      }

      entries.push({
        wallet,
        perTokenDecimal: formatBaseUnits(perToken, 9),
      });
      walletHoldings.set(wallet.name, new Set());
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
        `  ${plan.wallet.name}: ${plan.perTokenDecimal} SOL per token (post-reserve)`,
        "muted"
      )
    );
  }

  await measureAsync("buckshot:execute-swaps", async () => {
    for (const plan of planEntries) {
      const { wallet, perTokenDecimal } = plan;
      console.log(
        paint(
          `\n=== Buckshot plan for ${wallet.name} (${wallet.kp.publicKey.toBase58()}) — ${perTokenDecimal} SOL per token ===`,
          "label"
        )
      );

      const holdingsSet = walletHoldings.get(wallet.name) || new Set();

      for (const mint of BUCKSHOT_TOKEN_MINTS) {
        const symbol = symbolForMint(mint);
        console.log(
          paint(
            `  ${symbol}: swapping ${perTokenDecimal} SOL -> ${symbol}`,
            "info"
          )
        );
        await doSwapAcross(SOL_MINT, mint, perTokenDecimal, {
          wallets: [wallet],
          quietSkips: true,
          suppressMetadata: true,
          maxSlippageRetries: 7,
          slippageBoostAfter: 3,
          slippageBoostStrategy: "add",
          slippageBoostIncrementBps: 200,
        });
        holdingsSet.add(mint);
        await passiveSleep();
      }

      walletHoldings.set(wallet.name, holdingsSet);
    }
  });

  console.log(
    paint(
      "\nBuckshot distribution complete. Wallets will hold token positions until a new target mint is supplied.",
      "success"
    )
  );
  console.log(
    paint(
      "Enter a mint address to rotate all held tokens into the new target (blank to exit).",
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
    const lowered = rawInput.toLowerCase();
    if (lowered === "exit" || lowered === "quit" || lowered === "q") {
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
            `Skipping ${wallet.name}: disabled for swaps (<0.01 SOL).`,
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

  console.log(
    paint(
      "\nTarget loop mode — paste mint addresses to rotate holdings. Type 'sol' to flatten back to SOL, or 'exit' to leave.",
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
    console.log(
      paint(
        "Commands: paste a mint to rotate into it, 'list' to print the token catalog, 'sol' to swap current holdings back to SOL, 'help' to reprint this message, 'exit' to finish.",
        "info"
      )
    );
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
      const lowered = rawInput.toLowerCase();
      if (lowered === "exit" || lowered === "quit" || lowered === "q") {
        break;
      }
      if (lowered === "help" || lowered === "?") {
        printHelp();
        continue;
      }
      if (lowered === "list" || lowered === "catalog" || lowered === "tokens") {
        listTokenCatalog({ verbose: false });
        continue;
      }
      if (lowered === "sol" || lowered === "base") {
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
            "Input not recognised. Paste a valid mint address or type 'help'.",
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
            `Holding ${targetSymbol}. Paste another mint, type 'sol' to flatten, or 'exit' to leave.`,
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

  for (const w of wallets) {
    const lookupConnection = createRpcConnection("confirmed");
    const parsed = await getAllParsedTokenAccounts(lookupConnection, w.kp.publicKey);

    let closedCount = 0;
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

      try {
        const closeConnection = createRpcConnection("confirmed");
        const sig = await closeAccount(
          closeConnection,
          w.kp,
          pubkey,
          w.kp.publicKey,
          w.kp,
          [],
          undefined,
          programId
        );
        console.log(
          paint(
            `Closed token account ${pubkey.toBase58()} — tx ${sig}`,
            "success"
          )
        );
        closedCount += 1;
      } catch (err) {
        logDetailedError(`  close ${pubkey.toBase58()} failed`, err);
      }
    }

    if (closedCount === 0) {
      console.log(paint(`No empty token accounts for ${w.name}.`, "muted"));
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
    "clientOrderId",
    "orderId",
    "id",
    "data.clientOrderId",
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
    res = await fetch(`${JUPITER_SWAP_QUOTE_URL}?${params.toString()}`);
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
    res = await fetch(JUPITER_SWAP_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
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
  const payload = {
    inputMint,
    outputMint,
    amount: amountLamports.toString(),
    slippageBps,
    userPublicKey,
    wrapAndUnwrapSol,
    swapMode: "ExactIn",
  };
  const result = await ultraApiRequest({
    path: "order",
    method: "POST",
    body: payload,
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
  const body = {
    transaction: signedTransaction,
    swapTransaction: signedTransaction,
  };
  if (Array.isArray(signatureHint) && signatureHint.length > 0) {
    body.signatures = signatureHint;
  } else if (typeof signatureHint === "string" && signatureHint.length > 0) {
    body.signature = signatureHint;
    body.signatures = [signatureHint];
  }
  if (clientOrderId) {
    body.clientOrderId = clientOrderId;
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
        `  Warning: wallet guard disabled ${wallet.name}. Ensure it has at least 0.01 SOL before submitting.`,
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
  if (wallets.length === 0) {
    if (!fs.existsSync(KEYPAIR_DIR)) {
      console.log(paint(`No keypairs directory found at ${KEYPAIR_DIR}`, "warn"));
      console.log(paint("Use wallet menu (hotkey 'w') or 'generate <n> [prefix]' command to create wallets", "info"));
    } else {
      console.log(paint(`No wallets found in ${KEYPAIR_DIR}`, "warn"));
      console.log(paint("Use wallet menu (hotkey 'w') or 'generate <n> [prefix]' command to create wallets", "info"));
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
    "wallet wrap usage: wallet wrap <walletName> [amount|all] [--raw]";
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
    "wallet unwrap usage: wallet unwrap <walletName> [amount|all] [--raw]";
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
      "wallet usage: wallet <wrap|unwrap> <walletName> [amount|all] [--raw]"
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
  throw new Error(
    `Unknown wallet subcommand '${subcommandRaw}'. Expected 'wrap' or 'unwrap'.`
  );
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
  const targetIndex = wallets.findIndex((w) => w.name === targetWalletName);
  if (targetIndex === -1) {
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
  if (wallets.length === 0) {
    if (!fs.existsSync(KEYPAIR_DIR)) {
      console.log(paint(`No keypairs directory found at ${KEYPAIR_DIR}`, "warn"));
      console.log(paint("Use wallet menu (hotkey 'w') or 'generate <n> [prefix]' command to create wallets", "info"));
    } else {
      console.log(paint(`No wallets found in ${KEYPAIR_DIR}`, "muted"));
      console.log(paint("Use wallet menu (hotkey 'w') or 'generate <n> [prefix]' command to create wallets", "info"));
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
          "  status: disabled for swaps (<0.01 SOL). Fund and run balances or use force reset to re-enable.",
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
  if (!includeDisabled) {
    const disabledWithinScope = walletList.filter((w) =>
      isWalletDisabledByGuard(w.name)
    );
    if (disabledWithinScope.length > 0 && !quietSkips) {
      for (const disabledWallet of disabledWithinScope) {
        console.log(
          paint(
            `Skipping ${disabledWallet.name}: disabled for swaps (<0.01 SOL).`,
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
  const walletDelayMs = options.walletDelayMs ?? DELAY_BETWEEN_CALLS_MS;
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
    console.log(
      paint(
        `Swap path: ${describeMintLabel(inputMint)} → ${describeMintLabel(outputMint)}`,
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
    const markEndpoint = (reason) => {
      const delegate = connection.__markUnhealthy;
      if (typeof delegate === "function") {
        delegate(reason);
      }
    };
    const rotateConnection = (reason, label) => {
      const previous = getCurrentRpcEndpoint();
      markEndpoint(reason);
      connection = createRpcConnection("confirmed", forcedRpcEndpoint);
      const next = getCurrentRpcEndpoint();
      const message = `RPC ${previous} rate-limited during ${label}; switched to ${next}`;
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
    const logInfo = (message) => {
      ensureHeader();
      console.log(paint(message, "info"));
    };
    const logMuted = (message, value) => {
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
                if (status !== null && (status === 404 || status === 401 || status === 403) && engine === "ultra") {
                  if (!ultraUnavailableLogged) {
                    if (status === 404) {
                      logWarn("  Ultra order endpoint returned 404; falling back to legacy Lite API.");
                    } else {
                      logWarn("  Ultra order endpoint returned 401/403; falling back to legacy Lite API.");
                    }
                    ultraUnavailableLogged = true;
                  }
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

            await delay(DELAY_BETWEEN_CALLS_MS);
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

            await delay(DELAY_BETWEEN_CALLS_MS);
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
        } catch (innerErr) {
          const errorInfo = classifySwapError(innerErr);

          if (engine === "ultra") {
            const status = innerErr?.status ?? null;
            const message = innerErr?.message || "";
            if (status === 404 || status === 401 || status === 403) {
              if (!ultraUnavailableLogged) {
                if (status === 404) {
                  logWarn("  Ultra execute endpoint returned 404; falling back to legacy Lite API.");
                } else {
                  logWarn("  Ultra execute returned 401/403; falling back to legacy Lite API (check JUPITER_ULTRA_API_KEY / JUPITER_ULTRA_API_BASE).");
                }
                ultraUnavailableLogged = true;
              }
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
    await delay(walletDelayMs);
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
    computePrice: 100000,
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
  const computePrice = opts.computePrice || 100000;
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
  const DEFAULT_COMPUTE_UNITS = 1_400_000;
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
    computePrice: 100000,
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
  const computePrice = opts.computePrice || 100000;
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
  const DEFAULT_COMPUTE_UNITS = 1_400_000;
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
      if (!targetName) throw new Error("aggregate usage: aggregate <targetWalletFile>");
      await aggregateSol(targetName);
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
    } else if (cmd === "crew1-cycle") {
      await runCrew1Cycle();
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
  listWallets,
  ensureAtaForMint,
  ensureWrappedSolBalance,
  resolveTokenProgramForMint,
};
