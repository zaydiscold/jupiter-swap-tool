import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const WSOL_MINT = "So11111111111111111111111111111111111111112";
export const RANDOM_MINT_PLACEHOLDER = "RANDOM";
export const MAX_INFLIGHT = 4;
export const JITTER_FRACTION = 0.6;
export const CHECKPOINT_SOL_EVERY_MIN = 5;
export const CHECKPOINT_SOL_EVERY_MAX = 10;
export const WALLET_MIN_REST_SOL = 0.005;
export const WALLET_RETIRE_UNDER_SOL = 0.01;
export const GAS_BASE_RESERVE_SOL = 0.0015;
export const ATA_RENT_EST_SOL = 0.002;

const LAMPORTS_PER_SOL = 1_000_000_000n;
const SOL_LIKE_MINTS = new Set([WSOL_MINT, "11111111111111111111111111111111"]);
const SOL_TO_LAMPORTS = (value) => {
  const numeric = Number(value || 0);
  const scaled = Math.ceil(numeric * 1_000_000_000);
  return BigInt(scaled < 0 ? 0 : scaled);
};

const GAS_BASE_RESERVE_LAMPORTS = SOL_TO_LAMPORTS(GAS_BASE_RESERVE_SOL);
const WALLET_MIN_REST_LAMPORTS = SOL_TO_LAMPORTS(WALLET_MIN_REST_SOL);
const ATA_RENT_EST_LAMPORTS = SOL_TO_LAMPORTS(ATA_RENT_EST_SOL);
const FEE_LAMPORTS = SOL_TO_LAMPORTS(0.00001);
const JUP_BUFFER_LAMPORTS = SOL_TO_LAMPORTS(0.0005);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const TOKEN_CATALOG_PATH = path.resolve(__dirname, "../../token_catalog.json");

function mulberry32(seed) {
  return function random() {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function walletSeededRng(pubkeyBase58) {
  let hash = 2166136261;
  for (let i = 0; i < pubkeyBase58.length; i += 1) {
    const code = pubkeyBase58.charCodeAt(i);
    hash ^= code;
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return mulberry32(hash >>> 0);
}

function pickInt(rng, min, max) {
  return min + Math.floor(rng() * (max - min + 1));
}

function shuffle(rng, input) {
  const copy = input.slice();
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readTokenCatalog() {
  try {
    const raw = fs.readFileSync(TOKEN_CATALOG_PATH, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed;
    }
  } catch (_) {}
  return [];
}

function tokensByTags(tokens, tags) {
  return tokens.filter(
    (token) => Array.isArray(token.tags) && token.tags.some((tag) => tags.includes(tag))
  );
}

export function pickPortionLamports(rng, spendableLamports, options = {}) {
  const minRestLamports =
    typeof options.minRestLamports === "bigint" ? options.minRestLamports : WALLET_MIN_REST_LAMPORTS;
  const dustFloorLamports =
    typeof options.dustFloorLamports === "bigint" ? options.dustFloorLamports : 10_000n;
  if (spendableLamports <= minRestLamports) {
    return 0n;
  }
  const denominator = BigInt(pickInt(rng, 3, 9));
  let amount = spendableLamports / denominator;
  if (amount < dustFloorLamports) {
    amount = dustFloorLamports;
  }
  if (spendableLamports - amount < minRestLamports) {
    amount = spendableLamports - minRestLamports;
  }
  return amount > 0n ? amount : 0n;
}

function pickSpendFraction(rng) {
  const baseDenominator = pickInt(rng, 4, 9);
  const base = 1 / baseDenominator;
  const jitter = 0.8 + rng() * 0.4;
  const raw = base * jitter;
  return Math.min(0.65, Math.max(0.08, raw));
}

function clampSpendFraction(value) {
  if (!Number.isFinite(value)) {
    return 0.25;
  }
  return Math.min(0.65, Math.max(0.05, value));
}

function estimateStepCostLamports(step) {
  let total = FEE_LAMPORTS + JUP_BUFFER_LAMPORTS;
  if (step?.logicalStep?.requiresAta) {
    total += ATA_RENT_EST_LAMPORTS;
  }
  return total;
}

export function truncatePlanToBudget(planSteps, solBalanceLamports) {
  const balanceLamports =
    typeof solBalanceLamports === "bigint"
      ? solBalanceLamports
      : BigInt(solBalanceLamports ?? 0);
  if (balanceLamports <= WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS) {
    return [];
  }
  let remaining = balanceLamports - (WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS);
  const accepted = [];
  for (const step of planSteps) {
    const cost = estimateStepCostLamports(step);
    if (remaining < cost) {
      break;
    }
    accepted.push(step);
    remaining -= cost;
  }
  return accepted;
}

function planLongChainSteps(rng, poolMints) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }
  const hopCount = pickInt(rng, 10, 25);
  const steps = [];
  let currentMint = WSOL_MINT;
  for (let hop = 0; hop < hopCount; hop += 1) {
    const isFinalHop = hop === hopCount - 1;
    let nextMint;
    if (isFinalHop) {
      nextMint = WSOL_MINT;
    } else {
      const shuffled = shuffle(rng, poolMints);
      const pick = shuffled.find((entry) => entry?.mint && entry.mint !== currentMint);
      nextMint = pick?.mint;
    }
    if (!nextMint) {
      return [];
    }
    const step = {
      inMint: currentMint,
      outMint: nextMint,
      requiresAta: nextMint !== WSOL_MINT,
      sourceBalance: currentMint === WSOL_MINT ? { kind: "sol" } : { kind: "spl", mint: currentMint },
    };
    steps.push(step);
    currentMint = nextMint;
  }
  return steps;
}

function planBuckshotScatterTargets(rng, poolMints, count) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }
  const shuffled = shuffle(rng, poolMints);
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

function normalizeHoldings(rawHoldings) {
  if (!Array.isArray(rawHoldings)) return [];
  const results = [];
  for (const entry of rawHoldings) {
    const mint = entry?.mint;
    if (!mint || mint === WSOL_MINT) continue;
    let amountLamports = entry?.amountLamports ?? entry?.amount ?? entry?.uiAmount ?? 0;
    if (typeof amountLamports === "string") {
      try {
        amountLamports = BigInt(amountLamports);
      } catch (_) {
        amountLamports = 0n;
      }
    }
    if (typeof amountLamports === "number") {
      amountLamports = BigInt(Math.max(0, Math.floor(amountLamports)));
    }
    if (typeof amountLamports !== "bigint") {
      continue;
    }
    if (amountLamports <= 0n) continue;
    const locked = entry?.locked === true || entry?.isFrozen === true;
    if (locked) continue;
    const decimals = typeof entry?.decimals === "number" ? entry.decimals : 0;
    results.push({ mint, amountLamports, decimals });
  }
  return results;
}

function pickCycleFanTargets(rng, poolMints) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }
  const shuffled = shuffle(rng, poolMints);
  const minTargets = Math.min(2, shuffled.length);
  const maxTargets = Math.min(3, shuffled.length);
  const count = Math.max(minTargets, pickInt(rng, minTargets, maxTargets || minTargets));
  const picks = shuffled.slice(0, count);
  const weighted = rng() < 0.5;
  return picks.map((entry) => ({
    mint: entry.mint,
    weight: weighted ? pickInt(rng, 1, 100) : 1,
  }));
}

const SWEEP_MIN_DELAY_MS = 5_000;
const SWEEP_MAX_DELAY_MS = 10_000;

export function buildTimedPlanForWallet({
  pubkey,
  rng,
  targetSwaps,
  durationMs,
  kind,
  poolMints,
  holdings = [],
  solBalanceLamports = 0n,
}) {
  if (!pubkey || !rng || !Number.isFinite(targetSwaps) || targetSwaps <= 0) {
    return { schedule: [] };
  }
  const safeTarget = Math.max(1, Math.floor(targetSwaps));
  let basePath = [];
  if (kind === "meme-carousel" || kind === "btc-eth-circuit") {
    logicalSteps = planLongChainSteps(rng, poolMints);
  } else if (kind === "scatter-then-converge") {
    const bucketCount = Math.min(6, Math.max(3, Math.floor(safeTarget / 8)));
    const picks = planBuckshotScatterTargets(rng, poolMints, bucketCount);
    if (picks.length === 0) {
      basePath = [];
    } else {
      logicalSteps = Array.from({ length: safeTarget }, (_, idx) => ({
        inMint: WSOL_MINT,
        outMint: picks[idx % picks.length].mint,
        requiresAta: picks[idx % picks.length].mint !== WSOL_MINT,
        sourceBalance: { kind: "sol" },
      }));
    }
  } else if (kind === "icarus" || kind === "zenith" || kind === "aurora") {
    const pairs = Math.max(1, Math.ceil(safeTarget / 2));
    const steps = [];
    for (let pairIndex = 0; pairIndex < pairs; pairIndex += 1) {
      const sessionKey = `${kind}-${pubkey}-${pairIndex}`;
      steps.push({
        inMint: WSOL_MINT,
        outMint: RANDOM_MINT_PLACEHOLDER,
        requiresAta: true,
        sourceBalance: { kind: "sol" },
        randomization: {
          mode: "sol-to-random",
          sessionKey,
          poolMints,
          excludeMints: [WSOL_MINT],
        },
      });
      steps.push({
        inMint: RANDOM_MINT_PLACEHOLDER,
        outMint: WSOL_MINT,
        requiresAta: false,
        sourceBalance: {},
        randomization: {
          mode: "session-to-sol",
          sessionKey,
        },
      });
    }
    logicalSteps = steps;
  } else {
    logicalSteps = Array.from({ length: safeTarget }, () => {
      const choice = poolMints[Math.floor(rng() * poolMints.length)]?.mint;
      return {
        inMint: WSOL_MINT,
        outMint: choice,
        requiresAta: choice !== WSOL_MINT,
        sourceBalance: { kind: "sol" },
      };
    });
  }

  if (!basePath.length) {
    return { schedule: [] };
  }

  const fanSteps = logicalSteps.filter((step) => step.kind === "fanOutSwap" || !step.kind).length;
  const swapCountForInterval = kind === "btc-eth-circuit" && fanSteps > 0 ? fanSteps : safeTarget;
  const baseInterval = Math.max(10_000, Math.floor(durationMs / Math.max(1, swapCountForInterval)));
  const checkpointEvery = pickInt(rng, CHECKPOINT_SOL_EVERY_MIN, CHECKPOINT_SOL_EVERY_MAX);
  let dueAt = Date.now();
  let sinceCheckpoint = 0;
  let pathIdx = 0;
  let currentFromMint = WSOL_MINT;
  const schedule = [];

  for (let idx = 0; idx < safeTarget; idx += 1) {
    const template = basePath[pathIdx % basePath.length];
    const toMint = template?.toMint;
    if (!toMint) {
      break;
    }
    const spendFraction = clampSpendFraction(template?.spendFraction ?? pickSpendFraction(rng));
    const fromMintForStep = template?.forceFromSol ? WSOL_MINT : currentFromMint;
    const logical = {
      fromMint: fromMintForStep,
      toMint,
      spendFraction,
      requiresAta: toMint !== WSOL_MINT,
    };
    const jitterSign = rng() < 0.5 ? -1 : 1;
    const jitterAmount = 1 + jitterSign * (JITTER_FRACTION * rng());
    const delta =
      kind === "btc-eth-circuit" && logical?.kind === "sweepToSOL"
        ? pickInt(rng, SWEEP_MIN_DELAY_MS, SWEEP_MAX_DELAY_MS)
        : Math.max(3_000, Math.floor(baseInterval * jitterAmount));
    dueAt += delta;
    if (logical?.kind === "sweepToSOL") {
      schedule.push({
        kind: "sweepToSOL",
        dueAt,
        logicalStep: logical.logicalStep,
        idx,
      });
      continue;
    }

    const normalizedLogical = logical?.logicalStep ? logical.logicalStep : logical;
    schedule.push({
      kind: logical?.kind === "fanOutSwap" ? "fanOutSwap" : "swapHop",
      dueAt,
      logicalStep: normalizedLogical,
      idx,
    });
    sinceCheckpoint += 1;
    currentFromMint = toMint;
    pathIdx = (pathIdx + 1) % basePath.length;
    if (sinceCheckpoint >= checkpointEvery) {
      sinceCheckpoint = 0;
      const checkpointDelay = Math.max(750, Math.floor(delta * 0.25));
      schedule.push({
        kind: "checkpointToSOL",
        dueAt: dueAt + checkpointDelay,
        logicalStep: { inMint: WSOL_MINT, outMint: WSOL_MINT, requiresAta: false, sourceBalance: { kind: "sol" } },
        idx: idx + 0.1,
      });
      currentFromMint = WSOL_MINT;
    }
  }

  return { schedule, checkpointEvery };
}

export const CAMPAIGNS = {
  "meme-carousel": {
    kind: "meme-carousel",
    tokenTags: ["long-circle", "fanout", "default-sweep"],
    durations: {
      "30m": [20, 60],
      "1h": [60, 120],
      "2h": [140, 260],
      "6h": [300, 600],
    },
  },
  "scatter-then-converge": {
    kind: "scatter-then-converge",
    tokenTags: ["fanout", "default-sweep"],
    durations: {
      "30m": [20, 60],
      "1h": [60, 120],
      "2h": [140, 260],
      "6h": [300, 600],
    },
  },
  "btc-eth-circuit": {
    kind: "btc-eth-circuit",
    tokenTags: ["btc-eth", "long-circle"],
    durations: {
      "30m": [15, 45],
      "1h": [40, 100],
      "2h": [120, 220],
      "6h": [260, 520],
    },
  },
  icarus: {
    kind: "icarus",
    tokenTags: ["fanout", "default-sweep", "long-circle"],
    durations: {
      "30m": [24, 64],
      "1h": [60, 140],
      "2h": [140, 320],
      "6h": [360, 720],
    },
  },
  zenith: {
    kind: "zenith",
    tokenTags: ["default-sweep", "long-circle", "secondary-pool"],
    durations: {
      "30m": [18, 42],
      "1h": [48, 108],
      "2h": [110, 240],
      "6h": [280, 560],
    },
  },
  aurora: {
    kind: "aurora",
    tokenTags: ["fanout", "secondary-pool"],
    durations: {
      "30m": [12, 32],
      "1h": [36, 80],
      "2h": [90, 180],
      "6h": [220, 420],
    },
  },
};

export function instantiateCampaignForWallets({
  campaignKey,
  durationKey,
  walletPubkeys,
  walletHoldings = new Map(),
  walletSolBalances = new Map(),
}) {
  const preset = CAMPAIGNS[campaignKey];
  if (!preset) {
    throw new Error(`Unknown campaign ${campaignKey}`);
  }
  const range = preset.durations[durationKey];
  if (!range) {
    throw new Error(`Unknown duration ${durationKey}`);
  }

  const [minSwaps, maxSwaps] = range;
  const durationMs =
    durationKey === "30m"
      ? 30 * 60 * 1000
      : durationKey === "1h"
      ? 60 * 60 * 1000
      : durationKey === "2h"
      ? 2 * 60 * 60 * 1000
      : durationKey === "6h"
      ? 6 * 60 * 60 * 1000
      : 60 * 60 * 1000;

  const catalog = readTokenCatalog();
  const pool = tokensByTags(catalog, preset.tokenTags).filter((token) => token.symbol !== "PFP");
  const poolMints = pool
    .map((token) => ({
      mint: token.mint,
      symbol: token.symbol,
      decimals: token.decimals ?? 6,
    }))
    .filter((entry) => entry.mint);

  const plansByWallet = new Map();
  for (const pubkey of walletPubkeys) {
    const rng = walletSeededRng(pubkey);
    const targetSwaps = pickInt(rng, minSwaps, maxSwaps);
    const holdings = walletHoldings instanceof Map ? walletHoldings.get(pubkey) : null;
    const solBalanceLamports = walletSolBalances instanceof Map ? walletSolBalances.get(pubkey) : null;
    const plan = buildTimedPlanForWallet({
      pubkey,
      rng,
      targetSwaps,
      durationMs,
      kind: preset.kind,
      poolMints,
      holdings,
      solBalanceLamports,
    });
    plansByWallet.set(pubkey, {
      schedule: plan.schedule,
      rng,
      randomSessions: new Map(),
      poolMints,
    });
  }

  return {
    plansByWallet,
    meta: { campaignKey, durationKey, durationMs },
  };
}

let HOOKS = {
  getSolLamports: null,
  getSplLamports: null,
  jupiterLiteSwap: null,
  findLargestSplHolding: null,
  splToLamports: null,
  getSplBalanceLamports: null,
};

export function registerHooks(nextHooks) {
  HOOKS = { ...HOOKS, ...nextHooks };
}

export async function doSwapStep(pubkeyBase58, logicalStep, rng, planContext = {}) {
  if (!HOOKS.getSolLamports || !HOOKS.jupiterLiteSwap) {
    throw new Error("campaign hooks not registered");
  }
  const resolved = resolveRandomizedStep(logicalStep, rng, {
    sessionState: planContext?.randomSessions,
    poolMints: planContext?.poolMints,
  });
  const outMint = resolved?.outMint ?? logicalStep?.outMint;
  const inMint = resolved?.inMint ?? logicalStep?.inMint ?? WSOL_MINT;
  const sourceMeta = resolved?.sourceBalance ?? logicalStep?.sourceBalance;
  const usesSol = sourceMeta?.kind === "sol" || inMint === WSOL_MINT;
  if (!outMint) {
    throw new Error("missing out mint");
  }
  const balanceLamports = await HOOKS.getSolLamports(pubkeyBase58);
  const baseReserve = WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS;
  if (balanceLamports < baseReserve) {
    throw new Error("insufficient SOL balance for fees");
  }
  let amountLamports = 0n;
  if (usesSol) {
    const minRest =
      typeof sourceMeta?.minRestLamports === "bigint" ? sourceMeta.minRestLamports : WALLET_MIN_REST_LAMPORTS;
    const spendable = balanceLamports > baseReserve ? balanceLamports - baseReserve : 0n;
    if (spendable <= 0n) {
      throw new Error("insufficient spendable SOL");
    }
    amountLamports = pickPortionLamports(rng, spendable, {
      minRestLamports: minRest,
      dustFloorLamports: sourceMeta?.dustFloorLamports,
    });
  } else {
    if (!HOOKS.getSplBalanceLamports) {
      throw new Error("campaign hooks missing SPL balance reader");
    }
    const balanceMint = sourceMeta?.mint ?? inMint;
    const splBalanceLamports = await HOOKS.getSplBalanceLamports(pubkeyBase58, balanceMint);
    if (splBalanceLamports <= 0n) {
      throw new Error("insufficient SPL balance");
    }
    const minRest =
      typeof sourceMeta?.minRestLamports === "bigint" ? sourceMeta.minRestLamports : 0n;
    amountLamports = pickPortionLamports(rng, splBalanceLamports, {
      minRestLamports: minRest,
      dustFloorLamports: sourceMeta?.dustFloorLamports,
    });
  }
  if (amountLamports <= 0n) {
    throw new Error("amount below dust floor");
  }
  return HOOKS.jupiterLiteSwap(pubkeyBase58, inMint, outMint, amountLamports);
}

export async function doCheckpointToSOL(pubkeyBase58, rng) {
  if (!HOOKS.findLargestSplHolding || !HOOKS.splToLamports || !HOOKS.jupiterLiteSwap) {
    return null;
  }
  const holding = await HOOKS.findLargestSplHolding(pubkeyBase58);
  if (!holding || !holding.mint) {
    return null;
  }
  const baseLamportsRaw = await HOOKS.splToLamports(pubkeyBase58, holding.mint, holding.uiAmount);
  const baseLamports = BigInt(baseLamportsRaw ?? 0);
  if (baseLamports <= 0n) {
    return null;
  }
  const percent = BigInt(50 + Math.floor(rng() * 41));
  const lamportsIn = (baseLamports * percent) / 100n;
  if (lamportsIn <= 0n) {
    return null;
  }
  return HOOKS.jupiterLiteSwap(pubkeyBase58, holding.mint, WSOL_MINT, lamportsIn);
}

async function doSweepToSOLStep(pubkeyBase58, logicalStep) {
  if (!HOOKS.listSweepableHoldings || !HOOKS.jupiterLiteSwap) {
    return null;
  }
  const mint = logicalStep?.mint;
  if (!mint || mint === WSOL_MINT) {
    return null;
  }
  const holdings = await HOOKS.listSweepableHoldings(pubkeyBase58);
  if (!Array.isArray(holdings) || holdings.length === 0) {
    return null;
  }
  const target = holdings.find((entry) => entry?.mint === mint);
  if (!target) {
    return null;
  }
  let amountLamports = target?.amountLamports ?? target?.amount ?? 0n;
  if (typeof amountLamports === "string") {
    try {
      amountLamports = BigInt(amountLamports);
    } catch (_) {
      amountLamports = 0n;
    }
  }
  if (typeof amountLamports === "number") {
    amountLamports = BigInt(Math.max(0, Math.floor(amountLamports)));
  }
  if (typeof amountLamports !== "bigint" || amountLamports <= 0n) {
    return null;
  }
  const dustFloor = logicalStep?.dustFloorLamports;
  if (typeof dustFloor === "number" && dustFloor > 0 && amountLamports < BigInt(Math.floor(dustFloor))) {
    return null;
  }
  if (typeof dustFloor === "bigint" && amountLamports < dustFloor) {
    return null;
  }
  return HOOKS.jupiterLiteSwap(pubkeyBase58, mint, WSOL_MINT, amountLamports);
}

function ensurePlanState(planStates, pubkey) {
  if (!planStates.has(pubkey)) {
    planStates.set(pubkey, {
      fanOutCycles: new Map(),
    });
  }
  return planStates.get(pubkey);
}

async function doFanoutSwapStep(pubkeyBase58, logicalStep, rng, planStates) {
  if (!HOOKS.getSolLamports || !HOOKS.jupiterLiteSwap) {
    throw new Error("campaign hooks not registered");
  }
  const cycleId = logicalStep?.cycleId ?? 0;
  const state = ensurePlanState(planStates, pubkeyBase58);
  const cycleState = state.fanOutCycles.get(cycleId) || {
    totalSpendable: null,
    remaining: null,
    allocations: new Map(),
  };
  const balanceLamports = await HOOKS.getSolLamports(pubkeyBase58);
  const baseReserve = WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS;
  if (balanceLamports <= baseReserve) {
    throw new Error("insufficient spendable SOL for fan-out");
  }
  const spendable = balanceLamports - baseReserve;
  if (cycleState.totalSpendable === null || logicalStep?.targetIndex === 0) {
    cycleState.totalSpendable = spendable;
    cycleState.remaining = spendable;
    cycleState.allocations.clear();
  }
  if (cycleState.remaining === null || cycleState.remaining <= 0n) {
    state.fanOutCycles.set(cycleId, cycleState);
    return null;
  }
  const totalTargets = logicalStep?.totalTargets ?? 1;
  const weight = BigInt(logicalStep?.weight ?? 0);
  const totalWeight = BigInt(logicalStep?.totalWeight ?? 0);
  let amountLamports = 0n;
  if (logicalStep?.targetIndex === totalTargets - 1) {
    amountLamports = cycleState.remaining;
  } else if (totalWeight > 0n) {
    amountLamports = (cycleState.totalSpendable * weight) / totalWeight;
  } else {
    amountLamports = pickPortionLamports(rng, cycleState.remaining);
  }
  if (amountLamports > cycleState.remaining) {
    amountLamports = cycleState.remaining;
  }
  if (amountLamports <= 0n) {
    state.fanOutCycles.set(cycleId, cycleState);
    return null;
  }
  cycleState.remaining -= amountLamports;
  cycleState.allocations.set(logicalStep?.targetIndex ?? 0, amountLamports);
  state.fanOutCycles.set(cycleId, cycleState);
  if (logicalStep?.targetIndex === totalTargets - 1) {
    state.fanOutCycles.delete(cycleId);
  }
  const outMint = logicalStep?.outMint;
  if (!outMint) {
    throw new Error("missing fan-out target mint");
  }
  return HOOKS.jupiterLiteSwap(pubkeyBase58, WSOL_MINT, outMint, amountLamports);
}

async function withBackoff(fn) {
  const delays = [400, 900, 1800, 3600];
  let lastError = null;
  for (const delayMs of delays) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      const message = String(err ?? "").toLowerCase();
      if (
        message.includes("429") ||
        message.includes("rate") ||
        message.includes("stale") ||
        message.includes("timeout")
      ) {
        await sleep(delayMs);
        continue;
      }
      throw err;
    }
  }
  throw lastError ?? new Error("exhausted backoff attempts");
}

export async function executeTimedPlansAcrossWallets({ plansByWallet }) {
  if (!(plansByWallet instanceof Map)) {
    throw new Error("plansByWallet must be a Map");
  }
  const inflight = new Set();
  const planStates = new Map();
  const queue = [];
  for (const [pubkey, { schedule, rng }] of plansByWallet.entries()) {
    if (Array.isArray(schedule) && schedule.length > 0) {
      queue.push({ pubkey, idx: 0, dueAt: schedule[0].dueAt, rng });
    }
  }
  while (queue.length > 0) {
    queue.sort((a, b) => a.dueAt - b.dueAt);
    const current = queue.shift();
    if (!current) {
      continue;
    }
    const plan = plansByWallet.get(current.pubkey);
    if (!plan) {
      continue;
    }
    const step = plan.schedule[current.idx];
    if (!step) {
      continue;
    }
    const wait = Math.max(0, step.dueAt - Date.now());
    if (wait > 0) {
      await sleep(wait);
    }
    while (inflight.size >= MAX_INFLIGHT) {
      await sleep(100);
    }
    inflight.add(current.pubkey);
    try {
      if (step.kind === "checkpointToSOL") {
        await withBackoff(() => doCheckpointToSOL(current.pubkey, current.rng));
      } else if (step.kind === "sweepToSOL") {
        await withBackoff(() => doSweepToSOLStep(current.pubkey, step.logicalStep));
      } else if (step.kind === "fanOutSwap") {
        await withBackoff(() => doFanoutSwapStep(current.pubkey, step.logicalStep, current.rng, planStates));
      } else {
        await withBackoff(() =>
          doSwapStep(current.pubkey, step.logicalStep, current.rng, plan)
        );
      }
    } catch (err) {
      console.warn(`[${current.pubkey}] step ${step.idx ?? "?"} failed: ${err?.message ?? err}`);
    } finally {
      inflight.delete(current.pubkey);
    }
    const nextIdx = current.idx + 1;
    if (plan.schedule[nextIdx]) {
      queue.push({ pubkey: current.pubkey, idx: nextIdx, dueAt: plan.schedule[nextIdx].dueAt, rng: current.rng });
    }
  }
}

export function estimateCampaignVolumeSOL({ plansByWallet }) {
  if (!(plansByWallet instanceof Map)) {
    return 0;
  }
  let lamports = 0n;
  for (const { schedule } of plansByWallet.values()) {
    if (!Array.isArray(schedule)) continue;
    for (const step of schedule) {
      if (!step || typeof step !== "object") continue;
      if (step.kind === "fanOutSwap" || step.kind === "swapHop") {
        let estimated = step?.logicalStep?.estimatedLamports;
        if (typeof estimated === "string") {
          try {
            estimated = BigInt(estimated);
          } catch (_) {
            estimated = null;
          }
        }
        if (typeof estimated === "number") {
          estimated = BigInt(Math.max(0, Math.floor(estimated)));
        }
        if (typeof estimated === "bigint" && estimated > 0n) {
          lamports += estimated;
        } else {
          lamports += 10_000_000n;
        }
      } else if (step.kind === "sweepToSOL") {
        lamports += 5_000_000n;
      }
    }
  }
  return Number(lamports) / 1_000_000_000;
}
