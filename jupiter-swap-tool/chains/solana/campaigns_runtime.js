import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const WSOL_MINT = "So11111111111111111111111111111111111111112";
export const MAX_INFLIGHT = 4;
export const JITTER_FRACTION = 0.6;
export const CHECKPOINT_SOL_EVERY_MIN = 5;
export const CHECKPOINT_SOL_EVERY_MAX = 10;
export const WALLET_MIN_REST_SOL = 0.005;
export const WALLET_RETIRE_UNDER_SOL = 0.01;
export const GAS_BASE_RESERVE_SOL = 0.0015;
export const ATA_RENT_EST_SOL = 0.002;

const LAMPORTS_PER_SOL = 1_000_000_000n;
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

function applyTagFilters(pool, includeSets, excludeTags) {
  const safePool = Array.isArray(pool) ? pool : [];
  const includeList = Array.isArray(includeSets) ? includeSets : [];
  const fallbackSets = includeList.length > 0 ? [...includeList, null] : [null];
  const excludes = Array.isArray(excludeTags) ? excludeTags : [];

  const applyExclude = (entries) => {
    if (!Array.isArray(entries) || entries.length === 0) {
      return [];
    }
    if (!excludes.length) {
      return entries.filter((entry) => entry && entry.mint);
    }
    const filtered = entries.filter((entry) => {
      if (!entry || !entry.mint) return false;
      const tags = Array.isArray(entry.tags) ? entry.tags : [];
      const excluded = tags.some((tag) => excludes.includes(tag));
      return !excluded;
    });
    return filtered.length > 0 ? filtered : entries.filter((entry) => entry && entry.mint);
  };

  for (const tagSet of fallbackSets) {
    let candidates = safePool;
    if (Array.isArray(tagSet) && tagSet.length > 0) {
      candidates = safePool.filter((entry) => {
        if (!entry || !entry.mint) return false;
        const tags = Array.isArray(entry.tags) ? entry.tags : [];
        return tags.some((tag) => tagSet.includes(tag));
      });
    }
    const afterExclude = applyExclude(candidates);
    if (afterExclude.length > 0) {
      return afterExclude;
    }
  }

  return applyExclude(safePool);
}

export function resolveScheduledLogicalStep(logicalStep, rng) {
  if (!logicalStep) {
    return null;
  }
  if (logicalStep.resolved && logicalStep.resolved.outMint) {
    return logicalStep.resolved;
  }
  if (logicalStep.resolver !== "RANDOM") {
    return logicalStep;
  }

  const pool = Array.isArray(logicalStep.pool) ? logicalStep.pool : [];
  const validPool = pool.filter((entry) => entry && entry.mint);
  if (validPool.length === 0) {
    throw new Error("random logical step has empty pool");
  }

  const includeSets = [];
  if (Array.isArray(logicalStep.includeTags) && logicalStep.includeTags.length > 0) {
    includeSets.push(logicalStep.includeTags);
  }
  if (
    Array.isArray(logicalStep.fallbackIncludeTags) &&
    logicalStep.fallbackIncludeTags.length > 0
  ) {
    includeSets.push(logicalStep.fallbackIncludeTags);
  }

  const excludeTags = Array.isArray(logicalStep.excludeTags)
    ? logicalStep.excludeTags
    : [];

  let candidates = applyTagFilters(validPool, includeSets, excludeTags);
  const disallow = new Set();
  if (logicalStep.inMint) {
    disallow.add(logicalStep.inMint);
  }
  if (Array.isArray(logicalStep.disallowMints)) {
    for (const mint of logicalStep.disallowMints) {
      if (mint) disallow.add(mint);
    }
  }
  if (disallow.size > 0) {
    const filtered = candidates.filter((entry) => !disallow.has(entry.mint));
    if (filtered.length > 0) {
      candidates = filtered;
    }
  }

  if (candidates.length === 0) {
    throw new Error("random logical step has no eligible candidates");
  }

  const randomFn = typeof rng === "function" ? rng : Math.random;
  const idx = Math.floor(randomFn() * candidates.length);
  const chosen = candidates[Math.min(idx, candidates.length - 1)];
  if (!chosen?.mint) {
    throw new Error("random logical step produced invalid candidate");
  }

  const baseSource =
    logicalStep.sourceBalance && typeof logicalStep.sourceBalance === "object"
      ? { ...logicalStep.sourceBalance }
      : { kind: "sol" };

  const resolved = {
    inMint: logicalStep.inMint ?? WSOL_MINT,
    outMint: chosen.mint,
    requiresAta: chosen.mint !== WSOL_MINT,
    sourceBalance: baseSource,
    metadata: {
      random: true,
      flow: logicalStep.flow ?? null,
      symbol: chosen.symbol ?? null,
      tags: Array.isArray(chosen.tags) ? chosen.tags.slice() : [],
    },
  };

  logicalStep.resolved = resolved;
  logicalStep.requiresAta = resolved.requiresAta;
  logicalStep.resolvedCandidate = chosen;
  return resolved;
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

function clonePoolEntries(poolMints) {
  if (!Array.isArray(poolMints)) return [];
  return poolMints
    .map((entry) =>
      entry && entry.mint
        ? {
            mint: entry.mint,
            symbol: entry.symbol,
            decimals: entry.decimals,
            tags: Array.isArray(entry.tags) ? entry.tags.slice() : [],
          }
        : null
    )
    .filter(Boolean);
}

export function buildTimedPlanForWallet({
  pubkey,
  rng,
  targetSwaps,
  durationMs,
  kind,
  poolMints,
  randomConfig,
}) {
  if (!pubkey || !rng || !Number.isFinite(targetSwaps) || targetSwaps <= 0) {
    return { schedule: [] };
  }
  const safeTarget = Math.max(1, Math.floor(targetSwaps));
  let logicalSteps = [];
  if (kind === "meme-carousel" || kind === "btc-eth-circuit") {
    logicalSteps = planLongChainSteps(rng, poolMints);
  } else if (kind === "scatter-then-converge") {
    const bucketCount = Math.min(6, Math.max(3, Math.floor(safeTarget / 8)));
    const picks = planBuckshotScatterTargets(rng, poolMints, bucketCount);
    if (picks.length === 0) {
      logicalSteps = [];
    } else {
      logicalSteps = Array.from({ length: safeTarget }, (_, idx) => ({
        inMint: WSOL_MINT,
        outMint: picks[idx % picks.length].mint,
        requiresAta: picks[idx % picks.length].mint !== WSOL_MINT,
        sourceBalance: { kind: "sol" },
      }));
    }
  } else if (kind === "icarus" || kind === "zenith" || kind === "aurora") {
    const pool = clonePoolEntries(poolMints).filter((entry) => entry.mint && entry.mint !== WSOL_MINT);
    if (pool.length === 0) {
      logicalSteps = [];
    } else {
      const includeTags = Array.isArray(randomConfig?.includeTags)
        ? randomConfig.includeTags.slice()
        : [];
      const fallbackIncludeTags = Array.isArray(randomConfig?.fallbackIncludeTags)
        ? randomConfig.fallbackIncludeTags.slice()
        : [];
      const excludeTags = Array.isArray(randomConfig?.excludeTags)
        ? randomConfig.excludeTags.slice()
        : [];
      const disallowMints = Array.isArray(randomConfig?.disallowMints)
        ? randomConfig.disallowMints.slice()
        : [];
      logicalSteps = Array.from({ length: safeTarget }, () => ({
        resolver: "RANDOM",
        flow: randomConfig?.flow ?? kind,
        includeTags,
        fallbackIncludeTags,
        excludeTags,
        disallowMints: [WSOL_MINT, ...disallowMints],
        pool,
        sourceBalance: { kind: "sol" },
        requiresAta: true,
      }));
    }
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

  if (!logicalSteps.length) {
    return { schedule: [] };
  }

  const baseInterval = Math.max(10_000, Math.floor(durationMs / safeTarget));
  const checkpointEvery = pickInt(rng, CHECKPOINT_SOL_EVERY_MIN, CHECKPOINT_SOL_EVERY_MAX);
  let dueAt = Date.now();
  let sinceCheckpoint = 0;
  const schedule = [];

  for (let idx = 0; idx < safeTarget; idx += 1) {
    const logical = logicalSteps[idx % logicalSteps.length];
    const jitterSign = rng() < 0.5 ? -1 : 1;
    const jitterAmount = 1 + jitterSign * (JITTER_FRACTION * rng());
    const delta = Math.max(3_000, Math.floor(baseInterval * jitterAmount));
    dueAt += delta;
    schedule.push({
      kind: "swapHop",
      dueAt,
      logicalStep: logical,
      idx,
    });
    sinceCheckpoint += 1;
    if (sinceCheckpoint >= checkpointEvery) {
      sinceCheckpoint = 0;
      const checkpointDelay = Math.max(750, Math.floor(delta * 0.25));
      schedule.push({
        kind: "checkpointToSOL",
        dueAt: dueAt + checkpointDelay,
        logicalStep: { inMint: WSOL_MINT, outMint: WSOL_MINT, requiresAta: false, sourceBalance: { kind: "sol" } },
        idx: idx + 0.1,
      });
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
    tokenTags: ["long-circle", "default-sweep"],
    random: {
      flow: "icarus",
      includeTags: ["long-circle"],
      fallbackIncludeTags: ["default-sweep"],
      excludeTags: ["terminal", "secondary-terminal"],
    },
    durations: {
      "30m": [20, 60],
      "1h": [60, 120],
      "2h": [140, 260],
      "6h": [300, 600],
    },
  },
  zenith: {
    kind: "zenith",
    tokenTags: ["secondary-terminal", "long-circle", "default-sweep"],
    random: {
      flow: "zenith",
      includeTags: ["secondary-terminal"],
      fallbackIncludeTags: ["long-circle", "default-sweep"],
    },
    durations: {
      "30m": [15, 45],
      "1h": [40, 100],
      "2h": [120, 220],
      "6h": [260, 520],
    },
  },
  aurora: {
    kind: "aurora",
    tokenTags: ["fanout", "long-circle", "default-sweep"],
    random: {
      flow: "aurora",
      includeTags: ["fanout"],
      fallbackIncludeTags: ["long-circle", "default-sweep"],
      excludeTags: ["terminal"],
    },
    durations: {
      "30m": [20, 60],
      "1h": [60, 120],
      "2h": [140, 260],
      "6h": [300, 600],
    },
  },
};

export function instantiateCampaignForWallets({
  campaignKey,
  durationKey,
  walletPubkeys,
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
      tags: Array.isArray(token.tags) ? token.tags.slice() : [],
    }))
    .filter((entry) => entry.mint);

  const plansByWallet = new Map();
  for (const pubkey of walletPubkeys) {
    const rng = walletSeededRng(pubkey);
    const targetSwaps = pickInt(rng, minSwaps, maxSwaps);
    const plan = buildTimedPlanForWallet({
      pubkey,
      rng,
      targetSwaps,
      durationMs,
      kind: preset.kind,
      poolMints,
      randomConfig: preset.random,
    });
    plansByWallet.set(pubkey, { schedule: plan.schedule, rng });
  }

  return {
    plansByWallet,
    meta: { campaignKey, durationKey, durationMs },
  };
}

let HOOKS = {
  getSolLamports: null,
  jupiterLiteSwap: null,
  findLargestSplHolding: null,
  splToLamports: null,
  getSplBalanceLamports: null,
};

export function registerHooks(nextHooks) {
  HOOKS = { ...HOOKS, ...nextHooks };
}

export async function doSwapStep(pubkeyBase58, logicalStep, rng) {
  if (!HOOKS.getSolLamports || !HOOKS.jupiterLiteSwap) {
    throw new Error("campaign hooks not registered");
  }
  const resolvedStep = resolveScheduledLogicalStep(logicalStep, rng);
  const outMint = resolvedStep?.outMint;
  const inMint = resolvedStep?.inMint ?? WSOL_MINT;
  const sourceMeta = resolvedStep?.sourceBalance ?? logicalStep?.sourceBalance;
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
  return HOOKS.jupiterLiteSwap(pubkeyBase58, inMint, outMint, amountLamports, resolvedStep);
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
      } else {
        await withBackoff(() => doSwapStep(current.pubkey, step.logicalStep, current.rng));
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
    const swaps = schedule.filter((step) => step.kind === "swapHop").length;
    lamports += BigInt(swaps) * 10_000_000n;
  }
  return Number(lamports) / 1_000_000_000;
}
