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

export function pickPortionLamports(rng, spendableLamports) {
  if (spendableLamports <= WALLET_MIN_REST_LAMPORTS) {
    return 0n;
  }
  const denominator = BigInt(pickInt(rng, 3, 9));
  let amount = spendableLamports / denominator;
  if (amount < 10_000n) {
    amount = 10_000n;
  }
  if (spendableLamports - amount < WALLET_MIN_REST_LAMPORTS) {
    amount = spendableLamports - WALLET_MIN_REST_LAMPORTS;
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

function planLongChainMints(rng, poolMints, length) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }

  const targetLength = Math.max(10, Math.min(Math.max(length || 0, 10), 25));
  const hops = [];
  let fromMint = WSOL_MINT;
  const recentToMints = [];

  for (let i = 0; i < targetLength; i += 1) {
    const shuffled = shuffle(rng, poolMints);
    let pick = shuffled.find(
      (entry) =>
        entry?.mint &&
        entry.mint !== fromMint &&
        !recentToMints.includes(entry.mint)
    );
    if (!pick) {
      pick = shuffled.find((entry) => entry?.mint && entry.mint !== fromMint);
    }
    if (!pick) {
      pick = shuffled.find((entry) => entry?.mint);
    }
    if (!pick?.mint) {
      break;
    }

    hops.push({
      fromMint,
      toMint: pick.mint,
      spendFraction: pickSpendFraction(rng),
      forceFromSol: false,
    });

    recentToMints.push(pick.mint);
    while (recentToMints.length > 3) {
      recentToMints.shift();
    }
    fromMint = pick.mint;
  }

  if (hops.length > 0) {
    const tailWindow = hops.slice(-3).map((entry) => entry.toMint);
    const firstToMint = hops[0].toMint;
    if (tailWindow.includes(firstToMint)) {
      const shuffled = shuffle(rng, poolMints);
      const alternate = shuffled.find(
        (entry) =>
          entry?.mint &&
          entry.mint !== hops[hops.length - 1].toMint &&
          entry.mint !== firstToMint
      );
      if (alternate?.mint) {
        hops[hops.length - 1] = {
          fromMint: hops[hops.length - 1].fromMint,
          toMint: alternate.mint,
          spendFraction: hops[hops.length - 1].spendFraction,
          forceFromSol: false,
        };
      }
    }
  }

  return hops;
}

function planBuckshotScatterTargets(rng, poolMints, count) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }
  const shuffled = shuffle(rng, poolMints);
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

export function buildTimedPlanForWallet({
  pubkey,
  rng,
  targetSwaps,
  durationMs,
  kind,
  poolMints,
}) {
  if (!pubkey || !rng || !Number.isFinite(targetSwaps) || targetSwaps <= 0) {
    return { schedule: [] };
  }
  const safeTarget = Math.max(1, Math.floor(targetSwaps));
  let basePath = [];
  if (kind === "meme-carousel" || kind === "btc-eth-circuit") {
    const chainLength = Math.max(safeTarget, 12);
    basePath = planLongChainMints(rng, poolMints, chainLength);
  } else if (kind === "scatter-then-converge") {
    const bucketCount = Math.min(6, Math.max(3, Math.floor(safeTarget / 8)));
    const picks = planBuckshotScatterTargets(rng, poolMints, bucketCount);
    if (picks.length === 0) {
      basePath = [];
    } else {
      basePath = picks.map((pick) => ({
        fromMint: WSOL_MINT,
        toMint: pick.mint,
        spendFraction: pickSpendFraction(rng),
        forceFromSol: true,
      }));
    }
  } else {
    basePath = planLongChainMints(rng, poolMints, Math.max(safeTarget, 10));
  }

  if (!basePath.length) {
    return { schedule: [] };
  }

  const baseInterval = Math.max(10_000, Math.floor(durationMs / safeTarget));
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
    const delta = Math.max(3_000, Math.floor(baseInterval * jitterAmount));
    dueAt += delta;
    schedule.push({
      kind: "swapHop",
      dueAt,
      logicalStep: logical,
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
        logicalStep: {
          fromMint: currentFromMint,
          toMint: WSOL_MINT,
          spendFraction: 1,
          requiresAta: false,
        },
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
  getSplTokenLamports: null,
};

export function registerHooks(nextHooks) {
  HOOKS = { ...HOOKS, ...nextHooks };
}

export async function doSwapStep(pubkeyBase58, logicalStep, rng) {
  if (!HOOKS.getSolLamports || !HOOKS.jupiterLiteSwap) {
    throw new Error("campaign hooks not registered");
  }
  const fromMint = logicalStep?.fromMint || WSOL_MINT;
  const toMint = logicalStep?.toMint;
  const spendFraction = clampSpendFraction(
    typeof logicalStep?.spendFraction === "number" ? logicalStep.spendFraction : pickSpendFraction(rng)
  );
  if (!toMint) {
    throw new Error("missing destination mint");
  }

  let amountLamports = 0n;
  if (fromMint === WSOL_MINT) {
    const balanceLamports = await HOOKS.getSolLamports(pubkeyBase58);
    const baseReserve = WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS;
    const spendable = balanceLamports > baseReserve ? balanceLamports - baseReserve : 0n;
    if (spendable <= 0n) {
      throw new Error("insufficient spendable SOL");
    }
    const scaledFraction = BigInt(Math.floor(spendFraction * 10_000));
    amountLamports = (spendable * scaledFraction) / 10_000n;
    if (amountLamports < 10_000n) {
      amountLamports = pickPortionLamports(rng, spendable);
    }
    if (amountLamports > spendable) {
      amountLamports = spendable;
    }
  } else {
    if (!HOOKS.getSplTokenLamports) {
      throw new Error("spl balance hook not registered");
    }
    const balanceLamports = await HOOKS.getSplTokenLamports(pubkeyBase58, fromMint);
    if (balanceLamports <= 0n) {
      throw new Error("insufficient SPL balance for hop");
    }
    const scaledFraction = BigInt(Math.floor(spendFraction * 10_000));
    amountLamports = (balanceLamports * scaledFraction) / 10_000n;
    if (amountLamports <= 0n) {
      amountLamports = balanceLamports / 2n;
    }
    if (amountLamports > balanceLamports) {
      amountLamports = balanceLamports;
    }
  }

  if (amountLamports <= 0n) {
    throw new Error("amount below dust floor");
  }

  return HOOKS.jupiterLiteSwap(pubkeyBase58, fromMint, toMint, amountLamports);
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
