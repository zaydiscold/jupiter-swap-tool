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
const BPS_SCALE = 10_000n;

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
  const sequence = [];
  let last = null;
  let prev = null;
  for (let i = 0; i < length; i += 1) {
    const shuffled = shuffle(rng, poolMints);
    let pick = shuffled[0];
    if (last && pick?.mint === last.mint && shuffled[1]) {
      pick = shuffled[1];
    }
    if (prev && pick?.mint === prev.mint && shuffled[2]) {
      pick = shuffled[2];
    }
    sequence.push(pick);
    prev = last;
    last = pick;
  }
  return sequence;
}

function planBuckshotScatterTargets(rng, poolMints, count) {
  if (!Array.isArray(poolMints) || poolMints.length === 0) {
    return [];
  }
  const shuffled = shuffle(rng, poolMints);
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

function pickConvergenceMint(rng, picks, poolMints) {
  const lists = [Array.isArray(picks) ? picks : [], Array.isArray(poolMints) ? poolMints : []];
  const preferred = [];
  for (const entries of lists) {
    for (const entry of entries) {
      if (!entry?.mint || entry.mint === WSOL_MINT) continue;
      const symbol = (entry.symbol || "").toUpperCase();
      if (/USDC|USDT|USD\b|PYUSD|USDC\.E/.test(symbol)) {
        preferred.push(entry);
      }
    }
  }
  if (preferred.length === 0) {
    for (const entries of lists) {
      for (const entry of entries) {
        if (entry?.mint && entry.mint !== WSOL_MINT) {
          preferred.push(entry);
        }
      }
    }
  }
  if (preferred.length === 0) {
    return WSOL_MINT;
  }
  const pick = preferred[Math.floor(rng() * preferred.length)];
  return pick?.mint || WSOL_MINT;
}

function clampBps(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return 0;
  }
  if (value < 0) return 0;
  if (value > 10_000) return 10_000;
  return Math.floor(value);
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
  let swapEntries = [];
  const planMeta = {};

  if (kind === "meme-carousel" || kind === "btc-eth-circuit") {
    const chainLength = Math.max(safeTarget, 12);
    const chain = planLongChainMints(rng, poolMints, chainLength);
    swapEntries = Array.from({ length: safeTarget }, (_, idx) => {
      const entry = chain[idx % chain.length];
      return {
        phase: kind,
        inMint: WSOL_MINT,
        outMint: entry?.mint,
        requiresAta: entry?.mint !== WSOL_MINT,
      };
    });
  } else if (kind === "scatter-then-converge") {
    const bucketCount = Math.min(6, Math.max(3, Math.floor(safeTarget / 8)));
    const picks = planBuckshotScatterTargets(rng, poolMints, bucketCount);
    if (picks.length === 0) {
      swapEntries = [];
    } else {
      const convergeMint = pickConvergenceMint(rng, picks, poolMints);
      let scatterTargets = picks.filter((entry) => entry?.mint);
      if (convergeMint && scatterTargets.length > 1) {
        scatterTargets = scatterTargets.filter((entry) => entry.mint !== convergeMint);
      }
      if (scatterTargets.length === 0) {
        scatterTargets = picks.filter((entry) => entry?.mint);
      }

      const convergeTrigger =
        rng() < 0.5
          ? { mode: "manual" }
          : {
              mode: "auto",
              autoAfterRounds: Math.max(
                1,
                Math.min(4, Math.floor(safeTarget / Math.max(1, scatterTargets.length * 2)))
              ),
            };

      const scatterSummaries = [];
      const swapPlan = [];
      let swapCount = 0;

      const pushScatterEntry = (target, allocationBps, round) => {
        if (!target?.mint || swapCount >= safeTarget) {
          return false;
        }
        const normalizedBps = clampBps(allocationBps);
        if (normalizedBps <= 0) {
          return false;
        }
        swapPlan.push({
          phase: "scatter",
          inMint: WSOL_MINT,
          outMint: target.mint,
          allocationBps: normalizedBps,
          requiresAta: target.mint !== WSOL_MINT,
          round,
        });
        swapCount += 1;
        return true;
      };

      const pushConvergeEntry = (target, allocationBps, round) => {
        if (!target?.mint || target.mint === convergeMint || swapCount >= safeTarget) {
          return false;
        }
        const normalizedBps = clampBps(allocationBps);
        if (normalizedBps <= 0) {
          return false;
        }
        swapPlan.push({
          phase: "converge",
          inMint: target.mint,
          outMint: convergeMint,
          allocationBps: normalizedBps,
          requiresAta: convergeMint !== WSOL_MINT,
          round,
        });
        swapCount += 1;
        return true;
      };

      while (swapCount < safeTarget) {
        const cycleBudgetBps = pickInt(rng, 7000, 8000);
        let allocatedBps = 0;
        let roundsThisCycle = 0;
        let scatterDone = false;
        while (!scatterDone && swapCount < safeTarget) {
          roundsThisCycle += 1;
          const order = shuffle(rng, scatterTargets);
          let scatteredThisRound = false;
          for (const target of order) {
            if (swapCount >= safeTarget) {
              break;
            }
            const remainingBps = cycleBudgetBps - allocatedBps;
            if (remainingBps <= 0) {
              break;
            }
            let stepBps = pickInt(rng, 450, 2200);
            if (stepBps > remainingBps) {
              stepBps = remainingBps;
            }
            if (pushScatterEntry(target, stepBps, roundsThisCycle)) {
              allocatedBps += stepBps;
              scatteredThisRound = true;
            }
            if (swapCount >= safeTarget) {
              break;
            }
          }
          const autoStop =
            convergeTrigger.mode === "auto" &&
            roundsThisCycle >= (convergeTrigger.autoAfterRounds ?? 1) &&
            allocatedBps >= Math.floor(cycleBudgetBps * 0.65);
          if (allocatedBps >= cycleBudgetBps || autoStop || !scatteredThisRound) {
            scatterDone = true;
          }
        }
        scatterSummaries.push({
          rounds: roundsThisCycle,
          budgetBps: cycleBudgetBps,
          allocatedBps,
        });

        if (swapCount >= safeTarget) {
          break;
        }

        if (swapPlan.length > 0 && rng() < 0.55) {
          swapPlan[swapPlan.length - 1].checkpointAfter = true;
        }

        const remainingSwaps = safeTarget - swapCount;
        const perRound = Math.max(1, scatterTargets.length);
        const convergeRounds = Math.max(1, Math.min(2, Math.floor(remainingSwaps / perRound) || 1));
        for (let round = 1; round <= convergeRounds && swapCount < safeTarget; round += 1) {
          const order = shuffle(rng, scatterTargets);
          let convertedThisRound = false;
          for (const target of order) {
            if (swapCount >= safeTarget) {
              break;
            }
            if (pushConvergeEntry(target, pickInt(rng, 6000, 10000), round)) {
              convertedThisRound = true;
            }
          }
          if (convertedThisRound && rng() < 0.35) {
            swapPlan[swapPlan.length - 1].checkpointAfter = true;
          }
        }
      }

      while (swapCount < safeTarget) {
        const fallback =
          scatterTargets.length > 0 ? scatterTargets[swapCount % scatterTargets.length] : picks[0];
        if (!pushScatterEntry(fallback, pickInt(rng, 500, 1500), 0)) {
          break;
        }
      }

      swapEntries = swapPlan;
      planMeta.convergeTrigger = convergeTrigger;
      planMeta.convergeMint = convergeMint;
      planMeta.scatterTargets = scatterTargets.map((entry) => entry?.mint).filter(Boolean);
      planMeta.scatterSummaries = scatterSummaries;
    }
  } else {
    swapEntries = Array.from({ length: safeTarget }, () => {
      const pick = poolMints[Math.floor(rng() * poolMints.length)];
      return {
        phase: "default",
        inMint: WSOL_MINT,
        outMint: pick?.mint,
        requiresAta: (pick?.mint || "") !== WSOL_MINT,
      };
    });
  }

  if (!swapEntries.length) {
    return { schedule: [] };
  }

  const baseInterval = Math.max(10_000, Math.floor(durationMs / safeTarget));
  const checkpointEvery = pickInt(rng, CHECKPOINT_SOL_EVERY_MIN, CHECKPOINT_SOL_EVERY_MAX);
  let dueAt = Date.now();
  let sinceCheckpoint = 0;
  const schedule = [];

  let sequenceIdx = 0;
  for (let idx = 0; idx < swapEntries.length; idx += 1) {
    const entry = swapEntries[idx];
    const { checkpointAfter, ...logical } = entry;
    const jitterSign = rng() < 0.5 ? -1 : 1;
    const jitterAmount = 1 + jitterSign * (JITTER_FRACTION * rng());
    const delta = Math.max(3_000, Math.floor(baseInterval * jitterAmount));
    dueAt += delta;
    schedule.push({
      kind: "swapHop",
      dueAt,
      logicalStep: logical,
      idx: sequenceIdx,
    });
    sequenceIdx += 1;
    sinceCheckpoint += 1;

    let checkpointForced = checkpointAfter === true;
    if (checkpointForced) {
      const checkpointDelay = Math.max(750, Math.floor(delta * 0.25));
      schedule.push({
        kind: "checkpointToSOL",
        dueAt: dueAt + checkpointDelay,
        logicalStep: { outMint: WSOL_MINT, requiresAta: false },
        idx: sequenceIdx + 0.1,
      });
      sinceCheckpoint = 0;
    }

    if (!checkpointForced && sinceCheckpoint >= checkpointEvery) {
      const checkpointDelay = Math.max(750, Math.floor(delta * 0.25));
      schedule.push({
        kind: "checkpointToSOL",
        dueAt: dueAt + checkpointDelay,
        logicalStep: { outMint: WSOL_MINT, requiresAta: false },
        idx: sequenceIdx + 0.1,
      });
      sinceCheckpoint = 0;
    }
  }

  return { schedule, checkpointEvery, ...planMeta };
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
    plansByWallet.set(pubkey, {
      schedule: plan.schedule,
      rng,
      checkpointEvery: plan.checkpointEvery,
      convergeTrigger: plan.convergeTrigger,
      convergeMint: plan.convergeMint,
      scatterTargets: plan.scatterTargets,
      scatterSummaries: plan.scatterSummaries,
    });
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
  findSplHoldingForMint: null,
};

export function registerHooks(nextHooks) {
  HOOKS = { ...HOOKS, ...nextHooks };
}

export async function doSwapStep(pubkeyBase58, logicalStep, rng) {
  if (!HOOKS.getSolLamports || !HOOKS.jupiterLiteSwap) {
    throw new Error("campaign hooks not registered");
  }
  const outMint = logicalStep?.outMint;
  if (!outMint) {
    throw new Error("missing out mint");
  }
  const inMint = logicalStep?.inMint || WSOL_MINT;
  const allocationBps = clampBps(logicalStep?.allocationBps ?? 0);

  if (inMint === WSOL_MINT) {
    const balanceLamports = await HOOKS.getSolLamports(pubkeyBase58);
    const baseReserve = WALLET_MIN_REST_LAMPORTS + GAS_BASE_RESERVE_LAMPORTS;
    const spendable = balanceLamports > baseReserve ? balanceLamports - baseReserve : 0n;
    if (spendable <= 0n) {
      throw new Error("insufficient spendable SOL");
    }
    let amountLamports = 0n;
    if (allocationBps > 0) {
      amountLamports = (spendable * BigInt(allocationBps)) / BPS_SCALE;
    }
    if (amountLamports > 0n && allocationBps > 0) {
      const jitter = BigInt(9000 + Math.floor(rng() * 2000));
      amountLamports = (amountLamports * jitter) / BPS_SCALE;
    }
    if (amountLamports <= 0n) {
      amountLamports = pickPortionLamports(rng, spendable);
    }
    if (amountLamports <= 0n) {
      throw new Error("amount below dust floor");
    }
    if (amountLamports > spendable) {
      amountLamports = spendable;
    }
    if (amountLamports <= 0n) {
      throw new Error("unable to compute scatter amount");
    }
    return HOOKS.jupiterLiteSwap(pubkeyBase58, WSOL_MINT, outMint, amountLamports);
  }

  if (!HOOKS.findSplHoldingForMint || !HOOKS.splToLamports) {
    throw new Error("spl balance hooks not registered");
  }
  const holding = await HOOKS.findSplHoldingForMint(pubkeyBase58, inMint);
  if (!holding || !holding.uiAmount) {
    throw new Error("missing SPL holdings for converge");
  }
  const baseLamportsRaw = await HOOKS.splToLamports(pubkeyBase58, inMint, holding.uiAmount);
  const baseLamports = BigInt(baseLamportsRaw ?? 0);
  if (baseLamports <= 0n) {
    throw new Error("empty SPL holdings for converge");
  }
  let lamportsIn = baseLamports;
  if (allocationBps > 0) {
    lamportsIn = (baseLamports * BigInt(allocationBps)) / BPS_SCALE;
  }
  if (lamportsIn > 0n && allocationBps > 0) {
    const jitter = BigInt(9000 + Math.floor(rng() * 2000));
    lamportsIn = (lamportsIn * jitter) / BPS_SCALE;
  }
  if (lamportsIn > baseLamports) {
    lamportsIn = baseLamports;
  }
  if (lamportsIn <= 0n) {
    lamportsIn = baseLamports;
  }
  if (lamportsIn <= 0n) {
    throw new Error("unable to size converge swap");
  }
  return HOOKS.jupiterLiteSwap(pubkeyBase58, inMint, outMint, lamportsIn);
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
