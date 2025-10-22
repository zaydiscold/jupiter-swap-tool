import { PublicKey } from "@solana/web3.js";
import { withPerpsRpcRetry } from "./client.js";
import { structuredLog, toPublicKey, toSerializable } from "./utils.js";

function buildAccountFilters({ owner, pool, custody, collateralCustody } = {}) {
  const filters = [];
  if (owner) {
    const key = toPublicKey(owner, "owner");
    filters.push({ memcmp: { offset: 8, bytes: key.toBase58() } });
  }
  if (pool) {
    const key = toPublicKey(pool, "pool");
    filters.push({ memcmp: { offset: 40, bytes: key.toBase58() } });
  }
  if (custody) {
    const key = toPublicKey(custody, "custody");
    filters.push({ memcmp: { offset: 72, bytes: key.toBase58() } });
  }
  if (collateralCustody) {
    const key = toPublicKey(collateralCustody, "collateralCustody");
    filters.push({ memcmp: { offset: 104, bytes: key.toBase58() } });
  }
  return filters;
}

function buildCustodyFilters({ pool, mint } = {}) {
  const filters = [];
  if (pool) {
    filters.push({ memcmp: { offset: 8, bytes: toPublicKey(pool, "pool").toBase58() } });
  }
  if (mint) {
    filters.push({ memcmp: { offset: 40, bytes: toPublicKey(mint, "mint").toBase58() } });
  }
  return filters;
}

function logAccountScan({ label, accountType, endpoint, attempt, durationMs, count, size }) {
  structuredLog("info", "perps-account-scan", {
    label,
    accountType,
    endpoint,
    attempt,
    durationMs,
    count,
    estimatedBytes: size && count ? size * count : undefined,
  });
}

function mapAnchorAccounts(entries) {
  return entries.map((entry) => ({
    publicKey: entry.publicKey,
    account: entry.account,
  }));
}

function ensureFilters(filters) {
  if (!filters) return [];
  if (Array.isArray(filters)) return filters;
  return [filters];
}

function accountNamespace(program, accountType) {
  const namespace = program?.account?.[accountType];
  if (!namespace || typeof namespace.all !== "function") {
    throw new Error(`Perps program is missing account namespace '${accountType}'`);
  }
  return namespace;
}

async function fetchAccountEntries(accountType, { filters = [], commitment, label, options = {} }) {
  const resolvedLabel = label || `perps.fetch${accountType.charAt(0).toUpperCase()}${accountType.slice(1)}s`;
  const resolvedFilters = ensureFilters(filters);
  return withPerpsRpcRetry(
    resolvedLabel,
    async ({ program, endpoint, attempt }) => {
      const startedAt = Date.now();
      const namespace = accountNamespace(program, accountType);
      const rpcOptions = commitment ? { commitment } : undefined;
      const entries = await namespace.all(resolvedFilters, rpcOptions);
      const durationMs = Date.now() - startedAt;
      const accountSize = typeof namespace.size === "number" ? namespace.size : undefined;
      logAccountScan({
        label: resolvedLabel,
        accountType,
        endpoint,
        attempt,
        durationMs,
        count: entries.length,
        size: accountSize,
      });
      return mapAnchorAccounts(entries);
    },
    options
  );
}

export async function fetchPools(options = {}) {
  return fetchAccountEntries("pool", {
    commitment: options.commitment,
    label: options.label || "perps.fetchPools",
    options,
  });
}

export async function fetchCustodies(options = {}) {
  return fetchAccountEntries("custody", {
    filters: buildCustodyFilters(options.filters),
    commitment: options.commitment,
    label: options.label || "perps.fetchCustodies",
    options,
  });
}

export async function fetchPositions(options = {}) {
  return fetchAccountEntries("position", {
    filters: buildAccountFilters(options.filters),
    commitment: options.commitment,
    label: options.label || "perps.fetchPositions",
    options,
  });
}

export async function fetchMarkets(options = {}) {
  const label = options.label || "perps.fetchMarkets";
  const poolsPromise = options.pools
    ? Promise.resolve(options.pools)
    : fetchPools({ ...options, label: `${label}:pools` });
  const custodiesPromise = options.custodies
    ? Promise.resolve(options.custodies)
    : fetchCustodies({ ...options, label: `${label}:custodies` });
  const [pools, custodies] = await Promise.all([poolsPromise, custodiesPromise]);
  const custodyByKey = new Map(custodies.map((entry) => [entry.publicKey.toBase58(), entry]));
  const markets = [];
  for (const pool of pools) {
    const { account } = pool;
    if (!account?.custodies) continue;
    for (const custodyKey of account.custodies) {
      const custodyPubkey = custodyKey instanceof PublicKey ? custodyKey : new PublicKey(custodyKey);
      const custodyEntry = custodyByKey.get(custodyPubkey.toBase58());
      if (!custodyEntry) continue;
      markets.push({
        pool: pool.publicKey,
        poolName: account.name,
        custody: custodyEntry.publicKey,
        mint: custodyEntry.account.mint,
        decimals: custodyEntry.account.decimals,
        isStable: custodyEntry.account.isStable,
      });
    }
  }
  structuredLog("info", "perps-markets", {
    label,
    pools: pools.length,
    custodies: custodies.length,
    markets: markets.length,
  });
  return markets;
}

export function serializeAccountEntry(entry) {
  return {
    publicKey: entry.publicKey.toBase58(),
    account: toSerializable(entry.account),
  };
}
