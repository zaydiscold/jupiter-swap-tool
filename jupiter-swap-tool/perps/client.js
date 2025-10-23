import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Connection, PublicKey } from "@solana/web3.js";
import { AnchorProvider, Program } from "@coral-xyz/anchor";
import { structuredLog, toSerializable } from "./utils.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const IDL_PATH = path.join(__dirname, "idl", "jupiter_perps.json");

const DEFAULT_PROGRAM_ID = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu";
const DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com";
const DEFAULT_COMMITMENT = "confirmed";
const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_MAX_ATTEMPTS = 3;
const ZERO_PUBKEY = (() => {
  if (PublicKey.default) return PublicKey.default;
  return new PublicKey(new Uint8Array(32));
})();

function isBytesLike(value) {
  if (!value) return false;
  if (value instanceof Uint8Array) return true;
  if (typeof Buffer !== "undefined" && Buffer.isBuffer && Buffer.isBuffer(value)) {
    return true;
  }
  return false;
}

function coercePublicKey(value) {
  if (!value) return ZERO_PUBKEY;
  if (value?.publicKey) {
    return coercePublicKey(value.publicKey);
  }
  if (value instanceof PublicKey) {
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return ZERO_PUBKEY;
    }
    try {
      return new PublicKey(trimmed);
    } catch (err) {
      throw new Error(`Failed to parse public key string: ${err.message}`);
    }
  }
  if (isBytesLike(value)) {
    try {
      return new PublicKey(value);
    } catch (err) {
      throw new Error(`Failed to parse public key bytes: ${err.message}`);
    }
  }
  throw new TypeError("Unsupported public key input type");
}

let cachedIdl = null;
let cachedProgramIdString = null;
let cachedProgramId = null;

function readIdl() {
  if (!cachedIdl) {
    const raw = fs.readFileSync(IDL_PATH, "utf8");
    cachedIdl = JSON.parse(raw);
  }
  return cachedIdl;
}

function resolveWallet(options, fallbackWallet) {
  const providedWallet = options?.wallet;
  if (providedWallet) {
    if (providedWallet.publicKey instanceof PublicKey) {
      return providedWallet;
    }
    if (providedWallet.publicKey) {
      const coercedPublicKey = coercePublicKey(providedWallet.publicKey);
      try {
        providedWallet.publicKey = coercedPublicKey;
        return providedWallet;
      } catch (err) {
        return new ReadOnlyWallet(coercedPublicKey);
      }
    }
    return new ReadOnlyWallet(providedWallet);
  }
  if (fallbackWallet) {
    if (fallbackWallet.publicKey && !(fallbackWallet.publicKey instanceof PublicKey)) {
      const coercedPublicKey = coercePublicKey(fallbackWallet.publicKey);
      try {
        fallbackWallet.publicKey = coercedPublicKey;
        return fallbackWallet;
      } catch (err) {
        const clone = Object.create(Object.getPrototypeOf(fallbackWallet) || Object.prototype);
        return Object.assign(clone, fallbackWallet, { publicKey: coercedPublicKey });
      }
    }
    if (typeof fallbackWallet === "string" || isBytesLike(fallbackWallet)) {
      return new ReadOnlyWallet(fallbackWallet);
    }
    return fallbackWallet;
  }
  if (options?.publicKey) {
    return new ReadOnlyWallet(options.publicKey);
  }
  return new ReadOnlyWallet();
}

function parseListEnv(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return String(value)
    .split(/[\s,]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function parseIntEnv(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function resolveCommitment(explicit) {
  const candidate = explicit || process.env.JUPITER_PERPS_RPC_COMMITMENT || process.env.PERPS_RPC_COMMITMENT;
  if (!candidate) return DEFAULT_COMMITMENT;
  return candidate;
}

function resolveRpcUrl(explicit) {
  return (
    explicit ||
    process.env.JUPITER_PERPS_RPC_URL ||
    process.env.PERPS_RPC_URL ||
    process.env.SOLANA_RPC_URL ||
    process.env.RPC_URL ||
    DEFAULT_RPC_URL
  );
}

export function clearPerpsCaches() {
  cachedIdl = null;
  cachedProgramId = null;
  cachedProgramIdString = null;
}

export function loadPerpsIdl() {
  return readIdl();
}

export function getPerpsProgramId() {
  const id = process.env.JUPITER_PERPS_PROGRAM_ID || process.env.PERPS_PROGRAM_ID || DEFAULT_PROGRAM_ID;
  if (cachedProgramId && cachedProgramIdString === id) {
    return cachedProgramId;
  }
  cachedProgramId = new PublicKey(id);
  cachedProgramIdString = id;
  return cachedProgramId;
}

export function getPerpsRpcConfig(overrides = {}) {
  const primary = resolveRpcUrl(overrides.rpcUrl);
  const fallbacks = parseListEnv(
    overrides.rpcFallbackUrls ||
      process.env.JUPITER_PERPS_RPC_FALLBACKS ||
      process.env.PERPS_RPC_FALLBACKS
  );
  const timeoutMs = parseIntEnv(
    overrides.timeoutMs ?? process.env.JUPITER_PERPS_RPC_TIMEOUT_MS ?? process.env.PERPS_RPC_TIMEOUT_MS,
    DEFAULT_TIMEOUT_MS
  );
  const commitment = resolveCommitment(overrides.commitment);
  const maxAttempts = parseIntEnv(
    overrides.maxAttempts ?? process.env.JUPITER_PERPS_RPC_MAX_ATTEMPTS ?? process.env.PERPS_RPC_MAX_ATTEMPTS,
    DEFAULT_MAX_ATTEMPTS
  );
  return {
    primary,
    fallbacks,
    timeoutMs,
    commitment,
    maxAttempts: Math.max(1, maxAttempts || DEFAULT_MAX_ATTEMPTS),
  };
}

export class ReadOnlyWallet {
  constructor(publicKey = ZERO_PUBKEY) {
    this.publicKey = coercePublicKey(publicKey);
  }

  async signTransaction(transaction) {
    return transaction;
  }

  async signAllTransactions(transactions) {
    return transactions;
  }
}

export function createPerpsConnection(rpcUrl, config = {}) {
  const { timeoutMs, commitment } = config;
  const connection = new Connection(rpcUrl, {
    commitment: resolveCommitment(commitment),
    confirmTransactionInitialTimeout: timeoutMs || DEFAULT_TIMEOUT_MS,
  });
  return connection;
}

export function createPerpsProvider(options = {}) {
  const rpcConfig = getPerpsRpcConfig(options);
  const endpoint = options.rpcUrl || rpcConfig.primary;
  const connection =
    options.connection || createPerpsConnection(endpoint, { timeoutMs: rpcConfig.timeoutMs, commitment: rpcConfig.commitment });
  const wallet = resolveWallet(options, options.provider?.wallet);
  const provider = new AnchorProvider(connection, wallet, {
    commitment: rpcConfig.commitment,
    preflightCommitment: rpcConfig.commitment,
  });
  return provider;
}

export function createPerpetualsProgram(options = {}) {
  const idl = loadPerpsIdl();
  const programId = getPerpsProgramId();
  const provider = options.provider || createPerpsProvider(options);
  return new Program(idl, programId, provider);
}

function resolveEndpointEntries(options) {
  if (options.program && options.connection) {
    const endpoint = options.endpoint || options.connection.rpcEndpoint;
    return [
      {
        endpoint,
        connection: options.connection,
        program: options.program,
        provider: options.provider || options.program.provider,
        wallet: options.wallet || options.program?.provider?.wallet,
      },
    ];
  }
  const config = getPerpsRpcConfig(options);
  const endpoints = [options.rpcUrl || config.primary, ...(options.rpcFallbackUrls || config.fallbacks || [])]
    .filter(Boolean)
    .filter((value, index, array) => array.indexOf(value) === index);
  const limit = Math.max(1, options.maxAttempts || config.maxAttempts || endpoints.length);
  return endpoints.slice(0, limit).map((endpoint) => ({ endpoint }));
}

export async function withPerpsRpcRetry(label, handler, options = {}) {
  const attempts = resolveEndpointEntries(options);
  if (attempts.length === 0) {
    throw new Error("No RPC endpoints configured for perps client");
  }
  const baseConfig = getPerpsRpcConfig(options);
  const endpointCache = options.endpointCache;
  const idl = options.idl || loadPerpsIdl();
  const programId = options.programId ? new PublicKey(options.programId) : getPerpsProgramId();
  const attemptTotal = attempts.length;
  const errors = [];
  for (let idx = 0; idx < attempts.length; idx += 1) {
    const entry = attempts[idx];
    const attempt = idx + 1;
    const startedAt = Date.now();
    let endpoint = entry.endpoint;
    let connection = entry.connection || options.connection;
    let provider = entry.provider || options.provider;
    let program = entry.program || options.program;
    let wallet = entry.wallet || options.wallet;
    let cached = null;
    if (endpoint && endpointCache?.has(endpoint)) {
      cached = endpointCache.get(endpoint);
    }
    if (!connection && cached?.connection) {
      connection = cached.connection;
    }
    if (!provider && cached?.provider) {
      provider = cached.provider;
    }
    if (!program && cached?.program) {
      program = cached.program;
    }
    if (!wallet && cached?.wallet) {
      wallet = cached.wallet;
    }
    try {
      if (!connection) {
        const targetEndpoint = endpoint || options.rpcUrl || baseConfig.primary;
        connection = createPerpsConnection(targetEndpoint, {
          timeoutMs: baseConfig.timeoutMs,
          commitment: baseConfig.commitment,
        });
        endpoint = targetEndpoint;
      }
      endpoint = endpoint || connection.rpcEndpoint;
      if (endpointCache && endpoint && endpointCache.has(endpoint)) {
        cached = endpointCache.get(endpoint);
        if (!connection && cached?.connection) {
          connection = cached.connection;
        }
        if (!provider && cached?.provider) {
          provider = cached.provider;
        }
        if (!program && cached?.program) {
          program = cached.program;
        }
        if (!wallet && cached?.wallet) {
          wallet = cached.wallet;
        }
      }
      if (!provider) {
        const resolvedWallet = resolveWallet({ ...options, wallet }, cached?.wallet);
        provider = new AnchorProvider(connection, resolvedWallet, {
          commitment: baseConfig.commitment,
          preflightCommitment: baseConfig.commitment,
        });
      }
      if (!program) {
        program = new Program(idl, programId, provider);
      }
      const cacheEntry = {
        connection,
        provider,
        program,
        wallet: provider.wallet || wallet,
      };
      if (endpointCache && endpoint) {
        endpointCache.set(endpoint, cacheEntry);
      }
      const result = await handler({
        program,
        provider,
        connection,
        endpoint,
        attempt,
        commitment: baseConfig.commitment,
        rpcConfig: baseConfig,
      });
      const durationMs = Date.now() - startedAt;
      structuredLog("info", "perps-rpc", {
        label,
        attempt,
        endpoint,
        durationMs,
        status: "ok",
        maxAttempts: attemptTotal,
      });
      return result;
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      structuredLog("warn", "perps-rpc", {
        label,
        attempt,
        endpoint,
        durationMs,
        status: "error",
        error: error?.message || String(error),
        maxAttempts: attemptTotal,
      });
      errors.push({ endpoint, message: error?.message || String(error) });
    }
  }
  const aggregate = new Error(`${label} failed across all configured perps RPC endpoints`);
  aggregate.causes = toSerializable(errors);
  aggregate.attempts = attemptTotal;
  structuredLog("error", "perps-rpc", {
    label,
    status: "fatal",
    attempts: attemptTotal,
    errors: toSerializable(errors),
  });
  throw aggregate;
}

export function createPerpsClient(options = {}) {
  const endpointCache = options.endpointCache || new Map();
  const baseOptions = { ...options, endpointCache };

  function resolveEndpoint(endpoint) {
    if (endpoint) return endpoint;
    const config = getPerpsRpcConfig(baseOptions);
    return baseOptions.rpcUrl || config.primary;
  }

  function ensureEndpointResources(endpoint) {
    const targetEndpoint = resolveEndpoint(endpoint);
    const cached = endpointCache.get(targetEndpoint);
    if (cached) {
      return cached;
    }
    const provider = createPerpsProvider({ ...baseOptions, rpcUrl: targetEndpoint });
    const program = createPerpetualsProgram({ ...baseOptions, provider });
    const connection = provider.connection;
    const effectiveEndpoint = connection.rpcEndpoint || targetEndpoint;
    const resources = { connection, provider, program, wallet: provider.wallet };
    endpointCache.set(effectiveEndpoint, resources);
    if (effectiveEndpoint !== targetEndpoint) {
      endpointCache.set(targetEndpoint, resources);
    }
    return resources;
  }

  return {
    getRpcConfig() {
      return getPerpsRpcConfig(baseOptions);
    },
    getConnection(endpoint) {
      return ensureEndpointResources(endpoint).connection;
    },
    getProvider(endpoint) {
      return ensureEndpointResources(endpoint).provider;
    },
    getProgram(endpoint) {
      return ensureEndpointResources(endpoint).program;
    },
    withRetry(label, handler, overrides = {}) {
      const retryOptions = { ...baseOptions, ...overrides, endpointCache };
      return withPerpsRpcRetry(
        label,
        async (context) => {
          const targetEndpoint = context.endpoint || overrides.rpcUrl || resolveEndpoint();
          const cached = endpointCache.get(targetEndpoint) || ensureEndpointResources(targetEndpoint);
          return handler({ ...context, ...cached });
        },
        retryOptions
      );
    },
    clearCache(endpoint) {
      if (endpoint) {
        endpointCache.delete(endpoint);
      } else {
        endpointCache.clear();
      }
    },
    endpointCache,
  };
}
