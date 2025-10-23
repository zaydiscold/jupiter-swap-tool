import {
  createPerpetualsProgram,
  createPerpsConnection,
  createPerpsProvider,
  createPerpsClient,
  clearPerpsCaches,
  getPerpsProgramId,
  getPerpsRpcConfig,
  loadPerpsIdl,
  ReadOnlyWallet,
  withPerpsRpcRetry,
} from "./client.js";
import {
  fetchPools,
  fetchCustodies,
  fetchPositions,
  fetchMarkets,
  serializeAccountEntry,
} from "./data.js";
import { structuredLog, toSerializable, toPublicKey } from "./utils.js";
import {
  listWallets as helperListWallets,
  ensureAtaForMint as helperEnsureAtaForMint,
  ensureWrappedSolBalance as helperEnsureWrappedSolBalance,
} from "../shared/wallet_helpers.js";

export {
  createPerpetualsProgram,
  createPerpsConnection,
  createPerpsProvider,
  createPerpsClient,
  clearPerpsCaches,
  getPerpsProgramId,
  getPerpsRpcConfig,
  loadPerpsIdl,
  ReadOnlyWallet,
  withPerpsRpcRetry,
  fetchPools,
  fetchCustodies,
  fetchPositions,
  fetchMarkets,
  serializeAccountEntry,
  structuredLog,
  toSerializable,
  toPublicKey,
};

export function listWallets(...args) {
  return helperListWallets(...args);
}

export function ensureAtaForMint(...args) {
  return helperEnsureAtaForMint(...args);
}

export function ensureWrappedSolBalance(...args) {
  return helperEnsureWrappedSolBalance(...args);
}

export function resolveTokenProgramForMint(...args) {
  return cliResolveTokenProgramForMint(...args);
}
