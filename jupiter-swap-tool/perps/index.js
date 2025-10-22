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
  listWallets as cliListWallets,
  ensureAtaForMint as cliEnsureAtaForMint,
  ensureWrappedSolBalance as cliEnsureWrappedSolBalance,
} from "../cli_trader.js";

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
  return cliListWallets(...args);
}

export function ensureAtaForMint(...args) {
  return cliEnsureAtaForMint(...args);
}

export function ensureWrappedSolBalance(...args) {
  return cliEnsureWrappedSolBalance(...args);
}
