import anchorPkg from "@coral-xyz/anchor";
const { AnchorProvider, BN, Program, BorshAccountsCoder } = anchorPkg;
import {
  PublicKey,
  ComputeBudgetProgram,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  getAssociatedTokenAddress,
} from "@solana/spl-token";
import { createHash } from "crypto";
import { createRequire } from "module";
import { getPerpsProgramId } from "./perps/client.js";

const require = createRequire(import.meta.url);
const PERPS_IDL = require("./perps_idl.json");
if (!PERPS_IDL.metadata) {
  PERPS_IDL.metadata = {
    address: "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
  };
}
if (!PERPS_IDL.address) {
  PERPS_IDL.address = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu";
}
if (Array.isArray(PERPS_IDL.accounts)) {
  for (const accountDef of PERPS_IDL.accounts) {
    if (accountDef && accountDef.size === undefined) {
      accountDef.size = 0;
    }
  }
}
const SUPPORTED_ACCOUNT_NAMES = new Set([
  "pool",
  "custody",
  "position",
  "positionRequest",
]);
function normalizeIdlValue(value) {
  if (typeof value === "string") {
    if (value === "pubkey") {
      return "publicKey";
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeIdlValue(item));
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value).map(([key, val]) => [
      key,
      normalizeIdlValue(val),
    ]);
    const normalized = Object.fromEntries(entries);
    if (Object.prototype.hasOwnProperty.call(normalized, "defined")) {
      const definedVal = normalized.defined;
      if (definedVal && typeof definedVal === "object" && "name" in definedVal) {
        normalized.defined = definedVal.name;
      }
    }
    return normalized;
  }
  return value;
}

function normalizeTypeDef(typeDef) {
  if (!typeDef) return typeDef;
  return normalizeIdlValue(typeDef);
}

function computeAccountDiscriminator(name) {
  const hash = createHash("sha256")
    .update(`account:${name}`)
    .digest();
  return Array.from(hash.slice(0, 8));
}

const filteredAccounts = Array.isArray(PERPS_IDL.accounts)
  ? PERPS_IDL.accounts
      .filter((account) => SUPPORTED_ACCOUNT_NAMES.has(account?.name))
      .map((account) => ({
        ...account,
        type: normalizeTypeDef(account.type),
        discriminator:
          account.discriminator || computeAccountDiscriminator(account.name),
      }))
  : [];

const baseTypes = Array.isArray(PERPS_IDL.types)
  ? PERPS_IDL.types.map((entry) => ({
      ...entry,
      type: normalizeTypeDef(entry.type),
    }))
  : [];
const existingTypeNames = new Set(baseTypes.map((entry) => entry?.name));
for (const account of filteredAccounts) {
  if (!existingTypeNames.has(account.name)) {
    baseTypes.push({
      name: account.name,
      type: normalizeTypeDef(account.type),
    });
    existingTypeNames.add(account.name);
  }
}

const ACCOUNTS_IDL = {
  version: PERPS_IDL.version,
  name: PERPS_IDL.name,
  instructions: [],
  accounts: filteredAccounts,
  types: baseTypes,
};
const ACCOUNTS_CODER = new BorshAccountsCoder(ACCOUNTS_IDL);

export const JUPITER_PERPETUALS_EVENT_AUTHORITY = new PublicKey(
  "37hJBDnntwqhGbK7L6M1bLyvccj4u55CCUiLPdYkiqBN"
);

export const JLP_POOL_ACCOUNT = new PublicKey(
  "5BUwFW4nRbftYTDMbgxykoFWqWHPzahFSNAaaaJtVKsq"
);

export const KNOWN_CUSTODIES = [
  {
    symbol: "SOL",
    custody: new PublicKey("7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz"),
    mint: new PublicKey("So11111111111111111111111111111111111111112"),
  },
  {
    symbol: "ETH",
    custody: new PublicKey("AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn"),
    mint: new PublicKey("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"),
  },
  {
    symbol: "BTC",
    custody: new PublicKey("5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm"),
    mint: new PublicKey("3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh"),
  },
  {
    symbol: "USDC",
    custody: new PublicKey("G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa"),
    mint: new PublicKey("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
  },
  {
    symbol: "USDT",
    custody: new PublicKey("4vkNeXiYEUizLdrpdPS1eC2mccyM4NUPRtERrk6ZETkk"),
    mint: new PublicKey("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
  },
];

const custodyBySymbol = new Map(
  KNOWN_CUSTODIES.map((entry) => [entry.symbol.toLowerCase(), entry])
);
const custodyByPubkey = new Map(
  KNOWN_CUSTODIES.map((entry) => [entry.custody.toBase58(), entry])
);

class ReadonlyWallet {
  constructor(pubkey) {
    this.payerPublicKey = pubkey;
  }

  get publicKey() {
    return this.payerPublicKey;
  }

  async signTransaction(tx) {
    return tx;
  }

  async signAllTransactions(txs) {
    return txs;
  }
}

const programCache = new Map();

function getProgramCacheKey(connection, programId = null) {
  const endpoint =
    connection?.__rpcEndpoint || connection?._rpcEndpoint || "unknown";
  const commitment = connection?.commitment || connection?._commitment || "";
  const programIdString = (programId || getPerpsProgramId()).toBase58();
  return `${endpoint}::${commitment}::${programIdString}`;
}

export function getPerpsProgram(connection) {
  if (!connection) {
    throw new Error("connection is required to load the perps program");
  }
  const programId = getPerpsProgramId();
  const cacheKey = getProgramCacheKey(connection, programId);
  if (programCache.has(cacheKey)) {
    return programCache.get(cacheKey);
  }
  const programIdl = { ...PERPS_IDL };
  const provider = new AnchorProvider(
    connection,
    new ReadonlyWallet(PublicKey.default),
    {
      commitment: connection.commitment || connection._commitment || "confirmed",
      preflightCommitment:
        connection.commitment || connection._commitment || "confirmed",
    }
  );
  const program = new Program(
    programIdl,
    programId,
    provider
  );
  programCache.set(cacheKey, program);
  return program;
}

export function derivePerpetualsPda() {
  const programId = getPerpsProgramId();
  const [pubkey, bump] = PublicKey.findProgramAddressSync(
    [Buffer.from("perpetuals")],
    programId
  );
  return { pubkey, bump };
}

export function deriveEventAuthorityPda() {
  const programId = getPerpsProgramId();
  const [pubkey, bump] = PublicKey.findProgramAddressSync(
    [Buffer.from("__event_authority")],
    programId
  );
  return { pubkey, bump };
}

export function derivePositionPda({
  wallet,
  custody,
  collateralCustody,
  side,
}) {
  if (!wallet || !custody || !collateralCustody) {
    throw new Error("wallet, custody and collateralCustody are required");
  }
  const sideBytes =
    typeof side === "string"
      ? side.toLowerCase() === "long"
        ? [1]
        : [2]
      : Array.isArray(side)
      ? side
      : [1];
  const programId = getPerpsProgramId();
  const [pubkey, bump] = PublicKey.findProgramAddressSync(
    [
      Buffer.from("position"),
      wallet.toBuffer(),
      JLP_POOL_ACCOUNT.toBuffer(),
      custody.toBuffer(),
      collateralCustody.toBuffer(),
      Uint8Array.from(sideBytes),
    ],
    programId
  );
  return { pubkey, bump };
}

export function derivePositionRequestPda({ position, counter, change }) {
  if (!position) {
    throw new Error("position public key required");
  }
  let counterBn = null;
  if (typeof counter === "bigint") {
    counterBn = new BN(counter.toString());
  } else if (typeof counter === "number") {
    counterBn = new BN(counter);
  } else if (counter instanceof BN) {
    counterBn = counter;
  }
  if (!counterBn) {
    counterBn = new BN(Math.floor(Math.random() * 1_000_000_000));
  }
  const changeByte = change === "decrease" ? [2] : [1];
  const programId = getPerpsProgramId();
  const [pubkey, bump] = PublicKey.findProgramAddressSync(
    [
      Buffer.from("position_request"),
      position.toBuffer(),
      counterBn.toArrayLike(Buffer, "le", 8),
      Buffer.from(changeByte),
    ],
    programId
  );
  return { pubkey, bump, counter: counterBn };
}

export function resolveCustodyIdentifier(identifier) {
  if (!identifier) return null;
  const normalized = identifier.trim().toLowerCase();
  if (!normalized) return null;
  if (custodyBySymbol.has(normalized)) {
    return custodyBySymbol.get(normalized);
  }
  try {
    const pubkey = new PublicKey(identifier);
    const existing = custodyByPubkey.get(pubkey.toBase58());
    if (existing) {
      return existing;
    }
    return {
      symbol: null,
      custody: pubkey,
      mint: null,
    };
  } catch (_) {
    return null;
  }
}

function toBn(input) {
  if (input instanceof BN) return input;
  if (typeof input === "bigint") return new BN(input.toString());
  if (typeof input === "number") return new BN(input);
  if (typeof input === "string") return new BN(input, 10);
  throw new Error("Cannot convert value to BN");
}

function toOptionBn(input) {
  if (input === null || input === undefined) return null;
  return toBn(input);
}

function parseSide(side) {
  const normalized = String(side || "").toLowerCase();
  if (normalized === "short") return { short: {} };
  if (normalized === "long") return { long: {} };
  throw new Error("side must be 'long' or 'short'");
}

export async function buildIncreaseRequestInstruction({
  connection,
  owner,
  custody,
  collateralCustody,
  inputMint,
  sizeUsdDelta,
  collateralTokenDelta,
  side,
  priceSlippage,
  jupiterMinimumOut = null,
  counter = null,
  referral = null,
}) {
  const program = getPerpsProgram(connection);
  const programId = program.programId || getPerpsProgramId();
  const custodyPk = new PublicKey(custody);
  const collateralPk = new PublicKey(collateralCustody);
  const inputMintPk = new PublicKey(inputMint);
  const ownerPk = new PublicKey(owner);
  const { pubkey: position } = derivePositionPda({
    wallet: ownerPk,
    custody: custodyPk,
    collateralCustody: collateralPk,
    side,
  });
  const { pubkey: positionRequest, counter: counterBn } =
    derivePositionRequestPda({ position, counter, change: "increase" });
  const fundingAccount = await getAssociatedTokenAddress(
    inputMintPk,
    ownerPk,
    false,
    TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const positionRequestAta = await getAssociatedTokenAddress(
    inputMintPk,
    positionRequest,
    true,
    TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const params = {
    sizeUsdDelta: toBn(sizeUsdDelta),
    collateralTokenDelta: toBn(collateralTokenDelta),
    side: parseSide(side),
    priceSlippage: toBn(priceSlippage),
    jupiterMinimumOut: toOptionBn(jupiterMinimumOut),
    counter: counterBn,
  };
  const { pubkey: perpetuals } = derivePerpetualsPda();
  const { pubkey: eventAuthority } = deriveEventAuthorityPda();
  const ix = await program.methods
    .createIncreasePositionMarketRequest(params)
    .accounts({
      owner: ownerPk,
      fundingAccount,
      perpetuals,
      pool: JLP_POOL_ACCOUNT,
      position,
      positionRequest,
      positionRequestAta,
      custody: custodyPk,
      collateralCustody: collateralPk,
      inputMint: inputMintPk,
      referral,
      eventAuthority,
      program: programId,
    })
    .instruction();
  return {
    instruction: ix,
    position,
    positionRequest,
    counter: counterBn,
    fundingAccount,
    positionRequestAta,
    inputMint: inputMintPk,
  };
}

export async function buildDecreaseRequestInstruction({
  connection,
  owner,
  position,
  desiredMint,
  collateralUsdDelta,
  sizeUsdDelta,
  priceSlippage,
  jupiterMinimumOut = null,
  entirePosition = null,
  counter = null,
  referral = null,
}) {
  const program = getPerpsProgram(connection);
  const programId = program.programId || getPerpsProgramId();
  const ownerPk = new PublicKey(owner);
  const positionPk = new PublicKey(position);
  const positionAccount = await program.account.position.fetch(positionPk);
  const desiredMintPk = new PublicKey(desiredMint);
  const { pubkey: positionRequest, counter: counterBn } =
    derivePositionRequestPda({ position: positionPk, counter, change: "decrease" });
  const receivingAccount = await getAssociatedTokenAddress(
    desiredMintPk,
    ownerPk,
    true,
    TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const positionRequestAta = await getAssociatedTokenAddress(
    desiredMintPk,
    positionRequest,
    true,
    TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const params = {
    collateralUsdDelta: toBn(collateralUsdDelta ?? 0),
    sizeUsdDelta: toBn(sizeUsdDelta ?? 0),
    priceSlippage: toBn(priceSlippage),
    jupiterMinimumOut: toOptionBn(jupiterMinimumOut),
    entirePosition:
      entirePosition === null || entirePosition === undefined
        ? null
        : !!entirePosition,
    counter: counterBn,
  };
  const { pubkey: perpetuals } = derivePerpetualsPda();
  const { pubkey: eventAuthority } = deriveEventAuthorityPda();
  const ix = await program.methods
    .createDecreasePositionMarketRequest(params)
    .accounts({
      owner: ownerPk,
      receivingAccount,
      perpetuals,
      pool: JLP_POOL_ACCOUNT,
      position: positionPk,
      positionRequest,
      positionRequestAta,
      custody: positionAccount.custody,
      collateralCustody: positionAccount.collateralCustody,
      desiredMint: desiredMintPk,
      referral,
      eventAuthority,
      program: programId,
    })
    .instruction();
  return {
    instruction: ix,
    position: positionPk,
    positionAccount,
    positionRequest,
    counter: counterBn,
    receivingAccount,
    positionRequestAta,
    desiredMint: desiredMintPk,
  };
}

export async function simulatePerpsInstructions({
  connection,
  payer,
  instructions,
}) {
  const payerPk = new PublicKey(payer);
  const { blockhash } = await connection.getLatestBlockhash();
  const message = new TransactionMessage({
    payerKey: payerPk,
    recentBlockhash: blockhash,
    instructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  const simulation = await connection.simulateTransaction(tx, {
    replaceRecentBlockhash: true,
    sigVerify: false,
  });
  return simulation;
}

export async function fetchPoolAccount(connection) {
  const info = await connection.getAccountInfo(JLP_POOL_ACCOUNT);
  if (!info || !info.data) {
    throw new Error("Pool account not found");
  }
  const account = ACCOUNTS_CODER.decode("pool", info.data);
  return { pubkey: JLP_POOL_ACCOUNT, account };
}

export async function fetchCustodyAccounts(connection, custodyPubkeys) {
  const unique = Array.from(
    new Map(
      custodyPubkeys.map((pk) => {
        const pubkey = new PublicKey(pk);
        return [pubkey.toBase58(), pubkey];
      })
    ).values()
  );
  const infos = await connection.getMultipleAccountsInfo(unique);
  const accounts = [];
  for (let i = 0; i < unique.length; i += 1) {
    const info = infos[i];
    if (!info || !info.data) continue;
    const account = ACCOUNTS_CODER.decode("custody", info.data);
    accounts.push({ pubkey: unique[i], account });
  }
  const map = new Map();
  for (const entry of accounts) {
    map.set(entry.pubkey.toBase58(), entry);
  }
  return map;
}

export async function fetchPositionsForOwners(connection, ownerPubkeys) {
  const results = [];
  for (const owner of ownerPubkeys) {
    const ownerPk = new PublicKey(owner);
    const memcmpFilters = [
      {
        memcmp: {
          offset: 8,
          bytes: ownerPk.toBase58(),
        },
      },
    ];
    const programId = getPerpsProgramId();
    const rawAccounts = await connection.getProgramAccounts(programId, {
      filters: memcmpFilters,
    });
    const decoded = rawAccounts.map((entry) => ({
      publicKey: entry.pubkey,
      account: ACCOUNTS_CODER.decode("position", entry.account.data),
    }));
    results.push({ owner: ownerPk, positions: decoded });
  }
  return results;
}

export function buildComputeBudgetInstructions({
  units,
  microLamports,
}) {
  const instructions = [];
  if (typeof units === "number" && Number.isFinite(units)) {
    instructions.push(
      ComputeBudgetProgram.setComputeUnitLimit({
        units,
      })
    );
  }
  if (
    typeof microLamports === "number" &&
    Number.isFinite(microLamports) &&
    microLamports > 0
  ) {
    instructions.push(
      ComputeBudgetProgram.setComputeUnitPrice({
        microLamports,
      })
    );
  }
  return instructions;
}

export function preparePreviewTransaction({ instructions, payer }) {
  const message = new TransactionMessage({
    payerKey: payer,
    recentBlockhash: PublicKey.default.toString(),
    instructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  const raw = tx.serialize();
  return Buffer.from(raw).toString("base64");
}

export function extractSideLabel(side) {
  if (!side) return "unknown";
  if (typeof side === "object") {
    if (side.long) return "long";
    if (side.short) return "short";
  }
  return String(side);
}

export function convertDbpsToHourlyRate(dbps) {
  const value = typeof dbps === "bigint" ? dbps : BigInt(dbps || 0);
  const perUnit = Number(value) / 100000;
  return perUnit;
}
