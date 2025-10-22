import fs from "fs";
import path from "path";
import bs58 from "bs58";
import { Keypair, SystemProgram, Transaction } from "@solana/web3.js";
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  getAssociatedTokenAddress,
  createAssociatedTokenAccountInstruction,
  createSyncNativeInstruction,
  NATIVE_MINT,
} from "@solana/spl-token";

const DEFAULT_KEYPAIR_DIR = "./keypairs";

function defaultPaint(text) {
  return text;
}

function defaultFormatBaseUnits(amountLike, decimals) {
  let amount = amountLike;
  if (typeof amount === "string") {
    amount = BigInt(amount);
  } else if (typeof amount === "number") {
    amount = BigInt(amount);
  }
  if (typeof amount !== "bigint") {
    throw new TypeError("formatBaseUnits expects a bigint-compatible value");
  }
  const base = BigInt(10) ** BigInt(decimals);
  const whole = amount / base;
  const fraction = amount % base;
  if (fraction === 0n) return whole.toString();
  const fractionStr = fraction
    .toString()
    .padStart(decimals, "0")
    .replace(/0+$/, "");
  return `${whole}.${fractionStr}`;
}

function defaultSymbolForMint(mint) {
  if (!mint) return "????";
  return mint.toString().slice(0, 4);
}

function defaultLoadKeypairFromFile(filepath) {
  const raw = fs.readFileSync(filepath, "utf8");
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return Keypair.fromSecretKey(Uint8Array.from(parsed));
    }
    if (parsed && typeof parsed === "object") {
      if (Array.isArray(parsed.secretKey)) {
        return Keypair.fromSecretKey(Uint8Array.from(parsed.secretKey));
      }
      if (typeof parsed.secretKeyBase58 === "string") {
        return Keypair.fromSecretKey(bs58.decode(parsed.secretKeyBase58));
      }
    }
  } catch (e) {
    // Fall through to attempt base58 decoding below
  }
  try {
    const buf = bs58.decode(raw);
    return Keypair.fromSecretKey(buf);
  } catch (err) {
    throw new Error(`Cannot parse keyfile ${filepath}: ${err.message}`);
  }
}

let helperConfig = {
  keypairDir: DEFAULT_KEYPAIR_DIR,
  loadKeypairFromFile: defaultLoadKeypairFromFile,
  paint: defaultPaint,
  formatBaseUnits: defaultFormatBaseUnits,
  symbolForMint: defaultSymbolForMint,
  logger: console,
};

export function configureWalletHelpers(options = {}) {
  if (!options || typeof options !== "object") return;
  if (typeof options.keypairDir === "string" && options.keypairDir.length > 0) {
    helperConfig.keypairDir = options.keypairDir;
  }
  if (typeof options.loadKeypairFromFile === "function") {
    helperConfig.loadKeypairFromFile = options.loadKeypairFromFile;
  }
  if (typeof options.paint === "function") {
    helperConfig.paint = options.paint;
  }
  if (typeof options.formatBaseUnits === "function") {
    helperConfig.formatBaseUnits = options.formatBaseUnits;
  }
  if (typeof options.symbolForMint === "function") {
    helperConfig.symbolForMint = options.symbolForMint;
  }
  if (options.logger && typeof options.logger === "object") {
    helperConfig.logger = options.logger;
  }
}

function getPaint() {
  return helperConfig.paint || defaultPaint;
}

function getLogger() {
  return helperConfig.logger || console;
}

export function listWallets() {
  const keypairDir = helperConfig.keypairDir || DEFAULT_KEYPAIR_DIR;
  if (!fs.existsSync(keypairDir)) return [];
  const files = fs.readdirSync(keypairDir).filter((f) => !f.startsWith("."));
  const wallets = [];
  const paint = getPaint();
  const logger = getLogger();
  for (const f of files) {
    const fp = path.join(keypairDir, f);
    try {
      const kp = helperConfig.loadKeypairFromFile(fp);
      let birthMs = Date.now();
      try {
        const stats = fs.statSync(fp);
        birthMs = stats.birthtimeMs || stats.ctimeMs || stats.mtimeMs || birthMs;
      } catch (err) {
        // ignore stat errors
      }
      wallets.push({ name: f, kp, birthMs });
    } catch (err) {
      if (logger?.warn) {
        logger.warn(paint(`Skipping invalid key file ${f}: ${err.message}`, "warn"));
      }
    }
  }
  wallets.sort((a, b) => {
    if (a.birthMs !== b.birthMs) return a.birthMs - b.birthMs;
    return a.name.localeCompare(b.name);
  });
  return wallets;
}

export async function ensureAtaForMint(connection, wallet, mintPubkey, tokenProgram, { label } = {}) {
  const ata = await getAssociatedTokenAddress(
    mintPubkey,
    wallet.kp.publicKey,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const info = await connection.getAccountInfo(ata);
  if (info) return false;
  const ix = createAssociatedTokenAccountInstruction(
    wallet.kp.publicKey,
    ata,
    wallet.kp.publicKey,
    mintPubkey,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const tx = new Transaction().add(ix);
  tx.feePayer = wallet.kp.publicKey;
  const { blockhash } = await connection.getLatestBlockhash();
  tx.recentBlockhash = blockhash;
  tx.sign(wallet.kp);
  const sig = await connection.sendRawTransaction(tx.serialize());
  await connection.confirmTransaction(sig, "confirmed");
  const paint = getPaint();
  const logger = getLogger();
  const mintLabel = helperConfig.symbolForMint(
    typeof mintPubkey?.toBase58 === "function" ? mintPubkey.toBase58() : mintPubkey?.toString?.() || "unknown"
  );
  const contextLabel = label ? `${label}:` : "";
  if (logger?.log) {
    logger.log(
      paint(
        `  ${contextLabel} created ATA ${ata.toBase58()} for mint ${mintLabel}.`,
        "muted"
      )
    );
  }
  return true;
}

export async function ensureWrappedSolBalance(
  connection,
  wallet,
  requiredLamportsLike,
  existingLamportsOverride = null
) {
  let requiredLamports = requiredLamportsLike;
  if (typeof requiredLamports === "number") {
    requiredLamports = BigInt(requiredLamports);
  } else if (typeof requiredLamports === "string") {
    requiredLamports = BigInt(requiredLamports);
  }
  if (typeof requiredLamports !== "bigint") {
    throw new TypeError("requiredLamports must be bigint-compatible");
  }
  if (requiredLamports <= 0n) return;
  const mintPubkey = NATIVE_MINT;
  const tokenProgram = TOKEN_PROGRAM_ID;
  const ata = await getAssociatedTokenAddress(
    mintPubkey,
    wallet.kp.publicKey,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  try {
    await ensureAtaForMint(connection, wallet, mintPubkey, tokenProgram, {
      label: "wrap",
    });
  } catch (err) {
    const paint = getPaint();
    const logger = getLogger();
    if (logger?.error) {
      logger.error(
        paint(`  Failed to ensure SOL ATA for ${wallet.name}:`, "error"),
        err?.message || err
      );
    }
    throw err;
  }
  let existingLamports = 0n;
  if (typeof existingLamportsOverride === "bigint") {
    existingLamports = existingLamportsOverride;
  } else if (typeof existingLamportsOverride === "number") {
    existingLamports = BigInt(existingLamportsOverride);
  } else if (typeof existingLamportsOverride === "string") {
    existingLamports = BigInt(existingLamportsOverride);
  } else {
    try {
      const balanceInfo = await connection.getTokenAccountBalance(ata);
      existingLamports = BigInt(balanceInfo?.value?.amount ?? "0");
    } catch (err) {
      existingLamports = 0n;
    }
  }
  if (existingLamports >= requiredLamports) return;
  const lamportsToWrap = requiredLamports - existingLamports;
  const paint = getPaint();
  const logger = getLogger();
  const formatBaseUnits = helperConfig.formatBaseUnits || defaultFormatBaseUnits;
  const humanAmount = formatBaseUnits(lamportsToWrap, 9);
  if (logger?.log) {
    logger.log(
      paint(
        `  Wrapping ${humanAmount} SOL into wSOL for ${wallet.name} (existing ${formatBaseUnits(existingLamports, 9)} wSOL).`,
        "muted"
      )
    );
  }
  const tx = new Transaction().add(
    SystemProgram.transfer({
      fromPubkey: wallet.kp.publicKey,
      toPubkey: ata,
      lamports: Number(lamportsToWrap),
    }),
    createSyncNativeInstruction(ata, TOKEN_PROGRAM_ID)
  );
  tx.feePayer = wallet.kp.publicKey;
  const { blockhash } = await connection.getLatestBlockhash();
  tx.recentBlockhash = blockhash;
  tx.sign(wallet.kp);
  const sig = await connection.sendRawTransaction(tx.serialize());
  await connection.confirmTransaction(sig, "confirmed");
  if (logger?.log) {
    logger.log(
      paint(`  Wrapped ${humanAmount} SOL for ${wallet.name} — tx ${sig}`, "success")
    );
  }
}

export { defaultLoadKeypairFromFile as loadKeypairFromFile };
