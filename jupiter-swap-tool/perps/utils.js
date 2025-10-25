import { PublicKey } from "@solana/web3.js";

let BN = null;
try {
  const anchorModule = await import("@coral-xyz/anchor");
  const resolved = anchorModule?.default ?? anchorModule;
  BN = resolved?.BN ?? null;
} catch (_) {
  BN = null;
}

function isPlainObject(value) {
  if (value === null || typeof value !== "object") return false;
  if (Array.isArray(value)) return false;
  if (value instanceof Map || value instanceof Set) return false;
  if (value instanceof Date) return false;
  if (value instanceof PublicKey) return false;
  if (Buffer.isBuffer(value)) return false;
  if (ArrayBuffer.isView(value)) return false;
  return Object.getPrototypeOf(value) === Object.prototype;
}

export function toSerializable(value) {
  if (BN && value instanceof BN) {
    return value.toString();
  }
  if (value instanceof PublicKey) {
    return value.toBase58();
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Map) {
    return Array.from(value.entries()).map(([key, entryValue]) => [toSerializable(key), toSerializable(entryValue)]);
  }
  if (value instanceof Set) {
    return Array.from(value).map((entryValue) => toSerializable(entryValue));
  }
  if (Array.isArray(value)) {
    return value.map((entryValue) => toSerializable(entryValue));
  }
  if (isPlainObject(value)) {
    const result = {};
    for (const [key, entryValue] of Object.entries(value)) {
      result[key] = toSerializable(entryValue);
    }
    return result;
  }
  return value;
}

export function structuredLog(level, scope, payload = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    scope,
    level,
    ...toSerializable(payload),
  };
  const line = JSON.stringify(entry);
  if (level === "error") {
    console.error(line);
  } else if (level === "warn") {
    console.warn(line);
  } else {
    console.log(line);
  }
}

export function toPublicKey(value, fieldName = "pubkey") {
  if (!value) {
    throw new Error(`Missing ${fieldName}`);
  }
  if (value instanceof PublicKey) {
    return value;
  }
  if (typeof value === "string") {
    return new PublicKey(value);
  }
  throw new Error(`Unsupported ${fieldName} type: ${typeof value}`);
}
