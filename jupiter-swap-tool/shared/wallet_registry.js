/**
 * Dynamic Wallet Registry with Hierarchical Master-Slave Architecture
 *
 * Features:
 * - Auto-renumbering wallets when files are added/removed
 * - Hierarchical master-slave groups (groups of 5)
 * - Master-slave relationship tracking
 * - Number-based wallet resolution helpers
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const MANIFEST_PATH = path.join(__dirname, "..", "wallets_manifest.json");
export const GROUP_SIZE = 5;

function createEmptyManifest() {
  return {
    version: "1.0",
    lastSync: new Date().toISOString(),
    wallets: [],
  };
}

export function loadManifest() {
  if (!fs.existsSync(MANIFEST_PATH)) {
    return createEmptyManifest();
  }

  try {
    const raw = fs.readFileSync(MANIFEST_PATH, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("Failed to load wallet manifest, creating new one:", err.message);
    return createEmptyManifest();
  }
}

export function saveManifest(manifest) {
  const payload = {
    ...manifest,
    lastSync: new Date().toISOString(),
  };
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(payload, null, 2), "utf8");
}

export function getGroupNumber(walletNum) {
  return Math.floor((walletNum - 1) / GROUP_SIZE) + 1;
}

export function getMasterForWallet(walletNum) {
  if (walletNum === 1) return null; // wallet #1 is the master-master
  const groupNum = getGroupNumber(walletNum);
  return (groupNum - 1) * GROUP_SIZE + 1;
}

export function getWalletRole(walletNum) {
  if (walletNum === 1) return "master-master";
  if ((walletNum - 1) % GROUP_SIZE === 0) return "master";
  return "slave";
}

export function syncWalletsFromFilesystem(wallets) {
  const manifest = loadManifest();

  const sortedWallets = [...wallets].sort((a, b) => {
    if (a.birthMs !== b.birthMs) return a.birthMs - b.birthMs;
    return a.name.localeCompare(b.name);
  });

  manifest.wallets = sortedWallets.map((wallet, index) => {
    const walletNum = index + 1;
    return {
      number: walletNum,
      filename: wallet.name,
      role: getWalletRole(walletNum),
      master: getMasterForWallet(walletNum),
      group: getGroupNumber(walletNum),
    };
  });

  saveManifest(manifest);
  return manifest;
}

export function getWalletByNumber(walletNum) {
  const manifest = loadManifest();
  return manifest.wallets.find((w) => w.number === walletNum) || null;
}

export function getWalletByFilename(filename) {
  const manifest = loadManifest();
  return manifest.wallets.find((w) => w.filename === filename) || null;
}

export function getSlaves(masterNum) {
  const manifest = loadManifest();
  return manifest.wallets.filter((w) => w.master === masterNum);
}

export function getAllMasters() {
  const manifest = loadManifest();
  return manifest.wallets
    .filter((w) => w.role === "master" || w.role === "master-master")
    .map((w) => w.number);
}

export function getGroupMasters() {
  const manifest = loadManifest();
  return manifest.wallets.filter((w) => w.role === "master").map((w) => w.number);
}

export function getWalletsInGroup(groupNum) {
  const manifest = loadManifest();
  return manifest.wallets.filter((w) => w.group === groupNum);
}

export function getWalletCount() {
  const manifest = loadManifest();
  return manifest.wallets.length;
}

export function resolveWalletIdentifier(identifier) {
  if (typeof identifier === "number") {
    return getWalletByNumber(identifier);
  }

  const parsed = parseInt(identifier, 10);
  if (!Number.isNaN(parsed) && parsed.toString() === identifier.toString()) {
    return getWalletByNumber(parsed);
  }

  if (identifier === "master-master") {
    return getWalletByNumber(1);
  }

  return getWalletByFilename(identifier);
}

export function getHierarchySummary() {
  const manifest = loadManifest();
  const groups = new Map();

  for (const wallet of manifest.wallets) {
    if (!groups.has(wallet.group)) {
      groups.set(wallet.group, {
        groupNumber: wallet.group,
        master: null,
        slaves: [],
      });
    }

    const group = groups.get(wallet.group);
    if (wallet.role === "master" || wallet.role === "master-master") {
      group.master = wallet;
    } else {
      group.slaves.push(wallet);
    }
  }

  return {
    totalWallets: manifest.wallets.length,
    totalGroups: groups.size,
    groups: Array.from(groups.values()),
  };
}
