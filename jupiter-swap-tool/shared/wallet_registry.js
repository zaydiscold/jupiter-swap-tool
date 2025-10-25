/**
 * Dynamic Wallet Registry with Hierarchical Master-Slave Architecture
 *
 * Features:
 * - Auto-renumbering wallets when files are added/removed
 * - Hierarchical master-slave groups (groups of 5)
 * - Master-slave relationship tracking
 * - Number-based wallet resolution
 */

const fs = require('fs');
const path = require('path');

const MANIFEST_PATH = path.join(__dirname, '..', 'wallets_manifest.json');
const GROUP_SIZE = 5;

/**
 * Load the wallet manifest from disk
 * @returns {Object} manifest
 */
function loadManifest() {
  if (!fs.existsSync(MANIFEST_PATH)) {
    return createEmptyManifest();
  }

  try {
    const data = fs.readFileSync(MANIFEST_PATH, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    console.error('Failed to load wallet manifest, creating new one:', err.message);
    return createEmptyManifest();
  }
}

/**
 * Create an empty manifest structure
 * @returns {Object} empty manifest
 */
function createEmptyManifest() {
  return {
    version: '1.0',
    lastSync: new Date().toISOString(),
    wallets: []
  };
}

/**
 * Save the manifest to disk
 * @param {Object} manifest
 */
function saveManifest(manifest) {
  manifest.lastSync = new Date().toISOString();
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2), 'utf8');
}

/**
 * Calculate which group a wallet number belongs to
 * Groups are 1-5, 6-10, 11-15, etc.
 * @param {number} walletNum
 * @returns {number} group number (1-based)
 */
function getGroupNumber(walletNum) {
  return Math.floor((walletNum - 1) / GROUP_SIZE) + 1;
}

/**
 * Calculate the master wallet number for a given wallet
 * @param {number} walletNum
 * @returns {number|null} master wallet number, or null if this is master-master
 */
function getMasterForWallet(walletNum) {
  if (walletNum === 1) {
    return null; // Wallet #1 is the master-master
  }

  const groupNum = getGroupNumber(walletNum);
  const masterNum = (groupNum - 1) * GROUP_SIZE + 1;
  return masterNum;
}

/**
 * Calculate the role of a wallet
 * @param {number} walletNum
 * @returns {string} 'master-master' | 'master' | 'slave'
 */
function getWalletRole(walletNum) {
  if (walletNum === 1) {
    return 'master-master';
  }

  // Masters are at positions 1, 6, 11, 16, 21...
  // Which is: (groupNum - 1) * GROUP_SIZE + 1
  // Or equivalently: (walletNum - 1) % GROUP_SIZE === 0
  if ((walletNum - 1) % GROUP_SIZE === 0) {
    return 'master';
  }

  return 'slave';
}

/**
 * Sync wallets from filesystem and rebuild manifest with auto-renumbering
 * @param {Array} wallets - Array of wallet objects from listWallets() with {name, kp, birthMs}
 * @returns {Object} updated manifest
 */
function syncWalletsFromFilesystem(wallets) {
  const manifest = loadManifest();

  // Sort wallets by birth time, then by name
  const sortedWallets = [...wallets].sort((a, b) => {
    if (a.birthMs !== b.birthMs) {
      return a.birthMs - b.birthMs;
    }
    return a.name.localeCompare(b.name);
  });

  // Rebuild manifest with sequential numbering
  manifest.wallets = sortedWallets.map((wallet, index) => {
    const walletNum = index + 1;
    const master = getMasterForWallet(walletNum);
    const role = getWalletRole(walletNum);
    const group = getGroupNumber(walletNum);

    return {
      number: walletNum,
      filename: wallet.name,
      role: role,
      master: master,
      group: group
    };
  });

  saveManifest(manifest);
  return manifest;
}

/**
 * Get wallet entry by number
 * @param {number} walletNum
 * @returns {Object|null} wallet entry from manifest
 */
function getWalletByNumber(walletNum) {
  const manifest = loadManifest();
  return manifest.wallets.find(w => w.number === walletNum) || null;
}

/**
 * Get wallet entry by filename
 * @param {string} filename
 * @returns {Object|null} wallet entry from manifest
 */
function getWalletByFilename(filename) {
  const manifest = loadManifest();
  return manifest.wallets.find(w => w.filename === filename) || null;
}

/**
 * Get all slave wallets for a given master
 * @param {number} masterNum
 * @returns {Array} array of slave wallet entries
 */
function getSlaves(masterNum) {
  const manifest = loadManifest();
  return manifest.wallets.filter(w => w.master === masterNum);
}

/**
 * Get all master wallet numbers (including master-master)
 * @returns {Array<number>} array of master wallet numbers
 */
function getAllMasters() {
  const manifest = loadManifest();
  return manifest.wallets
    .filter(w => w.role === 'master' || w.role === 'master-master')
    .map(w => w.number);
}

/**
 * Get all non-master-master masters (excludes wallet #1)
 * @returns {Array<number>} array of master wallet numbers (6, 11, 16, ...)
 */
function getGroupMasters() {
  const manifest = loadManifest();
  return manifest.wallets
    .filter(w => w.role === 'master')
    .map(w => w.number);
}

/**
 * Get all wallets in a specific group
 * @param {number} groupNum
 * @returns {Array} array of wallet entries in the group
 */
function getWalletsInGroup(groupNum) {
  const manifest = loadManifest();
  return manifest.wallets.filter(w => w.group === groupNum);
}

/**
 * Get total number of wallets
 * @returns {number} count of wallets
 */
function getWalletCount() {
  const manifest = loadManifest();
  return manifest.wallets.length;
}

/**
 * Resolve a wallet identifier (number, filename, or special keyword)
 * @param {string|number} identifier - wallet number, filename, or keyword like 'master-master'
 * @returns {Object|null} wallet entry from manifest
 */
function resolveWalletIdentifier(identifier) {
  // If it's a number
  if (typeof identifier === 'number') {
    return getWalletByNumber(identifier);
  }

  // If it's a string that looks like a number
  const num = parseInt(identifier, 10);
  if (!isNaN(num) && num.toString() === identifier.toString()) {
    return getWalletByNumber(num);
  }

  // Special keywords
  if (identifier === 'master-master') {
    return getWalletByNumber(1);
  }

  // Otherwise treat as filename
  return getWalletByFilename(identifier);
}

/**
 * Get a summary of the wallet hierarchy
 * @returns {Object} hierarchy summary
 */
function getHierarchySummary() {
  const manifest = loadManifest();
  const groups = {};

  manifest.wallets.forEach(wallet => {
    if (!groups[wallet.group]) {
      groups[wallet.group] = {
        groupNumber: wallet.group,
        master: null,
        slaves: []
      };
    }

    if (wallet.role === 'master' || wallet.role === 'master-master') {
      groups[wallet.group].master = wallet;
    } else {
      groups[wallet.group].slaves.push(wallet);
    }
  });

  return {
    totalWallets: manifest.wallets.length,
    totalGroups: Object.keys(groups).length,
    groups: Object.values(groups)
  };
}

module.exports = {
  // Core functions
  loadManifest,
  saveManifest,
  syncWalletsFromFilesystem,

  // Lookup functions
  getWalletByNumber,
  getWalletByFilename,
  resolveWalletIdentifier,

  // Hierarchy functions
  getMasterForWallet,
  getWalletRole,
  getSlaves,
  getAllMasters,
  getGroupMasters,
  getWalletsInGroup,
  getGroupNumber,

  // Utility functions
  getWalletCount,
  getHierarchySummary,

  // Constants
  MANIFEST_PATH,
  GROUP_SIZE
};
