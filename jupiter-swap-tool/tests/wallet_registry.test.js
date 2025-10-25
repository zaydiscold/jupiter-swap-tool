import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import * as walletRegistry from "../shared/wallet_registry.js";

test("syncWalletsFromFilesystem assigns numbers and hierarchy", () => {
  const manifestPath = walletRegistry.MANIFEST_PATH;
  const hadManifest = fs.existsSync(manifestPath);
  const originalContent = hadManifest ? fs.readFileSync(manifestPath, "utf8") : null;

  try {
    const wallets = [
      { name: "crew_2.json", kp: { publicKey: {} }, birthMs: 200 },
      { name: "crew_1.json", kp: { publicKey: {} }, birthMs: 100 },
      { name: "crew_3.json", kp: { publicKey: {} }, birthMs: 300 },
      { name: "crew_4.json", kp: { publicKey: {} }, birthMs: 400 },
      { name: "crew_5.json", kp: { publicKey: {} }, birthMs: 500 },
      { name: "crew_6.json", kp: { publicKey: {} }, birthMs: 600 },
    ];

    const manifest = walletRegistry.syncWalletsFromFilesystem(wallets);

    assert.ok(manifest);
    assert.equal(manifest.wallets.length, wallets.length);
    assert.deepEqual(
      manifest.wallets.map((entry) => entry.number),
      [1, 2, 3, 4, 5, 6]
    );

    const roles = manifest.wallets.map((entry) => entry.role);
    assert.equal(roles[0], "master-master");
    assert.equal(roles[1], "slave");
    assert.equal(roles[5], "master");

    const masters = manifest.wallets.map((entry) => entry.master);
    assert.equal(masters[0], null);
    assert.equal(masters[1], 1);
    assert.equal(masters[5], 6);

    const persisted = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    assert.deepEqual(
      persisted.wallets.map(({ number, filename }) => [number, filename]),
      [
        [1, "crew_1.json"],
        [2, "crew_2.json"],
        [3, "crew_3.json"],
        [4, "crew_4.json"],
        [5, "crew_5.json"],
        [6, "crew_6.json"],
      ]
    );
  } finally {
    if (hadManifest) {
      fs.writeFileSync(manifestPath, originalContent, "utf8");
    } else if (fs.existsSync(manifestPath)) {
      fs.unlinkSync(manifestPath);
    }
  }
});

test("group helpers compute expected hierarchy", () => {
  assert.equal(walletRegistry.getGroupNumber(1), 1);
  assert.equal(walletRegistry.getGroupNumber(5), 1);
  assert.equal(walletRegistry.getGroupNumber(6), 2);
  assert.equal(walletRegistry.getGroupNumber(10), 2);

  assert.equal(walletRegistry.getWalletRole(1), "master-master");
  assert.equal(walletRegistry.getWalletRole(2), "slave");
  assert.equal(walletRegistry.getWalletRole(6), "master");

  assert.equal(walletRegistry.getMasterForWallet(1), null);
  assert.equal(walletRegistry.getMasterForWallet(2), 1);
  assert.equal(walletRegistry.getMasterForWallet(6), 6);
  assert.equal(walletRegistry.getMasterForWallet(7), 6);
});

test("resolveWalletIdentifier handles numbers, filenames, and master-master", () => {
  const manifestPath = walletRegistry.MANIFEST_PATH;
  const hadManifest = fs.existsSync(manifestPath);
  const originalContent = hadManifest ? fs.readFileSync(manifestPath, "utf8") : null;

  const customManifest = {
    version: "1.0",
    lastSync: new Date().toISOString(),
    wallets: [
      { number: 1, filename: "crew_1.json", role: "master-master", master: null, group: 1 },
      { number: 2, filename: "crew_2.json", role: "slave", master: 1, group: 1 },
      { number: 6, filename: "crew_6.json", role: "master", master: 1, group: 2 },
    ],
  };

  try {
    fs.writeFileSync(manifestPath, JSON.stringify(customManifest, null, 2), "utf8");

    const entryByNumber = walletRegistry.resolveWalletIdentifier(2);
    assert.equal(entryByNumber?.filename, "crew_2.json");

    const entryByNumericString = walletRegistry.resolveWalletIdentifier("6");
    assert.equal(entryByNumericString?.filename, "crew_6.json");

    const entryByFilename = walletRegistry.resolveWalletIdentifier("crew_1.json");
    assert.equal(entryByFilename?.number, 1);

    const entryMasterMaster = walletRegistry.resolveWalletIdentifier("master-master");
    assert.equal(entryMasterMaster?.number, 1);

    const missing = walletRegistry.resolveWalletIdentifier("unknown.json");
    assert.equal(missing, null);
  } finally {
    if (hadManifest) {
      fs.writeFileSync(manifestPath, originalContent, "utf8");
    } else if (fs.existsSync(manifestPath)) {
      fs.unlinkSync(manifestPath);
    }
  }
});
