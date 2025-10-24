import test from "node:test";
import assert from "node:assert/strict";
import { PublicKey } from "@solana/web3.js";

import { createPerpsProvider } from "./client.js";

const ZERO_PUBKEY_STRING = "11111111111111111111111111111111";

test("createPerpsProvider wraps string public keys", () => {
  const provider = createPerpsProvider({ publicKey: ZERO_PUBKEY_STRING });
  assert.ok(provider.wallet.publicKey instanceof PublicKey, "wallet publicKey should be a PublicKey instance");
  assert.strictEqual(
    provider.wallet.publicKey.toBase58(),
    new PublicKey(ZERO_PUBKEY_STRING).toBase58(),
    "wallet publicKey should match the provided base58 string"
  );
});
