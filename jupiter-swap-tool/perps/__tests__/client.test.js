import test from "node:test";
import assert from "node:assert/strict";
import { Keypair, PublicKey } from "@solana/web3.js";

import { createPerpsProvider } from "../client.js";

const { publicKey: generatedPublicKey } = Keypair.generate();

const base58PublicKey = generatedPublicKey.toBase58();

test("createPerpsProvider wraps string public key inputs", () => {
  const provider = createPerpsProvider({ publicKey: base58PublicKey });
  assert.ok(provider.wallet.publicKey instanceof PublicKey);
  assert.equal(provider.wallet.publicKey.toBase58(), base58PublicKey);
});
