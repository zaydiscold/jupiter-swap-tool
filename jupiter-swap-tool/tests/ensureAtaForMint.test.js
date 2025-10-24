import assert from "node:assert/strict";
import { test } from "node:test";
import { Keypair, Transaction } from "@solana/web3.js";
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
  getAssociatedTokenAddress,
} from "@solana/spl-token";
import {
  ensureAtaForMint,
  resolveTokenProgramForMint,
} from "../cli_trader.js";

function createStubConnection({ mintPubkey, mintOwner, existingAccounts = new Map() }) {
  const accounts = new Map(existingAccounts);
  return {
    async getAccountInfo(pubkey) {
      if (pubkey.equals(mintPubkey)) {
        return {
          owner: mintOwner,
        };
      }
      return accounts.get(pubkey.toBase58()) || null;
    },
  };
}

test("resolveTokenProgramForMint handles legacy mints", async () => {
  const wallet = { kp: Keypair.generate() };
  const mint = Keypair.generate().publicKey;
  const ata = await getAssociatedTokenAddress(
    mint,
    wallet.kp.publicKey,
    false,
    TOKEN_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const connection = createStubConnection({
    mintPubkey: mint,
    mintOwner: TOKEN_PROGRAM_ID,
    existingAccounts: new Map([[ata.toBase58(), { owner: TOKEN_PROGRAM_ID }]]),
  });
  const resolved = await resolveTokenProgramForMint(connection, mint);
  assert(resolved.equals(TOKEN_PROGRAM_ID));
  const created = await ensureAtaForMint(connection, wallet, mint, undefined, {
    label: "test",
  });
  assert.equal(created, false);
});

test("resolveTokenProgramForMint handles Token-2022 mints", async () => {
  const wallet = { kp: Keypair.generate() };
  const mint = Keypair.generate().publicKey;
  const ata = await getAssociatedTokenAddress(
    mint,
    wallet.kp.publicKey,
    false,
    TOKEN_2022_PROGRAM_ID,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const connection = createStubConnection({
    mintPubkey: mint,
    mintOwner: TOKEN_2022_PROGRAM_ID,
    existingAccounts: new Map([[ata.toBase58(), { owner: TOKEN_PROGRAM_ID }]]),
  });
  const resolved = await resolveTokenProgramForMint(connection, mint);
  assert(resolved.equals(TOKEN_2022_PROGRAM_ID));
  const created = await ensureAtaForMint(connection, wallet, mint, undefined, {
    label: "test",
  });
  assert.equal(created, false);
});

test("ensureAtaForMint throws for unsupported mint owners", async () => {
  const wallet = { kp: Keypair.generate() };
  const mint = Keypair.generate().publicKey;
  const connection = createStubConnection({
    mintPubkey: mint,
    mintOwner: Keypair.generate().publicKey,
  });
  await assert.rejects(() => resolveTokenProgramForMint(connection, mint));
  await assert.rejects(() => ensureAtaForMint(connection, wallet, mint, undefined, { label: "test" }));
});

test("ensureAtaForMint creates Token-2022 ATAs when missing", async () => {
  const wallet = { kp: Keypair.generate(), name: "test" };
  const mint = Keypair.generate().publicKey;
  const expectedProgram = TOKEN_2022_PROGRAM_ID;
  const expectedAta = await getAssociatedTokenAddress(
    mint,
    wallet.kp.publicKey,
    false,
    expectedProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  const fakeBlockhash = Keypair.generate().publicKey.toBase58();
  let sendCalled = false;
  const connection = {
    async getAccountInfo(pubkey) {
      if (pubkey.equals(mint)) {
        return { owner: expectedProgram };
      }
      return null;
    },
    async getLatestBlockhash() {
      return { blockhash: fakeBlockhash };
    },
    async sendRawTransaction(serialized) {
      const tx = Transaction.from(serialized);
      assert.equal(tx.recentBlockhash, fakeBlockhash);
      assert(tx.feePayer.equals(wallet.kp.publicKey));
      assert(tx.instructions.length >= 1);
      const ataIx = tx.instructions[0];
      assert(ataIx.programId.equals(ASSOCIATED_TOKEN_PROGRAM_ID));
      const derivedAta = ataIx.keys[1]?.pubkey;
      assert(derivedAta?.equals(expectedAta));
      sendCalled = true;
      return "test-signature";
    },
    async confirmTransaction(signature, commitment) {
      assert.equal(signature, "test-signature");
      assert.equal(commitment, "confirmed");
      return { value: {} };
    },
  };

  const created = await ensureAtaForMint(connection, wallet, mint, undefined, {
    label: "test",
  });
  assert.equal(created, true);
  assert.equal(sendCalled, true);
});
