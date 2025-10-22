import assert from "node:assert/strict";
import { Keypair } from "@solana/web3.js";
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

function createStubConnection({ mintPubkey, mintOwner, existingAta }) {
  return {
    async getAccountInfo(pubkey) {
      if (pubkey.equals(mintPubkey)) {
        return {
          owner: mintOwner,
        };
      }
      if (existingAta && pubkey.equals(existingAta)) {
        return {
          owner: TOKEN_PROGRAM_ID,
        };
      }
      return null;
    },
  };
}

async function testLegacyMintFlow() {
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
    existingAta: ata,
  });
  const resolved = await resolveTokenProgramForMint(connection, mint);
  assert(resolved.equals(TOKEN_PROGRAM_ID));
  const created = await ensureAtaForMint(connection, wallet, mint, undefined, {
    label: "test",
  });
  assert.equal(created, false);
}

async function testToken2022MintFlow() {
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
    existingAta: ata,
  });
  const resolved = await resolveTokenProgramForMint(connection, mint);
  assert(resolved.equals(TOKEN_2022_PROGRAM_ID));
  const created = await ensureAtaForMint(connection, wallet, mint, undefined, {
    label: "test",
  });
  assert.equal(created, false);
}

async function testUnsupportedMintOwner() {
  const wallet = { kp: Keypair.generate() };
  const mint = Keypair.generate().publicKey;
  const connection = createStubConnection({
    mintPubkey: mint,
    mintOwner: Keypair.generate().publicKey,
  });
  await assert.rejects(() => resolveTokenProgramForMint(connection, mint));
  await assert.rejects(() =>
    ensureAtaForMint(connection, wallet, mint, undefined, { label: "test" })
  );
}

async function main() {
  await testLegacyMintFlow();
  await testToken2022MintFlow();
  await testUnsupportedMintOwner();
  console.log("ensureAtaForMint token program detection tests passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
