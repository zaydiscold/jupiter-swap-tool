import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import {
  instantiateCampaignForWallets,
  WSOL_MINT,
} from "../chains/solana/campaigns_runtime.js";

const TEST_WALLET = "TestWallet111111111111111111111111111111111";
const FALLBACK_MINT = "FallbackMint1111111111111111111111111111111";

test("long-chain campaigns fall back to alternating WSOL swaps when pool is sparse", (t) => {
  t.mock.method(fs, "readFileSync", () =>
    JSON.stringify([
      {
        mint: FALLBACK_MINT,
        symbol: "FALL",
        decimals: 6,
        tags: ["swappable"],
      },
    ])
  );
  const instantiated = instantiateCampaignForWallets({
    campaignKey: "meme-carousel",
    durationKey: "30m",
    walletPubkeys: [TEST_WALLET],
  });
  const plan = instantiated.plansByWallet.get(TEST_WALLET);
  assert.ok(plan, "plan should exist for wallet");
  const swapSteps = plan.schedule.filter((entry) => entry.kind === "swapHop");
  assert.ok(swapSteps.length > 0, "fallback should yield at least one swap hop");
  const selfSwap = swapSteps.find(
    (step) => step.logicalStep.inMint === step.logicalStep.outMint
  );
  assert.ok(!selfSwap, "fallback sequence should not include WSOL self-swaps");
  const firstSwap = swapSteps[0];
  assert.equal(firstSwap.logicalStep.inMint, WSOL_MINT);
  assert.equal(firstSwap.logicalStep.outMint, FALLBACK_MINT);
  const roundTrip = swapSteps.find(
    (step) =>
      step.logicalStep.inMint === FALLBACK_MINT && step.logicalStep.outMint === WSOL_MINT
  );
  assert.ok(roundTrip, "fallback sequence should include a return to WSOL");
});
