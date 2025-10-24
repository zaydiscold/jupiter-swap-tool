import assert from "node:assert/strict";
import { test } from "node:test";

import {
  resolveRandomCatalogMint,
  stepsFromMints,
  snapshotTokenCatalog,
} from "../cli_trader.js";
import {
  resolveRandomizedStep,
  WSOL_MINT,
} from "../chains/solana/campaigns_runtime.js";

const SOL_MINT = "So11111111111111111111111111111111111111112";

test("RANDOM sentinels resolve to catalog mints without immediate repeats", () => {
  const catalogSnapshot = snapshotTokenCatalog();
  assert(Array.isArray(catalogSnapshot));
  const knownMints = new Set(
    catalogSnapshot
      .map((entry) => (entry?.mint ? String(entry.mint) : null))
      .filter(Boolean)
  );
  assert(knownMints.size > 1, "expected multiple non-SOL catalog entries");

  const deterministicRng = () => 0;
  const first = resolveRandomCatalogMint({ rng: deterministicRng });
  assert.equal(typeof first, "string");
  assert.notEqual(first, SOL_MINT);
  assert(knownMints.has(first));

  const second = resolveRandomCatalogMint({
    rng: deterministicRng,
    exclude: new Set([first]),
  });
  assert.equal(typeof second, "string");
  assert.notEqual(second, SOL_MINT);
  assert.notEqual(second, first);
  assert(knownMints.has(second));

  const sequence = [0, 0, 0, 0];
  let callIndex = 0;
  const sequencedRng = () => {
    const value = sequence[callIndex] ?? 0;
    callIndex += 1;
    return value;
  };
  const steps = stepsFromMints(["RANDOM", "RANDOM", SOL_MINT], {
    rng: sequencedRng,
  });
  assert.equal(steps.length, 2);
  const [firstStep, secondStep] = steps;
  assert(knownMints.has(firstStep.from));
  assert(knownMints.has(firstStep.to));
  assert.notEqual(firstStep.from, firstStep.to);
  assert.equal(secondStep.from, firstStep.to);
  assert.equal(secondStep.to, SOL_MINT);
});

test("session-to-sol randomization rehydrates SPL source metadata", () => {
  const RANDOM_MINT = "1r4nd0mMint1111111111111111111111111111111";
  const sessionKey = "test-session";
  const randomSessions = new Map([
    [
      sessionKey,
      {
        inMint: WSOL_MINT,
        outMint: RANDOM_MINT,
        sourceBalance: { kind: "sol", lamports: 123_000_000 },
      },
    ],
  ]);

  const resolved = resolveRandomizedStep(
    {
      inMint: RANDOM_MINT,
      outMint: SOL_MINT,
      randomization: { mode: "session-to-sol", sessionKey },
    },
    () => 0,
    { sessionState: randomSessions }
  );

  assert.equal(resolved?.inMint, RANDOM_MINT);
  assert.equal(resolved?.outMint, SOL_MINT);
  assert(resolved?.sourceBalance);
  assert.equal(resolved.sourceBalance.kind, "spl");
  assert.equal(resolved.sourceBalance.mint, RANDOM_MINT);
  const sessionRecord = randomSessions.get(sessionKey);
  assert(sessionRecord);
  assert.equal(sessionRecord.sourceBalance?.kind, "spl");
  assert.equal(sessionRecord.sourceBalance?.mint, RANDOM_MINT);
});
