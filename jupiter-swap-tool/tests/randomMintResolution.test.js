import assert from "node:assert/strict";
import { test } from "node:test";

import {
  resolveRandomCatalogMint,
  stepsFromMints,
  snapshotTokenCatalog,
} from "../cli_trader.js";

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
