import { test } from "node:test";
import assert from "node:assert/strict";

import {
  stepsFromMints,
  pickRandomCatalogMint,
  createDeterministicRng,
  SOL_MINT,
} from "../cli_trader.js";

test("pickRandomCatalogMint uses deterministic selection", () => {
  const rngA = createDeterministicRng("catalog-seed");
  const rngB = createDeterministicRng("catalog-seed");
  const first = pickRandomCatalogMint({ rng: rngA });
  const second = pickRandomCatalogMint({ rng: rngB });
  assert.ok(first, "expected a catalog entry");
  assert.equal(first.mint, second.mint);
  assert.notEqual(first.mint, SOL_MINT);
});

test("stepsFromMints resolves RANDOM placeholder deterministically", () => {
  const seed = "steps-seed";
  const steps = stepsFromMints([SOL_MINT, "RANDOM", SOL_MINT], {
    rng: createDeterministicRng(seed),
    logRandomResolutions: false,
  });
  const repeat = stepsFromMints([SOL_MINT, "RANDOM", SOL_MINT], {
    rng: createDeterministicRng(seed),
    logRandomResolutions: false,
  });
  assert.equal(steps.length, 2);
  assert.deepEqual(
    steps.map((step) => step.to),
    repeat.map((step) => step.to)
  );

  const firstStep = steps[0];
  assert.ok(firstStep, "expected at least one step");
  assert.notEqual(firstStep.to, SOL_MINT);
  assert.ok(Array.isArray(firstStep.randomResolutions));
  const toResolution = firstStep.randomResolutions.find(
    (entry) => entry.role === "to"
  );
  assert.ok(toResolution, "expected resolution metadata for random to mint");
  assert.equal(toResolution.entry.mint, firstStep.to);

  const expected = pickRandomCatalogMint({
    rng: createDeterministicRng(seed),
    excludeMints: [SOL_MINT],
  });
  assert.equal(firstStep.to, expected.mint);
});
