import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { withJitter } from "../src/backoff.js";

describe("withJitter", () => {
  it("returns between half and all of the delay", () => {
    assert.equal(withJitter(400, () => 0), 200);
    assert.equal(withJitter(400, () => 0.5), 300);
    assert.equal(withJitter(400, () => 0.999), 399.8);
  });

  it("stays within bounds with real randomness", () => {
    for (let i = 0; i < 1000; i++) {
      const d = withJitter(1000);
      assert.ok(d >= 500 && d <= 1000, `got ${d}`);
    }
  });
});
