import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { isPoisonPill, NonRetryableError } from "../src/utils.js";

describe("isPoisonPill", () => {
  it("treats SyntaxError (JSON parse failures) as a poison pill", () => {
    assert.equal(isPoisonPill(new SyntaxError("Unexpected token")), true);
  });

  it("treats NonRetryableError as a poison pill", () => {
    assert.equal(isPoisonPill(new NonRetryableError("Invalid payload schema")), true);
  });

  it("treats duck-typed isNonRetryable objects as poison pills", () => {
    assert.equal(isPoisonPill({ isNonRetryable: true }), true);
  });

  it("does NOT treat a generic TypeError/RangeError as a poison pill", () => {
    // These are just as likely to come from a transient downstream failure
    // (an uninitialized DB client, a flaky dependency) as from a genuinely
    // malformed message. Handlers that want to treat them as permanent
    // should throw NonRetryableError, or supply a custom isPoisonPill.
    assert.equal(isPoisonPill(new TypeError("Cannot read properties of undefined")), false);
    assert.equal(isPoisonPill(new RangeError("out of range")), false);
  });

  it("treats ordinary errors and non-error values as transient", () => {
    assert.equal(isPoisonPill(new Error("Connection timeout to Redis")), false);
    assert.equal(isPoisonPill("just a string"), false);
    assert.equal(isPoisonPill(null), false);
    assert.equal(isPoisonPill(undefined), false);
    assert.equal(isPoisonPill({ some: "object" }), false);
  });
});

describe("NonRetryableError", () => {
  it("carries an optional code and details alongside the message", () => {
    const err = new NonRetryableError("bad payload", "SCHEMA_MISMATCH", { field: "orderId" });
    assert.equal(err.message, "bad payload");
    assert.equal(err.code, "SCHEMA_MISMATCH");
    assert.deepEqual(err.details, { field: "orderId" });
    assert.equal(err.isNonRetryable, true);
    assert.equal(err.name, "NonRetryableError");
  });
});
