import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createLogger } from "../src/logger.js";

function captureStdout(fn: () => void): string[] {
  const written: string[] = [];
  const originalWrite = process.stdout.write.bind(process.stdout);
  // @ts-expect-error narrowing the signature for test capture is fine here
  process.stdout.write = (chunk: string) => {
    written.push(chunk);
    return true;
  };
  try {
    fn();
  } finally {
    process.stdout.write = originalWrite;
  }
  return written;
}

describe("createLogger", () => {
  it("returns the custom logger unchanged when one is provided", () => {
    const custom = { info: () => {}, warn: () => {}, error: () => {} };
    assert.equal(createLogger("svc", custom), custom);
  });

  it("defaults to the info threshold", () => {
    delete process.env.LOG_LEVEL;
    const written = captureStdout(() => {
      const logger = createLogger("svc");
      logger.debug?.("hidden");
      logger.info("shown");
    });
    assert.equal(written.length, 1);
    assert.ok(written[0]!.includes("shown"));
  });

  it("suppresses levels below LOG_LEVEL", () => {
    process.env.LOG_LEVEL = "warn";
    try {
      const written = captureStdout(() => {
        const logger = createLogger("svc");
        logger.info("hidden");
        logger.warn("shown");
        logger.error("also shown");
      });
      assert.equal(written.length, 2);
    } finally {
      delete process.env.LOG_LEVEL;
    }
  });

  it("LOG_LEVEL=silent suppresses everything", () => {
    process.env.LOG_LEVEL = "silent";
    try {
      const written = captureStdout(() => {
        const logger = createLogger("svc");
        logger.error("hidden too");
      });
      assert.equal(written.length, 0);
    } finally {
      delete process.env.LOG_LEVEL;
    }
  });

  it("falls back to info for an unrecognized LOG_LEVEL", () => {
    process.env.LOG_LEVEL = "not-a-level";
    try {
      const written = captureStdout(() => {
        const logger = createLogger("svc");
        logger.info("shown");
      });
      assert.equal(written.length, 1);
    } finally {
      delete process.env.LOG_LEVEL;
    }
  });

  it("emits well-formed JSON with service name and context merged in", () => {
    delete process.env.LOG_LEVEL;
    const written = captureStdout(() => {
      const logger = createLogger("my-service");
      logger.info("hello", { a: 1 });
    });
    const entry = JSON.parse(written[0]!);
    assert.equal(entry.service, "my-service");
    assert.equal(entry.message, "hello");
    assert.equal(entry.a, 1);
    assert.equal(entry.level, "info");
    assert.ok(entry.timestamp);
  });
});
