import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { logLevel } from "kafkajs";
import { createKafkaClient, getBrokerList } from "../src/client.js";

describe("getBrokerList", () => {
  const originalEnv = process.env.KAFKA_BROKERS;

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.KAFKA_BROKERS;
    else process.env.KAFKA_BROKERS = originalEnv;
  });

  it("uses explicit brokers when provided", () => {
    assert.deepEqual(getBrokerList(["a:9092", "b:9092"]), ["a:9092", "b:9092"]);
  });

  it("falls through to KAFKA_BROKERS when configuredBrokers is empty", () => {
    process.env.KAFKA_BROKERS = "a:9092, b:9092";
    assert.deepEqual(getBrokerList([]), ["a:9092", "b:9092"]);
  });

  it("falls back to localhost:9092 when nothing is configured", () => {
    delete process.env.KAFKA_BROKERS;
    assert.deepEqual(getBrokerList(), ["localhost:9092"]);
  });
});

describe("createKafkaClient", () => {
  it("throws for an empty or blank clientId", () => {
    assert.throws(() => createKafkaClient(""), /clientId is required/);
    assert.throws(() => createKafkaClient("   "), /clientId is required/);
  });

  it("builds a client and routes kafkajs WARN/ERROR logs through the injected logger", () => {
    const logs: Array<{ level: string; msg: string }> = [];
    const kafka = createKafkaClient("test-client", {
      brokers: ["broker:9092"],
      logger: {
        info: () => {},
        warn: (msg) => logs.push({ level: "warn", msg }),
        error: (msg) => logs.push({ level: "error", msg }),
      },
    });

    const internalLogger = kafka.logger();
    internalLogger.warn("a warning");
    internalLogger.error("an error");
    internalLogger.info("should be ignored by the bridge");

    assert.ok(logs.some((l) => l.level === "warn" && l.msg === "a warning"));
    assert.ok(logs.some((l) => l.level === "error" && l.msg === "an error"));
    assert.equal(logs.length, 2);
  });

  it("accepts raw kafkajs config overrides", () => {
    const kafka = createKafkaClient("test-client", {
      config: { logLevel: logLevel.NOTHING },
    });
    assert.ok(kafka);
  });
});
