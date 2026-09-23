import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  KafkaService,
  Producer,
  Consumer,
  getTopicConfig,
  NonRetryableError,
  isPoisonPill,
  createKafkaClient,
  getBrokerList,
} from "../src/index.js";

describe("kafka-kit package", () => {
  it("should configure partitions with sensible defaults and support overrides", () => {
    const defaultConfig = getTopicConfig("orders.created");
    assert.equal(defaultConfig.partitions, 16);
    assert.equal(defaultConfig.replicationFactor, 1);

    const customConfig = getTopicConfig("payments.charge", { partitions: 32, replicationFactor: 3 });
    assert.equal(customConfig.partitions, 32);
    assert.equal(customConfig.replicationFactor, 3);
  });

  it("should identify poison pills accurately", () => {
    assert.equal(isPoisonPill(new SyntaxError("Unexpected token")), true);
    assert.equal(isPoisonPill(new NonRetryableError("Invalid payload schema")), true);
    assert.equal(isPoisonPill({ isNonRetryable: true }), true);
    assert.equal(isPoisonPill(new Error("Connection timeout to Redis")), false);
  });

  it("should parse default broker list from environment or fallback", () => {
    const brokers = getBrokerList();
    assert.equal(Array.isArray(brokers), true);
    assert.ok(brokers.length > 0);

    const explicitBrokers = getBrokerList(["broker1:9092", "broker2:9092"]);
    assert.deepEqual(explicitBrokers, ["broker1:9092", "broker2:9092"]);
  });

  it("should throw error if clientId is omitted or empty", () => {
    // @ts-expect-error verifying runtime check
    assert.throws(() => new KafkaService(), /requires a non-empty 'clientId'/);
    assert.throws(() => new KafkaService({ clientId: "" }), /requires a non-empty 'clientId'/);
    assert.throws(() => new KafkaService({ clientId: "   " }), /requires a non-empty 'clientId'/);
    assert.throws(() => createKafkaClient(""), /clientId is required/);
  });

  it("should validate producer publish arguments strictly", async () => {
    const kafka = new KafkaService({ clientId: "producer-validation" });

    // @ts-expect-error verifying runtime check
    await assert.rejects(() => kafka.publish({}), /must include a non-empty 'topic'/);
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => kafka.publish({ topic: "" }), /must include a non-empty 'topic'/);
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => kafka.publish({ topic: "test", messages: [] }), /must include a non-empty 'messages'/);
  });

  it("should validate consumer arguments strictly", async () => {
    const client = createKafkaClient("consumer-validation");
    // @ts-expect-error verifying runtime check
    assert.throws(() => new Consumer(client, { topic: "test" }), /requires a non-empty 'groupId'/);
    // @ts-expect-error verifying runtime check
    assert.throws(() => new Consumer(client, { groupId: "test" }), /requires a non-empty 'topic'/);

    const validConsumer = new Consumer(client, { groupId: "grp", topic: "top" });
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => validConsumer.start(null), /requires a valid handler function/);
  });

  it("should validate ensureTopics argument strictly", async () => {
    const kafka = new KafkaService({ clientId: "admin-validation" });
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => kafka.ensureTopics("not-an-array"), /requires an array/);
  });

  it("should instantiate unified KafkaService when clientId is provided", () => {
    const kafka = new KafkaService({ clientId: "test-service" });
    assert.ok(kafka instanceof KafkaService);
    assert.equal(typeof kafka.publish, "function");
    assert.equal(typeof kafka.consume, "function");
    assert.equal(typeof kafka.ensureTopics, "function");
    assert.equal(typeof kafka.getClusterHealth, "function");
    assert.equal(typeof kafka.disconnect, "function");
  });

  it("should support custom logger injection", () => {
    const logs: string[] = [];
    const customLogger = {
      info: (msg: string) => logs.push(msg),
      warn: (msg: string) => logs.push(msg),
      error: (msg: string) => logs.push(msg),
    };

    const kafka = new KafkaService({
      clientId: "logging-service",
      logger: customLogger,
    });

    assert.ok(kafka);
    assert.ok(logs.some((l) => l.includes("Initializing shared Kafka client")));
  });
});
