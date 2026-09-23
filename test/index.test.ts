import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { KafkaService } from "../src/index.js";
import {
  makeFakeKafka,
  fakeTopicAlreadyExistsError,
  fakeAggregateTopicAlreadyExistsError,
  fakeAggregateMixedError,
} from "./helpers/fake-kafka.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeService(
  overrides?: Parameters<typeof makeFakeKafka>[0],
  serviceOverrides?: Partial<{ healthCheckTtlMs: number }>
) {
  const fake = makeFakeKafka(overrides);
  const service = new KafkaService({ clientId: "svc", kafka: fake.kafka, ...serviceOverrides });
  return { fake, service };
}

describe("KafkaService", () => {
  it("publishes via the underlying producer", async () => {
    const { fake, service } = makeService();
    await service.publish({ topic: "orders.created", messages: [{ value: { a: 1 } }] });
    assert.equal(fake.producer.sendCalls.length, 1);
  });

  it("consume() registers the consumer only after start() succeeds", async () => {
    const { fake, service } = makeService();
    const consumer = await service.consume({
      groupId: "g1",
      topic: "orders.created",
      handler: async () => {},
    });
    assert.ok(consumer);
    assert.equal(fake.consumers.length, 1);
  });

  it("consume() does not register a consumer that fails to start", async () => {
    const kafka = {
      producer: () => ({ connect: async () => {}, send: async () => {}, disconnect: async () => {} }),
      admin: () => ({
        connect: async () => {},
        disconnect: async () => {},
        listTopics: async () => [],
        createTopics: async () => {},
        describeCluster: async () => ({ brokers: [] }),
      }),
      consumer: () => ({
        connect: async () => {},
        subscribe: async () => {
          throw new Error("subscribe failed");
        },
        run: async () => {},
        disconnect: async () => {},
      }),
    } as unknown as import("kafkajs").Kafka;
    const service = new KafkaService({ clientId: "svc", kafka });

    await assert.rejects(
      () => service.consume({ groupId: "g1", topic: "t", handler: async () => {} }),
      /subscribe failed/
    );
  });

  it("ensureTopics rejects non-array input", async () => {
    const { service } = makeService();
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => service.ensureTopics("not-an-array"), /requires an array/);
  });

  it("ensureTopics creates only missing topics with resolved partition counts", async () => {
    const { fake, service } = makeService({ existingTopics: ["orders.created"] });
    await service.ensureTopics(["orders.created", { topic: "orders.dlq", partitions: 4 }]);

    assert.equal(fake.admin.createdTopics.length, 1);
    assert.equal(fake.admin.createdTopics[0]?.topic, "orders.dlq");
  });

  it("ensureTopics warns when creating a topic with replicationFactor 1", async () => {
    const logs: Array<{ msg: string }> = [];
    const fake = makeFakeKafka();
    const service = new KafkaService({
      clientId: "svc",
      kafka: fake.kafka,
      logger: { info: () => {}, warn: (msg) => logs.push({ msg }), error: () => {} },
    });

    await service.ensureTopics(["orders.created"]);
    assert.ok(
      logs.some((l) => l.msg.includes("replicationFactor 1"))
    );
  });

  it("ensureTopics does not warn when replicationFactor is explicitly set above 1", async () => {
    const logs: Array<{ msg: string }> = [];
    const fake = makeFakeKafka();
    const service = new KafkaService({
      clientId: "svc",
      kafka: fake.kafka,
      logger: { info: () => {}, warn: (msg) => logs.push({ msg }), error: () => {} },
    });

    await service.ensureTopics([{ topic: "orders.created", replicationFactor: 3 }]);
    assert.ok(!logs.some((l) => l.msg.includes("replicationFactor 1")));
  });

  it("ensureTopics is a no-op when every topic already exists", async () => {
    const { fake, service } = makeService({ existingTopics: ["orders.created"] });
    await service.ensureTopics(["orders.created"]);
    assert.equal(fake.admin.createdTopics.length, 0);
  });

  it("ensureTopics tolerates a single-topic TOPIC_ALREADY_EXISTS error", async () => {
    const { service } = makeService({ failCreateTopicsWith: fakeTopicAlreadyExistsError() });
    await assert.doesNotReject(() => service.ensureTopics(["orders.created"]));
  });

  it("ensureTopics tolerates a kafkajs aggregate error where every sub-error is TOPIC_ALREADY_EXISTS", async () => {
    const { service } = makeService({ failCreateTopicsWith: fakeAggregateTopicAlreadyExistsError() });
    await assert.doesNotReject(() => service.ensureTopics(["orders.created", "orders.dlq"]));
  });

  it("ensureTopics rethrows a kafkajs aggregate error with a mix of failure types", async () => {
    const { service } = makeService({ failCreateTopicsWith: fakeAggregateMixedError() });
    await assert.rejects(() => service.ensureTopics(["orders.created", "orders.dlq"]));
  });

  it("ensureTopics rethrows any other createTopics failure", async () => {
    const { service } = makeService({ failCreateTopicsWith: new Error("broker unavailable") });
    await assert.rejects(() => service.ensureTopics(["orders.created"]), /broker unavailable/);
  });

  it("getClusterHealth reports healthy when brokers are present", async () => {
    const { service } = makeService({ clusterBrokers: [{ nodeId: 1 }, { nodeId: 2 }] });
    const health = await service.getClusterHealth();
    assert.deepEqual(health, { isHealthy: true, brokers: 2 });
  });

  it("getClusterHealth caches a successful result within the TTL", async () => {
    const { fake, service } = makeService(
      { clusterBrokers: [{ nodeId: 1 }] },
      { healthCheckTtlMs: 60_000 }
    );
    await service.getClusterHealth();
    await service.getClusterHealth();
    await service.getClusterHealth();
    assert.equal(fake.admin.describeClusterCalls, 1);
  });

  it("getClusterHealth re-queries after the TTL expires", async () => {
    const { fake, service } = makeService(
      { clusterBrokers: [{ nodeId: 1 }] },
      { healthCheckTtlMs: 1 }
    );
    await service.getClusterHealth();
    await sleep(10);
    await service.getClusterHealth();
    assert.equal(fake.admin.describeClusterCalls, 2);
  });

  it("getClusterHealth does not cache a failed check, so it retries immediately", async () => {
    const { fake, service } = makeService(
      { failDescribeCluster: true, clusterBrokers: [{ nodeId: 1 }] },
      { healthCheckTtlMs: 60_000 }
    );
    await service.getClusterHealth();
    fake.admin.failDescribeCluster = false;
    const second = await service.getClusterHealth();
    assert.deepEqual(second, { isHealthy: true, brokers: 1 });
  });

  it("getClusterHealth recovers after admin.connect() itself fails (resets adminConnectPromise)", async () => {
    const { fake, service } = makeService({ failConnect: true, clusterBrokers: [{ nodeId: 1 }] });

    const first = await service.getClusterHealth();
    assert.deepEqual(first, { isHealthy: false, brokers: 0 });

    fake.admin.failConnect = false;
    const second = await service.getClusterHealth();
    assert.deepEqual(second, { isHealthy: true, brokers: 1 });
  });

  it("getClusterHealth reports unhealthy and logs on failure", async () => {
    const logs: Array<{ level: string; msg: string }> = [];
    const fake = makeFakeKafka({ failDescribeCluster: true });
    const service = new KafkaService({
      clientId: "svc",
      kafka: fake.kafka,
      logger: {
        info: () => {},
        warn: (msg) => logs.push({ level: "warn", msg }),
        error: () => {},
      },
    });

    const health = await service.getClusterHealth();
    assert.deepEqual(health, { isHealthy: false, brokers: 0 });
    assert.ok(logs.some((l) => l.msg === "Cluster health check failed"));
  });

  it("disconnect() tears down producer, admin (if connected) and consumers", async () => {
    const { fake, service } = makeService();
    await service.getClusterHealth(); // connects admin
    await service.consume({ groupId: "g1", topic: "t", handler: async () => {} });
    await service.publish({ topic: "t", messages: [{ value: 1 }] });

    await service.disconnect();
    assert.equal(fake.producer.disconnectCalls, 1);
  });

  it("Producer.disconnect() logs (via its own logger) instead of throwing when the underlying disconnect fails", async () => {
    const logs: Array<{ msg: string }> = [];
    const fake = makeFakeKafka();
    const service = new KafkaService({
      clientId: "svc",
      kafka: fake.kafka,
      logger: { info: () => {}, warn: (msg) => logs.push({ msg }), error: () => {} },
    });
    await service.publish({ topic: "t", messages: [{ value: 1 }] }); // connects the producer
    fake.producer.failDisconnect = true;

    await assert.doesNotReject(() => service.disconnect());
    assert.ok(logs.some((l) => l.msg === "Error while disconnecting Kafka producer"));
  });

  it("disconnect() still tears down the producer/admin and logs when a consumer fails to stop", async () => {
    const logs: Array<{ msg: string }> = [];
    const fake = makeFakeKafka();
    const service = new KafkaService({
      clientId: "svc",
      kafka: fake.kafka,
      logger: { info: () => {}, warn: (msg) => logs.push({ msg }), error: () => {} },
    });
    await service.getClusterHealth();
    await service.consume({ groupId: "g1", topic: "t", handler: async () => {} });
    fake.consumers[0]!.failDisconnect = true;

    await assert.doesNotReject(() => service.disconnect());
    assert.equal(fake.producer.disconnectCalls, 0); // never connected, but disconnect() still resolved
    assert.ok(logs.some((l) => l.msg === "Error while disconnecting a Kafka resource"));
  });

  it("disconnect() is safe when admin was never connected", async () => {
    const { service } = makeService();
    await assert.doesNotReject(() => service.disconnect());
  });

  it("rejects an empty or missing clientId", () => {
    // @ts-expect-error verifying runtime check
    assert.throws(() => new KafkaService(), /requires a non-empty 'clientId'/);
    assert.throws(() => new KafkaService({ clientId: "   " }), /requires a non-empty 'clientId'/);
  });
});
