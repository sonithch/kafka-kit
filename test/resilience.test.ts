import { describe, it } from "node:test";
import assert from "node:assert/strict";
import type { KafkaMessage } from "kafkajs";
import { KafkaService, NonRetryableError, retryTopicName } from "../src/index.js";
import { Consumer } from "../src/consumer.js";
import { makeFakeKafka } from "./helpers/fake-kafka.js";

type SentBatch = {
  topic: string;
  messages: Array<{ value: unknown; headers?: Record<string, string> }>;
};

function makeService() {
  const fake = makeFakeKafka();
  const service = new KafkaService({ clientId: "svc", kafka: fake.kafka });
  const sent = () => fake.producer.sendCalls as SentBatch[];
  return { fake, service, sent };
}

function msg(overrides?: Partial<KafkaMessage>): Partial<KafkaMessage> {
  return { value: Buffer.from("{}"), offset: "7", timestamp: String(Date.now()), ...overrides };
}

describe("Consumer startup failure", () => {
  it("disconnects the consumer when subscribe fails after connect", async () => {
    const fake = makeFakeKafka();
    const consumer = new Consumer(fake.kafka, { groupId: "g", topic: "orders" });
    fake.consumers[0]!.failSubscribe = true;

    await assert.rejects(() => consumer.start(async () => {}), /subscribe failed/);
    assert.equal(fake.consumers[0]!.disconnectCalls, 1);
  });

  it("disconnects every retry-stage consumer when startup fails", async () => {
    const fake = makeFakeKafka();
    const consumer = new Consumer(fake.kafka, {
      groupId: "g",
      topic: "orders",
      retry: { topicDelaysMs: [10, 20] },
    });
    fake.consumers[2]!.failSubscribe = true;

    await assert.rejects(() => consumer.start(async () => {}));
    assert.deepEqual(
      fake.consumers.map((c) => c.disconnectCalls),
      [1, 1, 1]
    );
  });
});

describe("KafkaService.disconnect order", () => {
  it("stops consumers before disconnecting the producer", async () => {
    const { fake, service } = makeService();
    const order: string[] = [];
    await service.publish({ topic: "t", messages: [{ value: 1 }] });
    await service.consume({ groupId: "g", topic: "t", handler: async () => {} });

    fake.consumers[0]!.onDisconnect = () => order.push("consumer");
    const rawDisconnect = fake.producer.disconnect;
    fake.producer.disconnect = async () => {
      order.push("producer");
      await rawDisconnect();
    };

    await service.disconnect();
    assert.deepEqual(order, ["consumer", "producer"]);
  });
});

describe("Consumer bounded retries", () => {
  it("retries a transient failure in place and succeeds", async () => {
    const { fake, service, sent } = makeService();
    let calls = 0;
    await service.consume({
      groupId: "g",
      topic: "orders",
      dlqTopic: "orders.dlq",
      retry: { attempts: 3, backoffMs: 0 },
      handler: async () => {
        if (++calls < 3) throw new Error("db timeout");
      },
    });

    await fake.consumers[0]!.deliver({ message: msg() });
    assert.equal(calls, 3);
    assert.equal(sent().length, 0);
  });

  it("forwards to the DLQ once in-place attempts are exhausted", async () => {
    const { fake, service, sent } = makeService();
    let calls = 0;
    await service.consume({
      groupId: "g",
      topic: "orders",
      dlqTopic: "orders.dlq",
      retry: { attempts: 2, backoffMs: 0 },
      handler: async () => {
        calls++;
        throw new Error("db timeout");
      },
    });

    await fake.consumers[0]!.deliver({ message: msg() });
    assert.equal(calls, 2);
    assert.equal(sent()[0]!.topic, "orders.dlq");
    assert.equal(sent()[0]!.messages[0]!.headers?.["x-retry-count"], "0");
  });

  it("does not retry a poison pill in place", async () => {
    const { fake, service, sent } = makeService();
    let calls = 0;
    await service.consume({
      groupId: "g",
      topic: "orders",
      dlqTopic: "orders.dlq",
      retry: { attempts: 5, backoffMs: 0 },
      handler: async () => {
        calls++;
        throw new NonRetryableError("bad");
      },
    });

    await fake.consumers[0]!.deliver({ message: msg() });
    assert.equal(calls, 1);
    assert.equal(sent()[0]!.topic, "orders.dlq");
  });

  it("still rethrows after retries when no dlqTopic is set", async () => {
    const { fake, service } = makeService();
    await service.consume({
      groupId: "g",
      topic: "orders",
      retry: { attempts: 2, backoffMs: 0 },
      handler: async () => {
        throw new Error("db timeout");
      },
    });

    await assert.rejects(() => fake.consumers[0]!.deliver({ message: msg() }), /db timeout/);
  });
});

describe("Consumer retry topics", () => {
  it("creates one consumer group per retry stage, each on its own topic", async () => {
    const { fake, service } = makeService();
    await service.consume({
      groupId: "g",
      topic: "orders",
      retry: { topicDelaysMs: [10, 20] },
      handler: async () => {},
    });

    assert.deepEqual(
      fake.consumerOptions.map((o) => o.groupId),
      ["g", "g.retry.1", "g.retry.2"]
    );
    assert.deepEqual(
      fake.consumers.map((c) => (c.subscribeCalls[0] as { topics: string[] }).topics),
      [["orders"], ["orders.retry.1"], ["orders.retry.2"]]
    );
  });

  it("moves a failing message through each retry topic, then to the DLQ", async () => {
    const { fake, service, sent } = makeService();
    await service.consume({
      groupId: "g",
      topic: "orders",
      dlqTopic: "orders.dlq",
      retry: { attempts: 1, topicDelaysMs: [0, 0] },
      handler: async () => {
        throw new Error("db timeout");
      },
    });

    await fake.consumers[0]!.deliver({ message: msg(), partition: 2 });
    const first = sent()[0]!;
    assert.equal(first.topic, retryTopicName("orders", 1));
    assert.equal(first.messages[0]!.headers?.["x-retry-count"], "1");
    assert.equal(first.messages[0]!.headers?.["x-origin-partition"], "2");

    // Redeliver what was published, as the next stage would receive it.
    const asMessage = (b: SentBatch) =>
      msg({ value: Buffer.from(String(b.messages[0]!.value)), headers: b.messages[0]!.headers, offset: "0" });

    await fake.consumers[1]!.deliver({ message: asMessage(first) });
    const second = sent()[1]!;
    assert.equal(second.topic, "orders.retry.2");
    assert.equal(second.messages[0]!.headers?.["x-retry-count"], "2");

    await fake.consumers[2]!.deliver({ message: asMessage(second) });
    const last = sent()[2]!;
    assert.equal(last.topic, "orders.dlq");
    assert.equal(last.messages[0]!.headers?.["x-retry-count"], "2");
    // The first origin survives the hops.
    assert.equal(last.messages[0]!.headers?.["x-origin-topic"], "orders");
    assert.equal(last.messages[0]!.headers?.["x-origin-partition"], "2");
    assert.equal(last.messages[0]!.headers?.["x-origin-offset"], "7");
  });

  it("waits until a retry message is due before handling it", async () => {
    const { fake, service } = makeService();
    let handledAt = 0;
    await service.consume({
      groupId: "g",
      topic: "orders",
      retry: { topicDelaysMs: [60] },
      handler: async () => {
        handledAt = Date.now();
      },
    });

    const publishedAt = Date.now();
    await fake.consumers[1]!.deliver({ message: msg({ timestamp: String(publishedAt) }) });
    assert.ok(handledAt - publishedAt >= 55, `handled after ${handledAt - publishedAt}ms`);
    assert.ok(fake.consumers[1]!.heartbeatCalls >= 1);
  });
});

describe("KafkaService.shutdownOnSignals", () => {
  it("disconnects on the signal and removes its handlers", async () => {
    const { fake, service } = makeService();
    await service.consume({ groupId: "g", topic: "t", handler: async () => {} });
    const before = process.listenerCount("SIGUSR2");

    const remove = service.shutdownOnSignals({ signals: ["SIGUSR2"], exit: false });
    assert.equal(process.listenerCount("SIGUSR2"), before + 1);

    process.emit("SIGUSR2", "SIGUSR2");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(fake.consumers[0]!.disconnectCalls, 1);

    remove();
    assert.equal(process.listenerCount("SIGUSR2"), before);
  });
});

describe("ensureTopics on KRaft brokers", () => {
  it("waits out not-leader errors while a new topic's leader comes up", async () => {
    const { fake, service } = makeService();
    fake.admin.unknownTopicResponses = 2;

    await service.ensureTopics(["orders"]);
    assert.equal(fake.admin.unknownTopicResponses, 0);
    assert.deepEqual(fake.admin.createdTopics.map((t) => t.topic), ["orders"]);
  });

  it("rethrows other metadata errors", async () => {
    const { fake, service } = makeService();
    fake.admin.fetchTopicOffsets = async () => {
      throw Object.assign(new Error("not authorized"), { type: "TOPIC_AUTHORIZATION_FAILED" });
    };

    await assert.rejects(() => service.ensureTopics(["orders"]), /not authorized/);
  });
});

describe("Consumer crash recovery", () => {
  // setTimeout is mocked in these tests; flush pending promises another way.
  const flush = () => new Promise((r) => setImmediate(r));

  async function started() {
    const fake = makeFakeKafka();
    const consumer = new Consumer(fake.kafka, { groupId: "g", topic: "orders" }, undefined, {
      info: () => {},
      warn: () => {},
      error: () => {},
    });
    await consumer.start(async () => {});
    return { fake, consumer, raw: fake.consumers[0]! };
  }

  it("restarts a consumer that kafkajs won't restart", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const { raw, consumer } = await started();

    raw.emit(raw.events.CRASH, { restart: false, error: new Error("coordinator not found") });
    t.mock.timers.tick(1_000);
    await flush();
    t.mock.timers.reset();

    assert.equal(raw.runCalls, 2);
    await consumer.stop();
  });

  it("leaves restarts to kafkajs when it will restart itself", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const { raw, consumer } = await started();

    raw.emit(raw.events.CRASH, { restart: true, error: new Error("retriable") });
    t.mock.timers.tick(60_000);
    t.mock.timers.reset();

    assert.equal(raw.runCalls, 1);
    await consumer.stop();
  });

  it("keeps retrying when the restart itself fails, and stops after stop()", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const { raw, consumer } = await started();
    raw.failConnect = true;

    raw.emit(raw.events.CRASH, { restart: false, error: new Error("down") });
    t.mock.timers.tick(1_000); // 1st restart (jittered 500–1000ms)
    await flush();
    t.mock.timers.tick(2_000); // 2nd restart (1000–2000ms)
    await flush();
    assert.equal(raw.connectCalls, 3);

    await consumer.stop();
    t.mock.timers.tick(60_000);
    await flush();
    t.mock.timers.reset();
    assert.equal(raw.connectCalls, 3);
  });
});
