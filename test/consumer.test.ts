import { describe, it } from "node:test";
import assert from "node:assert/strict";
import type { Kafka, KafkaMessage } from "kafkajs";
import { Consumer } from "../src/consumer.js";
import { NonRetryableError } from "../src/utils.js";
import type { Producer } from "../src/producer.js";
import type { ConsumerOptions, ConsumerContext } from "../src/types.js";

type EachMessageFn = (args: {
  message: KafkaMessage;
  partition: number;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
}) => Promise<void>;

function makeFakeKafka() {
  let eachMessage: EachMessageFn | undefined;
  const consumerSpyOptions: Record<string, unknown>[] = [];
  let heartbeatCalls = 0;

  const rawConsumer = {
    connect: async () => {},
    subscribe: async () => {},
    run: async (opts: { eachMessage: EachMessageFn }) => {
      eachMessage = opts.eachMessage;
    },
    disconnect: async () => {},
  };

  const kafka = {
    consumer: (opts: Record<string, unknown>) => {
      consumerSpyOptions.push(opts);
      return rawConsumer;
    },
  } as unknown as Kafka;

  return {
    kafka,
    consumerSpyOptions,
    get heartbeatCalls() {
      return heartbeatCalls;
    },
    deliver: async (message: Partial<KafkaMessage>, partition = 0) => {
      if (!eachMessage) throw new Error("consumer.run was not called yet");
      await eachMessage({
        message: message as KafkaMessage,
        partition,
        heartbeat: async () => {
          heartbeatCalls++;
        },
        pause: () => () => {},
      });
    },
  };
}

class FakeProducer {
  public published: Array<{
    topic: string;
    messages: Array<{ value?: unknown; headers?: Record<string, string> }>;
  }> = [];
  public failNextPublishes = 0;

  async publish(options: {
    topic: string;
    messages: Array<{ value?: unknown; headers?: Record<string, string> }>;
  }): Promise<void> {
    if (this.failNextPublishes > 0) {
      this.failNextPublishes--;
      throw new Error("DLQ broker unavailable");
    }
    this.published.push(options);
  }
}

function baseOptions(overrides?: Partial<ConsumerOptions>): ConsumerOptions {
  return {
    groupId: "test-group",
    topic: "orders.created",
    dlqTopic: "orders.dlq",
    ...overrides,
  };
}

describe("Consumer construction", () => {
  it("does not allow auto topic creation by default", () => {
    const { kafka, consumerSpyOptions } = makeFakeKafka();
    new Consumer(kafka, baseOptions(), undefined, undefined);
    assert.equal(consumerSpyOptions[0]?.allowAutoTopicCreation, false);
  });

  it("requires a non-empty groupId", () => {
    const { kafka } = makeFakeKafka();
    assert.throws(
      () => new Consumer(kafka, { topic: "t" } as ConsumerOptions),
      /requires a non-empty 'groupId'/
    );
  });

  it("requires a non-empty topic", () => {
    const { kafka } = makeFakeKafka();
    assert.throws(
      () => new Consumer(kafka, { groupId: "g" } as ConsumerOptions),
      /requires a non-empty 'topic'/
    );
  });

  it("defaults sessionTimeout to 30000 and respects an explicit override", () => {
    const { kafka, consumerSpyOptions } = makeFakeKafka();
    new Consumer(kafka, baseOptions());
    assert.equal(consumerSpyOptions[0]?.sessionTimeout, 30000);

    new Consumer(kafka, baseOptions({ sessionTimeout: 10_000 }));
    assert.equal(consumerSpyOptions[1]?.sessionTimeout, 10_000);
  });
});

describe("Consumer.start", () => {
  it("rejects a missing or invalid handler", async () => {
    const { kafka } = makeFakeKafka();
    const consumer = new Consumer(kafka, baseOptions());
    // @ts-expect-error verifying runtime check
    await assert.rejects(() => consumer.start(null), /requires a valid handler function/);
  });

  it("passes topic/partition/heartbeat/pause context to the handler", async () => {
    const { kafka, deliver } = makeFakeKafka();
    let receivedContext: ConsumerContext | undefined;
    const consumer = new Consumer(kafka, baseOptions());

    await consumer.start(async (_payload, _raw, context) => {
      receivedContext = context;
      await context.heartbeat();
    });
    await deliver({ value: Buffer.from("{}"), offset: "1" }, 5);

    assert.equal(receivedContext?.topic, "orders.created");
    assert.equal(receivedContext?.partition, 5);
    assert.equal(typeof receivedContext?.heartbeat, "function");
    assert.equal(typeof receivedContext?.pause, "function");
  });
});

describe("Consumer tombstones", () => {
  it("passes a Kafka tombstone (value: null) through as null, not {}", async () => {
    const { kafka, deliver } = makeFakeKafka();
    let received: unknown = "not-yet-set";
    const consumer = new Consumer(kafka, baseOptions());

    await consumer.start(async (payload) => {
      received = payload;
    });
    await deliver({ value: null, offset: "1" });

    assert.equal(received, null);
  });
});

describe("Consumer DLQ routing", () => {
  it("routes unparseable JSON (SyntaxError) to the DLQ, preserving headers and adding origin metadata", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {});
    await deliver(
      {
        value: Buffer.from("not json"),
        key: Buffer.from("order-1"),
        headers: { "x-request-id": Buffer.from("req-1") },
        offset: "42",
      },
      3
    );

    assert.equal(producer.published.length, 1);
    const dlqMessage = producer.published[0]!.messages[0]!;
    assert.equal(dlqMessage.value, "not json");
    assert.equal(dlqMessage.headers?.["x-request-id"], "req-1");
    assert.equal(dlqMessage.headers?.["x-origin-topic"], "orders.created");
    assert.equal(dlqMessage.headers?.["x-origin-partition"], "3");
    assert.equal(dlqMessage.headers?.["x-origin-offset"], "42");
  });

  it("routes NonRetryableError thrown by the handler to the DLQ", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {
      throw new NonRetryableError("missing orderId");
    });
    await deliver({ value: Buffer.from("{}"), offset: "1" });

    assert.equal(producer.published.length, 1);
  });

  it("does NOT treat a generic TypeError/RangeError as a poison pill by default", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {
      throw new TypeError("Cannot read properties of undefined (reading 'userId')");
    });

    await assert.rejects(() => deliver({ value: Buffer.from("{}"), offset: "1" }));
    assert.equal(producer.published.length, 0);
  });

  it("rethrows transient errors instead of routing them to the DLQ", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {
      throw new Error("connection reset");
    });

    await assert.rejects(
      () => deliver({ value: Buffer.from("{}"), offset: "1" }),
      /connection reset/
    );
    assert.equal(producer.published.length, 0);
  });

  it("rethrows a poison pill when no dlqTopic is configured", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(
      kafka,
      baseOptions({ dlqTopic: undefined }),
      producer as unknown as Producer
    );

    await consumer.start(async () => {});
    await assert.rejects(() => deliver({ value: Buffer.from("not json"), offset: "1" }));
    assert.equal(producer.published.length, 0);
  });

  it("rethrows a poison pill when no producer is configured", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const consumer = new Consumer(kafka, baseOptions(), undefined);

    await consumer.start(async () => {});
    await assert.rejects(() => deliver({ value: Buffer.from("not json"), offset: "1" }));
  });

  it("does NOT silently drop the message when the DLQ is unreachable after retries", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    producer.failNextPublishes = 3;
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {});
    await assert.rejects(
      () => deliver({ value: Buffer.from("not json"), offset: "1" }),
      /refusing to commit offset/
    );
    assert.equal(producer.published.length, 0);
  });

  it("respects a custom dlqRetry policy (fewer/faster attempts)", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    producer.failNextPublishes = 5; // more than maxAttempts below, so it always exhausts
    const consumer = new Consumer(
      kafka,
      baseOptions({ dlqRetry: { maxAttempts: 1, baseDelayMs: 0 } }),
      producer as unknown as Producer
    );

    await consumer.start(async () => {});
    await assert.rejects(
      () => deliver({ value: Buffer.from("not json"), offset: "1" }),
      /after 1 attempts/
    );
  });

  it("joins array-valued headers when forwarding to the DLQ", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(kafka, baseOptions(), producer as unknown as Producer);

    await consumer.start(async () => {});
    await deliver({
      value: Buffer.from("not json"),
      headers: { "x-trace": [Buffer.from("a"), Buffer.from("b")] },
      offset: "1",
    });

    const dlqMessage = producer.published[0]!.messages[0]!;
    assert.equal(dlqMessage.headers?.["x-trace"], "a,b");
  });

  it("supports a custom isPoisonPill predicate", async () => {
    const { kafka, deliver } = makeFakeKafka();
    const producer = new FakeProducer();
    const consumer = new Consumer(
      kafka,
      baseOptions({ isPoisonPill: (err) => err instanceof Error && err.message === "bad-shape" }),
      producer as unknown as Producer
    );

    await consumer.start(async () => {
      throw new Error("bad-shape");
    });
    await deliver({ value: Buffer.from("{}"), offset: "1" });

    assert.equal(producer.published.length, 1);
  });

  it("stop() disconnects the underlying consumer", async () => {
    const { kafka } = makeFakeKafka();
    const consumer = new Consumer(kafka, baseOptions());
    await assert.doesNotReject(() => consumer.stop());
  });
});
