import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Producer } from "../src/producer.js";
import { CompressionTypes } from "../src/compression.js";
import { makeFakeKafka } from "./helpers/fake-kafka.js";

describe("Producer construction", () => {
  it("defaults allowAutoTopicCreation to false", () => {
    const fake = makeFakeKafka();
    new Producer(fake.kafka);
    assert.equal(fake.producerOptions[0]?.allowAutoTopicCreation, false);
  });

  it("respects an explicit allowAutoTopicCreation: true", () => {
    const fake = makeFakeKafka();
    new Producer(fake.kafka, { allowAutoTopicCreation: true });
    assert.equal(fake.producerOptions[0]?.allowAutoTopicCreation, true);
  });
});

describe("Producer.publish compression", () => {
  it("defaults to Snappy when no compression is set", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    await producer.publish({ topic: "t", messages: [{ value: "a" }] });

    const sent = fake.producer.sendCalls[0] as { compression: CompressionTypes };
    assert.equal(sent.compression, CompressionTypes.Snappy);
  });

  it("respects ProducerOptions.defaultCompression", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka, { defaultCompression: CompressionTypes.GZIP });
    await producer.publish({ topic: "t", messages: [{ value: "a" }] });

    const sent = fake.producer.sendCalls[0] as { compression: CompressionTypes };
    assert.equal(sent.compression, CompressionTypes.GZIP);
  });

  it("respects a per-call PublishOptions.compression override", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka, { defaultCompression: CompressionTypes.Snappy });
    await producer.publish({
      topic: "t",
      messages: [{ value: "a" }],
      compression: CompressionTypes.None,
    });

    const sent = fake.producer.sendCalls[0] as { compression: CompressionTypes };
    assert.equal(sent.compression, CompressionTypes.None);
  });
});

describe("Producer.publish validation", () => {
  it("rejects a missing/empty topic without ever connecting", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);

    // @ts-expect-error verifying runtime check
    await assert.rejects(() => producer.publish({}), /must include a non-empty 'topic'/);
    await assert.rejects(
      // @ts-expect-error verifying runtime check
      () => producer.publish({ topic: "" }),
      /must include a non-empty 'topic'/
    );
    assert.equal(fake.producer.connectCalls, 0);
  });

  it("rejects an empty messages array", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);

    await assert.rejects(
      () => producer.publish({ topic: "t", messages: [] }),
      /must include a non-empty 'messages'/
    );
  });
});

describe("Producer.publish serialization", () => {
  it("passes string values through unchanged and serializes objects", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    await producer.publish({
      topic: "t",
      messages: [{ value: "raw-string" }, { value: { a: 1 } }],
    });

    const sent = fake.producer.sendCalls[0] as { messages: Array<{ value: unknown }> };
    assert.equal(sent.messages[0]?.value, "raw-string");
    assert.equal(sent.messages[1]?.value, JSON.stringify({ a: 1 }));
  });

  it("serializes null/undefined as a true Kafka tombstone (value: null), not the string 'null'", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    await producer.publish({
      topic: "t",
      messages: [{ key: "k1", value: null }, { key: "k2", value: undefined }],
    });

    const sent = fake.producer.sendCalls[0] as { messages: Array<{ value: unknown }> };
    assert.equal(sent.messages[0]?.value, null);
    assert.equal(sent.messages[1]?.value, null);
  });

  it("passes Buffer values through untouched instead of JSON-mangling them", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    const binary = Buffer.from([0x00, 0x01, 0xff]);
    await producer.publish({ topic: "t", messages: [{ value: binary }] });

    const sent = fake.producer.sendCalls[0] as { messages: Array<{ value: unknown }> };
    assert.ok(Buffer.isBuffer(sent.messages[0]?.value));
    assert.deepEqual(sent.messages[0]?.value, binary);
  });
});

describe("Producer connection lifecycle", () => {
  it("connects lazily and only once across concurrent publishes", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);

    await Promise.all([
      producer.publish({ topic: "t", messages: [{ value: "a" }] }),
      producer.publish({ topic: "t", messages: [{ value: "b" }] }),
    ]);

    assert.equal(fake.producer.connectCalls, 1);
    assert.equal(fake.producer.sendCalls.length, 2);
  });

  it("retries connecting after a failed attempt", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);

    fake.producer.failConnect = true;
    await assert.rejects(() => producer.publish({ topic: "t", messages: [{ value: "a" }] }));

    fake.producer.failConnect = false;
    await producer.publish({ topic: "t", messages: [{ value: "a" }] });
    assert.equal(fake.producer.connectCalls, 2);
  });

  it("logs and rethrows when send() fails", async () => {
    const logs: string[] = [];
    const fake = makeFakeKafka();
    fake.producer.failSend = true;
    const producer = new Producer(fake.kafka, {
      logger: { info: () => {}, warn: () => {}, error: (msg) => logs.push(msg) },
    });

    await assert.rejects(() => producer.publish({ topic: "t", messages: [{ value: "a" }] }));
    assert.ok(logs.includes("Failed to publish message batch to Kafka"));
  });

  it("disconnect() is a no-op when never connected", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    await producer.disconnect();
    assert.equal(fake.producer.disconnectCalls, 0);
  });

  it("disconnect() tears down an active connection and swallows disconnect errors", async () => {
    const fake = makeFakeKafka();
    const producer = new Producer(fake.kafka);
    await producer.publish({ topic: "t", messages: [{ value: "a" }] });

    fake.producer.disconnect = async () => {
      throw new Error("disconnect boom");
    };
    await assert.doesNotReject(() => producer.disconnect());
  });
});
