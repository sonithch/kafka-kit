// End-to-end workflows against a real broker. Start one with
// `docker compose up -d --wait`, then run `npm run test:integration`.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import type { Kafka, KafkaMessage } from "kafkajs";
import {
  KafkaService,
  NonRetryableError,
  CompressionTypes,
  createKafkaClient,
  retryTopicName,
  type Logger,
} from "../../src/index.js";

const BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const RUN = `it${Date.now()}`;
// IT_VERBOSE=1 shows kafka-kit and kafkajs logs.
const quiet: Logger | undefined = process.env.IT_VERBOSE
  ? undefined
  : { info: () => {}, warn: () => {}, error: () => {} };

function name(suffix: string): string {
  return `${RUN}.${suffix}`;
}

function newService(): KafkaService {
  return new KafkaService({ clientId: RUN, brokers: BROKERS, logger: quiet });
}

async function waitFor(check: () => boolean, timeoutMs = 20_000, label = "condition"): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

const collectors: Array<() => Promise<void>> = [];

/** Read raw messages from a topic with a plain kafkajs consumer (DLQ payloads may not be JSON). */
async function collect(kafka: Kafka, topic: string) {
  const messages: KafkaMessage[] = [];
  const consumer = kafka.consumer({ groupId: name(`collector.${topic}`) });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => void messages.push(message) });
  const stop = () => consumer.disconnect();
  collectors.push(stop); // also stopped in after(), in case a test fails first
  return { messages, stop };
}

function header(m: KafkaMessage, key: string): string | undefined {
  return m.headers?.[key]?.toString();
}

describe("kafka-kit against a real broker", { timeout: 120_000 }, () => {
  const kafka = newService();
  const raw = createKafkaClient(RUN, { brokers: BROKERS, logger: quiet });

  before(async () => {
    const health = await kafka.getClusterHealth();
    assert.ok(health.isHealthy, `broker not reachable at ${BROKERS.join(",")}`);
  });

  after(async () => {
    await Promise.allSettled(collectors.map((stop) => stop()));
    await kafka.disconnect();
  });

  it("ensureTopics creates topics and is idempotent", async () => {
    const topics = [{ topic: name("ensure"), partitions: 3 }];
    await kafka.ensureTopics(topics);
    await kafka.ensureTopics(topics);

    const admin = raw.admin();
    await admin.connect();
    const meta = await admin.fetchTopicMetadata({ topics: [name("ensure")] });
    await admin.disconnect();
    assert.equal(meta.topics[0]!.partitions.length, 3);
  });

  it("round-trips JSON, keys, headers and per-key ordering across partitions", async () => {
    const topic = name("orders");
    await kafka.ensureTopics([{ topic, partitions: 3 }]);

    const seen: Array<{ key: string; seq: number; requestId?: string }> = [];
    await kafka.consume<{ seq: number }>({
      groupId: name("orders.g"),
      topic,
      fromBeginning: true,
      handler: async (payload, msg) => {
        seen.push({ key: msg.key!.toString(), seq: payload.seq, requestId: header(msg, "x-request-id") });
      },
    });

    const keys = ["a", "b", "c", "d"];
    for (let seq = 0; seq < 10; seq++) {
      await kafka.publish({
        topic,
        messages: keys.map((key) => ({ key, value: { seq }, headers: { "x-request-id": `${key}-${seq}` } })),
      });
    }

    await waitFor(() => seen.length === 40, 20_000, "40 messages");
    for (const key of keys) {
      const seqs = seen.filter((s) => s.key === key).map((s) => s.seq);
      assert.deepEqual(seqs, [...Array(10).keys()], `ordering for key ${key}`);
    }
    assert.equal(seen.find((s) => s.key === "b" && s.seq === 3)?.requestId, "b-3");
  });

  it("publishes and delivers a real tombstone as null", async () => {
    const topic = name("compacted");
    await kafka.ensureTopics([{ topic, partitions: 1 }]);
    const values: unknown[] = [];
    await kafka.consume({
      groupId: name("compacted.g"),
      topic,
      fromBeginning: true,
      handler: async (payload) => void values.push(payload),
    });

    await kafka.publish({ topic, messages: [{ key: "k", value: { v: 1 } }, { key: "k", value: null }] });
    await waitFor(() => values.length === 2, 20_000, "tombstone");
    assert.deepEqual(values, [{ v: 1 }, null]);
  });

  it("works with GZIP and the default Snappy compression", async () => {
    const topic = name("compression");
    await kafka.ensureTopics([{ topic, partitions: 1 }]);
    const values: number[] = [];
    await kafka.consume<{ n: number }>({
      groupId: name("compression.g"),
      topic,
      fromBeginning: true,
      handler: async (p) => void values.push(p.n),
    });

    await kafka.publish({ topic, messages: [{ value: { n: 1 } }] });
    await kafka.publish({ topic, messages: [{ value: { n: 2 } }], compression: CompressionTypes.GZIP });
    await waitFor(() => values.length === 2, 20_000, "compressed messages");
    assert.deepEqual(values, [1, 2]);
  });

  it("routes poison pills to the DLQ and keeps consuming", async () => {
    const topic = name("payments");
    const dlq = name("payments.dlq");
    await kafka.ensureTopics([
      { topic, partitions: 1 },
      { topic: dlq, partitions: 1 },
    ]);

    const handled: string[] = [];
    await kafka.consume<{ id: string }>({
      groupId: name("payments.g"),
      topic,
      dlqTopic: dlq,
      fromBeginning: true,
      handler: async (p) => {
        if (p.id === "bad-shape") throw new NonRetryableError("missing amount");
        handled.push(p.id);
      },
    });
    const dlqReader = await collect(raw, dlq);

    await kafka.publish({
      topic,
      messages: [
        { key: "1", value: { id: "ok-1" } },
        { key: "2", value: "{not json", headers: { "x-request-id": "r2" } },
        { key: "3", value: { id: "bad-shape" } },
        { key: "4", value: { id: "ok-2" } },
      ],
    });

    await waitFor(() => handled.length === 2 && dlqReader.messages.length === 2, 20_000, "DLQ routing");
    await dlqReader.stop();

    assert.deepEqual(handled, ["ok-1", "ok-2"]);
    const [syntax, nonRetryable] = dlqReader.messages;
    assert.equal(syntax!.value!.toString(), "{not json");
    assert.equal(header(syntax!, "x-request-id"), "r2");
    assert.equal(header(syntax!, "x-origin-topic"), topic);
    assert.match(header(syntax!, "x-exception-message")!, /SyntaxError/);
    assert.match(header(nonRetryable!, "x-exception-message")!, /missing amount/);
  });

  it("retries a transient failure in place and then succeeds", async () => {
    const topic = name("flaky");
    await kafka.ensureTopics([{ topic, partitions: 1 }]);
    let calls = 0;
    let done = false;
    await kafka.consume({
      groupId: name("flaky.g"),
      topic,
      fromBeginning: true,
      retry: { attempts: 3, backoffMs: 100 },
      handler: async () => {
        if (++calls < 3) throw new Error("downstream 503");
        done = true;
      },
    });

    await kafka.publish({ topic, messages: [{ value: { a: 1 } }] });
    await waitFor(() => done, 20_000, "in-place retry success");
    assert.equal(calls, 3);
  });

  it("moves a failing message through delayed retry topics, then to the DLQ", async () => {
    const topic = name("shipping");
    const dlq = name("shipping.dlq");
    const delays = [1_000, 2_000];
    await kafka.ensureTopics([
      { topic, partitions: 1 },
      { topic: retryTopicName(topic, 1), partitions: 1 },
      { topic: retryTopicName(topic, 2), partitions: 1 },
      { topic: dlq, partitions: 1 },
    ]);

    const attemptsAt: number[] = [];
    await kafka.consume({
      groupId: name("shipping.g"),
      topic,
      dlqTopic: dlq,
      fromBeginning: true,
      retry: { attempts: 2, backoffMs: 50, topicDelaysMs: delays },
      handler: async () => {
        attemptsAt.push(Date.now());
        throw new Error("carrier API down");
      },
    });
    const dlqReader = await collect(raw, dlq);

    const publishedAt = Date.now();
    await kafka.publish({ topic, messages: [{ key: "s1", value: { id: "s1" } }] });
    await waitFor(() => dlqReader.messages.length === 1, 30_000, "message in DLQ");
    await dlqReader.stop();

    // 2 in-place attempts on each of 3 stages.
    assert.equal(attemptsAt.length, 6);
    const stage1Start = attemptsAt[2]! - attemptsAt[1]!;
    const stage2Start = attemptsAt[4]! - attemptsAt[3]!;
    assert.ok(stage1Start >= 900, `retry.1 waited ${stage1Start}ms`);
    assert.ok(stage2Start >= 1_900, `retry.2 waited ${stage2Start}ms`);
    assert.ok(Date.now() - publishedAt >= 3_000);

    const m = dlqReader.messages[0]!;
    assert.equal(header(m, "x-retry-count"), "2");
    assert.equal(header(m, "x-origin-topic"), topic);
    assert.equal(m.key!.toString(), "s1");
    assert.deepEqual(JSON.parse(m.value!.toString()), { id: "s1" });
  });

  it("shutdownOnSignals leaves the consumer group cleanly", async () => {
    const topic = name("shutdown");
    const groupId = name("shutdown.g");
    await kafka.ensureTopics([{ topic, partitions: 1 }]);

    const service = newService();
    await service.consume({ groupId, topic, handler: async () => {} });

    const admin = raw.admin();
    await admin.connect();
    const state = async () => (await admin.describeGroups([groupId])).groups[0]!.state;
    const deadline = Date.now() + 20_000;
    while ((await state()) !== "Stable" && Date.now() < deadline) await new Promise((r) => setTimeout(r, 200));
    assert.equal(await state(), "Stable");

    const remove = service.shutdownOnSignals({ signals: ["SIGUSR2"], exit: false });
    process.emit("SIGUSR2", "SIGUSR2");
    const emptyBy = Date.now() + 15_000;
    while ((await state()) !== "Empty" && Date.now() < emptyBy) await new Promise((r) => setTimeout(r, 200));
    remove();

    assert.equal(await state(), "Empty");
    await admin.disconnect();
  });
});
