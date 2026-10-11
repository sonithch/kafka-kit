# kafka-kit

[![npm version](https://img.shields.io/npm/v/kafka-kit.svg)](https://www.npmjs.com/package/kafka-kit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Node.js CI](https://img.shields.io/badge/Node.js-%3E%3D18.0.0-brightgreen.svg)](https://nodejs.org)

**Resilient, zero-boilerplate Kafka client for Node.js.**  
Built-in connection pooling, idempotent delivery by default, and automatic Dead Letter Queue (DLQ) poison-pill isolation.

---

## Why kafka-kit?

Using raw Kafka drivers like `kafkajs` in production often leads to:
1. **The Poison-Pill Death Loop:** An unparseable message crashes the consumer, triggers a rebalance, and re-crashes the group indefinitely.
2. **Socket Sprawl:** Multiple client connections for producers, consumers, and admin tasks.
3. **Complex Offset Math:** Hand-rolled offset committing and manual batch slicing.

`kafka-kit` provides a **single, unified client** with production guardrails enabled by default.

---

## Features

* **Single Connection Pool:** Reuses a single Kafka client instance across producer, consumer, and admin operations.
* **Poison-Pill DLQ Isolation:** Unparseable JSON (`SyntaxError`) and messages your handler explicitly marks with `NonRetryableError` are routed to your DLQ with the original payload/headers intact, instead of retrying forever. Everything else retries — kafka-kit deliberately does *not* guess at "schema failures," since a generic error can just as easily be a transient downstream issue; opt specific errors in via `NonRetryableError` or a custom `isPoisonPill`. If the DLQ itself is unreachable, kafka-kit retries with backoff and then refuses to drop the message rather than silently losing it.
* **Tombstone-Safe:** A Kafka tombstone (`value: null`) round-trips correctly — published as a true `null` (so log-compacted topics actually delete the key, not the string `"null"`), and delivered to your handler as `null`, not an empty object.
* **Idempotent Producer:** `idempotent: true` enabled by default to prevent duplicate writes during network blips.
* **Snappy Compression by Default:** Lower CPU than GZIP with no size-inflation penalty on small payloads. Override per-call, per-client, or register your own codec (e.g. LZ4).
* **Bounded Retries:** Opt-in in-place retries with backoff, then delayed retry topics (`orders.retry.1`, `orders.retry.2`, …), then the DLQ — a flaky dependency can't block a partition forever.
* **Self-Healing Consumers:** kafkajs leaves a consumer dead after a non-retriable crash (e.g. "group coordinator not found" while a broker restarts); kafka-kit restarts it with capped, jittered backoff.
* **KRaft-Ready Topic Creation:** `ensureTopics()` waits until new partitions actually have a serving leader, avoiding the not-leader errors that hit the first publish on Kafka 3.3+/4.x.
* **Graceful Shutdown:** `shutdownOnSignals()` stops consumers before the producer on SIGTERM/SIGINT, so in-flight DLQ forwards finish.
* **Managed-Cluster Ready:** `ssl`/`sasl` options for Confluent Cloud, MSK, Aiven and friends.
* **Slow-Handler Safe:** Handlers get a `heartbeat()`/`pause()` context so long-running work doesn't trigger a consumer-group rebalance; `sessionTimeout` is configurable per consumer.
* **Zero Magic:** No heavy framework dependencies (no NestJS). Works with Fastify, Express, Hono, Next.js, or standalone background workers.
* **Dual ESM & CommonJS:** Native support for both `import` and `require()`.
* **Pluggable Logging:** Out-of-the-box structured JSON logging with support for Pino, Winston, or custom loggers.

---

## Installation

```bash
npm install kafka-kit kafkajs
# or
yarn add kafka-kit kafkajs
# or
pnpm add kafka-kit kafkajs
```

*Requires Node.js >= 18.0.0*

---

## Quickstart

### 1. Initialize Client

```typescript
import { KafkaService } from "kafka-kit";

const kafka = new KafkaService({
  clientId: "order-service",
  brokers: ["localhost:9092"], // defaults to process.env.KAFKA_BROKERS or localhost:9092
});
```

---

### 2. Publishing Messages (Idempotent & Compressed)

```typescript
await kafka.publish({
  topic: "orders.created",
  messages: [
    {
      key: "order-456", // Partition key for ordering
      value: {
        orderId: "order-456",
        amount: 99.99,
        items: ["item-1", "item-2"],
      },
      headers: { "x-request-id": "req-xyz-123" },
    },
  ],
});
```

---

### 3. Reliable Consumption with DLQ Poison-Pill Routing

If a message fails JSON parsing, or your handler throws `NonRetryableError`, kafka-kit forwards the original payload/headers (plus origin metadata) to `dlqTopic` and commits the offset so the consumer group doesn't stall. Anything else is treated as transient and retried by kafkajs as usual. Handlers get a third `context` argument for `heartbeat()`/`pause()` on slow work:

```typescript
import { KafkaService, NonRetryableError } from "kafka-kit";

const consumer = await kafka.consume({
  groupId: "order-processing-group",
  topic: "orders.created",
  dlqTopic: "orders.dlq", // 👈 Poison pills route here instead of retrying forever
  handler: async (order, rawMessage, { heartbeat }) => {
    if (!order.orderId) {
      // Mark as permanent failure -> routes to DLQ
      throw new NonRetryableError("Missing orderId in payload");
    }

    await processOrder(order); // call heartbeat() here if this can run long
  },
});

// Graceful shutdown
await kafka.disconnect();
```

---

### 4. Bounded Retries & Retry Topics

By default a transient error is rethrown and kafkajs retries it indefinitely. Set `retry` to bound that:

```typescript
await kafka.consume({
  groupId: "order-processing-group",
  topic: "orders.created",
  dlqTopic: "orders.dlq",
  retry: {
    attempts: 3,                    // in-place tries per delivery (default 3)
    backoffMs: 200,                 // ~200ms, ~400ms, ... between tries (default 200)
    jitter: true,                   // randomize each wait to 50–100% of it (default true)
    topicDelaysMs: [5_000, 60_000], // optional: retry topics, then the DLQ
  },
  handler: async (order) => { /* ... */ },
});
```

Jitter keeps many consumers that failed together from retrying in lockstep against a recovering dependency; DLQ publish retries are jittered the same way (`dlqRetry.jitter`). Retry-topic delays are exact, since each stage relies on messages becoming due in order.

When in-place attempts run out, the message moves to `orders.created.retry.1` and is handled again after 5s, then `orders.created.retry.2` after 60s, then the DLQ. Each hop adds an `x-retry-count` header and keeps the original `x-origin-*` headers. Each stage runs as its own consumer group (`order-processing-group.retry.N`), so waiting on a delayed message never holds up the main topic. Create the retry topics up front:

```typescript
import { retryTopicName } from "kafka-kit";

await kafka.ensureTopics([
  "orders.created",
  retryTopicName("orders.created", 1),
  retryTopicName("orders.created", 2),
  "orders.dlq",
]);
```

Without `dlqTopic`, exhausted retries are rethrown as before.

---

### 5. Graceful Shutdown

```typescript
// On SIGTERM/SIGINT: stop consumers, then the producer and admin, then exit.
// A second signal or the timeout exits immediately.
kafka.shutdownOnSignals({ timeoutMs: 10_000 });
```

Pass `exit: false` to disconnect without calling `process.exit()`; the returned function removes the handlers.

---

### 6. Topic Management & Health Checks

```typescript
// Idempotently ensure topics exist before starting workers.
// Warns if it has to fall back to replicationFactor: 1 (fine for local dev,
// not for a real cluster), and tolerates a concurrent replica winning the
// create race.
await kafka.ensureTopics([
  { topic: "orders.created", partitions: 32 },
  { topic: "orders.dlq", partitions: 4 },
]);

// Kubernetes Readiness / Liveness Probe. Result is cached for
// healthCheckTtlMs (default 10s) so frequent probes don't flood the
// broker; a failed check is never cached, so recovery is picked up on
// the next call.
const health = await kafka.getClusterHealth();
console.log(health); // { isHealthy: true, brokers: 3 }
```

---

### 7. Custom Logger (e.g. Pino, Winston)

```typescript
import pino from "pino";
const pinoLogger = pino();

const kafka = new KafkaService({
  clientId: "payment-worker",
  logger: {
    info: (msg, ctx) => pinoLogger.info(ctx, msg),
    warn: (msg, ctx) => pinoLogger.warn(ctx, msg),
    error: (msg, ctx) => pinoLogger.error(ctx, msg),
  },
});
```

---

### 8. Security (SSL / SASL)

```typescript
const kafka = new KafkaService({
  clientId: "order-service",
  brokers: ["pkc-xxxx.confluent.cloud:9092"],
  ssl: true,
  sasl: { mechanism: "plain", username: process.env.KAFKA_KEY!, password: process.env.KAFKA_SECRET! },
  // config: { connectionTimeout: 5_000 }, // any other kafkajs KafkaConfig overrides
});
```

---

### 9. Advanced Options

```typescript
import { KafkaService, CompressionTypes, registerCompressionCodec } from "kafka-kit";

const kafka = new KafkaService({
  clientId: "order-service",
  defaultCompression: CompressionTypes.GZIP, // override the Snappy default
  healthCheckTtlMs: 15_000,
  allowAutoTopicCreation: false, // default; set true to let the broker auto-create topics
});

// Plug in another codec (e.g. LZ4 via the `kafkajs-lz4` package) globally:
// registerCompressionCodec(CompressionTypes.LZ4, new LZ4Codec().codec);

const consumer = await kafka.consume({
  groupId: "order-processing-group",
  topic: "orders.created",
  dlqTopic: "orders.dlq",
  sessionTimeout: 45_000, // raise if handlers can run long without calling heartbeat()
  dlqRetry: { maxAttempts: 5, baseDelayMs: 300 }, // DLQ publish retry policy
  handler: async (order) => { /* ... */ },
});
```

---

## License

MIT © [Sonith](https://github.com/sonithch)
