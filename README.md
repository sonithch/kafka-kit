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
* **Poison-Pill DLQ Isolation:** Automatically distinguishes permanent syntax/schema failures from transient network blips. Corrupted messages route to your DLQ; transient errors retry.
* **Idempotent Producer:** `idempotent: true` enabled by default to prevent duplicate writes during network blips.
* **GZIP Compression:** Automatic wire payload compression.
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

If an unparseable payload or non-retryable error occurs, `kafka-kit` logs a warning, forwards the failed payload and stack trace to `dlqTopic`, and commits the offset so the consumer group never stalls:

```typescript
import { KafkaService, NonRetryableError } from "kafka-kit";

const consumer = await kafka.consume({
  groupId: "order-processing-group",
  topic: "orders.created",
  dlqTopic: "orders.dlq", // 👈 Automatically captures poison pills
  handler: async (order, rawMessage) => {
    if (!order.orderId) {
      // Mark as permanent failure -> routes to DLQ
      throw new NonRetryableError("Missing orderId in payload");
    }

    await processOrder(order);
  },
});

// Graceful shutdown
await kafka.disconnect();
```

---

### 4. Topic Management & Health Checks

```typescript
// Idempotently ensure topics exist before starting workers
await kafka.ensureTopics([
  { topic: "orders.created", partitions: 32 },
  { topic: "orders.dlq", partitions: 4 },
]);

// Kubernetes Readiness / Liveness Probe
const health = await kafka.getClusterHealth();
console.log(health); // { isHealthy: true, brokers: 3 }
```

---

### 5. Custom Logger (e.g. Pino, Winston)

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

## License

MIT © [Sonith](https://github.com/sonith)
