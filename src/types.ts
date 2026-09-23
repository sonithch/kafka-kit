import type { CompressionTypes, Kafka, KafkaMessage } from "kafkajs";
import type { Logger } from "./logger.js";

export interface TopicConfig {
  topic: string;
  partitions?: number;
  replicationFactor?: number;
}

export interface PublishMessage {
  key?: string;
  value: unknown;
  headers?: Record<string, string>;
}

export interface PublishOptions {
  topic: string;
  messages: PublishMessage[];
  compression?: CompressionTypes;
}

/** Per-message context passed to a ConsumerHandler alongside the payload. */
export interface ConsumerContext {
  topic: string;
  partition: number;
  /** Call during a slow handler to avoid a session-timeout rebalance. */
  heartbeat: () => Promise<void>;
  /** Pause this topic's partitions; returns the matching resume function. */
  pause: () => () => void;
}

export interface ConsumerHandler<T = unknown> {
  (payload: T, rawMessage: KafkaMessage, context: ConsumerContext): Promise<void>;
}

/** Retry policy for forwarding a poison pill to the DLQ (see dlq.ts). */
export interface DlqRetryPolicy {
  /** Default 3. */
  maxAttempts?: number;
  /** Default 200ms; attempt N waits baseDelayMs * N before the next try. */
  baseDelayMs?: number;
}

export interface ConsumerOptions {
  groupId: string;
  topic: string;
  dlqTopic?: string;
  fromBeginning?: boolean;
  /** Override the default poison-pill detection (see utils.isPoisonPill). */
  isPoisonPill?: (error: unknown) => boolean;
  /**
   * kafkajs consumer group session timeout in ms. Defaults to 30000. Raise
   * this if handlers can run long (slow downstream calls, heavy batch work)
   * without calling ConsumerContext.heartbeat(), to avoid a rebalance.
   */
  sessionTimeout?: number;
  /** Retry policy for DLQ forwarding. See {@link DlqRetryPolicy}. */
  dlqRetry?: DlqRetryPolicy;
}

export interface ConsumeOptions<T = unknown> extends ConsumerOptions {
  handler: ConsumerHandler<T>;
}

export interface KafkaServiceOptions {
  clientId: string;
  brokers?: string[];
  logger?: Logger;
  /**
   * Let the broker auto-create topics on first publish/consume. Defaults to
   * false so partition counts stay under ensureTopics()'s control instead of
   * silently falling back to the broker default (often 1 partition).
   */
  allowAutoTopicCreation?: boolean;
  /**
   * Bring your own pre-configured kafkajs Kafka client instead of having
   * KafkaService build one from clientId/brokers. Also the seam tests use to
   * inject a fake client instead of hitting a real broker.
   */
  kafka?: Kafka;
  /**
   * How long getClusterHealth() caches its result before re-querying the
   * broker, in ms. Defaults to 10000. Avoids flooding the controller with
   * admin RPCs from frequent liveness/readiness probes.
   */
  healthCheckTtlMs?: number;
  /** Default compression for publish() calls; see ProducerOptions.defaultCompression. */
  defaultCompression?: CompressionTypes;
}
