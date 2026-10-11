import type { CompressionTypes, Kafka, KafkaConfig, KafkaMessage } from "kafkajs";
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
  /** Randomize each wait to between half and all of it. Default true. */
  jitter?: boolean;
}

/**
 * Bounded retry policy for transient (non-poison-pill) handler failures.
 * Without it, transient errors are rethrown and kafkajs retries them
 * indefinitely, blocking the partition.
 */
export interface ConsumerRetryPolicy {
  /** In-place handler attempts per delivery, including the first. Default 3. */
  attempts?: number;
  /** Delay before in-place attempt N+1 is backoffMs * 2^(N-1). Default 200ms. */
  backoffMs?: number;
  /**
   * Randomize each in-place wait to between half and all of it, so consumers
   * that failed together don't retry together. Default true. Retry-topic
   * delays are never jittered: each stage relies on FIFO due times.
   */
  jitter?: boolean;
  /**
   * Delays (ms) for retry topics. Stage N republishes to
   * `${topic}.retry.N` (see retryTopicName) and is redelivered after
   * delays[N-1]. Create these topics up front, e.g. via ensureTopics().
   */
  topicDelaysMs?: number[];
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
  /**
   * Bounded retries for transient failures. Once exhausted, the message goes
   * to `dlqTopic` (if set) instead of blocking the partition. See {@link ConsumerRetryPolicy}.
   */
  retry?: ConsumerRetryPolicy;
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
  /** TLS settings, e.g. `true` for managed clusters (Confluent Cloud, MSK, Aiven). */
  ssl?: KafkaConfig["ssl"];
  /** SASL auth, e.g. `{ mechanism: "plain", username, password }`. */
  sasl?: KafkaConfig["sasl"];
  /** Raw kafkajs config overrides, merged last. Ignored when `kafka` is given. */
  config?: Partial<KafkaConfig>;
}

export interface ShutdownOptions {
  /** Default ["SIGTERM", "SIGINT"]. */
  signals?: NodeJS.Signals[];
  /** Max time to wait for disconnect() before exiting anyway. Default 10000. */
  timeoutMs?: number;
  /** Call process.exit() after shutdown. Default true. */
  exit?: boolean;
}
