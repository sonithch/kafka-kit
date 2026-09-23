import type { CompressionTypes, KafkaMessage } from "kafkajs";
import type { Logger } from "./logger.js";

/**
 * Configuration for Kafka topic creation and partition management.
 */
export interface TopicConfig {
  /** The name of the Kafka topic. */
  topic: string;
  /** Number of partitions to provision. Defaults to 16 or process.env.KAFKA_PARTITIONS. */
  partitions?: number;
  /** Replication factor for the topic across brokers. Defaults to 1. */
  replicationFactor?: number;
}

/**
 * Individual message record to be published.
 */
export interface PublishMessage {
  /** Optional partition key used to guarantee partition affinity and order. */
  key?: string;
  /** Message payload. Will be JSON stringified automatically if not already a string. */
  value: unknown;
  /** Optional record headers (e.g. correlation IDs, traceparent). */
  headers?: Record<string, string>;
}

/**
 * Options for publishing message batches to Kafka.
 */
export interface PublishOptions {
  /** Target topic name. */
  topic: string;
  /** Non-empty array of messages to send as a batch. */
  messages: PublishMessage[];
  /** Optional wire compression type (defaults to GZIP). */
  compression?: CompressionTypes;
}

/**
 * Handler callback invoked for each received message.
 * @template T Type of the parsed JSON payload.
 */
export interface ConsumerHandler<T = unknown> {
  (payload: T, rawMessage: KafkaMessage): Promise<void>;
}

/**
 * Configuration options for starting a Kafka consumer.
 */
export interface ConsumerOptions {
  /** The consumer group ID. */
  groupId: string;
  /** The topic to subscribe to. */
  topic: string;
  /**
   * Optional Dead Letter Queue (DLQ) topic.
   * If specified, unparseable messages and non-retryable poison pills are forwarded here.
   */
  dlqTopic?: string;
  /** Whether to read from the earliest offset if no committed offset exists. */
  fromBeginning?: boolean;
}

/**
 * Options for `kafka.consume()`, extending ConsumerOptions with the message handler.
 * @template T Type of the parsed JSON payload.
 */
export interface ConsumeOptions<T = unknown> extends ConsumerOptions {
  /** The async handler executed for each message. */
  handler: ConsumerHandler<T>;
}

/**
 * Configuration options for initializing the KafkaService facade.
 */
export interface KafkaServiceOptions {
  /** Unique client identifier sent to Kafka brokers for connection tracking and metrics. */
  clientId: string;
  /** Optional explicit list of broker host:port addresses. Defaults to process.env.KAFKA_BROKERS or localhost:9092. */
  brokers?: string[];
  /** Optional custom logger instance (e.g. Pino, Winston, Roarr). Defaults to structured JSON stdout logger. */
  logger?: Logger;
}
