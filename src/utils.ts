import type { TopicConfig } from "./types.js";

export const DEFAULT_TOPIC_PARTITIONS = 16;
/** RF 1 has zero fault tolerance — fine for local dev, not for a real cluster. ensureTopics() warns when this default is used. */
export const DEFAULT_TOPIC_REPLICATION_FACTOR = 1;

/**
 * Resolve partition/replication-factor for a topic, in priority order:
 * explicit `overrides` > `KAFKA_PARTITIONS`/`KAFKA_REPLICATION_FACTOR` env vars > defaults.
 *
 * @param topic - Topic name (currently unused for resolution, kept for future per-topic policy).
 * @param overrides - Explicit values that win over everything else.
 */
export function getTopicConfig(
  topic: string,
  overrides?: Partial<TopicConfig>
): { partitions: number; replicationFactor: number } {
  const envPartitions = Number(process.env.KAFKA_PARTITIONS);
  const envReplication = Number(process.env.KAFKA_REPLICATION_FACTOR);

  return {
    partitions: overrides?.partitions ?? (envPartitions || DEFAULT_TOPIC_PARTITIONS),
    replicationFactor: overrides?.replicationFactor ?? (envReplication || DEFAULT_TOPIC_REPLICATION_FACTOR),
  };
}

/** Thrown by a consumer handler to mark a message as a permanent (non-retryable) failure. */
export class NonRetryableError extends Error {
  readonly isNonRetryable = true;

  /**
   * @param message - Human-readable failure reason.
   * @param code - Optional application-specific error code.
   * @param details - Optional structured context (e.g. the field that failed validation).
   */
  constructor(message: string, public readonly code?: string, public readonly details?: unknown) {
    super(message);
    this.name = "NonRetryableError";
  }
}

/**
 * Default poison-pill detection: JSON parse failures and NonRetryableError
 * only. Deliberately narrow — TypeError/RangeError can be transient (e.g. a
 * flaky DB client), so treating them as permanent by default would misroute
 * retryable errors to the DLQ. Override via ConsumerOptions.isPoisonPill.
 *
 * @param error - The error thrown by JSON.parse or the consumer handler.
 * @returns true if the message should be routed to the DLQ instead of retried.
 */
export function isPoisonPill(error: unknown): boolean {
  if (error instanceof SyntaxError) return true;
  if (error instanceof NonRetryableError) return true;
  if (typeof error === "object" && error !== null && "isNonRetryable" in error) return true;
  return false;
}
