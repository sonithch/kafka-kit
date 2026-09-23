import type { TopicConfig } from "./types.js";

/**
 * Resolves partition and replication configuration for a given topic with environment fallbacks.
 *
 * @param topic The name of the topic.
 * @param overrides Optional per-topic overrides for partitions and replicationFactor.
 * @returns An object containing resolved `partitions` and `replicationFactor`.
 */
export function getTopicConfig(
  topic: string,
  overrides?: Partial<TopicConfig>
): { partitions: number; replicationFactor: number } {
  const envPartitions = Number(process.env.KAFKA_PARTITIONS);
  const envReplication = Number(process.env.KAFKA_REPLICATION_FACTOR);

  return {
    partitions: overrides?.partitions ?? (envPartitions || 16),
    replicationFactor: overrides?.replicationFactor ?? (envReplication || 1),
  };
}

/**
 * Error indicating that a message cannot be processed successfully under any retry.
 * When thrown inside a consumer handler, the message is classified as a poison pill
 * and forwarded to the configured DLQ immediately without consumer group stalls.
 *
 * @example
 * ```typescript
 * if (!order.userId) {
 *   throw new NonRetryableError("Malformed payload: missing userId");
 * }
 * ```
 */
export class NonRetryableError extends Error {
  readonly isNonRetryable = true;

  constructor(message: string, public readonly code?: string, public readonly details?: unknown) {
    super(message);
    this.name = "NonRetryableError";
  }
}

/**
 * Helper to determine whether an error is a permanent, non-retryable poison pill.
 * Identifies SyntaxError (malformed JSON), NonRetryableError, or any error with `{ isNonRetryable: true }`.
 *
 * @param error The thrown error or rejection reason.
 * @returns `true` if the error should be diverted to DLQ, `false` if it is a transient error.
 */
export function isPoisonPill(error: unknown): boolean {
  if (error instanceof SyntaxError) return true;
  if (error instanceof NonRetryableError) return true;
  if (typeof error === "object" && error !== null && "isNonRetryable" in error) return true;
  return false;
}
