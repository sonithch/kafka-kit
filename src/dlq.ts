import type { KafkaMessage } from "kafkajs";
import { type Producer } from "./producer.js";
import { type Logger } from "./logger.js";
import { type DlqRetryPolicy } from "./types.js";

const DEFAULT_DLQ_PUBLISH_MAX_ATTEMPTS = 3;
const DEFAULT_DLQ_PUBLISH_BASE_DELAY_MS = 200;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Flatten kafkajs's `Buffer | string | (Buffer|string)[]` header values to plain strings. */
export function toStringHeaders(headers?: KafkaMessage["headers"]): Record<string, string> | undefined {
  if (!headers) return undefined;
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    result[key] = Array.isArray(value) ? value.map(String).join(",") : String(value);
  }
  return result;
}

export interface ForwardToDlqParams {
  producer: Producer;
  dlqTopic: string;
  sourceTopic: string;
  partition: number;
  message: KafkaMessage;
  rawValue: string;
  error: unknown;
  logger: Logger;
  /** See {@link DlqRetryPolicy}. Defaults to 3 attempts, 200ms base delay. */
  retry?: DlqRetryPolicy;
}

/**
 * Forward a poison-pill message to its DLQ topic, retrying with backoff.
 * Keeps the original payload/headers as-is and adds failure metadata as
 * extra headers (x-origin-*, x-exception-message, x-failed-at) so the DLQ
 * record stays directly replayable to the source topic.
 *
 * @param params.producer - Used to publish to `dlqTopic`.
 * @param params.dlqTopic - Destination topic for the poison-pill record.
 * @param params.sourceTopic - Topic the message originally came from.
 * @param params.partition - Partition the message originally came from.
 * @param params.message - The raw kafkajs message that failed processing.
 * @param params.rawValue - The message value as a string, forwarded unchanged.
 * @param params.error - The error that made this message a poison pill.
 * @param params.logger - Receives progress/failure logs for the forward attempts.
 * @throws {Error} If every attempt fails. Callers must not swallow that,
 *   since returning normally here is what commits the offset in kafkajs.
 */
export async function forwardToDlq(params: ForwardToDlqParams): Promise<void> {
  const { producer, dlqTopic, sourceTopic, partition, message, rawValue, error, logger } = params;
  const maxAttempts = params.retry?.maxAttempts ?? DEFAULT_DLQ_PUBLISH_MAX_ATTEMPTS;
  const baseDelayMs = params.retry?.baseDelayMs ?? DEFAULT_DLQ_PUBLISH_BASE_DELAY_MS;

  logger.warn("Poison pill detected, forwarding to DLQ", {
    topic: sourceTopic,
    partition,
    offset: message.offset,
    dlqTopic,
    error: String(error),
  });

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await producer.publish({
        topic: dlqTopic,
        messages: [
          {
            key: message.key?.toString(),
            value: rawValue,
            headers: {
              ...(toStringHeaders(message.headers) ?? {}),
              "x-origin-topic": sourceTopic,
              "x-origin-partition": String(partition),
              "x-origin-offset": message.offset,
              "x-exception-message": String(error),
              "x-failed-at": String(Date.now()),
            },
          },
        ],
      });
      return;
    } catch (dlqErr) {
      logger.error("Failed to publish poison pill to DLQ", {
        topic: sourceTopic,
        dlqTopic,
        offset: message.offset,
        attempt,
        error: String(dlqErr),
      });
      if (attempt < maxAttempts) {
        await sleep(baseDelayMs * attempt);
      }
    }
  }

  logger.error("DLQ publish exhausted retries; refusing to drop the message", {
    topic: sourceTopic,
    dlqTopic,
    offset: message.offset,
    payload: rawValue,
  });
  throw new Error(
    `kafka-kit: failed to forward poison-pill message to DLQ '${dlqTopic}' ` +
      `after ${maxAttempts} attempts; refusing to commit offset ${message.offset}`
  );
}
