import {
  type Kafka,
  type Consumer as KafkaJSConsumer,
} from "kafkajs";
import { type ConsumerOptions, type ConsumerHandler } from "./types.js";
import { isPoisonPill as defaultIsPoisonPill } from "./utils.js";
import { forwardToDlq } from "./dlq.js";
import { type Producer } from "./producer.js";
import { createLogger, type Logger } from "./logger.js";

const defaultLogger = createLogger("kafka-kit-consumer");

/** JSON-message consumer with automatic DLQ routing for poison pills. */
export class Consumer {
  private rawConsumer: KafkaJSConsumer;
  private readonly logger: Logger;
  private readonly isPoisonPill: (error: unknown) => boolean;

  /**
   * @param kafka - Shared kafkajs client.
   * @param options.groupId - Required, non-empty consumer group id.
   * @param options.topic - Required, non-empty topic to subscribe to.
   * @param options.dlqTopic - If set (with a producer), poison pills are forwarded here instead of retried.
   * @param options.isPoisonPill - Overrides the default poison-pill classification.
   * @param options.sessionTimeout - kafkajs session timeout in ms; defaults to 30000.
   * @param options.dlqRetry - Retry policy for DLQ forwarding; see {@link DlqRetryPolicy}.
   * @param producer - Used to publish to `dlqTopic`; required for DLQ routing.
   * @param allowAutoTopicCreation - Passed through to the underlying kafkajs consumer.
   * @throws {Error} If `groupId` or `topic` is empty.
   */
  constructor(
    kafka: Kafka,
    private readonly options: ConsumerOptions,
    private readonly producer?: Producer,
    logger?: Logger,
    allowAutoTopicCreation = false
  ) {
    if (!options?.groupId || !options.groupId.trim()) {
      throw new Error("Consumer requires a non-empty 'groupId'");
    }
    if (!options?.topic || !options.topic.trim()) {
      throw new Error("Consumer requires a non-empty 'topic'");
    }

    this.logger = logger ?? defaultLogger;
    this.isPoisonPill = options.isPoisonPill ?? defaultIsPoisonPill;
    this.rawConsumer = kafka.consumer({
      groupId: options.groupId,
      allowAutoTopicCreation,
      sessionTimeout: options.sessionTimeout ?? 30000,
    });
  }

  /**
   * Connect, subscribe, and start consuming. Each message is JSON-parsed and
   * passed to `handler` along with a {@link ConsumerContext} (heartbeat/pause
   * for slow handlers); a poison pill (see {@link defaultIsPoisonPill}) is
   * forwarded to the DLQ instead of retried, everything else is rethrown so
   * kafkajs retries it. A Kafka tombstone (value: null) is passed through as
   * `null`, not JSON-parsed.
   *
   * @param handler - Invoked with the parsed payload, the raw kafkajs message, and the consumer context.
   * @throws {Error} If `handler` is not a function.
   */
  async start<T>(handler: ConsumerHandler<T>): Promise<void> {
    if (typeof handler !== "function") {
      throw new Error("Consumer.start() requires a valid handler function");
    }

    await this.rawConsumer.connect();
    await this.rawConsumer.subscribe({
      topic: this.options.topic,
      fromBeginning: this.options.fromBeginning ?? false,
    });

    this.logger.info("Kafka Consumer started listening", {
      topic: this.options.topic,
      groupId: this.options.groupId,
    });

    await this.rawConsumer.run({
      eachMessage: async ({ message, partition, heartbeat, pause }) => {
        const rawValue = message.value === null || message.value === undefined ? null : message.value.toString();
        const isTombstone = rawValue === null;

        try {
          const parsed = (isTombstone ? null : JSON.parse(rawValue as string)) as T;
          await handler(parsed, message, {
            topic: this.options.topic,
            partition,
            heartbeat,
            pause,
          });
        } catch (err) {
          if (this.options.dlqTopic && this.producer && this.isPoisonPill(err)) {
            await forwardToDlq({
              producer: this.producer,
              dlqTopic: this.options.dlqTopic,
              sourceTopic: this.options.topic,
              partition,
              message,
              rawValue: rawValue ?? "null",
              error: err,
              logger: this.logger,
              retry: this.options.dlqRetry,
            });
            return;
          }

          this.logger.error("Transient error during message processing, triggering retry", {
            topic: this.options.topic,
            offset: message.offset,
            error: String(err),
          });
          throw err;
        }
      },
    });
  }

  /** Disconnect the underlying kafkajs consumer. */
  async stop(): Promise<void> {
    await this.rawConsumer.disconnect();
    this.logger.info("Kafka Consumer stopped", { groupId: this.options.groupId });
  }
}
