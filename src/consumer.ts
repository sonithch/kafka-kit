import {
  type Kafka,
  type Consumer as KafkaJSConsumer,
  type KafkaMessage,
} from "kafkajs";
import { type ConsumerOptions, type ConsumerHandler } from "./types.js";
import { isPoisonPill } from "./utils.js";
import { type Producer } from "./producer.js";
import { createLogger, type Logger } from "./logger.js";

const defaultLogger = createLogger("kafka-kit-consumer");

/**
 * High-level message consumer with automatic offset management, heartbeating, and Dead Letter Queue routing.
 */
export class Consumer {
  private rawConsumer: KafkaJSConsumer;
  private readonly logger: Logger;

  /**
   * Creates a new Consumer instance.
   *
   * @param kafka The shared KafkaJS client instance.
   * @param options Consumer configuration options (groupId, topic, dlqTopic, etc.).
   * @param producer Optional Producer instance used to forward poison pills to the DLQ.
   * @param logger Optional custom logger.
   */
  constructor(
    kafka: Kafka,
    private readonly options: ConsumerOptions,
    private readonly producer?: Producer,
    logger?: Logger
  ) {
    if (!options?.groupId || !options.groupId.trim()) {
      throw new TypeError("Consumer requires a non-empty 'groupId'");
    }
    if (!options?.topic || !options.topic.trim()) {
      throw new TypeError("Consumer requires a non-empty 'topic'");
    }

    this.logger = logger ?? defaultLogger;
    this.rawConsumer = kafka.consumer({
      groupId: options.groupId.trim(),
      allowAutoTopicCreation: true,
      sessionTimeout: 30000,
    });
  }

  /**
   * Connects the consumer, subscribes to the topic, and begins processing incoming messages.
   *
   * @template T Expected parsed JSON payload type.
   * @param handler Async function invoked for each message.
   * @throws {TypeError} If handler is not a function.
   */
  async start<T>(handler: ConsumerHandler<T>): Promise<void> {
    if (typeof handler !== "function") {
      throw new TypeError("Consumer.start requires a valid handler function");
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
      eachMessage: async ({ message }) => {
        const rawValue = message.value?.toString() ?? "{}";

        try {
          const parsed = JSON.parse(rawValue) as T;
          await handler(parsed, message);
        } catch (err) {
          // If a DLQ is configured and the error is a permanent poison pill, divert to DLQ
          if (this.options.dlqTopic && this.producer && isPoisonPill(err)) {
            this.logger.warn("Poison pill detected, forwarding to DLQ", {
              topic: this.options.topic,
              offset: message.offset,
              dlqTopic: this.options.dlqTopic,
              error: String(err),
            });

            await this.producer.publish({
              topic: this.options.dlqTopic,
              messages: [
                {
                  key: message.key?.toString(),
                  value: {
                    sourceTopic: this.options.topic,
                    offset: message.offset,
                    payload: rawValue,
                    error: String(err),
                    failedAt: Date.now(),
                  },
                },
              ],
            });
            return; // Resolves offset automatically so the consumer group does not block
          }

          // Transient errors are rethrown to let Kafka retry with backoff
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

  /**
   * Gracefully stops the consumer and disconnects from the Kafka group.
   */
  async stop(): Promise<void> {
    await this.rawConsumer.disconnect();
    this.logger.info("Kafka Consumer stopped", { groupId: this.options.groupId });
  }
}
