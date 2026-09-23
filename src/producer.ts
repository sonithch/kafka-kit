import {
  type Kafka,
  type Producer as KafkaJSProducer,
  type Message,
  CompressionTypes,
  Partitioners,
} from "kafkajs";
import { createLogger, type Logger } from "./logger.js";
import { type PublishOptions } from "./types.js";

const defaultLogger = createLogger("kafka-kit-producer");

/**
 * Resilient Kafka message producer with idempotent semantics, GZIP compression, and promise memoization.
 */
export class Producer {
  private rawProducer: KafkaJSProducer;
  private connectPromise: Promise<void> | null = null;
  private readonly logger: Logger;

  /**
   * Creates a new Producer instance using the shared Kafka connection pool.
   *
   * @param kafka The shared KafkaJS client instance.
   * @param logger Optional custom logger.
   */
  constructor(kafka: Kafka, logger?: Logger) {
    this.logger = logger ?? defaultLogger;
    this.rawProducer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      allowAutoTopicCreation: true,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  /**
   * Idempotently connects the producer to the Kafka broker cluster.
   * Concurrent invocations during startup safely share a single connection handshake.
   */
  async connect(): Promise<void> {
    if (!this.connectPromise) {
      this.connectPromise = this.rawProducer.connect().catch((err) => {
        this.connectPromise = null;
        throw err;
      });
    }
    return this.connectPromise;
  }

  /**
   * Publishes a batch of messages to a Kafka topic with automatic wire compression and serialization.
   *
   * @param options Publishing options including topic name, messages array, and optional compression.
   * @throws {TypeError} If topic is not a non-empty string or messages array is empty.
   * @throws {Error} If publishing fails across all configured broker retries.
   */
  async publish(options: PublishOptions): Promise<void> {
    if (!options?.topic || !options.topic.trim()) {
      throw new TypeError("Publish options must include a non-empty 'topic'");
    }
    if (!Array.isArray(options?.messages) || options.messages.length === 0) {
      throw new TypeError("Publish options must include a non-empty 'messages' array");
    }

    await this.connect();

    const kafkaMessages: Message[] = options.messages.map((m) => ({
      key: m.key,
      value: typeof m.value === "string" ? m.value : JSON.stringify(m.value),
      headers: m.headers,
    }));

    try {
      await this.rawProducer.send({
        topic: options.topic.trim(),
        messages: kafkaMessages,
        compression: options.compression ?? CompressionTypes.GZIP,
      });
    } catch (error) {
      this.logger.error("Failed to publish message batch to Kafka", {
        topic: options.topic,
        batchSize: options.messages.length,
        error: String(error),
      });
      throw error;
    }
  }

  /**
   * Gracefully disconnects the producer from Kafka brokers, ensuring in-flight connects finish cleanly.
   */
  async disconnect(): Promise<void> {
    if (this.connectPromise) {
      try {
        await this.connectPromise;
        await this.rawProducer.disconnect();
      } catch {
        // Suppress disconnection error during shutdown
      } finally {
        this.connectPromise = null;
      }
      this.logger.info("Kafka Producer disconnected");
    }
  }
}
