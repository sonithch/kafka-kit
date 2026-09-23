import {
  type Kafka,
  type Producer as KafkaJSProducer,
  type Message,
  Partitioners,
} from "kafkajs";
import { CompressionTypes } from "./compression.js";
import { createLogger, type Logger } from "./logger.js";
import { type PublishOptions } from "./types.js";

const defaultLogger = createLogger("kafka-kit-producer");

export interface ProducerOptions {
  logger?: Logger;
  allowAutoTopicCreation?: boolean;
  /**
   * Default compression for publish() calls that don't set their own.
   * Defaults to Snappy (registered automatically; see compression.ts) —
   * lower CPU than GZIP and no header-overhead penalty on small payloads.
   * Override per-call via PublishOptions.compression, or register another
   * codec (e.g. LZ4) via registerCompressionCodec and set it here.
   */
  defaultCompression?: CompressionTypes;
}

/**
 * null/undefined -> true tombstone (compaction needs value: null, not "null").
 * Buffers/strings pass through untouched (avoids JSON-mangling binary payloads).
 * Everything else is JSON-serialized.
 */
function serializeValue(value: unknown): string | Buffer | null {
  if (value === null || value === undefined) return null;
  if (Buffer.isBuffer(value) || typeof value === "string") return value;
  return JSON.stringify(value);
}

/** Idempotent, Snappy-compressed-by-default kafkajs producer with lazy, memoized connect. */
export class Producer {
  private rawProducer: KafkaJSProducer;
  private connectPromise: Promise<void> | null = null;
  private readonly logger: Logger;
  private readonly defaultCompression: CompressionTypes;

  constructor(kafka: Kafka, options?: ProducerOptions) {
    this.logger = options?.logger ?? defaultLogger;
    this.defaultCompression = options?.defaultCompression ?? CompressionTypes.Snappy;
    this.rawProducer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      allowAutoTopicCreation: options?.allowAutoTopicCreation ?? false,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  /** Connect once; concurrent callers share the same in-flight connect. Retried automatically on next call if it failed. */
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
   * Publish a batch of messages to a topic, connecting first if needed.
   *
   * @param options.topic - Required, non-empty target topic.
   * @param options.messages - Required, non-empty message batch. See {@link serializeValue} for value handling.
   * @param options.compression - Defaults to {@link ProducerOptions.defaultCompression} (Snappy).
   * @throws {Error} If `topic` is empty or `messages` is empty/not an array.
   * @throws Rethrows the underlying kafkajs send error after logging it (e.g. KafkaJSNotImplemented if `compression` names an unregistered codec).
   */
  async publish(options: PublishOptions): Promise<void> {
    if (!options?.topic) {
      throw new Error("kafka-kit: publish() options must include a non-empty 'topic'");
    }
    if (!Array.isArray(options.messages) || options.messages.length === 0) {
      throw new Error("kafka-kit: publish() options must include a non-empty 'messages' array");
    }

    await this.connect();

    const kafkaMessages: Message[] = options.messages.map((m) => ({
      key: m.key,
      value: serializeValue(m.value),
      headers: m.headers,
    }));

    try {
      await this.rawProducer.send({
        topic: options.topic,
        messages: kafkaMessages,
        compression: options.compression ?? this.defaultCompression,
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

  /** Disconnect if connected; a no-op otherwise. Logs (never throws) if the underlying disconnect fails. */
  async disconnect(): Promise<void> {
    if (this.connectPromise) {
      try {
        await this.connectPromise;
        await this.rawProducer.disconnect();
      } catch (err) {
        this.logger.warn("Error while disconnecting Kafka producer", { error: String(err) });
      } finally {
        this.connectPromise = null;
      }
      this.logger.info("Kafka Producer disconnected");
    }
  }
}
