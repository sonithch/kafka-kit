import {
  type Kafka,
  type Admin as KafkaJSAdmin,
} from "kafkajs";
import { createLogger, type Logger } from "./logger.js";
import { createKafkaClient } from "./client.js";
import { Producer } from "./producer.js";
import { Consumer } from "./consumer.js";
import {
  type TopicConfig,
  type PublishOptions,
  type ConsumeOptions,
  type KafkaServiceOptions,
} from "./types.js";
import { getTopicConfig } from "./utils.js";

/**
 * Unified, high-level Kafka facade managing connection pooling, idempotent publishing,
 * reliable consumption with Dead Letter Queue routing, and topic provisioning.
 *
 * @example
 * ```typescript
 * import { KafkaService } from "kafkakit";
 *
 * const kafka = new KafkaService({ clientId: "order-service" });
 *
 * // Publish
 * await kafka.publish({
 *   topic: "orders.created",
 *   messages: [{ key: "order-1", value: { id: "order-1", total: 42 } }],
 * });
 *
 * // Consume with automatic DLQ isolation
 * await kafka.consume({
 *   groupId: "order-workers",
 *   topic: "orders.created",
 *   dlqTopic: "orders.dlq",
 *   handler: async (order) => {
 *     console.log("Processing order:", order);
 *   },
 * });
 * ```
 */
export class KafkaService {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly admin: KafkaJSAdmin;
  private readonly consumers: Consumer[] = [];
  private readonly logger: Logger;
  private adminConnectPromise: Promise<void> | null = null;

  /**
   * Initializes the unified KafkaService facade with a single connection pool.
   *
   * @param options Configuration options including required non-empty `clientId`.
   * @throws {TypeError} If `clientId` is missing or empty.
   */
  constructor(options: KafkaServiceOptions) {
    if (!options?.clientId || !options.clientId.trim()) {
      throw new TypeError("KafkaService requires a non-empty 'clientId'");
    }

    this.logger = options.logger ?? createLogger("kafka-kit");
    this.kafka = createKafkaClient(options.clientId, {
      brokers: options.brokers,
      logger: this.logger,
    });
    this.producer = new Producer(this.kafka, this.logger);
    this.admin = this.kafka.admin();
  }

  private async ensureAdminConnected(): Promise<void> {
    if (!this.adminConnectPromise) {
      this.adminConnectPromise = this.admin.connect().catch((err) => {
        this.adminConnectPromise = null;
        throw err;
      });
    }
    return this.adminConnectPromise;
  }

  /**
   * Publishes a batch of messages to a Kafka topic.
   * Automatically serializes JSON and compresses wire payloads using GZIP.
   *
   * @param options Publishing options including `topic` and `messages`.
   */
  async publish(options: PublishOptions): Promise<void> {
    return this.producer.publish(options);
  }

  /**
   * Starts a resilient consumer for the specified topic and group.
   *
   * @template T The expected parsed JSON payload type.
   * @param options Consumer options including `groupId`, `topic`, optional `dlqTopic`, and `handler`.
   * @returns The active Consumer instance.
   */
  async consume<T>(options: ConsumeOptions<T>): Promise<Consumer> {
    const consumer = new Consumer(this.kafka, options, this.producer, this.logger);
    this.consumers.push(consumer);
    await consumer.start(options.handler);
    return consumer;
  }

  /**
   * Idempotently ensures that the specified topics exist in the Kafka cluster.
   * If a topic does not exist, it is created with the requested or default partition count.
   *
   * @param topics Array of topic names (string) or detailed TopicConfig objects.
   * @throws {TypeError} If topics is not an array.
   */
  async ensureTopics(topics: (string | TopicConfig)[]): Promise<void> {
    if (!Array.isArray(topics)) {
      throw new TypeError("ensureTopics requires an array of topic names or TopicConfig objects");
    }

    await this.ensureAdminConnected();

    const existingTopics = await this.admin.listTopics();
    const targetTopics: TopicConfig[] = topics.map((item) =>
      typeof item === "string"
        ? { topic: item, ...getTopicConfig(item) }
        : { topic: item.topic, ...getTopicConfig(item.topic, item) }
    );

    const topicsToCreate = targetTopics
      .filter((t) => !existingTopics.includes(t.topic))
      .map((t) => ({
        topic: t.topic,
        numPartitions: t.partitions ?? 16,
        replicationFactor: t.replicationFactor ?? 1,
      }));

    if (topicsToCreate.length > 0) {
      this.logger.info("Creating Kafka topics", {
        topics: topicsToCreate.map((t) => `${t.topic} (${t.numPartitions}p)`),
      });
      await this.admin.createTopics({
        topics: topicsToCreate,
        waitForLeaders: true,
      });
    }
  }

  /**
   * Retrieves broker cluster health for Kubernetes liveness/readiness probes.
   *
   * @returns Health status object with `isHealthy: boolean` and active `brokers: number`.
   */
  async getClusterHealth(): Promise<{ isHealthy: boolean; brokers: number }> {
    try {
      await this.ensureAdminConnected();
      const cluster = await this.admin.describeCluster();
      return { isHealthy: cluster.brokers.length > 0, brokers: cluster.brokers.length };
    } catch {
      return { isHealthy: false, brokers: 0 };
    }
  }

  /**
   * Gracefully disconnects all underlying Kafka resources (producers, consumers, and admin).
   */
  async disconnect(): Promise<void> {
    this.logger.info("Disconnecting KafkaService resources");
    await Promise.all([
      this.producer.disconnect(),
      this.adminConnectPromise
        ? this.adminConnectPromise.then(() => this.admin.disconnect()).catch(() => {})
        : Promise.resolve(),
      ...this.consumers.map((c) => c.stop()),
    ]);
    this.adminConnectPromise = null;
    this.consumers.length = 0;
  }
}

export * from "./types.js";
export * from "./utils.js";
export * from "./producer.js";
export * from "./consumer.js";
export * from "./client.js";
export * from "./logger.js";
