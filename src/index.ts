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
import { getTopicConfig, DEFAULT_TOPIC_PARTITIONS, DEFAULT_TOPIC_REPLICATION_FACTOR } from "./utils.js";

const DEFAULT_HEALTH_CHECK_TTL_MS = 10_000;

/** Unified, pooled Kafka client: one producer/admin connection shared across publish/consume/ensureTopics. */
export class KafkaService {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly admin: KafkaJSAdmin;
  private readonly consumers: Consumer[] = [];
  private readonly logger: Logger;
  private readonly allowAutoTopicCreation: boolean;
  private readonly healthCheckTtlMs: number;
  private adminConnectPromise: Promise<void> | null = null;
  private healthCache: { result: { isHealthy: boolean; brokers: number }; expiresAt: number } | null = null;

  /**
   * @param options.clientId - Required, non-empty kafkajs client id.
   * @param options.brokers - See {@link getBrokerList}.
   * @param options.logger - Custom structured logger; defaults to JSON-on-stdout.
   * @param options.allowAutoTopicCreation - See {@link KafkaServiceOptions.allowAutoTopicCreation}.
   * @param options.kafka - Bring your own kafkajs client instead of building one.
   * @param options.healthCheckTtlMs - See {@link KafkaServiceOptions.healthCheckTtlMs}.
   * @param options.defaultCompression - See {@link ProducerOptions.defaultCompression}.
   * @throws {Error} If `clientId` is empty or whitespace-only.
   */
  constructor(options: KafkaServiceOptions) {
    if (!options?.clientId || !options.clientId.trim()) {
      throw new Error("KafkaService requires a non-empty 'clientId'");
    }

    this.logger = options.logger ?? createLogger("kafka-kit");
    this.allowAutoTopicCreation = options.allowAutoTopicCreation ?? false;
    this.healthCheckTtlMs = options.healthCheckTtlMs ?? DEFAULT_HEALTH_CHECK_TTL_MS;
    this.kafka =
      options.kafka ??
      createKafkaClient(options.clientId, {
        brokers: options.brokers,
        logger: this.logger,
      });
    this.producer = new Producer(this.kafka, {
      logger: this.logger,
      allowAutoTopicCreation: this.allowAutoTopicCreation,
      defaultCompression: options.defaultCompression,
    });
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

  /** See {@link Producer.publish}. */
  async publish(options: PublishOptions): Promise<void> {
    return this.producer.publish(options);
  }

  /**
   * Start a new {@link Consumer} and register it for {@link disconnect}.
   * The consumer is only registered once `start()` succeeds; a failed start
   * is logged and rethrown without leaving an orphaned consumer behind.
   *
   * @throws Rethrows any error from {@link Consumer.start}.
   */
  async consume<T>(options: ConsumeOptions<T>): Promise<Consumer> {
    const consumer = new Consumer(
      this.kafka,
      options,
      this.producer,
      this.logger,
      this.allowAutoTopicCreation
    );

    try {
      await consumer.start(options.handler);
    } catch (err) {
      this.logger.error("Failed to start Kafka consumer", {
        topic: options.topic,
        groupId: options.groupId,
        error: String(err),
      });
      throw err;
    }

    this.consumers.push(consumer);
    return consumer;
  }

  /**
   * Create any topics in `topics` that don't already exist, with partition
   * counts resolved via {@link getTopicConfig}. Tolerates a concurrent
   * caller winning the create race (TOPIC_ALREADY_EXISTS).
   *
   * @param topics - Topic names, or {@link TopicConfig} objects for per-topic partition/RF overrides.
   * @throws {Error} If `topics` is not an array, or on any topic-creation failure other than TOPIC_ALREADY_EXISTS.
   */
  async ensureTopics(topics: (string | TopicConfig)[]): Promise<void> {
    if (!Array.isArray(topics)) {
      throw new Error("ensureTopics() requires an array of topic names or TopicConfig objects");
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
      // t.partitions/replicationFactor are always set here (getTopicConfig
      // already resolved them above); these fallbacks just satisfy
      // TopicConfig's optional typing, not a live default path.
      .map((t) => ({
        topic: t.topic,
        numPartitions: t.partitions ?? DEFAULT_TOPIC_PARTITIONS,
        replicationFactor: t.replicationFactor ?? DEFAULT_TOPIC_REPLICATION_FACTOR,
      }));

    if (topicsToCreate.length > 0) {
      this.logger.info("Creating Kafka topics", {
        topics: topicsToCreate.map((t) => `${t.topic} (${t.numPartitions}p)`),
      });

      const rf1Topics = topicsToCreate.filter((t) => t.replicationFactor === 1);
      if (rf1Topics.length > 0) {
        this.logger.warn(
          "ensureTopics: creating topic(s) with replicationFactor 1 — no fault tolerance; fine for local dev, risky on a real cluster",
          { topics: rf1Topics.map((t) => t.topic) }
        );
      }

      try {
        await this.admin.createTopics({
          topics: topicsToCreate,
          waitForLeaders: true,
        });
      } catch (err) {
        // Concurrent replicas can race here; the loser gets
        // TOPIC_ALREADY_EXISTS, which isn't a real failure.
        if (isTopicAlreadyExistsError(err)) {
          this.logger.warn("ensureTopics: topic already created by a concurrent caller", {
            topics: topicsToCreate.map((t) => t.topic),
          });
        } else {
          throw err;
        }
      }
    }
  }

  /**
   * Check cluster reachability, caching the result for `healthCheckTtlMs`
   * (default 10s) to avoid flooding the broker with admin RPCs from
   * frequent liveness/readiness probes. Never throws — any failure
   * (including a failed admin connect) is logged and reported as unhealthy.
   *
   * @returns `isHealthy: true` iff the admin connection and `describeCluster()` succeed and report at least one broker.
   */
  async getClusterHealth(): Promise<{ isHealthy: boolean; brokers: number }> {
    const now = Date.now();
    if (this.healthCache && this.healthCache.expiresAt > now) {
      return this.healthCache.result;
    }

    try {
      await this.ensureAdminConnected();
      const cluster = await this.admin.describeCluster();
      const result = { isHealthy: cluster.brokers.length > 0, brokers: cluster.brokers.length };
      // Only cache successes: caching a failure would delay detecting
      // recovery by up to a full TTL on a liveness/readiness probe.
      this.healthCache = { result, expiresAt: now + this.healthCheckTtlMs };
      return result;
    } catch (err) {
      this.logger.warn("Cluster health check failed", { error: String(err) });
      this.healthCache = null;
      return { isHealthy: false, brokers: 0 };
    }
  }

  /**
   * Disconnect the producer, admin (if connected), and every registered
   * consumer. Uses allSettled so one resource failing to disconnect doesn't
   * stop the others from being torn down; failures are logged, not thrown.
   */
  async disconnect(): Promise<void> {
    this.logger.info("Disconnecting KafkaService resources");
    const results = await Promise.allSettled([
      this.producer.disconnect(),
      this.adminConnectPromise
        ? this.adminConnectPromise.then(() => this.admin.disconnect())
        : Promise.resolve(),
      ...this.consumers.map((c) => c.stop()),
    ]);

    for (const result of results) {
      if (result.status === "rejected") {
        this.logger.warn("Error while disconnecting a Kafka resource", {
          error: String(result.reason),
        });
      }
    }

    this.adminConnectPromise = null;
    this.healthCache = null;
    this.consumers.length = 0;
  }
}

function isTopicAlreadyExistsError(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;

  if ("type" in err && (err as { type?: unknown }).type === "TOPIC_ALREADY_EXISTS") {
    return true;
  }

  // kafkajs surfaces multi-topic failures as an aggregate error with an
  // `errors` array rather than a top-level `type`.
  if ("errors" in err && Array.isArray((err as { errors?: unknown[] }).errors)) {
    const errors = (err as { errors: Array<{ type?: unknown }> }).errors;
    return errors.length > 0 && errors.every((e) => e?.type === "TOPIC_ALREADY_EXISTS");
  }

  return false;
}

export * from "./types.js";
export * from "./utils.js";
export { Producer, type ProducerOptions } from "./producer.js";
export { Consumer } from "./consumer.js";
export { createKafkaClient, getBrokerList } from "./client.js";
export { CompressionTypes, registerCompressionCodec } from "./compression.js";
export type { Logger } from "./logger.js";
// dlq.js and logger.js's createLogger are internal, not public API.
