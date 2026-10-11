import {
  type Kafka,
  type Consumer as KafkaJSConsumer,
  type EachMessagePayload,
} from "kafkajs";
import { type ConsumerOptions, type ConsumerHandler } from "./types.js";
import { isPoisonPill as defaultIsPoisonPill, retryTopicName } from "./utils.js";
import { forwardToDlq } from "./dlq.js";
import { type Producer } from "./producer.js";
import { createLogger, type Logger } from "./logger.js";
import { sleep, withJitter } from "./backoff.js";

const defaultLogger = createLogger("kafka-kit-consumer");

const DEFAULT_RETRY_ATTEMPTS = 3;
const DEFAULT_RETRY_BACKOFF_MS = 200;
/** Max sleep between heartbeats while waiting for a retry message to be due. */
const RETRY_WAIT_HEARTBEAT_MS = 3000;
const CRASH_RESTART_BASE_MS = 1000;
const CRASH_RESTART_MAX_MS = 30_000;

interface Stage {
  /** 0 = main topic, N = retry topic N. */
  stage: number;
  topic: string;
  raw: KafkaJSConsumer;
  /** Consecutive restarts since the group was last joined. */
  restarts: number;
  restartTimer?: NodeJS.Timeout;
}

/** JSON-message consumer with automatic DLQ routing for poison pills and optional bounded retries. */
export class Consumer {
  private readonly stages: Stage[];
  private readonly logger: Logger;
  private readonly isPoisonPill: (error: unknown) => boolean;
  private stopped = false;

  /**
   * @param kafka - Shared kafkajs client.
   * @param options.groupId - Required, non-empty consumer group id.
   * @param options.topic - Required, non-empty topic to subscribe to.
   * @param options.dlqTopic - If set (with a producer), poison pills are forwarded here instead of retried.
   * @param options.isPoisonPill - Overrides the default poison-pill classification.
   * @param options.sessionTimeout - kafkajs session timeout in ms; defaults to 30000.
   * @param options.dlqRetry - Retry policy for DLQ forwarding; see {@link DlqRetryPolicy}.
   * @param options.retry - Bounded retries for transient failures; see {@link ConsumerRetryPolicy}.
   * @param producer - Used to publish to `dlqTopic` and retry topics; required for either.
   * @param allowAutoTopicCreation - Passed through to the underlying kafkajs consumers.
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

    const make = (groupId: string) =>
      kafka.consumer({
        groupId,
        allowAutoTopicCreation,
        sessionTimeout: options.sessionTimeout ?? 30000,
      });

    // Each retry stage gets its own group, so waiting for a delayed message
    // only holds that stage, never the main topic.
    this.stages = [
      { stage: 0, topic: options.topic, raw: make(options.groupId), restarts: 0 },
      ...(options.retry?.topicDelaysMs ?? []).map((_, i) => ({
        stage: i + 1,
        topic: retryTopicName(options.topic, i + 1),
        raw: make(retryTopicName(options.groupId, i + 1)),
        restarts: 0,
      })),
    ];
  }

  /**
   * Connect, subscribe, and start consuming. Each message is JSON-parsed and
   * passed to `handler` along with a {@link ConsumerContext} (heartbeat/pause
   * for slow handlers). A poison pill (see {@link defaultIsPoisonPill}) goes
   * to the DLQ. Other errors are retried per `options.retry`, then moved to
   * the next retry topic or the DLQ; without `options.retry` they are
   * rethrown so kafkajs retries them. A Kafka tombstone (value: null) is
   * passed through as `null`, not JSON-parsed.
   *
   * If startup fails, every consumer is disconnected before rethrowing.
   *
   * @param handler - Invoked with the parsed payload, the raw kafkajs message, and the consumer context.
   * @throws {Error} If `handler` is not a function.
   */
  async start<T>(handler: ConsumerHandler<T>): Promise<void> {
    if (typeof handler !== "function") {
      throw new Error("Consumer.start() requires a valid handler function");
    }

    try {
      for (const s of this.stages) {
        this.watchCrashes(s, handler);
        await this.run(s, handler);
      }
    } catch (err) {
      // Don't leave connected consumers behind when startup fails.
      await Promise.allSettled(this.stages.map((s) => s.raw.disconnect()));
      throw err;
    }

    this.logger.info("Kafka Consumer started listening", {
      topic: this.options.topic,
      groupId: this.options.groupId,
      retryStages: this.stages.length - 1,
    });
  }

  private async run<T>(s: Stage, handler: ConsumerHandler<T>): Promise<void> {
    await s.raw.connect();
    await s.raw.subscribe({
      topics: [s.topic],
      // Retry topics must always be read in full.
      fromBeginning: s.stage > 0 || (this.options.fromBeginning ?? false),
    });
    await s.raw.run({ eachMessage: (payload) => this.handleMessage(handler, s.stage, payload) });
  }

  /**
   * kafkajs only restarts a crashed consumer for retriable errors; others
   * (e.g. group coordinator not found while a broker starts) leave it dead
   * for good. Restart those ourselves with capped, jittered backoff.
   */
  private watchCrashes<T>(s: Stage, handler: ConsumerHandler<T>): void {
    s.raw.on(s.raw.events.GROUP_JOIN, () => {
      s.restarts = 0;
    });
    s.raw.on(s.raw.events.CRASH, ({ payload }) => {
      if (!payload.restart) this.scheduleRestart(s, handler, payload.error);
    });
  }

  private scheduleRestart<T>(s: Stage, handler: ConsumerHandler<T>, error: unknown): void {
    if (this.stopped) return;

    const delayMs = withJitter(Math.min(CRASH_RESTART_BASE_MS * 2 ** s.restarts, CRASH_RESTART_MAX_MS));
    s.restarts++;
    this.logger.error("Kafka consumer crashed; restarting", {
      topic: s.topic,
      restart: s.restarts,
      delayMs: Math.round(delayMs),
      error: String(error),
    });

    if (s.restartTimer) clearTimeout(s.restartTimer);
    s.restartTimer = setTimeout(() => {
      s.restartTimer = undefined;
      if (this.stopped) return;
      // A crash after run() emits CRASH again; a failure inside it lands here.
      this.run(s, handler).catch((err) => this.scheduleRestart(s, handler, err));
    }, delayMs);
  }

  private async handleMessage<T>(
    handler: ConsumerHandler<T>,
    stage: number,
    { message, partition, heartbeat, pause }: EachMessagePayload
  ): Promise<void> {
    if (stage > 0) {
      // Each retry topic has one fixed delay, so messages become due in
      // order: waiting on the head message is enough.
      const delayMs = this.options.retry!.topicDelaysMs![stage - 1]!;
      const dueAt = Number(message.timestamp) + delayMs;
      for (let wait = dueAt - Date.now(); wait > 0; wait = dueAt - Date.now()) {
        await sleep(Math.min(wait, RETRY_WAIT_HEARTBEAT_MS));
        await heartbeat();
      }
    }

    const rawValue = message.value === null || message.value === undefined ? null : message.value.toString();
    const isTombstone = rawValue === null;

    try {
      const parsed = (isTombstone ? null : JSON.parse(rawValue as string)) as T;
      await this.runWithRetries(
        () => handler(parsed, message, { topic: this.options.topic, partition, heartbeat, pause }),
        heartbeat
      );
    } catch (err) {
      const forward = (topic: string, extra?: { reason: string; headers: Record<string, string> }) =>
        forwardToDlq({
          producer: this.producer!,
          dlqTopic: topic,
          sourceTopic: this.options.topic,
          partition,
          message,
          rawValue: rawValue ?? "null",
          error: err,
          logger: this.logger,
          retry: this.options.dlqRetry,
          ...extra,
        });

      if (this.options.dlqTopic && this.producer && this.isPoisonPill(err)) {
        await forward(this.options.dlqTopic);
        return;
      }

      if (this.options.retry && this.producer) {
        if (stage < this.stages.length - 1) {
          await forward(retryTopicName(this.options.topic, stage + 1), {
            reason: "Handler retries exhausted, moving message to retry topic",
            headers: { "x-retry-count": String(stage + 1) },
          });
          return;
        }
        if (this.options.dlqTopic) {
          await forward(this.options.dlqTopic, {
            reason: "All retries exhausted, forwarding to DLQ",
            headers: { "x-retry-count": String(stage) },
          });
          return;
        }
      }

      this.logger.error("Transient error during message processing, triggering retry", {
        topic: this.options.topic,
        offset: message.offset,
        error: String(err),
      });
      throw err;
    }
  }

  /** Run `fn` up to retry.attempts times with exponential backoff (jittered by default); poison pills are never retried. */
  private async runWithRetries(fn: () => Promise<void>, heartbeat: () => Promise<void>): Promise<void> {
    const attempts = this.options.retry ? (this.options.retry.attempts ?? DEFAULT_RETRY_ATTEMPTS) : 1;
    const backoffMs = this.options.retry?.backoffMs ?? DEFAULT_RETRY_BACKOFF_MS;
    const jitter = this.options.retry?.jitter ?? true;

    for (let attempt = 1; ; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (attempt >= attempts || this.isPoisonPill(err)) throw err;
        this.logger.warn("Handler failed, retrying in place", {
          topic: this.options.topic,
          attempt,
          error: String(err),
        });
        const delayMs = backoffMs * 2 ** (attempt - 1);
        await sleep(jitter ? withJitter(delayMs) : delayMs);
        await heartbeat();
      }
    }
  }

  /** Disconnect the underlying kafkajs consumers. Throws the first failure after trying them all. */
  async stop(): Promise<void> {
    this.stopped = true;
    for (const s of this.stages) if (s.restartTimer) clearTimeout(s.restartTimer);
    const results = await Promise.allSettled(this.stages.map((s) => s.raw.disconnect()));
    const failed = results.find((r) => r.status === "rejected");
    if (failed) throw failed.reason;
    this.logger.info("Kafka Consumer stopped", { groupId: this.options.groupId });
  }
}
