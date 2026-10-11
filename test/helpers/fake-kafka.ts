import type { Kafka, KafkaMessage } from "kafkajs";

export type EachMessageFn = (args: {
  message: KafkaMessage;
  partition: number;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
}) => Promise<void>;

export interface FakeRawProducer {
  connect: () => Promise<void>;
  send: (args: unknown) => Promise<void>;
  disconnect: () => Promise<void>;
  connectCalls: number;
  sendCalls: unknown[];
  disconnectCalls: number;
  failConnect?: boolean;
  failSend?: boolean;
  failDisconnect?: boolean;
}

export interface FakeRawConsumer {
  connect: () => Promise<void>;
  subscribe: (args: unknown) => Promise<void>;
  run: (opts: { eachMessage: EachMessageFn }) => Promise<void>;
  disconnect: () => Promise<void>;
  deliver: (args: { message: Partial<KafkaMessage>; partition?: number }) => Promise<void>;
  heartbeatCalls: number;
  disconnectCalls: number;
  subscribeCalls: unknown[];
  failDisconnect?: boolean;
  failSubscribe?: boolean;
  /** Called on disconnect, e.g. to record shutdown order. */
  onDisconnect?: () => void;
  events: { CRASH: string; GROUP_JOIN: string };
  on: (event: string, listener: (e: { payload: Record<string, unknown> }) => void) => void;
  /** Fire a kafkajs instrumentation event at the registered listeners. */
  emit: (event: string, payload: Record<string, unknown>) => void;
  connectCalls: number;
  runCalls: number;
  failConnect?: boolean;
}

export interface FakeRawAdmin {
  connect: () => Promise<void>;
  disconnect: () => Promise<void>;
  listTopics: () => Promise<string[]>;
  createTopics: (args: { topics: Array<{ topic: string }> }) => Promise<void>;
  describeCluster: () => Promise<{ brokers: unknown[] }>;
  fetchTopicOffsets: (topic: string) => Promise<Array<{ partition: number; offset: string }>>;
  /** Number of fetchTopicOffsets calls that fail with a "not ready" error first. */
  unknownTopicResponses: number;
  existingTopics: string[];
  createdTopics: Array<{ topic: string }>;
  clusterBrokers: unknown[];
  describeClusterCalls: number;
  failConnect?: boolean;
  failDescribeCluster?: boolean;
  failCreateTopicsWith?: unknown;
}

export function makeFakeRawProducer(): FakeRawProducer {
  const producer: FakeRawProducer = {
    connectCalls: 0,
    sendCalls: [],
    disconnectCalls: 0,
    connect: async () => {
      producer.connectCalls++;
      if (producer.failConnect) throw new Error("producer connect failed");
    },
    send: async (args: unknown) => {
      producer.sendCalls.push(args);
      if (producer.failSend) throw new Error("producer send failed");
    },
    disconnect: async () => {
      producer.disconnectCalls++;
      if (producer.failDisconnect) throw new Error("producer disconnect failed");
    },
  };
  return producer;
}

export function makeFakeRawConsumer(): FakeRawConsumer {
  let eachMessage: EachMessageFn = async () => {};
  const listeners: Record<string, Array<(e: { payload: Record<string, unknown> }) => void>> = {};
  const consumer: FakeRawConsumer = {
    heartbeatCalls: 0,
    disconnectCalls: 0,
    connectCalls: 0,
    runCalls: 0,
    subscribeCalls: [],
    events: { CRASH: "consumer.crash", GROUP_JOIN: "consumer.group_join" },
    on: (event, listener) => {
      (listeners[event] ??= []).push(listener);
    },
    emit: (event, payload) => listeners[event]?.forEach((l) => l({ payload })),
    connect: async () => {
      consumer.connectCalls++;
      if (consumer.failConnect) throw new Error("consumer connect failed");
    },
    subscribe: async (args) => {
      consumer.subscribeCalls.push(args);
      if (consumer.failSubscribe) throw new Error("subscribe failed");
    },
    run: async (opts) => {
      consumer.runCalls++;
      eachMessage = opts.eachMessage;
    },
    disconnect: async () => {
      consumer.disconnectCalls++;
      consumer.onDisconnect?.();
      if (consumer.failDisconnect) throw new Error("consumer disconnect failed");
    },
    deliver: async ({ message, partition = 0 }) =>
      eachMessage({
        message: message as KafkaMessage,
        partition,
        heartbeat: async () => {
          consumer.heartbeatCalls++;
        },
        pause: () => () => {},
      }),
  };
  return consumer;
}

export function makeFakeRawAdmin(overrides?: Partial<FakeRawAdmin>): FakeRawAdmin {
  const admin: FakeRawAdmin = {
    existingTopics: [],
    createdTopics: [],
    clusterBrokers: [{ nodeId: 1 }],
    describeClusterCalls: 0,
    unknownTopicResponses: 0,
    fetchTopicOffsets: async () => {
      if (admin.unknownTopicResponses > 0) {
        admin.unknownTopicResponses--;
        // Shaped like kafkajs: a retries-exceeded error wrapping the protocol error.
        const cause = Object.assign(new Error("This server is not the leader for that topic-partition"), {
          type: "NOT_LEADER_FOR_PARTITION",
        });
        throw Object.assign(new Error(cause.message), { name: "KafkaJSNumberOfRetriesExceeded", cause });
      }
      return [{ partition: 0, offset: "0" }];
    },
    connect: async () => {
      if (admin.failConnect) throw new Error("admin connect failed");
    },
    disconnect: async () => {},
    listTopics: async () => admin.existingTopics,
    createTopics: async (args) => {
      if (admin.failCreateTopicsWith) throw admin.failCreateTopicsWith;
      admin.createdTopics.push(...args.topics);
    },
    describeCluster: async () => {
      admin.describeClusterCalls++;
      if (admin.failDescribeCluster) throw new Error("describeCluster failed");
      return { brokers: admin.clusterBrokers };
    },
    ...overrides,
  };
  return admin;
}

export interface FakeKafka {
  kafka: Kafka;
  producer: FakeRawProducer;
  admin: FakeRawAdmin;
  consumers: FakeRawConsumer[];
  consumerOptions: Record<string, unknown>[];
  producerOptions: Record<string, unknown>[];
}

export function makeFakeKafka(adminOverrides?: Partial<FakeRawAdmin>): FakeKafka {
  const producer = makeFakeRawProducer();
  const admin = makeFakeRawAdmin(adminOverrides);
  const consumers: FakeRawConsumer[] = [];
  const consumerOptions: Record<string, unknown>[] = [];
  const producerOptions: Record<string, unknown>[] = [];

  const kafka = {
    producer: (opts: Record<string, unknown>) => {
      producerOptions.push(opts);
      return producer;
    },
    admin: () => admin,
    consumer: (opts: Record<string, unknown>) => {
      consumerOptions.push(opts);
      const c = makeFakeRawConsumer();
      consumers.push(c);
      return c;
    },
  } as unknown as Kafka;

  return { kafka, producer, admin, consumers, consumerOptions, producerOptions };
}

export function fakeTopicAlreadyExistsError(): Error {
  return Object.assign(new Error("Topic with this name already exists"), {
    type: "TOPIC_ALREADY_EXISTS",
    name: "KafkaJSProtocolError",
  });
}

export function fakeAggregateTopicAlreadyExistsError(): Error {
  return Object.assign(new Error("all topics already exist"), {
    name: "KafkaJSCreateTopicError",
    errors: [{ type: "TOPIC_ALREADY_EXISTS" }, { type: "TOPIC_ALREADY_EXISTS" }],
  });
}

export function fakeAggregateMixedError(): Error {
  return Object.assign(new Error("mixed failure"), {
    name: "KafkaJSCreateTopicError",
    errors: [{ type: "TOPIC_ALREADY_EXISTS" }, { type: "INVALID_REPLICATION_FACTOR" }],
  });
}
