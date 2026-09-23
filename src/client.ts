import { Kafka, type KafkaConfig, logLevel } from "kafkajs";
import { createLogger, type Logger } from "./logger.js";

const defaultLogger = createLogger("kafka-kit-client");

export function getBrokerList(configuredBrokers?: string[]): string[] {
  if (configuredBrokers && configuredBrokers.length > 0) {
    return configuredBrokers;
  }
  const envBrokers = process.env.KAFKA_BROKERS;
  if (!envBrokers) {
    return ["localhost:9092"];
  }
  return envBrokers.split(",").map((b) => b.trim());
}

export function createKafkaClient(
  clientId: string,
  options?: { brokers?: string[]; logger?: Logger; config?: Partial<KafkaConfig> }
): Kafka {
  if (!clientId || !clientId.trim()) {
    throw new Error("kafkajs clientId is required and cannot be empty");
  }

  const cleanClientId = clientId.trim();
  const brokers = getBrokerList(options?.brokers);
  const log = options?.logger ?? defaultLogger;
  log.info("Initializing shared Kafka client", { brokers, clientId: cleanClientId });

  return new Kafka({
    clientId: cleanClientId,
    brokers,
    logLevel: logLevel.WARN,
    logCreator: () => (entry) => {
      const { level, log: entryLog } = entry;
      const { message, ...extra } = entryLog;
      if (level === logLevel.ERROR) {
        log.error(message, extra);
      } else if (level === logLevel.WARN) {
        log.warn(message, extra);
      }
    },
    retry: {
      initialRetryTime: 100,
      maxRetryTime: 30000,
      factor: 2,
    },
    ...options?.config,
  });
}
