import { Admin, Consumer, type Message, type Offsets } from "@platformatic/kafka";
import { Effect } from "effect";
import type { KafkaSourceConfig, RowObject } from "../config/index.ts";
import { kafkaIngestFailed, isViewServerError, type ViewServerError } from "../errors.ts";
import type { KafkaTopicConsumer, KafkaTopicVerifier } from "./types.ts";

export type PlatformaticKafkaTopicConsumerOptions = {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly batchSize?: number | undefined;
  readonly sessionTimeout?: number | undefined;
  readonly heartbeatInterval?: number | undefined;
  readonly lagMonitoringIntervalMs?: number | undefined;
};

export type PlatformaticKafkaConsumerFactoryOptions = Omit<
  PlatformaticKafkaTopicConsumerOptions,
  "brokers" | "clientId"
> & {
  readonly clientIdPrefix?: string | undefined;
};

export type PlatformaticKafkaTopicVerifierOptions = {
  readonly clientId?: string | undefined;
  readonly timeout?: number | undefined;
  readonly retries?: number | boolean | undefined;
};

type PlatformaticMessage = Message<
  Buffer | undefined,
  Buffer | undefined,
  Buffer | undefined,
  Buffer | undefined
>;

export function createPlatformaticKafkaTopicConsumer(
  options: PlatformaticKafkaTopicConsumerOptions,
): KafkaTopicConsumer {
  return {
    run: (args) =>
      Effect.fn("view-server.kafka.platformatic.run")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": args.topic,
        });
        yield* Effect.tryPromise({
          try: async (signal) => {
            const consumer = new Consumer({
              clientId: options.clientId,
              groupId: args.groupId,
              bootstrapBrokers: [...options.brokers],
            });
            const latestLagByPartition = new Map<number, bigint>();
            const onLag = (lag: Offsets) => {
              latestLagByPartition.clear();
              const topicLag = lag.get(args.topic);
              if (topicLag === undefined) {
                return;
              }
              topicLag.forEach((partitionLag, partition) => {
                if (partitionLag >= 0n) {
                  latestLagByPartition.set(partition, partitionLag);
                }
              });
            };
            consumer.on("consumer:lag", onLag);
            const stream = await consumer.consume({
              topics: [args.topic],
              autocommit: false,
              sessionTimeout: options.sessionTimeout,
              heartbeatInterval: options.heartbeatInterval,
            });
            consumer.startLagMonitoring(
              { topics: [args.topic] },
              options.lagMonitoringIntervalMs ?? 1_000,
            );
            const pending: PlatformaticMessage[] = [];

            const flush = async () => {
              if (pending.length === 0) {
                return;
              }
              const messages = pending.splice(0, pending.length);
              const lag = maxLagForMessages(messages, latestLagByPartition);
              await Effect.runPromise(
                args.onBatch({
                  records: messages.map(toKafkaConsumerRecord),
                  ...(lag === undefined ? {} : { lag }),
                  commit:
                    args.commitPolicy === "after-ingest"
                      ? commitMessages(args.topic, messages, lag)
                      : Effect.void,
                }),
              );
            };

            signal.addEventListener("abort", () => {
              void stream.close().finally(() => {
                void closeConsumer(consumer);
              });
            });

            try {
              for await (const message of stream) {
                if (signal.aborted) {
                  break;
                }
                pending.push(message);
                if (pending.length >= (options.batchSize ?? 100)) {
                  await flush();
                }
              }
              await flush();
            } finally {
              consumer.stopLagMonitoring();
              consumer.off("consumer:lag", onLag);
              await stream.close();
              await closeConsumer(consumer);
            }
          },
          catch: (error) =>
            isViewServerError(error) ? error : kafkaIngestFailed(args.topic, error),
        });
      })(),
  };
}

export function createPlatformaticKafkaConsumerFactory(
  options: PlatformaticKafkaConsumerFactoryOptions = {},
): (source: KafkaSourceConfig<RowObject, string>) => KafkaTopicConsumer {
  return (source) =>
    createPlatformaticKafkaTopicConsumer({
      brokers: source.brokers,
      clientId: `${options.clientIdPrefix ?? "view-server"}-${source.topic}`,
      batchSize: options.batchSize,
      sessionTimeout: options.sessionTimeout,
      heartbeatInterval: options.heartbeatInterval,
    });
}

export function createPlatformaticKafkaTopicVerifier(
  options: PlatformaticKafkaTopicVerifierOptions = {},
): KafkaTopicVerifier {
  return {
    verifyTopics: (args) =>
      Effect.fn("view-server.kafka.platformatic.verify_topics")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.batch_size": args.topics.length,
        });
        yield* Effect.tryPromise({
          try: async () => {
            const admin = new Admin({
              clientId: options.clientId ?? "view-server-topic-verifier",
              bootstrapBrokers: [...args.brokers],
              timeout: options.timeout,
              retries: options.retries,
            });
            try {
              const metadata = await admin.metadata({
                topics: [...args.topics],
                autocreateTopics: false,
                forceUpdate: true,
              });
              const missing = args.topics.filter((topic) => !metadata.topics.has(topic));
              if (missing.length > 0) {
                throw new Error(`Kafka topics not found: ${missing.join(", ")}`);
              }
            } finally {
              await admin.close();
            }
          },
          catch: (error) => kafkaIngestFailed(args.topics[0] ?? "__kafka", error),
        });
      })(),
  };
}

function closeConsumer(consumer: Consumer): Promise<void> {
  return new Promise((resolve, reject) => {
    consumer.close(true, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function commitMessages(
  topic: string,
  messages: readonly PlatformaticMessage[],
  lag: number | undefined,
): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.kafka.platformatic.commit")(function* () {
    const lastMessage = messages[messages.length - 1];
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": topic,
      "view_server.batch_size": messages.length,
      ...(lag === undefined ? {} : { "view_server.kafka.lag": lag }),
      ...(lastMessage === undefined
        ? {}
        : {
            "view_server.kafka.partition": lastMessage.partition,
            "view_server.kafka.offset": lastMessage.offset.toString(),
          }),
    });
    yield* Effect.tryPromise({
      try: async () => {
        for (const message of messages) {
          await message.commit();
        }
      },
      catch: (error) => kafkaIngestFailed(topic, error),
    });
  })();
}

function maxLagForMessages(
  messages: readonly PlatformaticMessage[],
  latestLagByPartition: ReadonlyMap<number, bigint>,
): number | undefined {
  let maxLag: bigint | undefined;
  for (const message of messages) {
    const lag = latestLagByPartition.get(message.partition);
    if (lag !== undefined && (maxLag === undefined || lag > maxLag)) {
      maxLag = lag;
    }
  }
  return spanSafeNumber(maxLag);
}

function spanSafeNumber(value: bigint | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value <= 0n) {
    return 0;
  }
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number.MAX_SAFE_INTEGER;
  }
  return Number(value);
}

function toKafkaConsumerRecord(message: PlatformaticMessage) {
  const headers: Record<string, Uint8Array | undefined> = {};
  for (const [key, value] of message.headers) {
    if (key !== undefined) {
      headers[key.toString("utf8")] = value;
    }
  }
  return {
    topic: message.topic,
    key: message.key,
    value: message.value ?? null,
    headers,
    partition: message.partition,
    offset: message.offset.toString(),
    timestamp: message.timestamp,
  };
}
