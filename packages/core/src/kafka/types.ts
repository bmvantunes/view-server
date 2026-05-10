import type { Effect } from "effect";
import type { KafkaConsumerRecord } from "../config/index.ts";
import type { ViewServerError } from "../errors.ts";

export type KafkaRecordBatch = {
  readonly records: readonly KafkaConsumerRecord[];
  readonly commit: Effect.Effect<void, ViewServerError>;
};

export type KafkaTopicConsumerRunArgs = {
  readonly topic: string;
  readonly groupId: string;
  readonly commitPolicy: "after-ingest" | "none";
  readonly onBatch: (batch: KafkaRecordBatch) => Effect.Effect<void, ViewServerError>;
};

export type KafkaTopicConsumer = {
  readonly run: (args: KafkaTopicConsumerRunArgs) => Effect.Effect<void, ViewServerError>;
};

export type KafkaTopicVerificationArgs = {
  readonly brokers: readonly string[];
  readonly topics: readonly string[];
};

export type KafkaTopicVerifier = {
  readonly verifyTopics: (args: KafkaTopicVerificationArgs) => Effect.Effect<void, ViewServerError>;
};
