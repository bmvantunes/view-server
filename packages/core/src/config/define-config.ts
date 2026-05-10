import { Effect, Schema } from "effect";
import type { ViewServerError } from "../errors.ts";

export const VIEW_SERVER_HEALTH_TOPIC = "__view_server_health";
export const RESERVED_TOPIC_PREFIX = "__";

export type RowObject = Record<string, unknown>;
export type IdValue = string | number;

export type KafkaConsumerRecord = {
  readonly topic?: string | undefined;
  readonly key?: string | Uint8Array | null | undefined;
  readonly value: string | Uint8Array | null;
  readonly headers?: Readonly<Record<string, string | Uint8Array | undefined>> | undefined;
  readonly partition?: number | undefined;
  readonly offset?: string | number | undefined;
  readonly endOffset?: string | number | undefined;
  readonly highWatermark?: string | number | undefined;
  readonly timestamp?: number | bigint | undefined;
};

export type MigrationContext = {
  readonly topic: string;
  readonly fromVersion: number | undefined;
  readonly toVersion: number | undefined;
};

export type KafkaSourceMessage<TRow extends RowObject, TId extends keyof TRow & string> =
  | TRow
  | { readonly type: "publish"; readonly row: TRow }
  | { readonly type: "delta-publish"; readonly patch: Partial<TRow> & Pick<TRow, TId> }
  | { readonly type: "delete"; readonly id: Extract<TRow[TId], IdValue> };

export type KafkaSourceConfig<TRow extends RowObject, TId extends keyof TRow & string> = {
  readonly _tag: "KafkaSource";
  readonly brokers: readonly string[];
  readonly topic: string;
  readonly groupId: string;
  readonly decode: (
    record: KafkaConsumerRecord,
  ) => Effect.Effect<KafkaSourceMessage<TRow, TId>, ViewServerError>;
  readonly commitPolicy?: "after-ingest" | "none" | undefined;
  readonly maxIngestRetries?: number | undefined;
};

export type EffectSourceContext<TRow extends RowObject, TId extends keyof TRow & string> = {
  readonly topic: string;
  readonly idField: TId;
  readonly publish: (row: TRow) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: (
    patch: Partial<TRow> & Pick<TRow, TId>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: (id: Extract<TRow[TId], IdValue>) => Effect.Effect<void, ViewServerError>;
};

export type EffectSourceConfig<TRow extends RowObject, TId extends keyof TRow & string> = {
  readonly _tag: "EffectSource";
  readonly run: (context: EffectSourceContext<TRow, TId>) => Effect.Effect<void, ViewServerError>;
};

export type TopicSource<TRow extends RowObject, TId extends keyof TRow & string> =
  | KafkaSourceConfig<TRow, TId>
  | EffectSourceConfig<TRow, TId>;

export type HttpPath = `/${string}`;

export type TopicConfig<
  TRow extends RowObject = RowObject,
  TId extends keyof TRow & string = keyof TRow & string,
> = {
  readonly id: TId;
  readonly schema: Schema.Decoder<TRow, never>;
  readonly schemaVersion?: number | undefined;
  readonly migrate?: (
    row: unknown,
    context: MigrationContext,
  ) => Effect.Effect<TRow, ViewServerError>;
  readonly source?: TopicSource<TRow, TId> | undefined;
  readonly snapshot?:
    | {
        readonly backend?: "chdb" | "memory" | undefined;
        readonly flushBatchSize?: number | undefined;
        readonly flushIntervalMs?: number | undefined;
        readonly maxVersionLagBeforeMemoryFallback?: number | undefined;
      }
    | undefined;
};

export type TopicConfigMap = Readonly<Record<string, TopicConfig>>;

export type AuthorizationContext = {
  readonly topic: string;
  readonly operation: "subscribe" | "query" | "publish" | "delta-publish" | "delete" | "health";
  readonly payload: unknown;
  readonly transport: "rpc" | "testing" | "internal";
};

export type ViewServerAuth = {
  readonly authorizeConnection?: (
    context: AuthorizationContext,
  ) => Effect.Effect<boolean, ViewServerError>;
  readonly authorizePublish?: (
    context: AuthorizationContext,
  ) => Effect.Effect<boolean, ViewServerError>;
  readonly authorizeQuery?: (
    context: AuthorizationContext,
  ) => Effect.Effect<boolean, ViewServerError>;
};

export type ViewServerConfig<TTopics extends TopicConfigMap = TopicConfigMap> = {
  readonly topics: TTopics;
  readonly auth?: ViewServerAuth | undefined;
  readonly rpc?:
    | {
        readonly serialization?: "ndjson" | undefined;
        readonly path?: HttpPath | undefined;
      }
    | undefined;
  readonly worker?:
    | {
        readonly maxQueueDepth?: number | undefined;
        readonly mutationLogSize?: number | undefined;
      }
    | undefined;
  readonly health?:
    | {
        readonly path?: HttpPath | undefined;
        readonly readyPath?: HttpPath | undefined;
      }
    | undefined;
};

export type NormalizedTopicConfigMap = Readonly<Record<string, TopicConfig>>;

export type NormalizedViewServerConfig = {
  readonly topics: NormalizedTopicConfigMap;
  readonly auth: {
    readonly authorizeConnection: (
      context: AuthorizationContext,
    ) => Effect.Effect<boolean, ViewServerError>;
    readonly authorizePublish: (
      context: AuthorizationContext,
    ) => Effect.Effect<boolean, ViewServerError>;
    readonly authorizeQuery: (
      context: AuthorizationContext,
    ) => Effect.Effect<boolean, ViewServerError>;
  };
  readonly rpc: {
    readonly serialization: "ndjson";
    readonly path: HttpPath;
  };
  readonly worker: {
    readonly maxQueueDepth: number;
    readonly mutationLogSize: number;
  };
  readonly health: {
    readonly path: HttpPath;
    readonly readyPath: HttpPath;
  };
};

export type SystemTopicName = typeof VIEW_SERVER_HEALTH_TOPIC;
export type TopicName<TConfig extends ViewServerConfig> = Extract<keyof TConfig["topics"], string>;
export type ReadableTopicName<TConfig extends ViewServerConfig> =
  | TopicName<TConfig>
  | SystemTopicName;
export type TopicConfigByName<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = TConfig["topics"][TTopic];
export type TopicRowFromConfig<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = TopicConfigByName<TConfig, TTopic>["schema"] extends Schema.Schema<infer TRow> ? TRow : never;
export type ReadableTopicRowFromConfig<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
> = TTopic extends SystemTopicName
  ? ViewServerHealthRow
  : TTopic extends TopicName<TConfig>
    ? TopicRowFromConfig<TConfig, TTopic>
    : never;
export type TopicIdFieldFromConfig<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = TopicConfigByName<TConfig, TTopic>["id"];
export type TopicPatchFromConfig<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Partial<TopicRowFromConfig<TConfig, TTopic>> &
  Record<TopicIdFieldFromConfig<TConfig, TTopic>, IdValue>;

export const KafkaSource = <TRow extends RowObject, TId extends keyof TRow & string>(
  config: Omit<KafkaSourceConfig<TRow, TId>, "_tag">,
): KafkaSourceConfig<TRow, TId> => ({
  _tag: "KafkaSource",
  ...config,
});

export const EffectSource = <TRow extends RowObject, TId extends keyof TRow & string>(
  config: Omit<EffectSourceConfig<TRow, TId>, "_tag">,
): EffectSourceConfig<TRow, TId> => ({
  _tag: "EffectSource",
  ...config,
});

export function defineConfig<const TConfig extends ViewServerConfig>(config: TConfig): TConfig {
  return config;
}

const ViewServerHealthRowSchema = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["server", "topic"]),
  topic: Schema.optional(Schema.String),
  rows: Schema.Number,
  subscribers: Schema.Number,
  queueDepth: Schema.Number,
  workerLagP95Ms: Schema.Number,
  deltaFanoutP95Ms: Schema.Number,
  publishLatencyP95Ms: Schema.Number,
  snapshotLatencyP95Ms: Schema.Number,
  chdbSnapshotLatencyP95Ms: Schema.Number,
  kafkaLagTotal: Schema.Number,
  kafkaLagMax: Schema.Number,
  kafkaPartitions: Schema.Number,
  lastKafkaOffset: Schema.Number,
  lastKafkaEndOffset: Schema.Number,
  rssMb: Schema.Number,
  status: Schema.Literals(["ready", "degraded", "stopping"]),
  updatedAt: Schema.BigInt,
});

export type ViewServerHealthRow = typeof ViewServerHealthRowSchema.Type;

const healthTopic = {
  id: "id",
  schema: ViewServerHealthRowSchema,
  snapshot: {
    backend: "memory",
  },
} satisfies TopicConfig<ViewServerHealthRow, "id">;

export function normalizeConfig(config: ViewServerConfig): NormalizedViewServerConfig {
  for (const topic of Object.keys(config.topics)) {
    if (topic.startsWith(RESERVED_TOPIC_PREFIX)) {
      throw new Error(`User-defined topic ${topic} uses reserved prefix ${RESERVED_TOPIC_PREFIX}`);
    }
  }

  return {
    topics: {
      ...config.topics,
      [VIEW_SERVER_HEALTH_TOPIC]: healthTopic,
    },
    auth: {
      authorizeConnection: config.auth?.authorizeConnection ?? (() => Effect.succeed(true)),
      authorizePublish: config.auth?.authorizePublish ?? (() => Effect.succeed(true)),
      authorizeQuery: config.auth?.authorizeQuery ?? (() => Effect.succeed(true)),
    },
    rpc: {
      serialization: "ndjson",
      path: config.rpc?.path ?? "/rpc",
    },
    worker: {
      maxQueueDepth: config.worker?.maxQueueDepth ?? 100_000,
      mutationLogSize: config.worker?.mutationLogSize ?? 10_000,
    },
    health: {
      path: config.health?.path ?? "/health",
      readyPath: config.health?.readyPath ?? "/ready",
    },
  };
}

export function isReservedTopic(topic: string): boolean {
  return topic.startsWith(RESERVED_TOPIC_PREFIX);
}
