import * as Schema from "effect/Schema";

export class MissingTopic extends Schema.TaggedErrorClass<MissingTopic>()("MissingTopic", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class MissingTopicId extends Schema.TaggedErrorClass<MissingTopicId>()("MissingTopicId", {
  topic: Schema.String,
  idField: Schema.String,
  message: Schema.String,
}) {}

export class InvalidQuery extends Schema.TaggedErrorClass<InvalidQuery>()("InvalidQuery", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class InvalidFilter extends Schema.TaggedErrorClass<InvalidFilter>()("InvalidFilter", {
  topic: Schema.String,
  field: Schema.optional(Schema.String),
  message: Schema.String,
}) {}

export class InvalidPublish extends Schema.TaggedErrorClass<InvalidPublish>()("InvalidPublish", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class Unauthorized extends Schema.TaggedErrorClass<Unauthorized>()("Unauthorized", {
  topic: Schema.String,
  operation: Schema.String,
  message: Schema.String,
}) {}

export class UnauthorizedSystemTopic extends Schema.TaggedErrorClass<UnauthorizedSystemTopic>()(
  "UnauthorizedSystemTopic",
  {
    topic: Schema.String,
    operation: Schema.String,
    message: Schema.String,
  },
) {}

export class WorkerUnavailable extends Schema.TaggedErrorClass<WorkerUnavailable>()(
  "WorkerUnavailable",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class SnapshotBackendLagExceeded extends Schema.TaggedErrorClass<SnapshotBackendLagExceeded>()(
  "SnapshotBackendLagExceeded",
  {
    topic: Schema.String,
    backendVersion: Schema.String,
    targetVersion: Schema.String,
    message: Schema.String,
  },
) {}

export class SnapshotBackendFailed extends Schema.TaggedErrorClass<SnapshotBackendFailed>()(
  "SnapshotBackendFailed",
  {
    topic: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class SnapshotBackendUnavailable extends Schema.TaggedErrorClass<SnapshotBackendUnavailable>()(
  "SnapshotBackendUnavailable",
  {
    topic: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class SnapshotReplayGap extends Schema.TaggedErrorClass<SnapshotReplayGap>()(
  "SnapshotReplayGap",
  {
    topic: Schema.String,
    backendVersion: Schema.String,
    targetVersion: Schema.String,
    message: Schema.String,
  },
) {}

export class KafkaIngestFailed extends Schema.TaggedErrorClass<KafkaIngestFailed>()(
  "KafkaIngestFailed",
  {
    topic: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class SourceFailed extends Schema.TaggedErrorClass<SourceFailed>()("SourceFailed", {
  topic: Schema.String,
  source: Schema.String,
  message: Schema.String,
  cause: Schema.optional(Schema.Defect),
}) {}

export class SchemaDecodeFailed extends Schema.TaggedErrorClass<SchemaDecodeFailed>()(
  "SchemaDecodeFailed",
  {
    topic: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class VersionGap extends Schema.TaggedErrorClass<VersionGap>()("VersionGap", {
  topic: Schema.String,
  expectedVersion: Schema.String,
  receivedVersion: Schema.String,
  message: Schema.String,
}) {}

export class SubscriptionClosed extends Schema.TaggedErrorClass<SubscriptionClosed>()(
  "SubscriptionClosed",
  {
    requestId: Schema.String,
    message: Schema.String,
  },
) {}

export class TransportError extends Schema.TaggedErrorClass<TransportError>()("TransportError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect),
}) {}

export class BackpressureExceeded extends Schema.TaggedErrorClass<BackpressureExceeded>()(
  "BackpressureExceeded",
  {
    requestId: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidStartupEnv extends Schema.TaggedErrorClass<InvalidStartupEnv>()(
  "InvalidStartupEnv",
  {
    variable: Schema.optional(Schema.String),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class QueryLimitExceeded extends Schema.TaggedErrorClass<QueryLimitExceeded>()(
  "QueryLimitExceeded",
  {
    topic: Schema.String,
    field: Schema.String,
    limit: Schema.Number,
    actual: Schema.Number,
    message: Schema.String,
  },
) {}

export class InvalidConfig extends Schema.TaggedErrorClass<InvalidConfig>()("InvalidConfig", {
  field: Schema.optional(Schema.String),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect),
}) {}

export class ServerShutdown extends Schema.TaggedErrorClass<ServerShutdown>()("ServerShutdown", {
  topic: Schema.optional(Schema.String),
  requestId: Schema.optional(Schema.String),
  message: Schema.String,
}) {}

export class ChdbChildExited extends Schema.TaggedErrorClass<ChdbChildExited>()("ChdbChildExited", {
  topic: Schema.String,
  message: Schema.String,
  pid: Schema.optional(Schema.Number),
  code: Schema.optional(Schema.Number),
  signal: Schema.optional(Schema.String),
}) {}

export const ViewServerError = Schema.Union([
  MissingTopic,
  MissingTopicId,
  InvalidQuery,
  InvalidFilter,
  InvalidPublish,
  Unauthorized,
  UnauthorizedSystemTopic,
  WorkerUnavailable,
  SnapshotBackendLagExceeded,
  SnapshotBackendFailed,
  SnapshotBackendUnavailable,
  SnapshotReplayGap,
  KafkaIngestFailed,
  SourceFailed,
  SchemaDecodeFailed,
  VersionGap,
  SubscriptionClosed,
  TransportError,
  BackpressureExceeded,
  InvalidStartupEnv,
  QueryLimitExceeded,
  InvalidConfig,
  ServerShutdown,
  ChdbChildExited,
]);

export type ViewServerError = typeof ViewServerError.Type;

export const missingTopic = (topic: string): MissingTopic =>
  new MissingTopic({ topic, message: `Unknown topic: ${topic}` });

export const missingTopicId = (topic: string, idField: string): MissingTopicId =>
  new MissingTopicId({
    topic,
    idField,
    message: `Topic ${topic} requires id field ${idField}`,
  });

export const invalidQuery = (topic: string, message: string): InvalidQuery =>
  new InvalidQuery({ topic, message });

export const invalidFilter = (topic: string, message: string, field?: string): InvalidFilter =>
  new InvalidFilter({ topic, message, ...(field === undefined ? {} : { field }) });

export const invalidPublish = (topic: string, message: string): InvalidPublish =>
  new InvalidPublish({ topic, message });

export const unauthorized = (topic: string, operation: string): Unauthorized =>
  new Unauthorized({
    topic,
    operation,
    message: `Unauthorized ${operation} for topic ${topic}`,
  });

export const unauthorizedSystemTopic = (
  topic: string,
  operation: string,
): UnauthorizedSystemTopic =>
  new UnauthorizedSystemTopic({
    topic,
    operation,
    message: `System topic ${topic} is private for ${operation}`,
  });

export const workerUnavailable = (topic: string): WorkerUnavailable =>
  new WorkerUnavailable({ topic, message: `Worker unavailable for topic ${topic}` });

export const snapshotBackendFailed = (topic: string, error: unknown): SnapshotBackendFailed =>
  new SnapshotBackendFailed({
    topic,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const snapshotBackendUnavailable = (
  topic: string,
  error: unknown,
): SnapshotBackendUnavailable =>
  new SnapshotBackendUnavailable({
    topic,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const snapshotReplayGap = (
  topic: string,
  backendVersion: bigint | string,
  targetVersion: bigint | string,
): SnapshotReplayGap =>
  new SnapshotReplayGap({
    topic,
    backendVersion: String(backendVersion),
    targetVersion: String(targetVersion),
    message: `Snapshot replay gap for topic ${topic}: backend ${String(backendVersion)} cannot catch up to ${String(targetVersion)}`,
  });

export const kafkaIngestFailed = (topic: string, error: unknown): KafkaIngestFailed =>
  new KafkaIngestFailed({
    topic,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const sourceFailed = (topic: string, source: string, error: unknown): SourceFailed =>
  new SourceFailed({
    topic,
    source,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const schemaDecodeFailed = (topic: string, error: unknown): SchemaDecodeFailed =>
  new SchemaDecodeFailed({
    topic,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const versionGap = (
  topic: string,
  expectedVersion: bigint,
  receivedVersion: bigint,
): VersionGap =>
  new VersionGap({
    topic,
    expectedVersion: expectedVersion.toString(),
    receivedVersion: receivedVersion.toString(),
    message: `Version gap for topic ${topic}: expected ${expectedVersion}, received ${receivedVersion}`,
  });

export const transportError = (error: unknown): TransportError =>
  new TransportError({
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });

export const backpressureExceeded = (requestId: string, message: string): BackpressureExceeded =>
  new BackpressureExceeded({
    requestId,
    message,
  });

export const queryLimitExceeded = (
  topic: string,
  field: string,
  limit: number,
  actual: number,
): QueryLimitExceeded =>
  new QueryLimitExceeded({
    topic,
    field,
    limit,
    actual,
    message: `Query ${field} ${actual} exceeds configured limit ${limit}`,
  });

export const invalidStartupEnv = (
  message: string,
  error?: unknown,
  variable?: string,
): InvalidStartupEnv =>
  new InvalidStartupEnv({
    message,
    ...(variable === undefined ? {} : { variable }),
    ...(error === undefined ? {} : { cause: error }),
  });

export const invalidConfig = (message: string, field?: string, error?: unknown): InvalidConfig =>
  new InvalidConfig({
    message,
    ...(field === undefined ? {} : { field }),
    ...(error === undefined ? {} : { cause: error }),
  });

export const serverShutdown = (
  message: string,
  topic?: string,
  requestId?: string,
): ServerShutdown =>
  new ServerShutdown({
    message,
    ...(topic === undefined ? {} : { topic }),
    ...(requestId === undefined ? {} : { requestId }),
  });

export const chdbChildExited = (args: {
  readonly topic: string;
  readonly message: string;
  readonly pid?: number | undefined;
  readonly code?: number | undefined;
  readonly signal?: string | undefined;
}): ChdbChildExited =>
  new ChdbChildExited({
    topic: args.topic,
    message: args.message,
    ...(args.pid === undefined ? {} : { pid: args.pid }),
    ...(args.code === undefined ? {} : { code: args.code }),
    ...(args.signal === undefined ? {} : { signal: args.signal }),
  });

export type ViewServerErrorRetryAction = "fail" | "retry" | "resubscribe";

export function viewServerErrorRetryAction(error: ViewServerError): ViewServerErrorRetryAction {
  switch (error._tag) {
    case "TransportError":
      return "retry";
    case "BackpressureExceeded":
      return "resubscribe";
    default:
      return "fail";
  }
}

export function isRetryableViewServerError(error: ViewServerError): boolean {
  return viewServerErrorRetryAction(error) !== "fail";
}

export function isViewServerError(error: unknown): error is ViewServerError {
  return (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    typeof error._tag === "string" &&
    viewServerErrorTags.has(error._tag)
  );
}

const viewServerErrorTags = new Set([
  "MissingTopic",
  "MissingTopicId",
  "InvalidQuery",
  "InvalidFilter",
  "InvalidPublish",
  "Unauthorized",
  "UnauthorizedSystemTopic",
  "WorkerUnavailable",
  "SnapshotBackendLagExceeded",
  "SnapshotBackendFailed",
  "SnapshotBackendUnavailable",
  "SnapshotReplayGap",
  "KafkaIngestFailed",
  "SourceFailed",
  "SchemaDecodeFailed",
  "VersionGap",
  "SubscriptionClosed",
  "TransportError",
  "BackpressureExceeded",
  "InvalidStartupEnv",
  "QueryLimitExceeded",
  "InvalidConfig",
  "ServerShutdown",
  "ChdbChildExited",
]);
