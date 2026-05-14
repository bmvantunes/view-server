import { BigDecimal, Schema, SchemaGetter } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { ViewServerError } from "../errors.ts";

const RpcBigInt = Schema.Struct({
  __viewServerType: Schema.Literal("bigint"),
  value: Schema.String,
}).pipe(
  Schema.decodeTo(Schema.BigInt, {
    decode: SchemaGetter.transform((input) => BigInt(input.value)),
    encode: SchemaGetter.transform((input) => ({
      __viewServerType: "bigint",
      value: input.toString(),
    })),
  }),
);

const RpcBigDecimal = Schema.Struct({
  __viewServerType: Schema.Literal("bigdecimal"),
  value: Schema.String,
}).pipe(
  Schema.decodeTo(Schema.BigDecimal, {
    decode: SchemaGetter.transform((input) => BigDecimal.fromStringUnsafe(input.value)),
    encode: SchemaGetter.transform((input) => ({
      __viewServerType: "bigdecimal",
      value: BigDecimal.format(input),
    })),
  }),
);

export type RpcWireValue =
  | null
  | string
  | number
  | boolean
  | bigint
  | BigDecimal.BigDecimal
  | readonly RpcWireValue[]
  | { readonly [key: string]: RpcWireValue };

export const RpcWireValue: Schema.Codec<RpcWireValue> = Schema.Union([
  Schema.Null,
  Schema.String,
  Schema.Number,
  Schema.Boolean,
  RpcBigInt,
  RpcBigDecimal,
  Schema.Array(Schema.suspend((): Schema.Codec<RpcWireValue> => RpcWireValue)),
  Schema.Record(
    Schema.String,
    Schema.suspend((): Schema.Codec<RpcWireValue> => RpcWireValue),
  ),
]);

export const RpcRow = Schema.Record(Schema.String, RpcWireValue);
export const RpcRows = Schema.Array(RpcRow);

const RpcComparator = Schema.Literals([
  "equals",
  "not_equals",
  "greater_than",
  "greater_than_or_equal",
  "less_than",
  "less_than_or_equal",
  "contains",
  "starts_with",
  "one_of",
]);

type RpcFilterNodeType =
  | {
      readonly field: string;
      readonly comparator: typeof RpcComparator.Type;
      readonly value: RpcWireValue;
    }
  | {
      readonly op: "and" | "or";
      readonly conditions: readonly RpcFilterNodeType[];
    };

const RpcFilterNode: Schema.Codec<RpcFilterNodeType> = Schema.Union([
  Schema.Struct({
    field: Schema.String,
    comparator: RpcComparator,
    value: RpcWireValue,
  }),
  Schema.Struct({
    op: Schema.Literals(["and", "or"]),
    conditions: Schema.Array(Schema.suspend((): Schema.Codec<RpcFilterNodeType> => RpcFilterNode)),
  }),
]);

const RpcOrderBy = Schema.Array(
  Schema.Struct({
    field: Schema.String,
    direction: Schema.Literals(["asc", "desc"]),
  }),
);

const RpcAggregate = Schema.Union([
  Schema.Struct({
    aggFunc: Schema.Literals(["count", "count_distinct", "sum", "avg", "min", "max"]),
    field: Schema.String,
  }),
  Schema.Struct({
    aggFunc: Schema.Literals(["string_concat", "string_concat_distinct"]),
    field: Schema.String,
    joiner: Schema.String,
    sort: Schema.optional(Schema.Literals(["asc", "desc"])),
  }),
]);

export const RpcQuery = Schema.Union([
  Schema.Struct({
    fields: Schema.Record(Schema.String, Schema.Literal(true)),
    where: Schema.optional(RpcFilterNode),
    orderBy: Schema.optional(RpcOrderBy),
    offset: Schema.optional(Schema.Number),
    limit: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    groupBy: Schema.Array(Schema.String),
    aggregates: Schema.Record(Schema.String, RpcAggregate),
    where: Schema.optional(RpcFilterNode),
    orderBy: Schema.optional(RpcOrderBy),
    offset: Schema.optional(Schema.Number),
    limit: Schema.optional(Schema.Number),
  }),
]);

export const RpcSubscribePayload = Schema.Struct({
  requestId: Schema.String,
  topic: Schema.String,
  query: RpcQuery,
});

export const RpcUnsubscribePayload = Schema.Struct({
  requestId: Schema.String,
});

export const RpcQueryPayload = Schema.Struct({
  topic: Schema.String,
  query: RpcQuery,
});

export const RpcPublishPayload = Schema.Struct({
  topic: Schema.String,
  row: RpcRow,
});

export const RpcDeltaPublishPayload = Schema.Struct({
  topic: Schema.String,
  patch: RpcRow,
});

export const RpcDeleteByIdPayload = Schema.Struct({
  topic: Schema.String,
  id: Schema.Union([Schema.String, Schema.Number]),
});

export const RpcHealthPayload = Schema.Struct({
  includeTopics: Schema.optional(Schema.Boolean),
});

export const RpcSnapshotMeta = Schema.Struct({
  version: Schema.String,
  totalRows: Schema.Number,
  backendVersion: Schema.optional(Schema.String),
  serverTime: Schema.Number,
});

export const RpcDeltaMeta = Schema.Struct({
  fromVersion: Schema.String,
  toVersion: Schema.String,
  totalRows: Schema.Number,
  sourceUpdatedAt: Schema.optional(Schema.Union([Schema.Number, RpcBigInt])),
  serverTime: Schema.Number,
});

export const RpcSnapshotEvent = Schema.Struct({
  type: Schema.Literal("snapshot"),
  requestId: Schema.String,
  rows: RpcRows,
  meta: RpcSnapshotMeta,
});

export const RpcDeltaOperation = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("upsert"),
    row: RpcRow,
    key: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
    index: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    type: Schema.Literal("patch"),
    key: Schema.Union([Schema.String, Schema.Number]),
    changes: RpcRow,
    index: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    type: Schema.Literal("remove"),
    key: Schema.Union([Schema.String, Schema.Number]),
  }),
]);

export const RpcDeltaEvent = Schema.Struct({
  type: Schema.Literal("delta"),
  requestId: Schema.String,
  ops: Schema.Array(RpcDeltaOperation),
  meta: RpcDeltaMeta,
});

export const RpcLiveQueryStatusEvent = Schema.Struct({
  type: Schema.Literal("status"),
  requestId: Schema.String,
  status: Schema.Literal("stale"),
  meta: Schema.Struct({
    version: Schema.String,
    totalRows: Schema.Number,
    serverTime: Schema.Number,
  }),
});

export const RpcSubscriptionEvent = Schema.Union([
  RpcSnapshotEvent,
  RpcDeltaEvent,
  RpcLiveQueryStatusEvent,
]);

export const RpcQueryResponse = Schema.Struct({
  rows: RpcRows,
  totalRows: Schema.Number,
  version: Schema.String,
});

export const RpcHealthTopic = Schema.Struct({
  rows: Schema.Number,
  subscribers: Schema.Number,
  queueDepth: Schema.Number,
  maxSubscriptionLagVersions: Schema.Number,
  totalSubscriptionLagVersions: Schema.Number,
  activePlanCount: Schema.Number,
  activeViewCount: Schema.Number,
  activePlanRows: Schema.Number,
  activePlanIndexEstimatedBytes: Schema.Number,
  activePlanBuildQueueDepth: Schema.Number,
  activePlanBuildingCount: Schema.Number,
  activePlanPendingCount: Schema.Number,
  activePlanBuildMs: Schema.Number,
  activePlanBuildMsTotal: Schema.Number,
  activePlanBuildMsMax: Schema.Number,
  activePlanFallbackCount: Schema.Number,
  version: Schema.String,
  kafkaLagTotal: Schema.Number,
  kafkaLagMax: Schema.Number,
  kafkaPartitions: Schema.Number,
  lastKafkaOffset: Schema.Number,
  lastKafkaEndOffset: Schema.Number,
  status: Schema.Literals(["ready", "degraded", "stopping"]),
});

export const RpcHealthResponse = Schema.Struct({
  ok: Schema.Boolean,
  topics: Schema.Record(Schema.String, RpcHealthTopic),
});

export const ViewServerRpcs = RpcGroup.make(
  Rpc.make("Subscribe", {
    payload: RpcSubscribePayload,
    success: RpcSubscriptionEvent,
    error: ViewServerError,
    stream: true,
  }),
  Rpc.make("Unsubscribe", {
    payload: RpcUnsubscribePayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("Query", {
    payload: RpcQueryPayload,
    success: RpcQueryResponse,
    error: ViewServerError,
  }),
  Rpc.make("Publish", {
    payload: RpcPublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("DeltaPublish", {
    payload: RpcDeltaPublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("DeleteById", {
    payload: RpcDeleteByIdPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("Health", {
    payload: RpcHealthPayload,
    success: RpcHealthResponse,
    error: ViewServerError,
  }),
);

export type RpcSubscribePayload = typeof RpcSubscribePayload.Type;
export type RpcUnsubscribePayload = typeof RpcUnsubscribePayload.Type;
export type RpcQueryPayload = typeof RpcQueryPayload.Type;
export type RpcPublishPayload = typeof RpcPublishPayload.Type;
export type RpcDeltaPublishPayload = typeof RpcDeltaPublishPayload.Type;
export type RpcDeleteByIdPayload = typeof RpcDeleteByIdPayload.Type;
export type RpcSubscriptionEvent = typeof RpcSubscriptionEvent.Type;
export type RpcQueryResponse = typeof RpcQueryResponse.Type;
export type RpcHealthPayload = typeof RpcHealthPayload.Type;
export type RpcHealthResponse = typeof RpcHealthResponse.Type;
