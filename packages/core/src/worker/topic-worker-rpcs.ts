import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { ViewServerError } from "../errors.ts";
import { RpcQuery, RpcQueryResponse, RpcRow, RpcRows, RpcSubscriptionEvent } from "../rpc/index.ts";

export const TopicWorkerInitialMessage = Schema.Struct({
  configModuleUrl: Schema.String,
  topic: Schema.String,
  initialRows: Schema.optional(RpcRows),
  maxQueueDepth: Schema.optional(Schema.Number),
  mutationLogSize: Schema.optional(Schema.Number),
  snapshotBackend: Schema.optional(Schema.Literals(["config", "memory", "chdb"])),
});

const TopicWorkerSubscribePayload = Schema.Struct({
  requestId: Schema.String,
  query: RpcQuery,
});

const TopicWorkerUnsubscribePayload = Schema.Struct({
  requestId: Schema.String,
});

const TopicWorkerQueryPayload = Schema.Struct({
  query: RpcQuery,
});

const TopicWorkerPublishPayload = Schema.Struct({
  row: RpcRow,
});

const TopicWorkerDeltaPublishPayload = Schema.Struct({
  patch: RpcRow,
});

const TopicWorkerDeleteByIdPayload = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
});

export const TopicWorkerMetricsSchema = Schema.Struct({
  rows: Schema.Number,
  subscribers: Schema.Number,
  queueDepth: Schema.Number,
  version: Schema.String,
  status: Schema.Literals(["ready", "degraded", "stopping"]),
});

export const TopicWorkerRpcs = RpcGroup.make(
  Rpc.make("Subscribe", {
    payload: TopicWorkerSubscribePayload,
    success: RpcSubscriptionEvent,
    error: ViewServerError,
    stream: true,
  }),
  Rpc.make("Unsubscribe", {
    payload: TopicWorkerUnsubscribePayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("Query", {
    payload: TopicWorkerQueryPayload,
    success: RpcQueryResponse,
    error: ViewServerError,
  }),
  Rpc.make("Publish", {
    payload: TopicWorkerPublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("DeltaPublish", {
    payload: TopicWorkerDeltaPublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("DeleteById", {
    payload: TopicWorkerDeleteByIdPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("RowsForTest", {
    success: RpcRows,
    error: ViewServerError,
  }),
  Rpc.make("Metrics", {
    success: TopicWorkerMetricsSchema,
    error: ViewServerError,
  }),
  Rpc.make("Shutdown", {
    success: Schema.Void,
    error: ViewServerError,
  }),
);

export type TopicWorkerInitialMessage = typeof TopicWorkerInitialMessage.Type;
