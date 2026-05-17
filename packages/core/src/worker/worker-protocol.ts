import * as Schema from "effect/Schema";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { ViewServerError } from "../errors.ts";
import {
  fromWireRows,
  RpcQuery,
  RpcQueryResponse,
  RpcRow,
  RpcRows,
  RpcSubscriptionEvent,
  toWireRow,
} from "../rpc/index.ts";
import type { RuntimeRow } from "../protocol/index.ts";
import type { TopicWorkerMetrics } from "./worker-health-projection.ts";

export const TOPIC_WORKER_RPC_NAMES = [
  "Subscribe",
  "Unsubscribe",
  "Query",
  "Publish",
  "DeltaPublish",
  "DeleteById",
  "RowsForTest",
  "Metrics",
  "Shutdown",
] as const;

export type TopicWorkerRpcName = (typeof TOPIC_WORKER_RPC_NAMES)[number];

export const TopicWorkerInitialMessage = Schema.Struct({
  configModuleUrl: Schema.String,
  topic: Schema.String,
  initialRows: Schema.optional(RpcRows),
  maxQueueDepth: Schema.optional(Schema.Number),
  mutationLogSize: Schema.optional(Schema.Number),
  deltaCoalescing: Schema.optional(Schema.Boolean),
  maxActivePlans: Schema.optional(Schema.Number),
  maxActivePlanEstimatedBytes: Schema.optional(Schema.Number),
  activePlanAutoBuildMaxRows: Schema.optional(Schema.Number),
  activePlanBuildConcurrency: Schema.optional(Schema.Number),
  groupedRefreshDebounceMs: Schema.optional(Schema.Number),
  snapshotBackend: Schema.optional(Schema.Literals(["memory", "chdb"])),
});

export const TopicWorkerSubscribePayload = Schema.Struct({
  requestId: Schema.String,
  query: RpcQuery,
});

export const TopicWorkerUnsubscribePayload = Schema.Struct({
  requestId: Schema.String,
});

export const TopicWorkerQueryPayload = Schema.Struct({
  query: RpcQuery,
});

export const TopicWorkerPublishPayload = Schema.Struct({
  row: RpcRow,
});

export const TopicWorkerDeltaPublishPayload = Schema.Struct({
  patch: RpcRow,
});

export const TopicWorkerDeleteByIdPayload = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
});

export const TopicWorkerQueryResponse = RpcQueryResponse;
export const TopicWorkerRows = RpcRows;
export const TopicWorkerSubscriptionEvent = RpcSubscriptionEvent;

export const TopicWorkerMetricsSchema = Schema.Struct({
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
  activePlanAutoBuildSkippedCount: Schema.Number,
  chdbStatus: Schema.Literals(["ready", "degraded", "restarting", "stopped"]),
  chdbPid: Schema.Number,
  chdbRestarts: Schema.Number,
  chdbPendingRequests: Schema.Number,
  chdbLastError: Schema.String,
  chdbBackendVersion: Schema.String,
  version: Schema.String,
  status: Schema.Literals(["ready", "degraded", "stopping"]),
});

export const TopicWorkerRpcs = RpcGroup.make(
  Rpc.make("Subscribe", {
    payload: TopicWorkerSubscribePayload,
    success: TopicWorkerSubscriptionEvent,
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
    success: TopicWorkerQueryResponse,
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
    success: TopicWorkerRows,
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
export type TopicWorkerMetricsWire = typeof TopicWorkerMetricsSchema.Type;

export function encodeTopicWorkerRows(rows: readonly RuntimeRow[]): typeof TopicWorkerRows.Type {
  return rows.map(toWireRow);
}

export function decodeTopicWorkerRows(rows: typeof TopicWorkerRows.Type): readonly RuntimeRow[] {
  return fromWireRows(rows);
}

export function encodeTopicWorkerMetrics(metrics: TopicWorkerMetrics): TopicWorkerMetricsWire {
  return {
    rows: metrics.rows,
    subscribers: metrics.subscribers,
    queueDepth: metrics.queueDepth,
    maxSubscriptionLagVersions: metrics.maxSubscriptionLagVersions,
    totalSubscriptionLagVersions: metrics.totalSubscriptionLagVersions,
    activePlanCount: metrics.activePlanCount,
    activeViewCount: metrics.activeViewCount,
    activePlanRows: metrics.activePlanRows,
    activePlanIndexEstimatedBytes: metrics.activePlanIndexEstimatedBytes,
    activePlanBuildQueueDepth: metrics.activePlanBuildQueueDepth,
    activePlanBuildingCount: metrics.activePlanBuildingCount,
    activePlanPendingCount: metrics.activePlanPendingCount,
    activePlanBuildMs: metrics.activePlanBuildMs,
    activePlanBuildMsTotal: metrics.activePlanBuildMsTotal,
    activePlanBuildMsMax: metrics.activePlanBuildMsMax,
    activePlanFallbackCount: metrics.activePlanFallbackCount,
    activePlanAutoBuildSkippedCount: metrics.activePlanAutoBuildSkippedCount,
    chdbStatus: metrics.chdbStatus,
    chdbPid: metrics.chdbPid,
    chdbRestarts: metrics.chdbRestarts,
    chdbPendingRequests: metrics.chdbPendingRequests,
    chdbLastError: metrics.chdbLastError,
    chdbBackendVersion: metrics.chdbBackendVersion.toString(),
    version: metrics.version.toString(),
    status: metrics.status,
  };
}

export function decodeTopicWorkerMetrics(metrics: TopicWorkerMetricsWire): TopicWorkerMetrics {
  return {
    rows: metrics.rows,
    subscribers: metrics.subscribers,
    queueDepth: metrics.queueDepth,
    maxSubscriptionLagVersions: metrics.maxSubscriptionLagVersions,
    totalSubscriptionLagVersions: metrics.totalSubscriptionLagVersions,
    activePlanCount: metrics.activePlanCount,
    activeViewCount: metrics.activeViewCount,
    activePlanRows: metrics.activePlanRows,
    activePlanIndexEstimatedBytes: metrics.activePlanIndexEstimatedBytes,
    activePlanBuildQueueDepth: metrics.activePlanBuildQueueDepth,
    activePlanBuildingCount: metrics.activePlanBuildingCount,
    activePlanPendingCount: metrics.activePlanPendingCount,
    activePlanBuildMs: metrics.activePlanBuildMs,
    activePlanBuildMsTotal: metrics.activePlanBuildMsTotal,
    activePlanBuildMsMax: metrics.activePlanBuildMsMax,
    activePlanFallbackCount: metrics.activePlanFallbackCount,
    activePlanAutoBuildSkippedCount: metrics.activePlanAutoBuildSkippedCount,
    chdbStatus: metrics.chdbStatus,
    chdbPid: metrics.chdbPid,
    chdbRestarts: metrics.chdbRestarts,
    chdbPendingRequests: metrics.chdbPendingRequests,
    chdbLastError: metrics.chdbLastError,
    chdbBackendVersion: BigInt(metrics.chdbBackendVersion),
    version: BigInt(metrics.version),
    status: metrics.status,
  };
}
