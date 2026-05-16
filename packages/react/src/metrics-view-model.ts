import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { VIEW_SERVER_HEALTH_TOPIC, type ViewServerHealthRow } from "@view-server/core/config";
import type { LiveQueryResult, LiveQueryValue } from "@view-server/core/client";
import type { InferReadableQueryResult, RawQuery } from "@view-server/core/query";

export const viewServerHealthQuery = {
  fields: {
    id: true,
    kind: true,
    topic: true,
    rows: true,
    subscribers: true,
    queueDepth: true,
    maxSubscriptionLagVersions: true,
    totalSubscriptionLagVersions: true,
    activePlanCount: true,
    activeViewCount: true,
    activePlanRows: true,
    activePlanIndexEstimatedBytes: true,
    activePlanBuildQueueDepth: true,
    activePlanBuildingCount: true,
    activePlanPendingCount: true,
    activePlanBuildMs: true,
    activePlanBuildMsTotal: true,
    activePlanBuildMsMax: true,
    activePlanFallbackCount: true,
    activePlanAutoBuildSkippedCount: true,
    queryRejectedCount: true,
    chdbStatus: true,
    chdbPid: true,
    chdbRestarts: true,
    chdbPendingRequests: true,
    chdbLastError: true,
    chdbBackendVersion: true,
    workerLagP95Ms: true,
    deltaFanoutP95Ms: true,
    publishLatencyP95Ms: true,
    snapshotLatencyP95Ms: true,
    chdbSnapshotLatencyP95Ms: true,
    kafkaLagTotal: true,
    kafkaLagMax: true,
    kafkaPartitions: true,
    lastKafkaOffset: true,
    lastKafkaEndOffset: true,
    rssMb: true,
    status: true,
    updatedAt: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 50,
} satisfies RawQuery<
  ViewServerHealthRow,
  {
    readonly id: true;
    readonly kind: true;
    readonly topic: true;
    readonly rows: true;
    readonly subscribers: true;
    readonly queueDepth: true;
    readonly maxSubscriptionLagVersions: true;
    readonly totalSubscriptionLagVersions: true;
    readonly activePlanCount: true;
    readonly activeViewCount: true;
    readonly activePlanRows: true;
    readonly activePlanIndexEstimatedBytes: true;
    readonly activePlanBuildQueueDepth: true;
    readonly activePlanBuildingCount: true;
    readonly activePlanPendingCount: true;
    readonly activePlanBuildMs: true;
    readonly activePlanBuildMsTotal: true;
    readonly activePlanBuildMsMax: true;
    readonly activePlanFallbackCount: true;
    readonly activePlanAutoBuildSkippedCount: true;
    readonly queryRejectedCount: true;
    readonly chdbStatus: true;
    readonly chdbPid: true;
    readonly chdbRestarts: true;
    readonly chdbPendingRequests: true;
    readonly chdbLastError: true;
    readonly chdbBackendVersion: true;
    readonly workerLagP95Ms: true;
    readonly deltaFanoutP95Ms: true;
    readonly publishLatencyP95Ms: true;
    readonly snapshotLatencyP95Ms: true;
    readonly chdbSnapshotLatencyP95Ms: true;
    readonly kafkaLagTotal: true;
    readonly kafkaLagMax: true;
    readonly kafkaPartitions: true;
    readonly lastKafkaOffset: true;
    readonly lastKafkaEndOffset: true;
    readonly rssMb: true;
    readonly status: true;
    readonly updatedAt: true;
  }
>;

type MetricsViewServerConfig = { readonly topics: {} };
export type ViewServerMetricsRow = InferReadableQueryResult<
  MetricsViewServerConfig,
  typeof VIEW_SERVER_HEALTH_TOPIC,
  typeof viewServerHealthQuery
>[number];

export type ViewServerMetricsHooks = {
  readonly useLiveQuery: (
    topic: typeof VIEW_SERVER_HEALTH_TOPIC,
    query: typeof viewServerHealthQuery,
  ) => LiveQueryResult<ViewServerMetricsRow>;
};

export type MetricsCellViewModel = {
  readonly label: string;
  readonly value: string;
};

export type MetricsTopicViewModel = {
  readonly id: string;
  readonly topic: string | undefined;
  readonly status: ViewServerHealthRow["status"];
  readonly chdbStatus: ViewServerHealthRow["chdbStatus"];
  readonly chdbTitle: string;
  readonly rows: string;
  readonly subscribers: string;
  readonly queueDepth: string;
  readonly maxSubscriptionLagVersions: string;
  readonly activePlanCount: string;
  readonly activePlanBuilds: string;
  readonly activePlanPendingCount: string;
  readonly activeViewCount: string;
  readonly activePlanFallbackCount: string;
  readonly activePlanAutoBuildSkippedCount: string;
  readonly queryRejectedCount: string;
  readonly activePlanRows: string;
  readonly activePlanIndexEstimatedBytes: string;
  readonly kafkaLagTotal: string;
  readonly chdbPendingRequests: string;
  readonly chdbRestarts: string;
  readonly chdbBackendVersion: string;
  readonly chdbPid: string;
  readonly updatedAt: string;
};

export type MetricsDashboardViewModel = {
  readonly status: ViewServerHealthRow["status"];
  readonly liveStatus: LiveQueryValue<ViewServerMetricsRow>["status"];
  readonly summary: readonly MetricsCellViewModel[];
  readonly latency: readonly MetricsCellViewModel[];
  readonly topics: readonly MetricsTopicViewModel[];
};

export const emptyMetricsValue: LiveQueryValue<ViewServerMetricsRow> = {
  rows: [],
  totalRows: 0,
  status: "connecting",
  connection: {
    connected: false,
    attempt: 0,
  },
};

export function metricsValueFromResult(
  result: LiveQueryResult<ViewServerMetricsRow>,
): LiveQueryValue<ViewServerMetricsRow> {
  return AsyncResult.match(result, {
    onInitial: () => emptyMetricsValue,
    onFailure: (failure) => Option.getOrElse(AsyncResult.value(failure), () => emptyMetricsValue),
    onSuccess: (success) => success.value,
  });
}

export function metricsDashboardViewModel(
  value: LiveQueryValue<ViewServerMetricsRow>,
): MetricsDashboardViewModel {
  const rows = value.rows;
  const server = rows.find((row) => row.kind === "server");
  const topics = rows
    .filter((row) => row.kind === "topic")
    .toSorted((left, right) => String(left.topic ?? "").localeCompare(String(right.topic ?? "")))
    .map(topicViewModel);
  const status = server?.status ?? (value.status === "reconnecting" ? "degraded" : "stopping");

  return {
    status,
    liveStatus: value.status,
    summary: [
      { label: "rows", value: formatCount(server?.rows) },
      { label: "subscribers", value: formatCount(server?.subscribers) },
      { label: "queue", value: formatCount(server?.queueDepth) },
      { label: "sub lag", value: formatCount(server?.maxSubscriptionLagVersions) },
      { label: "plans", value: formatCount(server?.activePlanCount) },
      { label: "plan queue", value: formatCount(server?.activePlanBuildQueueDepth) },
      { label: "indexed rows", value: formatCount(server?.activePlanRows) },
      { label: "fallbacks", value: formatCount(server?.activePlanFallbackCount) },
      {
        label: "skipped plans",
        value: formatCount(server?.activePlanAutoBuildSkippedCount),
      },
      { label: "query rejects", value: formatCount(server?.queryRejectedCount) },
    ],
    latency: [
      { label: "publish p95", value: formatMs(server?.publishLatencyP95Ms) },
      { label: "fanout p95", value: formatMs(server?.deltaFanoutP95Ms) },
      { label: "snapshot p95", value: formatMs(server?.snapshotLatencyP95Ms) },
      { label: "worker lag", value: formatMs(server?.workerLagP95Ms) },
      { label: "kafka lag", value: formatCount(server?.kafkaLagTotal) },
      { label: "chDB pending", value: formatCount(server?.chdbPendingRequests) },
      { label: "plan build", value: formatMs(server?.activePlanBuildMs) },
      { label: "plan build max", value: formatMs(server?.activePlanBuildMsMax) },
      { label: "plan index", value: formatBytes(server?.activePlanIndexEstimatedBytes) },
    ],
    topics,
  };
}

function topicViewModel(topic: ViewServerMetricsRow): MetricsTopicViewModel {
  return {
    id: topic.id,
    topic: topic.topic,
    status: topic.status,
    chdbStatus: topic.chdbStatus,
    chdbTitle: topic.chdbLastError,
    rows: formatCount(topic.rows),
    subscribers: formatCount(topic.subscribers),
    queueDepth: formatCount(topic.queueDepth),
    maxSubscriptionLagVersions: formatCount(topic.maxSubscriptionLagVersions),
    activePlanCount: formatCount(topic.activePlanCount),
    activePlanBuilds: `${formatCount(topic.activePlanBuildingCount)}/${formatCount(topic.activePlanBuildQueueDepth)}`,
    activePlanPendingCount: formatCount(topic.activePlanPendingCount),
    activeViewCount: formatCount(topic.activeViewCount),
    activePlanFallbackCount: formatCount(topic.activePlanFallbackCount),
    activePlanAutoBuildSkippedCount: formatCount(topic.activePlanAutoBuildSkippedCount),
    queryRejectedCount: formatCount(topic.queryRejectedCount),
    activePlanRows: formatCount(topic.activePlanRows),
    activePlanIndexEstimatedBytes: formatBytes(topic.activePlanIndexEstimatedBytes),
    kafkaLagTotal: formatCount(topic.kafkaLagTotal),
    chdbPendingRequests: formatCount(topic.chdbPendingRequests),
    chdbRestarts: formatCount(topic.chdbRestarts),
    chdbBackendVersion: topic.chdbBackendVersion,
    chdbPid: formatCount(topic.chdbPid),
    updatedAt: formatTime(topic.updatedAt),
  };
}

export function formatCount(value: number | undefined): string {
  return value === undefined ? "0" : Intl.NumberFormat("en-US").format(value);
}

export function formatMs(value: number | undefined): string {
  return `${value ?? 0}ms`;
}

export function formatBytes(value: number | undefined): string {
  if (value === undefined || value === 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB"] as const;
  let unitIndex = 0;
  let scaled = value;
  while (scaled >= 1024 && unitIndex < units.length - 1) {
    scaled = scaled / 1024;
    unitIndex++;
  }
  const digits = scaled >= 100 || unitIndex === 0 ? 0 : 1;
  return `${scaled.toFixed(digits)} ${units[unitIndex] ?? "GB"}`;
}

export function formatTime(value: bigint | undefined): string {
  if (value === undefined) {
    return "pending";
  }
  return new Date(Number(value)).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
