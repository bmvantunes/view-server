import type { ViewServerHealthRow } from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import type { KafkaBatchMetrics } from "../kafka/index.ts";
import type { TopicWorkerMetrics } from "../worker/topic-worker-core.ts";

export type KafkaRuntimeMetrics = {
  readonly lagTotal: number;
  readonly lagMax: number;
  readonly partitions: number;
  readonly offset: number;
  readonly endOffset: number;
};

export type HealthTopicMetrics = {
  readonly rows: number;
  readonly subscribers: number;
  readonly queueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly totalSubscriptionLagVersions: number;
  readonly activePlanCount: number;
  readonly activeViewCount: number;
  readonly activePlanRows: number;
  readonly activePlanIndexEstimatedBytes: number;
  readonly activePlanBuildQueueDepth: number;
  readonly activePlanBuildingCount: number;
  readonly activePlanPendingCount: number;
  readonly activePlanBuildMs: number;
  readonly activePlanBuildMsTotal: number;
  readonly activePlanBuildMsMax: number;
  readonly activePlanFallbackCount: number;
  readonly activePlanAutoBuildSkippedCount: number;
  readonly chdbStatus: "ready" | "degraded" | "restarting" | "stopped";
  readonly chdbPid: number;
  readonly chdbRestarts: number;
  readonly chdbPendingRequests: number;
  readonly chdbLastError: string;
  readonly chdbBackendVersion: string;
  readonly version: string;
  readonly kafkaLagTotal: number;
  readonly kafkaLagMax: number;
  readonly kafkaPartitions: number;
  readonly lastKafkaOffset: number;
  readonly lastKafkaEndOffset: number;
  readonly status: "ready" | "degraded" | "stopping";
};

export type HealthResponse = {
  readonly ok: boolean;
  readonly topics: Readonly<Record<string, HealthTopicMetrics>>;
};

export type RuntimeHealthProjectionTopicInput = {
  readonly worker: TopicWorkerMetrics;
  readonly kafka: KafkaRuntimeMetrics;
  readonly sourceFailed: boolean;
};

export const emptyKafkaRuntimeMetrics: KafkaRuntimeMetrics = {
  lagTotal: 0,
  lagMax: 0,
  partitions: 0,
  offset: 0,
  endOffset: 0,
};

export function kafkaRuntimeMetrics(metrics: KafkaBatchMetrics): KafkaRuntimeMetrics {
  return {
    lagTotal: metrics.lagTotal,
    lagMax: metrics.lagMax,
    partitions: metrics.partitions,
    offset: metrics.offset ?? 0,
    endOffset: metrics.endOffset ?? 0,
  };
}

export function projectRuntimeHealth(args: {
  readonly closing: boolean;
  readonly topics: Readonly<Record<string, RuntimeHealthProjectionTopicInput>>;
}): HealthResponse {
  const topics: Record<string, HealthTopicMetrics> = {};
  for (const [topic, input] of Object.entries(args.topics)) {
    topics[topic] = projectTopicHealth({
      closing: args.closing,
      metrics: input.worker,
      kafka: input.kafka,
      sourceFailed: input.sourceFailed,
    });
  }
  return {
    ok: !args.closing && Object.values(topics).every((topic) => topic.status === "ready"),
    topics,
  };
}

export function healthRowsFromResponse(
  health: HealthResponse,
  now: () => number = Date.now,
): readonly ViewServerHealthRow[] {
  const updatedAt = BigInt(now());
  const topicEntries = Object.entries(health.topics).filter(
    ([topic]) => topic !== VIEW_SERVER_HEALTH_TOPIC,
  );
  const serverStatus = topicEntries.some(([, topic]) => topic.status === "stopping")
    ? "stopping"
    : health.ok
      ? "ready"
      : "degraded";
  const serverRow = healthRow({
    id: "server",
    kind: "server",
    rows: sumTopicMetric(topicEntries, "rows"),
    subscribers: sumTopicMetric(topicEntries, "subscribers"),
    queueDepth: sumTopicMetric(topicEntries, "queueDepth"),
    maxSubscriptionLagVersions: maxTopicMetric(topicEntries, "maxSubscriptionLagVersions"),
    totalSubscriptionLagVersions: sumTopicMetric(topicEntries, "totalSubscriptionLagVersions"),
    activePlanCount: sumTopicMetric(topicEntries, "activePlanCount"),
    activeViewCount: sumTopicMetric(topicEntries, "activeViewCount"),
    activePlanRows: sumTopicMetric(topicEntries, "activePlanRows"),
    activePlanIndexEstimatedBytes: sumTopicMetric(topicEntries, "activePlanIndexEstimatedBytes"),
    activePlanBuildQueueDepth: sumTopicMetric(topicEntries, "activePlanBuildQueueDepth"),
    activePlanBuildingCount: sumTopicMetric(topicEntries, "activePlanBuildingCount"),
    activePlanPendingCount: sumTopicMetric(topicEntries, "activePlanPendingCount"),
    activePlanBuildMs: maxTopicMetric(topicEntries, "activePlanBuildMs"),
    activePlanBuildMsTotal: sumTopicMetric(topicEntries, "activePlanBuildMsTotal"),
    activePlanBuildMsMax: maxTopicMetric(topicEntries, "activePlanBuildMsMax"),
    activePlanFallbackCount: sumTopicMetric(topicEntries, "activePlanFallbackCount"),
    activePlanAutoBuildSkippedCount: sumTopicMetric(
      topicEntries,
      "activePlanAutoBuildSkippedCount",
    ),
    chdbStatus: aggregateChdbStatus(topicEntries),
    chdbPid: 0,
    chdbRestarts: sumTopicMetric(topicEntries, "chdbRestarts"),
    chdbPendingRequests: sumTopicMetric(topicEntries, "chdbPendingRequests"),
    chdbLastError: firstTopicTextMetric(topicEntries, "chdbLastError"),
    chdbBackendVersion: maxTopicVersionString(topicEntries, "chdbBackendVersion"),
    kafkaLagTotal: sumTopicMetric(topicEntries, "kafkaLagTotal"),
    kafkaLagMax: maxTopicMetric(topicEntries, "kafkaLagMax"),
    kafkaPartitions: sumTopicMetric(topicEntries, "kafkaPartitions"),
    lastKafkaOffset: maxTopicMetric(topicEntries, "lastKafkaOffset"),
    lastKafkaEndOffset: maxTopicMetric(topicEntries, "lastKafkaEndOffset"),
    status: serverStatus,
    updatedAt,
  });
  const topicRows = topicEntries.map(([topic, metrics]) =>
    healthRow({
      id: `topic:${topic}`,
      kind: "topic",
      topic,
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
      chdbBackendVersion: metrics.chdbBackendVersion,
      kafkaLagTotal: metrics.kafkaLagTotal,
      kafkaLagMax: metrics.kafkaLagMax,
      kafkaPartitions: metrics.kafkaPartitions,
      lastKafkaOffset: metrics.lastKafkaOffset,
      lastKafkaEndOffset: metrics.lastKafkaEndOffset,
      status: metrics.status,
      updatedAt,
    }),
  );
  return [serverRow, ...topicRows];
}

function projectTopicHealth(args: {
  readonly closing: boolean;
  readonly metrics: TopicWorkerMetrics;
  readonly kafka: KafkaRuntimeMetrics;
  readonly sourceFailed: boolean;
}): HealthTopicMetrics {
  return {
    rows: args.metrics.rows,
    subscribers: args.metrics.subscribers,
    queueDepth: args.metrics.queueDepth,
    maxSubscriptionLagVersions: args.metrics.maxSubscriptionLagVersions,
    totalSubscriptionLagVersions: args.metrics.totalSubscriptionLagVersions,
    activePlanCount: args.metrics.activePlanCount,
    activeViewCount: args.metrics.activeViewCount,
    activePlanRows: args.metrics.activePlanRows,
    activePlanIndexEstimatedBytes: args.metrics.activePlanIndexEstimatedBytes,
    activePlanBuildQueueDepth: args.metrics.activePlanBuildQueueDepth,
    activePlanBuildingCount: args.metrics.activePlanBuildingCount,
    activePlanPendingCount: args.metrics.activePlanPendingCount,
    activePlanBuildMs: args.metrics.activePlanBuildMs,
    activePlanBuildMsTotal: args.metrics.activePlanBuildMsTotal,
    activePlanBuildMsMax: args.metrics.activePlanBuildMsMax,
    activePlanFallbackCount: args.metrics.activePlanFallbackCount,
    activePlanAutoBuildSkippedCount: args.metrics.activePlanAutoBuildSkippedCount,
    chdbStatus: args.metrics.chdbStatus,
    chdbPid: args.metrics.chdbPid,
    chdbRestarts: args.metrics.chdbRestarts,
    chdbPendingRequests: args.metrics.chdbPendingRequests,
    chdbLastError: args.metrics.chdbLastError,
    chdbBackendVersion: args.metrics.chdbBackendVersion.toString(),
    version: args.metrics.version.toString(),
    kafkaLagTotal: args.kafka.lagTotal,
    kafkaLagMax: args.kafka.lagMax,
    kafkaPartitions: args.kafka.partitions,
    lastKafkaOffset: args.kafka.offset,
    lastKafkaEndOffset: args.kafka.endOffset,
    status: args.closing ? "stopping" : args.sourceFailed ? "degraded" : args.metrics.status,
  };
}

function healthRow(input: {
  readonly id: string;
  readonly kind: ViewServerHealthRow["kind"];
  readonly topic?: string | undefined;
  readonly rows: number;
  readonly subscribers: number;
  readonly queueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly totalSubscriptionLagVersions: number;
  readonly activePlanCount: number;
  readonly activeViewCount: number;
  readonly activePlanRows: number;
  readonly activePlanIndexEstimatedBytes: number;
  readonly activePlanBuildQueueDepth: number;
  readonly activePlanBuildingCount: number;
  readonly activePlanPendingCount: number;
  readonly activePlanBuildMs: number;
  readonly activePlanBuildMsTotal: number;
  readonly activePlanBuildMsMax: number;
  readonly activePlanFallbackCount: number;
  readonly activePlanAutoBuildSkippedCount: number;
  readonly chdbStatus: ViewServerHealthRow["chdbStatus"];
  readonly chdbPid: number;
  readonly chdbRestarts: number;
  readonly chdbPendingRequests: number;
  readonly chdbLastError: string;
  readonly chdbBackendVersion: string;
  readonly kafkaLagTotal: number;
  readonly kafkaLagMax: number;
  readonly kafkaPartitions: number;
  readonly lastKafkaOffset: number;
  readonly lastKafkaEndOffset: number;
  readonly status: ViewServerHealthRow["status"];
  readonly updatedAt: bigint;
}): ViewServerHealthRow {
  return {
    id: input.id,
    kind: input.kind,
    ...(input.topic === undefined ? {} : { topic: input.topic }),
    rows: input.rows,
    subscribers: input.subscribers,
    queueDepth: input.queueDepth,
    maxSubscriptionLagVersions: input.maxSubscriptionLagVersions,
    totalSubscriptionLagVersions: input.totalSubscriptionLagVersions,
    activePlanCount: input.activePlanCount,
    activeViewCount: input.activeViewCount,
    activePlanRows: input.activePlanRows,
    activePlanIndexEstimatedBytes: input.activePlanIndexEstimatedBytes,
    activePlanBuildQueueDepth: input.activePlanBuildQueueDepth,
    activePlanBuildingCount: input.activePlanBuildingCount,
    activePlanPendingCount: input.activePlanPendingCount,
    activePlanBuildMs: input.activePlanBuildMs,
    activePlanBuildMsTotal: input.activePlanBuildMsTotal,
    activePlanBuildMsMax: input.activePlanBuildMsMax,
    activePlanFallbackCount: input.activePlanFallbackCount,
    activePlanAutoBuildSkippedCount: input.activePlanAutoBuildSkippedCount,
    chdbStatus: input.chdbStatus,
    chdbPid: input.chdbPid,
    chdbRestarts: input.chdbRestarts,
    chdbPendingRequests: input.chdbPendingRequests,
    chdbLastError: input.chdbLastError,
    chdbBackendVersion: input.chdbBackendVersion,
    workerLagP95Ms: 0,
    deltaFanoutP95Ms: 0,
    publishLatencyP95Ms: 0,
    snapshotLatencyP95Ms: 0,
    chdbSnapshotLatencyP95Ms: 0,
    kafkaLagTotal: input.kafkaLagTotal,
    kafkaLagMax: input.kafkaLagMax,
    kafkaPartitions: input.kafkaPartitions,
    lastKafkaOffset: input.lastKafkaOffset,
    lastKafkaEndOffset: input.lastKafkaEndOffset,
    rssMb: 0,
    status: input.status,
    updatedAt: input.updatedAt,
  };
}

function sumTopicMetric(
  entries: readonly (readonly [string, HealthTopicMetrics])[],
  field:
    | "rows"
    | "subscribers"
    | "queueDepth"
    | "totalSubscriptionLagVersions"
    | "activePlanCount"
    | "activeViewCount"
    | "activePlanRows"
    | "activePlanIndexEstimatedBytes"
    | "activePlanBuildQueueDepth"
    | "activePlanBuildingCount"
    | "activePlanPendingCount"
    | "activePlanBuildMsTotal"
    | "activePlanFallbackCount"
    | "activePlanAutoBuildSkippedCount"
    | "chdbRestarts"
    | "chdbPendingRequests"
    | "kafkaLagTotal"
    | "kafkaPartitions",
): number {
  return entries.reduce((sum, [, metrics]) => sum + metrics[field], 0);
}

function maxTopicMetric(
  entries: readonly (readonly [string, HealthTopicMetrics])[],
  field:
    | "maxSubscriptionLagVersions"
    | "activePlanBuildMs"
    | "activePlanBuildMsMax"
    | "kafkaLagMax"
    | "lastKafkaOffset"
    | "lastKafkaEndOffset",
): number {
  return entries.reduce((max, [, metrics]) => Math.max(max, metrics[field]), 0);
}

function aggregateChdbStatus(
  entries: readonly (readonly [string, HealthTopicMetrics])[],
): ViewServerHealthRow["chdbStatus"] {
  if (entries.some(([, metrics]) => metrics.chdbStatus === "degraded")) {
    return "degraded";
  }
  if (entries.some(([, metrics]) => metrics.chdbStatus === "restarting")) {
    return "restarting";
  }
  if (entries.length > 0 && entries.every(([, metrics]) => metrics.chdbStatus === "stopped")) {
    return "stopped";
  }
  return "ready";
}

function firstTopicTextMetric(
  entries: readonly (readonly [string, HealthTopicMetrics])[],
  field: "chdbLastError",
): string {
  return entries.find(([, metrics]) => metrics[field].length > 0)?.[1][field] ?? "";
}

function maxTopicVersionString(
  entries: readonly (readonly [string, HealthTopicMetrics])[],
  field: "chdbBackendVersion",
): string {
  let max = 0n;
  for (const [, metrics] of entries) {
    const value = BigInt(metrics[field]);
    if (value > max) {
      max = value;
    }
  }
  return max.toString();
}
