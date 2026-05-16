import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import {
  chdbHealthFromSnapshotBackendHealth,
  type ChdbHealth,
  type SnapshotBackendHealth,
} from "../snapshot/index.ts";
import type { ActivePlanCoordinatorMetrics } from "./active-plan-coordinator.ts";
import type { WorkerVersion } from "./mutation-log.ts";
import type { ActiveSubscription } from "./subscription-registry.ts";

export type TopicWorkerStatus = "ready" | "degraded" | "stopping";

export type TopicWorkerMetrics = {
  readonly rows: number;
  readonly subscribers: number;
  readonly version: WorkerVersion;
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
  readonly chdbStatus: ChdbHealth["status"];
  readonly chdbPid: number;
  readonly chdbRestarts: number;
  readonly chdbPendingRequests: number;
  readonly chdbLastError: string;
  readonly chdbBackendVersion: WorkerVersion;
  readonly status: TopicWorkerStatus;
};

export class WorkerHealthProjection {
  #status: TopicWorkerStatus = "ready";
  readonly #options: {
    readonly topic: string;
    readonly rows: () => number;
    readonly version: () => WorkerVersion;
    readonly subscriptionCount: () => number;
    readonly subscriptions: () => Iterable<ActiveSubscription>;
    readonly activePlanMetrics: () => ActivePlanCoordinatorMetrics;
    readonly activePlanLimitNear: (metrics: ActivePlanCoordinatorMetrics) => boolean;
    readonly queueAtLimit: (depth: number) => boolean;
    readonly lagForDepth: (depth: number, pendingLagVersions: bigint) => bigint;
    readonly backendHealth: () => Effect.Effect<SnapshotBackendHealth>;
  };

  constructor(options: {
    readonly topic: string;
    readonly rows: () => number;
    readonly version: () => WorkerVersion;
    readonly subscriptionCount: () => number;
    readonly subscriptions: () => Iterable<ActiveSubscription>;
    readonly activePlanMetrics: () => ActivePlanCoordinatorMetrics;
    readonly activePlanLimitNear: (metrics: ActivePlanCoordinatorMetrics) => boolean;
    readonly queueAtLimit: (depth: number) => boolean;
    readonly lagForDepth: (depth: number, pendingLagVersions: bigint) => bigint;
    readonly backendHealth: () => Effect.Effect<SnapshotBackendHealth>;
  }) {
    this.#options = options;
  }

  markDegraded(): void {
    if (this.#status !== "stopping") {
      this.#status = "degraded";
    }
  }

  markReadyIfDegraded(): void {
    if (this.#status === "degraded") {
      this.#status = "ready";
    }
  }

  markStopping(): void {
    this.#status = "stopping";
  }

  metrics(): Effect.Effect<TopicWorkerMetrics> {
    return Effect.fn("view-server.worker.health.metrics")(function* (
      projection: WorkerHealthProjection,
    ) {
      const depth = yield* projection.#queueDepth();
      const lagStats = yield* projection.#subscriptionLagStats();
      const planStats = projection.#options.activePlanMetrics();
      const snapshotHealth = chdbHealthFromSnapshotBackendHealth(
        yield* projection.#options.backendHealth(),
      );
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": projection.#options.topic,
        "view_server.rows": projection.#options.rows(),
      });
      return {
        rows: projection.#options.rows(),
        subscribers: projection.#options.subscriptionCount(),
        version: projection.#options.version(),
        queueDepth: depth,
        maxSubscriptionLagVersions: lagStats.maxSubscriptionLagVersions,
        totalSubscriptionLagVersions: lagStats.totalSubscriptionLagVersions,
        activePlanCount: planStats.activePlanCount,
        activeViewCount: planStats.activeViewCount,
        activePlanRows: planStats.activePlanRows,
        activePlanIndexEstimatedBytes: planStats.activePlanIndexEstimatedBytes,
        activePlanBuildQueueDepth: planStats.activePlanBuildQueueDepth,
        activePlanBuildingCount: planStats.activePlanBuildingCount,
        activePlanPendingCount: planStats.activePlanPendingCount,
        activePlanBuildMs: planStats.activePlanBuildMs,
        activePlanBuildMsTotal: planStats.activePlanBuildMsTotal,
        activePlanBuildMsMax: planStats.activePlanBuildMsMax,
        activePlanFallbackCount: planStats.activePlanFallbackCount,
        activePlanAutoBuildSkippedCount: planStats.activePlanAutoBuildSkippedCount,
        chdbStatus: snapshotHealth.status,
        chdbPid: snapshotHealth.pid,
        chdbRestarts: snapshotHealth.restarts,
        chdbPendingRequests: snapshotHealth.pendingRequests,
        chdbLastError: snapshotHealth.lastError,
        chdbBackendVersion: snapshotHealth.backendVersion,
        status: projection.#statusForPressure(depth, planStats, snapshotHealth),
      };
    })(this);
  }

  #queueDepth(): Effect.Effect<number> {
    return Effect.fnUntraced(function* (projection: WorkerHealthProjection) {
      let total = 0;
      for (const subscription of projection.#options.subscriptions()) {
        total += yield* Queue.size(subscription.queue);
      }
      return total;
    })(this);
  }

  #subscriptionLagStats(): Effect.Effect<{
    readonly maxSubscriptionLagVersions: number;
    readonly totalSubscriptionLagVersions: number;
  }> {
    return Effect.fnUntraced(function* (projection: WorkerHealthProjection) {
      let maxLag = 0n;
      let totalLag = 0n;
      for (const subscription of projection.#options.subscriptions()) {
        const depth = yield* Queue.size(subscription.queue);
        const queuedLag = projection.#options.lagForDepth(depth, subscription.pendingLagVersions);
        const dirtyLag =
          subscription.dirtyTargetVersion !== undefined &&
          subscription.dirtyTargetVersion > subscription.lastVersion
            ? subscription.dirtyTargetVersion - subscription.lastVersion
            : 0n;
        const lag = queuedLag > dirtyLag ? queuedLag : dirtyLag;
        if (lag > maxLag) {
          maxLag = lag;
        }
        totalLag += lag;
      }
      return {
        maxSubscriptionLagVersions: bigintMetricNumber(maxLag),
        totalSubscriptionLagVersions: bigintMetricNumber(totalLag),
      };
    })(this);
  }

  #statusForPressure(
    depth: number,
    planStats: ActivePlanCoordinatorMetrics,
    snapshotHealth: ChdbHealth,
  ): TopicWorkerStatus {
    if (snapshotHealth.status === "degraded" || snapshotHealth.status === "restarting") {
      return "degraded";
    }
    if (
      this.#status === "ready" &&
      (this.#options.queueAtLimit(depth) ||
        planStats.activePlanFallbackCount > 0 ||
        planStats.activePlanAutoBuildSkippedCount > 0 ||
        this.#options.activePlanLimitNear(planStats))
    ) {
      return "degraded";
    }
    return this.#status;
  }
}

function bigintMetricNumber(value: bigint): number {
  const max = BigInt(Number.MAX_SAFE_INTEGER);
  return value > max ? Number.MAX_SAFE_INTEGER : Number(value);
}
