import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import type { ActivePlanCoordinatorMetrics } from "../src/worker/active-plan-coordinator.ts";
import type { ActiveSubscription } from "../src/worker/subscription-registry.ts";
import { WorkerHealthProjection } from "../src/worker/worker-health-projection.ts";

describe("WorkerHealthProjection", () => {
  it.effect("projects queue depth, logical lag, active plan metrics, and chDB health", () =>
    Effect.gen(function* () {
      const firstQueue = yield* subscriptionQueue();
      const secondQueue = yield* subscriptionQueue();
      yield* Queue.offer(firstQueue, snapshot("first"));
      const subscriptions = [
        subscription({
          requestId: "first",
          queue: firstQueue,
          pendingLagVersions: 5n,
          lastVersion: 10n,
        }),
        subscription({
          requestId: "second",
          queue: secondQueue,
          pendingLagVersions: 0n,
          lastVersion: 10n,
          dirtyTargetVersion: 20n,
        }),
      ];
      const projection = new WorkerHealthProjection({
        topic: "orders",
        rows: () => 123,
        version: () => 55n,
        subscriptionCount: () => subscriptions.length,
        subscriptions: () => subscriptions.values(),
        activePlanMetrics: () => activePlanMetrics({ activePlanCount: 2, activeViewCount: 3 }),
        activePlanLimitNear: () => false,
        queueAtLimit: () => false,
        lagForDepth: (depth, pendingLagVersions) => (depth === 0 ? 0n : pendingLagVersions),
        backendHealth: () =>
          Effect.succeed({
            status: "ready",
            pid: 1234,
            restarts: 2,
            pendingRequests: 1,
            backendVersion: 54n,
          }),
      });

      const metrics = yield* projection.metrics();

      expect(metrics).toMatchObject({
        rows: 123,
        subscribers: 2,
        version: 55n,
        queueDepth: 1,
        maxSubscriptionLagVersions: 10,
        totalSubscriptionLagVersions: 15,
        activePlanCount: 2,
        activeViewCount: 3,
        chdbStatus: "ready",
        chdbPid: 1234,
        chdbRestarts: 2,
        chdbPendingRequests: 1,
        chdbBackendVersion: 54n,
        status: "ready",
      });
    }),
  );

  it.effect(
    "marks status degraded for backend, queue, active-plan pressure, and recovers explicitly",
    () =>
      Effect.gen(function* () {
        const subscriptions: ActiveSubscription[] = [];
        let backendStatus: "ready" | "degraded" = "ready";
        let queueLimit = false;
        let fallbackCount = 0;
        const projection = new WorkerHealthProjection({
          topic: "orders",
          rows: () => 0,
          version: () => 0n,
          subscriptionCount: () => subscriptions.length,
          subscriptions: () => subscriptions.values(),
          activePlanMetrics: () => activePlanMetrics({ activePlanFallbackCount: fallbackCount }),
          activePlanLimitNear: () => false,
          queueAtLimit: () => queueLimit,
          lagForDepth: () => 0n,
          backendHealth: () => Effect.succeed({ status: backendStatus }),
        });

        queueLimit = true;
        expect((yield* projection.metrics()).status).toBe("degraded");

        queueLimit = false;
        fallbackCount = 1;
        expect((yield* projection.metrics()).status).toBe("degraded");

        fallbackCount = 0;
        backendStatus = "degraded";
        expect((yield* projection.metrics()).status).toBe("degraded");

        backendStatus = "ready";
        projection.markDegraded();
        expect((yield* projection.metrics()).status).toBe("degraded");

        projection.markReadyIfDegraded();
        expect((yield* projection.metrics()).status).toBe("ready");
      }),
  );

  it.effect("keeps stopping status stable and clamps enormous lag metrics", () =>
    Effect.gen(function* () {
      const queue = yield* subscriptionQueue();
      yield* Queue.offer(queue, snapshot("request"));
      const subscriptions = [
        subscription({
          requestId: "request",
          queue,
          pendingLagVersions: BigInt(Number.MAX_SAFE_INTEGER) + 100n,
          lastVersion: 0n,
        }),
      ];
      const projection = new WorkerHealthProjection({
        topic: "orders",
        rows: () => 0,
        version: () => 0n,
        subscriptionCount: () => subscriptions.length,
        subscriptions: () => subscriptions.values(),
        activePlanMetrics: () => activePlanMetrics(),
        activePlanLimitNear: () => false,
        queueAtLimit: () => false,
        lagForDepth: (_depth, pendingLagVersions) => pendingLagVersions,
        backendHealth: () => Effect.succeed({ status: "ready" }),
      });

      projection.markStopping();
      projection.markDegraded();
      projection.markReadyIfDegraded();
      const metrics = yield* projection.metrics();

      expect(metrics.status).toBe("stopping");
      expect(metrics.maxSubscriptionLagVersions).toBe(Number.MAX_SAFE_INTEGER);
      expect(metrics.totalSubscriptionLagVersions).toBe(Number.MAX_SAFE_INTEGER);
    }),
  );
});

function activePlanMetrics(
  overrides: Partial<ActivePlanCoordinatorMetrics> = {},
): ActivePlanCoordinatorMetrics {
  return {
    activePlanCount: 0,
    activeViewCount: 0,
    activePlanRows: 0,
    activePlanIndexEstimatedBytes: 0,
    activePlanBuildQueueDepth: 0,
    activePlanBuildingCount: 0,
    activePlanPendingCount: 0,
    activePlanBuildMs: 0,
    activePlanBuildMsTotal: 0,
    activePlanBuildMsMax: 0,
    activePlanFallbackCount: 0,
    activePlanAutoBuildSkippedCount: 0,
    ...overrides,
  };
}

function subscription(args: {
  readonly requestId: string;
  readonly queue: ActiveSubscription["queue"];
  readonly pendingLagVersions: bigint;
  readonly lastVersion: bigint;
  readonly dirtyTargetVersion?: bigint | undefined;
}): ActiveSubscription {
  return {
    requestId: args.requestId,
    query: { fields: { id: true } },
    dependencyFields: new Set(["id"]),
    queue: args.queue,
    lastRows: [],
    lastTotalRows: 0,
    lastVersion: args.lastVersion,
    pendingLagVersions: args.pendingLagVersions,
    dirtyTargetVersion: args.dirtyTargetVersion,
  };
}

function subscriptionQueue() {
  return Queue.unbounded<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError | Cause.Done>();
}

function snapshot(requestId: string): SubscriptionEvent<readonly RuntimeRow[]> {
  return {
    type: "snapshot",
    requestId,
    rows: [],
    meta: {
      version: "0",
      totalRows: 0,
      serverTime: 0,
    },
  };
}
