import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import { RuntimeShutdownController } from "../src/server/runtime-shutdown-controller.ts";
import type { TopicWorkerHost, TopicWorkerMetrics } from "../src/worker/index.ts";

describe("RuntimeShutdownController", () => {
  it.effect("flips closing, rejects new work, and shuts workers down once", () =>
    Effect.gen(function* () {
      let healthSyncs = 0;
      let workerShutdowns = 0;
      const controller = new RuntimeShutdownController();
      const worker = workerHost(() => {
        workerShutdowns += 1;
      });

      yield* controller.close({
        syncHealth: Effect.sync(() => {
          healthSyncs += 1;
        }),
        stopSources: Effect.void,
        workers: [worker],
      });
      yield* controller.close({
        syncHealth: Effect.sync(() => {
          healthSyncs += 1;
        }),
        stopSources: Effect.void,
        workers: [worker],
      });
      const closed = yield* Effect.exit(controller.ensureOpen("query", "orders", "request"));

      expect(controller.isClosing()).toBe(true);
      expect(healthSyncs).toBe(1);
      expect(workerShutdowns).toBe(1);
      expect(Exit.isFailure(closed)).toBe(true);
    }),
  );
});

function workerHost(onShutdown: () => void): TopicWorkerHost {
  return {
    topic: "orders",
    idField: "id",
    version: Effect.succeed(0n),
    metrics: Effect.succeed(topicMetrics()),
    query: () => Effect.succeed({ rows: [], totalRows: 0, version: "0" }),
    subscribe: () => Stream.empty,
    unsubscribe: () => Effect.void,
    publish: () => Effect.void,
    deltaPublish: () => Effect.void,
    deleteById: () => Effect.void,
    getRowsForTest: Effect.succeed([]),
    shutdown: Effect.sync(onShutdown),
  };
}

function topicMetrics(): TopicWorkerMetrics {
  return {
    rows: 0,
    subscribers: 0,
    version: 0n,
    queueDepth: 0,
    maxSubscriptionLagVersions: 0,
    totalSubscriptionLagVersions: 0,
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
    chdbStatus: "ready",
    chdbPid: 0,
    chdbRestarts: 0,
    chdbPendingRequests: 0,
    chdbLastError: "",
    chdbBackendVersion: 0n,
    status: "ready",
  };
}
