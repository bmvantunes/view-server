import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { defineConfig, normalizeConfig, VIEW_SERVER_HEALTH_TOPIC } from "../src/config/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../src/snapshot/index.ts";
import { makeViewServerRuntime } from "../src/server/index.ts";
import { createTopicPlacements } from "../src/server/topic-placement.ts";
import type {
  TopicWorkerHost,
  TopicWorkerHostOptions,
  TopicWorkerMetrics,
} from "../src/worker/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
    trades: {
      id: "id",
      schema: Trade,
    },
  },
});

describe("TopicPlacement", () => {
  it.effect("creates one worker and one distinct snapshot backend per user topic", () =>
    Effect.gen(function* () {
      const backendFactoryTopics: string[] = [];
      const workerBackends = new Map<string, SnapshotBackend | undefined>();

      const placements = yield* createTopicPlacements(normalizeConfig(config), {
        __testingSnapshotBackendFactory: (topic) => {
          backendFactoryTopics.push(topic);
          return createMemorySnapshotBackend();
        },
        topicWorkerFactory: (topic, _topicConfig, options) =>
          Effect.sync(() => {
            workerBackends.set(topic, options.snapshotBackend);
            return workerHost(topic, options);
          }),
      });

      expect(placements.placements.map((placement) => placement.topic)).toEqual([
        "orders",
        "trades",
        VIEW_SERVER_HEALTH_TOPIC,
      ]);
      expect(Array.from(placements.workers.keys())).toEqual([
        "orders",
        "trades",
        VIEW_SERVER_HEALTH_TOPIC,
      ]);
      expect(backendFactoryTopics).toEqual(["orders", "trades"]);
      expect(workerBackends.get("orders")).toBeDefined();
      expect(workerBackends.get("trades")).toBeDefined();
      expect(workerBackends.get("orders")).not.toBe(workerBackends.get("trades"));
      expect(workerBackends.get(VIEW_SERVER_HEALTH_TOPIC)).toBeDefined();
      expect(workerBackends.get(VIEW_SERVER_HEALTH_TOPIC)).not.toBe(workerBackends.get("orders"));
    }).pipe(Effect.scoped),
  );

  it.effect("lets the runtime use the placement policy without sharing topic backends", () =>
    Effect.gen(function* () {
      const workerBackends = new Map<string, SnapshotBackend | undefined>();

      const runtime = yield* makeViewServerRuntime(config, {
        __testingSnapshotBackendFactory: () => createMemorySnapshotBackend(),
        topicWorkerFactory: (topic, _topicConfig, options) =>
          Effect.sync(() => {
            workerBackends.set(topic, options.snapshotBackend);
            return workerHost(topic, options);
          }),
      });

      expect((yield* runtime.health).ok).toBe(true);
      expect(workerBackends.get("orders")).toBeDefined();
      expect(workerBackends.get("trades")).toBeDefined();
      expect(workerBackends.get("orders")).not.toBe(workerBackends.get("trades"));
      yield* runtime.close;
    }).pipe(Effect.scoped),
  );
});

function workerHost(topic: string, options: TopicWorkerHostOptions): TopicWorkerHost {
  let rows = options.initialRows?.length ?? 0;
  return {
    topic,
    idField: "id",
    version: Effect.succeed(0n),
    metrics: Effect.sync(() => topicMetrics(rows)),
    query: () => Effect.succeed({ rows: [], totalRows: rows, version: "0" }),
    subscribe: () => Stream.empty,
    unsubscribe: () => Effect.void,
    publish: () =>
      Effect.sync(() => {
        rows += 1;
      }),
    deltaPublish: () => Effect.void,
    deleteById: () => Effect.void,
    getRowsForTest: Effect.succeed([]),
    shutdown: Effect.void,
  };
}

function topicMetrics(rows: number): TopicWorkerMetrics {
  return {
    rows,
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
