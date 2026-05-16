import { describe, expect, it } from "@effect/vitest";
import { VIEW_SERVER_HEALTH_TOPIC } from "../src/config/index.ts";
import {
  emptyKafkaRuntimeMetrics,
  healthRowsFromResponse,
  projectRuntimeHealth,
} from "../src/server/runtime-health-projection.ts";
import type { TopicWorkerMetrics } from "../src/worker/topic-worker-core.ts";

describe("RuntimeHealthProjection", () => {
  it("marks runtime ok only when open and every topic is ready", () => {
    const ready = projectRuntimeHealth({
      closing: false,
      topics: {
        orders: {
          worker: topicMetrics({ status: "ready" }),
          kafka: emptyKafkaRuntimeMetrics,
          sourceFailed: false,
        },
      },
    });
    const sourceFailed = projectRuntimeHealth({
      closing: false,
      topics: {
        orders: {
          worker: topicMetrics({ status: "ready" }),
          kafka: emptyKafkaRuntimeMetrics,
          sourceFailed: true,
        },
      },
    });
    const closing = projectRuntimeHealth({
      closing: true,
      topics: {
        orders: {
          worker: topicMetrics({ status: "ready" }),
          kafka: emptyKafkaRuntimeMetrics,
          sourceFailed: false,
        },
      },
    });

    expect(ready.ok).toBe(true);
    expect(ready.topics.orders?.status).toBe("ready");
    expect(sourceFailed.ok).toBe(false);
    expect(sourceFailed.topics.orders?.status).toBe("degraded");
    expect(closing.ok).toBe(false);
    expect(closing.topics.orders?.status).toBe("stopping");
  });

  it("aggregates health topic rows and ignores the private health topic in server totals", () => {
    const health = projectRuntimeHealth({
      closing: false,
      topics: {
        orders: {
          worker: topicMetrics({
            rows: 10,
            subscribers: 2,
            chdbStatus: "degraded",
            chdbLastError: "boom",
            activePlanAutoBuildSkippedCount: 1,
          }),
          kafka: { ...emptyKafkaRuntimeMetrics, lagTotal: 7, lagMax: 5, partitions: 2 },
          sourceFailed: false,
        },
        [VIEW_SERVER_HEALTH_TOPIC]: {
          worker: topicMetrics({ rows: 99, subscribers: 99 }),
          kafka: emptyKafkaRuntimeMetrics,
          sourceFailed: false,
        },
      },
    });

    const rows = healthRowsFromResponse(health, () => 123);

    expect(rows[0]).toMatchObject({
      id: "server",
      rows: 10,
      subscribers: 2,
      chdbStatus: "degraded",
      chdbLastError: "boom",
      kafkaLagTotal: 7,
      kafkaLagMax: 5,
      kafkaPartitions: 2,
      activePlanAutoBuildSkippedCount: 1,
      updatedAt: 123n,
    });
    expect(rows.map((row) => row.id)).toEqual(["server", "topic:orders"]);
  });
});

function topicMetrics(overrides: Partial<TopicWorkerMetrics> = {}): TopicWorkerMetrics {
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
    ...overrides,
  };
}
