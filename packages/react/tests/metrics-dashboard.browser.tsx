import React from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { AsyncResult } from "effect/unstable/reactivity";
import { afterEach, describe, expect, test } from "vite-plus/test";
import { VIEW_SERVER_HEALTH_TOPIC, type ViewServerHealthRow } from "@view-server/core/config";
import {
  ViewServerMetricsDashboard,
  viewServerHealthQuery,
  type ViewServerMetricsHooks,
} from "../src/index.ts";

const roots: Root[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  document.body.innerHTML = "";
});

describe("ViewServerMetricsDashboard", () => {
  test("renders live health topic rows through the public subscription hook", () => {
    const calls: { topic?: unknown; query?: unknown } = {};
    const hooks: ViewServerMetricsHooks = {
      useLiveQuery(topic, query) {
        calls.topic = topic;
        calls.query = query;
        return AsyncResult.success({
          rows: healthRows,
          totalRows: healthRows.length,
          status: "live",
          connection: {
            connected: true,
            attempt: 1,
          },
        });
      },
    };

    render(<ViewServerMetricsDashboard hooks={hooks} />);

    expect(calls.topic).toBe(VIEW_SERVER_HEALTH_TOPIC);
    expect(calls.query).toBe(viewServerHealthQuery);
    expect(document.body.textContent).toContain("Realtime view control");
    expect(document.body.textContent).toContain("ready");
    expect(document.body.textContent).toContain("orders");
    expect(document.body.textContent).toContain("1,024");
    expect(document.body.textContent).toContain("6");
    expect(document.body.textContent).toContain("8,123");
    expect(document.body.textContent).toContain("12ms");
  });
});

function render(element: React.ReactNode): void {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(element));
}

const now = BigInt(Date.UTC(2026, 4, 10, 12, 0, 0));

const healthRows: readonly ViewServerHealthRow[] = [
  {
    id: "server",
    kind: "server",
    rows: 1_024,
    subscribers: 3,
    queueDepth: 0,
    maxSubscriptionLagVersions: 6,
    totalSubscriptionLagVersions: 8,
    activePlanCount: 2,
    activeViewCount: 3,
    activePlanRows: 2_048,
    activePlanIndexEstimatedBytes: 48_000,
    activePlanBuildQueueDepth: 0,
    activePlanBuildingCount: 0,
    activePlanPendingCount: 0,
    activePlanBuildMs: 12,
    activePlanBuildMsTotal: 22,
    activePlanBuildMsMax: 12,
    activePlanFallbackCount: 0,
    chdbStatus: "ready",
    chdbPid: 8123,
    chdbRestarts: 0,
    chdbPendingRequests: 0,
    chdbLastError: "",
    chdbBackendVersion: "1024",
    workerLagP95Ms: 2,
    deltaFanoutP95Ms: 4,
    publishLatencyP95Ms: 12,
    snapshotLatencyP95Ms: 31,
    chdbSnapshotLatencyP95Ms: 9,
    kafkaLagTotal: 0,
    kafkaLagMax: 0,
    kafkaPartitions: 0,
    lastKafkaOffset: 0,
    lastKafkaEndOffset: 0,
    rssMb: 96,
    status: "ready",
    updatedAt: now,
  },
  {
    id: "topic:orders",
    kind: "topic",
    topic: "orders",
    rows: 1_024,
    subscribers: 3,
    queueDepth: 0,
    maxSubscriptionLagVersions: 6,
    totalSubscriptionLagVersions: 8,
    activePlanCount: 2,
    activeViewCount: 3,
    activePlanRows: 2_048,
    activePlanIndexEstimatedBytes: 48_000,
    activePlanBuildQueueDepth: 0,
    activePlanBuildingCount: 0,
    activePlanPendingCount: 0,
    activePlanBuildMs: 12,
    activePlanBuildMsTotal: 22,
    activePlanBuildMsMax: 12,
    activePlanFallbackCount: 0,
    chdbStatus: "ready",
    chdbPid: 8123,
    chdbRestarts: 0,
    chdbPendingRequests: 0,
    chdbLastError: "",
    chdbBackendVersion: "1024",
    workerLagP95Ms: 2,
    deltaFanoutP95Ms: 4,
    publishLatencyP95Ms: 12,
    snapshotLatencyP95Ms: 31,
    chdbSnapshotLatencyP95Ms: 9,
    kafkaLagTotal: 0,
    kafkaLagMax: 0,
    kafkaPartitions: 0,
    lastKafkaOffset: 0,
    lastKafkaEndOffset: 0,
    rssMb: 96,
    status: "ready",
    updatedAt: now,
  },
];
