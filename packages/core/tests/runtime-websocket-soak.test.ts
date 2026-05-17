import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import type { ActiveSubscription, ViewServerClient } from "../src/client/index.ts";
import { defineConfig } from "../src/config/index.ts";
import type { ViewServerError } from "../src/errors.ts";
import type {
  GroupedQuery,
  RawQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../src/protocol/index.ts";
import { layerViewServerWebsocketServer, makeNodeWebsocketClient } from "../src/rpc/websocket.ts";
import { layerViewServerRuntime, type HealthResponse } from "../src/server/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  status: Schema.String,
  price: Schema.Number,
});

type OrderRow = typeof Order.Type;

const runtimeWebsocketSoakConfig = defineConfig({
  worker: {
    maxQueueDepth: 4_096,
    deltaCoalescing: true,
    groupedRefreshDebounceMs: 5,
    activePlanAutoBuildMaxRows: 100_000,
  },
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const rawFields = {
  id: true,
  symbol: true,
  status: true,
  price: true,
} as const;

type RawOrderQuery = RawQuery<OrderRow, typeof rawFields>;

const groupedQuery = {
  groupBy: ["symbol"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    totalPrice: { aggFunc: "sum", field: "price" },
  },
  orderBy: [
    { field: "totalPrice", direction: "desc" },
    { field: "symbol", direction: "asc" },
  ],
  limit: 20,
} satisfies GroupedQuery<
  OrderRow,
  ["symbol"],
  {
    readonly orders: { readonly aggFunc: "count"; readonly field: "id" };
    readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
  }
>;

describe("runtime websocket soak", () => {
  it.effect(
    "keeps real websocket subscriptions, chDB runtime health, and cleanup stable under mixed load",
    () =>
      Effect.gen(function* () {
        const shape = websocketSoakShape();
        const startedAt = performance.now();
        const initialRows = Array.from({ length: shape.rows }, (_, index) => orderRow(index));
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(runtimeWebsocketSoakConfig, {
              initialRows: {
                orders: initialRows,
              },
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        yield* Effect.gen(function* () {
          const url = yield* websocketUrl();
          const publisher = yield* makeNodeWebsocketClient(url, runtimeWebsocketSoakConfig);
          const clients: SoakClient[] = [];
          let activeClients: SoakClient[] = [];
          let reconnects = 0;
          let nextId = shape.rows;
          let deleteCursor = 0;
          const mutationLatenciesMs: number[] = [];

          const subscriptionStartedAt = performance.now();
          const descriptors = clientDescriptors(shape);
          const connected = yield* Effect.forEach(
            descriptors,
            (descriptor) => connectSoakClient(url, descriptor, undefined),
            { concurrency: shape.connectConcurrency },
          );
          clients.push(...connected);
          activeClients = connected;
          const subscriptionSetupMs = performance.now() - subscriptionStartedAt;

          const ready = yield* waitForClientHealth(
            publisher,
            (health) => health.topics.orders?.subscribers === activeClients.length,
            "initial subscriptions",
          );
          expect(ready.topics.orders?.chdbStatus).toBe("ready");
          expect(ready.topics.orders?.chdbPendingRequests).toBe(0);

          const mutationStartedAt = performance.now();
          for (let index = 0; index < shape.mutations; index++) {
            if (index === Math.floor(shape.mutations / 2) && shape.reconnectClients > 0) {
              const reconnectResult = yield* reconnectSoakClients({
                url,
                healthClient: publisher,
                activeClients,
                reconnectCount: shape.reconnectClients,
                concurrency: shape.connectConcurrency,
              });
              activeClients = reconnectResult.activeClients;
              clients.push(...reconnectResult.reconnected);
              reconnects += reconnectResult.reconnected.length;
            }

            const mutationStartedAt = performance.now();
            yield* applyMixedMutation(publisher, index, nextId, deleteCursor, shape.rows);
            mutationLatenciesMs.push(performance.now() - mutationStartedAt);
            if (index % 10 === 0) {
              yield* Effect.yieldNow;
            }
            if (index % 10 === 4) {
              nextId += 1;
            }
            if (index % 10 >= 8) {
              deleteCursor += 1;
            }
          }
          const mutationLoopMs = performance.now() - mutationStartedAt;

          const settled = yield* waitForClientHealth(
            publisher,
            (health) => {
              const topic = health.topics.orders;
              return (
                topic !== undefined &&
                topic.subscribers === activeClients.length &&
                topic.queueDepth === 0 &&
                topic.activePlanBuildQueueDepth === 0 &&
                topic.activePlanBuildingCount === 0 &&
                topic.activePlanPendingCount === 0 &&
                topic.chdbPendingRequests === 0
              );
            },
            "post-mutation settle",
          );
          expect(settled.ok).toBe(true);
          expect(settled.topics.orders?.chdbStatus).toBe("ready");
          expect(settled.topics.__view_server_health?.rows).toBeGreaterThan(0);

          for (const client of activeClients) {
            expect(
              client.requestIds.every((requestId) => requestId === client.subscription.requestId),
            ).toBe(true);
          }

          const cleanupStartedAt = performance.now();
          yield* Effect.forEach(activeClients, (client) => client.subscription.close, {
            discard: true,
            concurrency: shape.connectConcurrency,
          });
          const released = yield* waitForClientHealth(
            publisher,
            (health) => {
              const topic = health.topics.orders;
              return (
                topic !== undefined &&
                topic.subscribers === 0 &&
                topic.queueDepth === 0 &&
                topic.maxSubscriptionLagVersions === 0 &&
                topic.totalSubscriptionLagVersions === 0 &&
                topic.activePlanCount === 0 &&
                topic.activeViewCount === 0 &&
                topic.activePlanBuildQueueDepth === 0 &&
                topic.activePlanBuildingCount === 0 &&
                topic.activePlanPendingCount === 0 &&
                topic.chdbPendingRequests === 0
              );
            },
            "cleanup",
          );
          const cleanupMs = performance.now() - cleanupStartedAt;
          expect(released.topics.orders?.activePlanIndexEstimatedBytes).toBe(0);

          const events = totalEvents(clients);
          const lifecycle = totalLifecycle(clients);
          expect(events.snapshots).toBeGreaterThanOrEqual(descriptors.length + reconnects);
          expect(events.deltas + events.status).toBeGreaterThan(0);

          const summary: RuntimeWebsocketSoakSummary = {
            shape,
            durationMs: roundMs(performance.now() - startedAt),
            subscriptionSetupMs: roundMs(subscriptionSetupMs),
            mutationLoopMs: roundMs(mutationLoopMs),
            cleanupMs: roundMs(cleanupMs),
            mutationLatencyMs: latencyStats(mutationLatenciesMs),
            maxSubscriptionLagVersionsBeforeCleanup:
              settled.topics.orders?.maxSubscriptionLagVersions ?? -1,
            totalSubscriptionLagVersionsBeforeCleanup:
              settled.topics.orders?.totalSubscriptionLagVersions ?? -1,
            chdbBackendVersionBeforeCleanup: settled.topics.orders?.chdbBackendVersion ?? "0",
            workerVersionBeforeCleanup: settled.topics.orders?.version ?? "0",
            finalRows: released.topics.orders?.rows ?? 0,
            finalVersion: released.topics.orders?.version ?? "0",
            subscribersAfterCleanup: released.topics.orders?.subscribers ?? -1,
            activePlanCountAfterCleanup: released.topics.orders?.activePlanCount ?? -1,
            activeViewCountAfterCleanup: released.topics.orders?.activeViewCount ?? -1,
            activePlanBuildQueueDepthAfterCleanup:
              released.topics.orders?.activePlanBuildQueueDepth ?? -1,
            activePlanBuildingCountAfterCleanup:
              released.topics.orders?.activePlanBuildingCount ?? -1,
            activePlanPendingCountAfterCleanup:
              released.topics.orders?.activePlanPendingCount ?? -1,
            queueDepthAfterCleanup: released.topics.orders?.queueDepth ?? -1,
            maxSubscriptionLagVersionsAfterCleanup:
              released.topics.orders?.maxSubscriptionLagVersions ?? -1,
            totalSubscriptionLagVersionsAfterCleanup:
              released.topics.orders?.totalSubscriptionLagVersions ?? -1,
            chdbStatusAfterCleanup: released.topics.orders?.chdbStatus ?? "stopped",
            chdbPendingRequestsAfterCleanup: released.topics.orders?.chdbPendingRequests ?? -1,
            events,
            retries: lifecycle.retries,
            backpressureErrors: lifecycle.backpressureErrors,
            reconnects,
          };
          yield* writeRuntimeWebsocketSoakSummary(summary);
          yield* Effect.logInfo(
            `runtime websocket soak clients=${descriptors.length} reconnects=${reconnects} mutations=${shape.mutations} snapshots=${events.snapshots} deltas=${events.deltas} status=${events.status} mutationP99Ms=${summary.mutationLatencyMs.p99Ms}`,
          );
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    envNumber("VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS", 45_000),
  );
});

type RuntimeWebsocketSoakShape = {
  readonly rows: number;
  readonly rawClients: number;
  readonly groupedClients: number;
  readonly mutations: number;
  readonly reconnectClients: number;
  readonly connectConcurrency: number;
  readonly rawPageCycle: number;
};

type RuntimeWebsocketSoakSummary = {
  readonly shape: RuntimeWebsocketSoakShape;
  readonly durationMs: number;
  readonly subscriptionSetupMs: number;
  readonly mutationLoopMs: number;
  readonly cleanupMs: number;
  readonly mutationLatencyMs: LatencyStats;
  readonly maxSubscriptionLagVersionsBeforeCleanup: number;
  readonly totalSubscriptionLagVersionsBeforeCleanup: number;
  readonly chdbBackendVersionBeforeCleanup: string;
  readonly workerVersionBeforeCleanup: string;
  readonly finalRows: number;
  readonly finalVersion: string;
  readonly subscribersAfterCleanup: number;
  readonly activePlanCountAfterCleanup: number;
  readonly activeViewCountAfterCleanup: number;
  readonly activePlanBuildQueueDepthAfterCleanup: number;
  readonly activePlanBuildingCountAfterCleanup: number;
  readonly activePlanPendingCountAfterCleanup: number;
  readonly queueDepthAfterCleanup: number;
  readonly maxSubscriptionLagVersionsAfterCleanup: number;
  readonly totalSubscriptionLagVersionsAfterCleanup: number;
  readonly chdbStatusAfterCleanup: HealthResponse["topics"][string]["chdbStatus"];
  readonly chdbPendingRequestsAfterCleanup: number;
  readonly events: SoakEventCounts;
  readonly retries: number;
  readonly backpressureErrors: number;
  readonly reconnects: number;
};

type SoakClientDescriptor =
  | {
      readonly kind: "raw";
      readonly index: number;
      readonly label: string;
      readonly offset: number;
    }
  | {
      readonly kind: "grouped";
      readonly index: number;
      readonly label: string;
    };

type SoakClient = {
  readonly descriptor: SoakClientDescriptor;
  readonly subscription: ActiveSubscription;
  readonly lifecycle: SoakLifecycleCounts;
  readonly events: SoakEventCounts;
  readonly requestIds: readonly string[];
};

type SoakLifecycleCounts = {
  attempts: number;
  retries: number;
  backpressureErrors: number;
};

type SoakEventCounts = {
  snapshots: number;
  deltas: number;
  status: number;
};

type LatencyStats = {
  readonly count: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
};

type ReconnectResult = {
  readonly activeClients: SoakClient[];
  readonly reconnected: readonly SoakClient[];
};

function connectSoakClient(
  url: string,
  descriptor: SoakClientDescriptor,
  previousRequestId: string | undefined,
): Effect.Effect<SoakClient, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.test.runtime_websocket_soak.connect")(function* () {
    const client = yield* makeNodeWebsocketClient(url, runtimeWebsocketSoakConfig);
    const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
    const events: SoakEventCounts = eventCounts();
    const lifecycle: SoakLifecycleCounts = { attempts: 0, retries: 0, backpressureErrors: 0 };
    const requestIds: string[] = [];
    const subscription = yield* client.subscribe(
      "orders",
      queryForDescriptor(descriptor),
      (event) =>
        Effect.sync(() => {
          requestIds.push(event.requestId);
          recordEvent(events, event);
        }).pipe(
          Effect.flatMap(() =>
            event.type === "snapshot"
              ? Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      (event) =>
        Effect.sync(() => {
          if (event.type === "attempt") {
            lifecycle.attempts += 1;
            return;
          }
          lifecycle.retries += 1;
          if (event.error._tag === "BackpressureExceeded") {
            lifecycle.backpressureErrors += 1;
          }
        }),
    );
    const snapshot = yield* Deferred.await(firstSnapshot).pipe(
      Effect.timeout("5 seconds"),
      Effect.orDie,
    );
    expect(snapshot.type).toBe("snapshot");
    expect(snapshot.requestId).toBe(subscription.requestId);
    if (previousRequestId !== undefined) {
      expect(snapshot.requestId).not.toBe(previousRequestId);
    }
    yield* Effect.annotateCurrentSpan({
      "view_server.subscription_id": descriptor.label,
      "view_server.request_id": subscription.requestId,
      "view_server.total_rows": snapshot.meta.totalRows,
    });
    return {
      descriptor,
      subscription,
      lifecycle,
      events,
      requestIds,
    };
  })();
}

function reconnectSoakClients(args: {
  readonly url: string;
  readonly healthClient: ViewServerClient<typeof runtimeWebsocketSoakConfig>;
  readonly activeClients: readonly SoakClient[];
  readonly reconnectCount: number;
  readonly concurrency: number;
}): Effect.Effect<ReconnectResult, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.test.runtime_websocket_soak.reconnect")(function* () {
    const reconnecting = args.activeClients.slice(0, args.reconnectCount);
    const staying = args.activeClients.slice(args.reconnectCount);
    yield* Effect.forEach(reconnecting, (client) => client.subscription.close, {
      discard: true,
      concurrency: args.concurrency,
    });
    yield* waitForClientHealth(
      args.healthClient,
      (health) => health.topics.orders?.subscribers === staying.length,
      "mid-load disconnect",
    );
    const reconnected = yield* Effect.forEach(
      reconnecting,
      (client) => connectSoakClient(args.url, client.descriptor, client.subscription.requestId),
      { concurrency: args.concurrency },
    );
    yield* waitForClientHealth(
      args.healthClient,
      (health) => health.topics.orders?.subscribers === staying.length + reconnected.length,
      "mid-load reconnect",
    );
    return {
      activeClients: [...staying, ...reconnected],
      reconnected,
    };
  })();
}

function queryForDescriptor(descriptor: SoakClientDescriptor): RawOrderQuery | typeof groupedQuery {
  return descriptor.kind === "raw" ? rawQueryWithOffset(descriptor.offset) : groupedQuery;
}

function rawQueryWithOffset(offset: number): RawOrderQuery {
  return {
    fields: rawFields,
    where: {
      field: "price",
      comparator: "greater_than_or_equal",
      value: 0,
    },
    orderBy: [
      { field: "price", direction: "desc" },
      { field: "id", direction: "asc" },
    ],
    offset,
    limit: 20,
  } satisfies RawOrderQuery;
}

function applyMixedMutation(
  client: ViewServerClient<typeof runtimeWebsocketSoakConfig>,
  index: number,
  nextId: number,
  deleteCursor: number,
  initialRows: number,
): Effect.Effect<void, ViewServerError> {
  const operation = index % 10;
  if (operation < 5) {
    return client.publish("orders", orderRow(nextId));
  }
  if (operation < 8) {
    const deltaId = 100 + (index % Math.max(1, initialRows - 100));
    return client.deltaPublish("orders", {
      id: `o-${deltaId}`,
      price: 10_000 + index,
      status: "updated",
    });
  }
  return client.deleteById("orders", `o-${deleteCursor}`);
}

function websocketUrl() {
  return Effect.fn("view-server.test.runtime_websocket_soak.url")(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address;
    if (address._tag !== "TcpAddress") {
      return yield* Effect.die(new Error("Expected test server to listen on TCP"));
    }
    return `ws://127.0.0.1:${address.port}/rpc`;
  })();
}

function waitForClientHealth(
  client: Pick<ViewServerClient<typeof runtimeWebsocketSoakConfig>, "health">,
  predicate: (health: HealthResponse) => boolean,
  label: string,
): Effect.Effect<HealthResponse, ViewServerError> {
  return Effect.gen(function* () {
    let lastHealth: HealthResponse | undefined;
    for (let attempt = 0; attempt < 250; attempt++) {
      const health = yield* client.health();
      lastHealth = health;
      if (predicate(health)) {
        return health;
      }
      yield* sleepHost(20);
    }
    return yield* Effect.die(
      new Error(
        `Timed out waiting for runtime websocket soak ${label}: ${JSON.stringify(lastHealth?.topics.orders)}`,
      ),
    );
  });
}

function sleepHost(milliseconds: number): Effect.Effect<void> {
  return Effect.promise(() => new Promise((resolve) => setTimeout(resolve, milliseconds)));
}

function clientDescriptors(shape: RuntimeWebsocketSoakShape): readonly SoakClientDescriptor[] {
  const raw = Array.from({ length: shape.rawClients }, (_, index) => ({
    kind: "raw" as const,
    index,
    label: `raw-${index}`,
    offset: (index % shape.rawPageCycle) * 20,
  }));
  const grouped = Array.from({ length: shape.groupedClients }, (_, index) => ({
    kind: "grouped" as const,
    index,
    label: `grouped-${index}`,
  }));
  return [...raw, ...grouped];
}

function recordEvent(
  counts: SoakEventCounts,
  event: SubscriptionEvent<readonly RuntimeRow[]>,
): void {
  if (event.type === "snapshot") {
    counts.snapshots += 1;
    return;
  }
  if (event.type === "delta") {
    counts.deltas += 1;
    return;
  }
  counts.status += 1;
}

function eventCounts(): SoakEventCounts {
  return {
    snapshots: 0,
    deltas: 0,
    status: 0,
  };
}

function totalEvents(clients: readonly SoakClient[]): SoakEventCounts {
  return clients.reduce(
    (total, client) => ({
      snapshots: total.snapshots + client.events.snapshots,
      deltas: total.deltas + client.events.deltas,
      status: total.status + client.events.status,
    }),
    eventCounts(),
  );
}

function totalLifecycle(clients: readonly SoakClient[]): SoakLifecycleCounts {
  return clients.reduce(
    (total, client) => ({
      attempts: total.attempts + client.lifecycle.attempts,
      retries: total.retries + client.lifecycle.retries,
      backpressureErrors: total.backpressureErrors + client.lifecycle.backpressureErrors,
    }),
    { attempts: 0, retries: 0, backpressureErrors: 0 },
  );
}

function latencyStats(samples: readonly number[]): LatencyStats {
  if (samples.length === 0) {
    return {
      count: 0,
      p50Ms: 0,
      p95Ms: 0,
      p99Ms: 0,
      maxMs: 0,
    };
  }
  const sorted = [...samples].sort((left, right) => left - right);
  return {
    count: samples.length,
    p50Ms: roundMs(percentile(sorted, 0.5)),
    p95Ms: roundMs(percentile(sorted, 0.95)),
    p99Ms: roundMs(percentile(sorted, 0.99)),
    maxMs: roundMs(sorted[sorted.length - 1] ?? 0),
  };
}

function percentile(sortedSamples: readonly number[], quantile: number): number {
  const index = Math.min(
    sortedSamples.length - 1,
    Math.max(0, Math.ceil(sortedSamples.length * quantile) - 1),
  );
  return sortedSamples[index] ?? 0;
}

function roundMs(value: number): number {
  return Math.round(value * 100) / 100;
}

function writeRuntimeWebsocketSoakSummary(
  summary: RuntimeWebsocketSoakSummary,
): Effect.Effect<void> {
  const summaryPath = process.env.VS_RUNTIME_WEBSOCKET_SOAK_SUMMARY_PATH;
  if (summaryPath === undefined || summaryPath.length === 0) {
    return Effect.void;
  }
  return Effect.promise(async () => {
    await mkdir(dirname(summaryPath), { recursive: true });
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  });
}

function websocketSoakShape(): RuntimeWebsocketSoakShape {
  const rawClients = envNumber("VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS", 12);
  const groupedClients = envNumber("VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS", 3);
  const reconnectFallback = Math.min(10, rawClients + groupedClients);
  return {
    rows: envNumber("VS_RUNTIME_WEBSOCKET_SOAK_ROWS", 500),
    rawClients,
    groupedClients,
    mutations: envNumber("VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS", 120),
    reconnectClients: envNumber("VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS", reconnectFallback),
    connectConcurrency: Math.max(1, envNumber("VS_RUNTIME_WEBSOCKET_SOAK_CONNECT_CONCURRENCY", 10)),
    rawPageCycle: Math.max(1, envNumber("VS_RUNTIME_WEBSOCKET_SOAK_RAW_PAGE_CYCLE", 6)),
  };
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : fallback;
}

function orderRow(index: number): OrderRow {
  return {
    id: `o-${index}`,
    symbol: `SYM-${index % 25}`,
    status: index % 3 === 0 ? "open" : index % 3 === 1 ? "pending" : "closed",
    price: index % 1_000,
  };
}
