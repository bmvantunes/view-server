import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";
import { Buffer } from "node:buffer";
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
import {
  layerViewServerWebsocketServer,
  makeNodeWebsocketClient,
  type WebsocketFanoutMetricsSnapshot,
  ViewServerWebsocketFanoutMetrics,
} from "../src/rpc/websocket.ts";
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
          const websocketFanoutMetrics = yield* ViewServerWebsocketFanoutMetrics;
          const publisher = yield* makeNodeWebsocketClient(url, runtimeWebsocketSoakConfig);
          const clients: SoakClient[] = [];
          let activeClients: SoakClient[] = [];
          let reconnects = 0;
          let nextId = shape.rows;
          let deleteCursor = 0;
          const mutationLatenciesMs: number[] = [];
          const topSlowMutations: SlowMutationSample[] = [];
          let reconnectActive = false;
          let mutationsDuringReconnect = 0;
          let reconnectStartedAtMs: number | null = null;
          let reconnectCompletedAtMs: number | null = null;
          let reconnectFiber: Fiber.Fiber<ReconnectResult, ViewServerError> | undefined;
          const observed = observedMetrics();

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
          recordHealthObservation(observed, ready);
          expect(ready.topics.orders?.chdbStatus).toBe("ready");
          expect(ready.topics.orders?.chdbPendingRequests).toBe(0);

          const mutationStartedAt = performance.now();
          for (let index = 0; index < shape.mutations; index++) {
            if (
              index === Math.floor(shape.mutations / 2) &&
              shape.reconnectClients > 0 &&
              reconnectFiber === undefined
            ) {
              reconnectActive = true;
              reconnectStartedAtMs = roundMs(performance.now() - startedAt);
              reconnectFiber = yield* reconnectSoakClients({
                url,
                healthClient: publisher,
                activeClients,
                reconnectCount: shape.reconnectClients,
                concurrency: shape.connectConcurrency,
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    reconnectActive = false;
                    reconnectCompletedAtMs = roundMs(performance.now() - startedAt);
                  }),
                ),
                Effect.forkScoped,
              );
            }

            if (reconnectActive) {
              mutationsDuringReconnect += 1;
            }
            const operation = mutationOperation(index, nextId, deleteCursor, shape.rows);
            const operationStartedAt = performance.now();
            yield* applyMixedMutation(publisher, operation);
            const latencyMs = performance.now() - operationStartedAt;
            mutationLatenciesMs.push(latencyMs);
            yield* recordSlowMutationSample({
              samples: topSlowMutations,
              client: publisher,
              websocketFanoutMetrics,
              observed,
              index,
              operation,
              latencyMs,
              reconnectActive,
              elapsedMs: performance.now() - startedAt,
              events: totalEvents(clients),
              payloadBytes: totalPayloadBytes(clients),
            });
            if (index % shape.healthSampleInterval === 0) {
              const health = yield* publisher.health();
              recordHealthObservation(observed, health);
            }
            if (index % 10 === 0) {
              yield* Effect.yieldNow;
            }
            if (operation.type === "publish") {
              nextId += 1;
            }
            if (operation.type === "deleteById") {
              deleteCursor += 1;
            }
          }
          if (reconnectFiber !== undefined) {
            const reconnectResult = yield* Fiber.join(reconnectFiber);
            activeClients = reconnectResult.activeClients;
            clients.push(...reconnectResult.reconnected);
            reconnects += reconnectResult.reconnected.length;
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
          recordHealthObservation(observed, settled);
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
          recordHealthObservation(observed, released);
          const cleanupMs = performance.now() - cleanupStartedAt;
          expect(released.topics.orders?.activePlanIndexEstimatedBytes).toBe(0);

          const events = totalEvents(clients);
          const payloadBytes = totalPayloadBytes(clients);
          const lifecycle = totalLifecycle(clients);
          const websocketFanout = yield* waitForWebsocketFanout(
            websocketFanoutMetrics,
            (snapshot) => snapshot.activeClients === 0,
            "cleanup",
          );
          expect(websocketFanout.activeClients).toBe(0);
          expect(websocketFanout.totalMessages).toBeGreaterThan(0);
          expect(websocketFanout.maxClientQueuedBytes).toBeGreaterThan(0);
          expect(websocketFanout.maxBatchBytes).toBeGreaterThan(0);
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
            chdbBackendVersionAfterCleanup: released.topics.orders?.chdbBackendVersion ?? "0",
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
            payloadBytes,
            retries: lifecycle.retries,
            backpressureErrors: lifecycle.backpressureErrors,
            reconnects,
            reconnectWindow: {
              startedAtMs: reconnectStartedAtMs,
              completedAtMs: reconnectCompletedAtMs,
              mutationsDuringReconnect,
            },
            observed,
            websocketFanout,
            groupedRefreshCountsAvailable: false,
            topSlowMutations,
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
  readonly healthSampleInterval: number;
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
  readonly chdbBackendVersionAfterCleanup: string;
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
  readonly payloadBytes: SoakPayloadBytes;
  readonly retries: number;
  readonly backpressureErrors: number;
  readonly reconnects: number;
  readonly reconnectWindow: {
    readonly startedAtMs: number | null;
    readonly completedAtMs: number | null;
    readonly mutationsDuringReconnect: number;
  };
  readonly observed: RuntimeWebsocketObservedMetrics;
  readonly websocketFanout: WebsocketFanoutMetricsSnapshot;
  readonly groupedRefreshCountsAvailable: boolean;
  readonly topSlowMutations: readonly SlowMutationSample[];
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
  readonly payloadBytes: SoakPayloadBytes;
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

type SoakPayloadBytes = {
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

type MutationOperation =
  | {
      readonly type: "publish";
      readonly id: string;
    }
  | {
      readonly type: "deltaPublish";
      readonly id: string;
    }
  | {
      readonly type: "deleteById";
      readonly id: string;
    };

type SlowMutationSample = {
  readonly index: number;
  readonly operation: MutationOperation;
  readonly latencyMs: number;
  readonly reconnectActive: boolean;
  readonly elapsedMs: number;
  readonly events: SoakEventCounts;
  readonly payloadBytes: SoakPayloadBytes;
  readonly health: MutationHealthSnapshot;
  readonly websocketFanout: WebsocketFanoutMetricsSnapshot;
};

type MutationHealthSnapshot = {
  readonly subscribers: number;
  readonly queueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly totalSubscriptionLagVersions: number;
  readonly activePlanCount: number;
  readonly activeViewCount: number;
  readonly activePlanBuildQueueDepth: number;
  readonly activePlanBuildingCount: number;
  readonly activePlanPendingCount: number;
  readonly chdbStatus: HealthResponse["topics"][string]["chdbStatus"];
  readonly chdbPendingRequests: number;
  readonly chdbBackendVersion: string;
  readonly workerVersion: string;
  readonly chdbBackendLagVersions: number;
};

type RuntimeWebsocketObservedMetrics = {
  maxQueueDepth: number;
  maxSubscriptionLagVersions: number;
  maxChdbPendingRequests: number;
  maxChdbBackendLagVersions: number;
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
    const payloadBytes: SoakPayloadBytes = payloadByteCounts();
    const lifecycle: SoakLifecycleCounts = { attempts: 0, retries: 0, backpressureErrors: 0 };
    const requestIds: string[] = [];
    const subscription = yield* client.subscribe(
      "orders",
      queryForDescriptor(descriptor),
      (event) =>
        Effect.sync(() => {
          requestIds.push(event.requestId);
          recordEvent(events, payloadBytes, event);
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
      payloadBytes,
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

function mutationOperation(
  index: number,
  nextId: number,
  deleteCursor: number,
  initialRows: number,
): MutationOperation {
  const operation = index % 10;
  if (operation < 5) {
    return { type: "publish", id: `o-${nextId}` };
  }
  if (operation < 8) {
    const deltaId = 100 + (index % Math.max(1, initialRows - 100));
    return { type: "deltaPublish", id: `o-${deltaId}` };
  }
  return { type: "deleteById", id: `o-${deleteCursor}` };
}

function applyMixedMutation(
  client: ViewServerClient<typeof runtimeWebsocketSoakConfig>,
  operation: MutationOperation,
): Effect.Effect<void, ViewServerError> {
  if (operation.type === "publish") {
    return client.publish("orders", orderRowFromId(operation.id));
  }
  if (operation.type === "deltaPublish") {
    return client.deltaPublish("orders", {
      id: operation.id,
      price: 10_000 + numericOrderId(operation.id),
      status: "updated",
    });
  }
  return client.deleteById("orders", operation.id);
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

function waitForWebsocketFanout(
  metrics: WebsocketFanoutMetricsSnapshotService,
  predicate: (snapshot: WebsocketFanoutMetricsSnapshot) => boolean,
  label: string,
): Effect.Effect<WebsocketFanoutMetricsSnapshot> {
  return Effect.gen(function* () {
    let lastSnapshot: WebsocketFanoutMetricsSnapshot | undefined;
    for (let attempt = 0; attempt < 250; attempt++) {
      const snapshot = yield* metrics.snapshot;
      lastSnapshot = snapshot;
      if (predicate(snapshot)) {
        return snapshot;
      }
      yield* sleepHost(20);
    }
    return yield* Effect.die(
      new Error(
        `Timed out waiting for runtime websocket fanout ${label}: ${JSON.stringify(lastSnapshot)}`,
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
  payloadBytes: SoakPayloadBytes,
  event: SubscriptionEvent<readonly RuntimeRow[]>,
): void {
  const bytes = Buffer.byteLength(JSON.stringify(event));
  if (event.type === "snapshot") {
    counts.snapshots += 1;
    payloadBytes.snapshots += bytes;
    return;
  }
  if (event.type === "delta") {
    counts.deltas += 1;
    payloadBytes.deltas += bytes;
    return;
  }
  counts.status += 1;
  payloadBytes.status += bytes;
}

function eventCounts(): SoakEventCounts {
  return {
    snapshots: 0,
    deltas: 0,
    status: 0,
  };
}

function payloadByteCounts(): SoakPayloadBytes {
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

function totalPayloadBytes(clients: readonly SoakClient[]): SoakPayloadBytes {
  return clients.reduce(
    (total, client) => ({
      snapshots: total.snapshots + client.payloadBytes.snapshots,
      deltas: total.deltas + client.payloadBytes.deltas,
      status: total.status + client.payloadBytes.status,
    }),
    payloadByteCounts(),
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

function recordSlowMutationSample(args: {
  readonly samples: SlowMutationSample[];
  readonly client: Pick<ViewServerClient<typeof runtimeWebsocketSoakConfig>, "health">;
  readonly websocketFanoutMetrics: WebsocketFanoutMetricsSnapshotService;
  readonly observed: RuntimeWebsocketObservedMetrics;
  readonly index: number;
  readonly operation: MutationOperation;
  readonly latencyMs: number;
  readonly reconnectActive: boolean;
  readonly elapsedMs: number;
  readonly events: SoakEventCounts;
  readonly payloadBytes: SoakPayloadBytes;
}): Effect.Effect<void, ViewServerError> {
  return Effect.fnUntraced(function* () {
    if (!isSlowMutationCandidate(args.samples, args.latencyMs)) {
      return;
    }
    const health = yield* args.client.health();
    const websocketFanout = yield* args.websocketFanoutMetrics.snapshot;
    recordHealthObservation(args.observed, health);
    args.samples.push({
      index: args.index,
      operation: args.operation,
      latencyMs: roundMs(args.latencyMs),
      reconnectActive: args.reconnectActive,
      elapsedMs: roundMs(args.elapsedMs),
      events: args.events,
      payloadBytes: args.payloadBytes,
      health: mutationHealthSnapshot(health),
      websocketFanout,
    });
    args.samples.sort((left, right) => right.latencyMs - left.latencyMs);
    if (args.samples.length > 10) {
      args.samples.pop();
    }
  })();
}

type WebsocketFanoutMetricsSnapshotService = {
  readonly snapshot: Effect.Effect<WebsocketFanoutMetricsSnapshot>;
};

function observedMetrics(): RuntimeWebsocketObservedMetrics {
  return {
    maxQueueDepth: 0,
    maxSubscriptionLagVersions: 0,
    maxChdbPendingRequests: 0,
    maxChdbBackendLagVersions: 0,
  };
}

function recordHealthObservation(
  observed: RuntimeWebsocketObservedMetrics,
  health: HealthResponse,
): void {
  const snapshot = mutationHealthSnapshot(health);
  observed.maxQueueDepth = Math.max(observed.maxQueueDepth, snapshot.queueDepth);
  observed.maxSubscriptionLagVersions = Math.max(
    observed.maxSubscriptionLagVersions,
    snapshot.maxSubscriptionLagVersions,
  );
  observed.maxChdbPendingRequests = Math.max(
    observed.maxChdbPendingRequests,
    snapshot.chdbPendingRequests,
  );
  observed.maxChdbBackendLagVersions = Math.max(
    observed.maxChdbBackendLagVersions,
    snapshot.chdbBackendLagVersions,
  );
}

function isSlowMutationCandidate(
  samples: readonly SlowMutationSample[],
  latencyMs: number,
): boolean {
  if (samples.length < 10) {
    return true;
  }
  const slowestKept = samples[samples.length - 1];
  return slowestKept === undefined || latencyMs > slowestKept.latencyMs;
}

function mutationHealthSnapshot(health: HealthResponse): MutationHealthSnapshot {
  const topic = health.topics.orders;
  return {
    subscribers: topic?.subscribers ?? -1,
    queueDepth: topic?.queueDepth ?? -1,
    maxSubscriptionLagVersions: topic?.maxSubscriptionLagVersions ?? -1,
    totalSubscriptionLagVersions: topic?.totalSubscriptionLagVersions ?? -1,
    activePlanCount: topic?.activePlanCount ?? -1,
    activeViewCount: topic?.activeViewCount ?? -1,
    activePlanBuildQueueDepth: topic?.activePlanBuildQueueDepth ?? -1,
    activePlanBuildingCount: topic?.activePlanBuildingCount ?? -1,
    activePlanPendingCount: topic?.activePlanPendingCount ?? -1,
    chdbStatus: topic?.chdbStatus ?? "stopped",
    chdbPendingRequests: topic?.chdbPendingRequests ?? -1,
    chdbBackendVersion: topic?.chdbBackendVersion ?? "0",
    workerVersion: topic?.version ?? "0",
    chdbBackendLagVersions: versionLag(topic?.version ?? "0", topic?.chdbBackendVersion ?? "0"),
  };
}

function versionLag(workerVersion: string, backendVersion: string): number {
  const lag = BigInt(workerVersion) - BigInt(backendVersion);
  if (lag <= 0n) {
    return 0;
  }
  return lag > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(lag);
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
    healthSampleInterval: Math.max(
      1,
      envNumber("VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL", 25),
    ),
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

function orderRowFromId(id: string): OrderRow {
  return orderRow(numericOrderId(id));
}

function numericOrderId(id: string): number {
  return Number(id.slice(2));
}
