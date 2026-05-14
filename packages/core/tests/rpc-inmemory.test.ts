import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import * as RpcTest from "effect/unstable/rpc/RpcTest";
import { createViewServerClient, type ViewServerRpcTransport } from "../src/client/index.ts";
import { applyDeltaOperations } from "../src/client/live-query-store.ts";
import {
  defineConfig,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthRow,
} from "../src/config/index.ts";
import { transportError, type ViewServerError } from "../src/errors.ts";
import type {
  GroupedQuery,
  RawQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../src/protocol/index.ts";
import { ViewServerHandlersLive, ViewServerRpcs } from "../src/rpc/index.ts";
import { makeViewServerRuntime, ViewServerRuntime } from "../src/server/index.ts";
import {
  makeTopicWorkerCore,
  subscriptionLagVersionsForQueueDepth,
  type TopicWorkerCore,
  type TopicWorkerMetrics,
} from "../src/worker/topic-worker-core.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../src/snapshot/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const backpressureConfig = defineConfig({
  worker: {
    maxQueueDepth: 1,
    deltaCoalescing: false,
  },
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const coalesceQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const idFallbackQuery = {
  fields: {
    id: true,
    price: true,
  },
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const firstByPriceQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 1,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const groupedOrdersQuery = {
  groupBy: ["symbol"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    totalPrice: { aggFunc: "sum", field: "price" },
  },
  orderBy: [{ field: "totalPrice", direction: "desc" }],
  limit: 10,
} satisfies GroupedQuery<
  OrderRow,
  ["symbol"],
  {
    readonly orders: { readonly aggFunc: "count"; readonly field: "id" };
    readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
  }
>;

const healthQuery = {
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
    status: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
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
    readonly status: true;
  }
>;

describe("Effect RPC in-memory", () => {
  it.effect("streams a snapshot and following delta without a subscription mode field", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      });
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );

      const events = yield* client
        .Subscribe({
          requestId: "rpc-sub",
          topic: "orders",
          query,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      expect(snapshot.type).toBe("snapshot");
      expect(snapshot.meta.totalRows).toBe(2);

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });

      const delta = yield* Queue.take(events);
      if (delta.type !== "delta") {
        throw new Error("Expected delta");
      }
      expect(delta.meta.totalRows).toBe(3);
      expect(
        delta.ops.some((operation) => operation.type === "upsert" && operation.row.id === "o-3"),
      ).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("starts client subscription stream consumers immediately", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      });
      const rpcClient = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const client = createViewServerClient<typeof config>(rpcClient, config);
      const firstEvent = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();

      yield* client.subscribe("orders", query, (event) =>
        Deferred.succeed(firstEvent, event).pipe(Effect.asVoid),
      );

      const snapshot = yield* Deferred.await(firstEvent).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");
      expect(snapshot.meta.totalRows).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("resubscribes after a transient transport failure", () =>
    Effect.gen(function* () {
      let attempts = 0;
      const unsubscribed: string[] = [];
      const firstEvent = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const transport: ViewServerRpcTransport = {
        Query: () => Effect.succeed({ rows: [], totalRows: 0, version: "0" }),
        Subscribe: (payload) => {
          attempts += 1;
          if (attempts === 1) {
            return Stream.fail(transportError("socket closed"));
          }
          return Stream.concat(
            Stream.make({
              type: "snapshot" as const,
              requestId: payload.requestId,
              rows: [{ id: "o-3", price: 300 }],
              meta: {
                version: "1",
                totalRows: 1,
                serverTime: 1,
              },
            }),
            Stream.never,
          );
        },
        Unsubscribe: ({ requestId }) =>
          Effect.sync(() => {
            unsubscribed.push(requestId);
          }),
        Publish: () => Effect.void,
        DeltaPublish: () => Effect.void,
        DeleteById: () => Effect.void,
        Health: () => Effect.succeed({ ok: true, topics: {} }),
      };
      const client = createViewServerClient<typeof config>(transport, config);

      const subscription = yield* client.subscribe("orders", query, (event) =>
        Deferred.succeed(firstEvent, event).pipe(Effect.asVoid),
      );
      const snapshotFiber = yield* Deferred.await(firstEvent).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* TestClock.adjust(250);

      const snapshot = yield* Fiber.join(snapshotFiber);
      expect(snapshot.type).toBe("snapshot");
      expect(snapshot.meta.totalRows).toBe(1);
      expect(attempts).toBe(2);
      expect(snapshot.requestId).toBe(subscription.requestId);

      yield* subscription.close;
      expect(unsubscribed).toEqual([subscription.requestId]);
    }).pipe(Effect.scoped),
  );

  it.effect("ignores stale events from old subscription request ids after resubscribe", () =>
    Effect.gen(function* () {
      const seen: SubscriptionEvent<readonly RuntimeRow[]>[] = [];
      const requestIds: string[] = [];
      const freshSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const freshDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const transport: ViewServerRpcTransport = {
        Query: () => Effect.succeed({ rows: [], totalRows: 0, version: "0" }),
        Subscribe: (payload) => {
          requestIds.push(payload.requestId);
          if (requestIds.length === 1) {
            return Stream.fail(transportError("socket closed"));
          }
          const staleRequestId = requestIds[0] ?? "stale";
          return Stream.concat(
            Stream.make(
              {
                type: "snapshot" as const,
                requestId: payload.requestId,
                rows: [{ id: "fresh-snapshot", price: 100 }],
                meta: {
                  version: "1",
                  totalRows: 1,
                  serverTime: 1,
                },
              },
              {
                type: "delta" as const,
                requestId: staleRequestId,
                ops: [
                  {
                    type: "upsert" as const,
                    row: { id: "stale-delta", price: 999 },
                  },
                ],
                meta: {
                  fromVersion: "1",
                  toVersion: "2",
                  totalRows: 2,
                  serverTime: 2,
                },
              },
              {
                type: "delta" as const,
                requestId: payload.requestId,
                ops: [
                  {
                    type: "upsert" as const,
                    row: { id: "fresh-delta", price: 200 },
                  },
                ],
                meta: {
                  fromVersion: "2",
                  toVersion: "3",
                  totalRows: 2,
                  serverTime: 3,
                },
              },
            ),
            Stream.never,
          );
        },
        Unsubscribe: () => Effect.void,
        Publish: () => Effect.void,
        DeltaPublish: () => Effect.void,
        DeleteById: () => Effect.void,
        Health: () => Effect.succeed({ ok: true, topics: {} }),
      };
      const client = createViewServerClient<typeof config>(transport, config);

      const subscription = yield* client.subscribe("orders", query, (event) =>
        Effect.sync(() => {
          seen.push(event);
        }).pipe(
          Effect.flatMap(() =>
            event.type === "snapshot"
              ? Deferred.succeed(freshSnapshot, event)
              : Deferred.succeed(freshDelta, event),
          ),
          Effect.asVoid,
        ),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(250);

      const snapshot = yield* Deferred.await(freshSnapshot).pipe(Effect.timeout("1 second"));
      const delta = yield* Deferred.await(freshDelta).pipe(Effect.timeout("1 second"));

      expect(snapshot.requestId).toBe(requestIds[1]);
      expect(delta.requestId).toBe(requestIds[1]);
      expect(seen.map((event) => event.requestId)).toEqual([requestIds[1], requestIds[1]]);
      expect(
        seen.some(
          (event) =>
            event.type === "delta" &&
            event.ops.some(
              (operation) => operation.type === "upsert" && operation.row.id === "stale-delta",
            ),
        ),
      ).toBe(false);

      yield* subscription.close;
    }).pipe(Effect.scoped),
  );

  it.effect("Unsubscribe closes the stream and stops future deltas", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      });
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );

      const events = yield* client
        .Subscribe({
          requestId: "rpc-unsubscribe",
          topic: "orders",
          query,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      expect(snapshot.type).toBe("snapshot");

      yield* client.Unsubscribe({ requestId: "rpc-unsubscribe" });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });

      const nextEvent = yield* Queue.take(events).pipe(Effect.timeout("100 millis"), Effect.exit);
      expect(Exit.isSuccess(nextEvent)).toBe(false);
    }).pipe(Effect.scoped),
  );

  it.effect("deletes rows through the generated Effect RPC client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      });
      const rpcClient = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const client = createViewServerClient<typeof config>(rpcClient, config);

      yield* client.deleteById("orders", "o-1");
      const result = yield* client.query("orders", idFallbackQuery);

      expect(result.totalRows).toBe(1);
      expect(result.rows).toEqual([{ id: "o-2", price: 200 }]);
    }).pipe(Effect.scoped),
  );

  it.effect("keeps the internal health topic in sync with live worker metrics", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      });
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );

      const initialHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(initialHealth.rows, "server").rows).toBe(2);
      expect(rowById(initialHealth.rows, "topic:orders")).toMatchObject({
        kind: "topic",
        topic: "orders",
        rows: 2,
        subscribers: 0,
        queueDepth: 0,
        maxSubscriptionLagVersions: 0,
        totalSubscriptionLagVersions: 0,
        status: "ready",
      });

      const events = yield* client
        .Subscribe({
          requestId: "health-topic-sync",
          topic: "orders",
          query,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));
      const snapshot = yield* Queue.take(events);
      expect(snapshot.type).toBe("snapshot");
      yield* Effect.yieldNow;

      const subscribedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(subscribedHealth.rows, "topic:orders").subscribers).toBe(1);

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });

      const publishedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(publishedHealth.rows, "server").rows).toBe(3);
      expect(rowById(publishedHealth.rows, "topic:orders")).toMatchObject({
        rows: 3,
        subscribers: 1,
      });

      yield* client.Unsubscribe({ requestId: "health-topic-sync" });
      const unsubscribedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(unsubscribedHealth.rows, "topic:orders").subscribers).toBe(0);
    }).pipe(Effect.scoped),
  );

  it.effect("reports shared active plan metrics for raw subscriptions", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
            { id: "o-3", symbol: "NVDA", price: 300 },
          ],
        },
      });
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );

      const firstEvents = yield* client
        .Subscribe({
          requestId: "active-plan-first",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));
      const secondEvents = yield* client
        .Subscribe({
          requestId: "active-plan-second",
          topic: "orders",
          query: {
            ...coalesceQuery,
            offset: 1,
            limit: 2,
          },
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      expect((yield* Queue.take(firstEvents)).type).toBe("snapshot");
      expect((yield* Queue.take(secondEvents)).type).toBe("snapshot");

      const subscribedTopicHealth = yield* waitForHealthRow(
        () =>
          client.Query({
            topic: VIEW_SERVER_HEALTH_TOPIC,
            query: healthQuery,
          }),
        "topic:orders",
        (row) => row.activePlanCount === 1 && row.activeViewCount === 2,
      );
      expect(subscribedTopicHealth).toMatchObject({
        subscribers: 2,
        activePlanCount: 1,
        activeViewCount: 2,
        activePlanRows: 3,
      });
      expect(subscribedTopicHealth.activePlanIndexEstimatedBytes).toBeGreaterThan(0);

      yield* client.Unsubscribe({ requestId: "active-plan-first" });
      const partiallyUnsubscribedTopicHealth = yield* waitForHealthRow(
        () =>
          client.Query({
            topic: VIEW_SERVER_HEALTH_TOPIC,
            query: healthQuery,
          }),
        "topic:orders",
        (row) => row.activePlanCount === 1 && row.activeViewCount === 1,
      );
      expect(partiallyUnsubscribedTopicHealth).toMatchObject({
        activePlanCount: 1,
        activeViewCount: 1,
        activePlanRows: 3,
      });

      yield* client.Unsubscribe({ requestId: "active-plan-second" });
      const unsubscribedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(unsubscribedHealth.rows, "topic:orders")).toMatchObject({
        activePlanCount: 0,
        activeViewCount: 0,
        activePlanRows: 0,
        activePlanIndexEstimatedBytes: 0,
      });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps the topic worker responsive during active plan construction", () =>
    Effect.gen(function* () {
      const initialRows = Array.from({ length: 5_000 }, (_, index) => ({
        id: `o-${index}`,
        symbol: `SYM-${index % 100}`,
        price: 5_000 - index,
      }));
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows,
        activePlanBuildChunkSize: 1,
      });
      const events = yield* worker
        .subscribe("cooperative-active-plan-build", coalesceQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      expect((yield* Queue.take(events)).type).toBe("snapshot");

      const buildingMetrics = yield* waitForWorkerMetrics(
        worker,
        (metrics) => metrics.activePlanBuildingCount === 1,
      );
      expect(buildingMetrics.activePlanPendingCount).toBe(1);

      yield* worker
        .publish({ id: "o-live", symbol: "LIVE", price: 50 })
        .pipe(Effect.timeout("1 second"));
      const responsiveMetrics = yield* worker.metrics.pipe(Effect.timeout("1 second"));
      expect(responsiveMetrics.rows).toBe(initialRows.length + 1);
      expect(responsiveMetrics.version).toBe(1n);

      yield* worker.unsubscribe("cooperative-active-plan-build");
    }).pipe(Effect.scoped),
  );

  it.effect("marks pending active-plan subscriptions stale and refreshes once after catch-up", () =>
    Effect.gen(function* () {
      const rowCount = 1_000;
      const initialRows = Array.from({ length: rowCount }, (_, index) => ({
        id: `o-${index}`,
        symbol: `SYM-${index % 100}`,
        price: rowCount - index,
      }));
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows,
        activePlanBuildChunkSize: 1,
      });
      const events = yield* worker
        .subscribe("active-plan-dirty-catch-up", coalesceQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      const initial = yield* Queue.take(events);
      expect(initial.type).toBe("snapshot");

      const buildingMetrics = yield* waitForWorkerMetrics(
        worker,
        (metrics) => metrics.activePlanBuildingCount === 1,
      );
      expect(buildingMetrics.activePlanPendingCount).toBe(1);

      yield* worker.publish({ id: "live-1", symbol: "LIVE", price: -3 });
      yield* worker.publish({ id: "live-2", symbol: "LIVE", price: -2 });
      yield* worker.publish({ id: "live-3", symbol: "LIVE", price: -1 });

      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");
      if (stale.type !== "status") {
        throw new Error("Expected stale status");
      }
      expect(stale.status).toBe("stale");
      expect(stale.meta.version).toBe("3");
      expect(stale.meta.totalRows).toBe(initialRows.length + 3);

      const laggingMetrics = yield* worker.metrics;
      expect(laggingMetrics.maxSubscriptionLagVersions).toBeGreaterThanOrEqual(3);

      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("5 seconds"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected catch-up snapshot");
      }
      expect(refreshed.meta.version).toBe("3");
      expect(refreshed.meta.totalRows).toBe(initialRows.length + 3);
      expect(refreshed.rows.map((row) => row.id).slice(0, 3)).toEqual([
        "live-1",
        "live-2",
        "live-3",
      ]);

      const caughtUpMetrics = yield* waitForWorkerMetrics(
        worker,
        (metrics) => metrics.activePlanCount === 1 && metrics.maxSubscriptionLagVersions === 0,
      );
      expect(caughtUpMetrics.activePlanPendingCount).toBe(0);

      yield* worker.unsubscribe("active-plan-dirty-catch-up");
    }).pipe(Effect.scoped),
  );

  it.effect("marks grouped subscriptions stale and refreshes grouped snapshots on debounce", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "AAPL", price: 200 },
          { id: "o-3", symbol: "MSFT", price: 50 },
        ],
        groupedRefreshDebounceMs: 250,
      });
      const events = yield* worker
        .subscribe("grouped-dirty-refresh", groupedOrdersQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      const initial = yield* Queue.take(events);
      expect(initial.type).toBe("snapshot");
      if (initial.type !== "snapshot") {
        throw new Error("Expected initial grouped snapshot");
      }
      expect(initial.meta.totalRows).toBe(2);
      expect(initial.rows).toEqual([
        { symbol: "AAPL", orders: 2, totalPrice: 300 },
        { symbol: "MSFT", orders: 1, totalPrice: 50 },
      ]);

      yield* worker.deltaPublish({ id: "o-3", price: 400 });
      yield* worker.publish({ id: "o-4", symbol: "NVDA", price: 500 });

      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");
      if (stale.type !== "status") {
        throw new Error("Expected grouped stale status");
      }
      expect(stale.status).toBe("stale");
      expect(["1", "2"]).toContain(stale.meta.version);
      expect(stale.meta.totalRows).toBe(2);

      const laggingMetrics = yield* worker.metrics;
      expect(laggingMetrics.maxSubscriptionLagVersions).toBeGreaterThan(0);

      yield* TestClock.adjust(250);
      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected grouped refresh snapshot");
      }
      expect(refreshed.meta.version).toBe("2");
      expect(refreshed.meta.totalRows).toBe(3);
      expect(refreshed.rows).toEqual([
        { symbol: "NVDA", orders: 1, totalPrice: 500 },
        { symbol: "MSFT", orders: 1, totalPrice: 400 },
        { symbol: "AAPL", orders: 2, totalPrice: 300 },
      ]);

      const caughtUpMetrics = yield* worker.metrics;
      expect(caughtUpMetrics.maxSubscriptionLagVersions).toBe(0);
      yield* worker.unsubscribe("grouped-dirty-refresh");
    }).pipe(Effect.scoped),
  );

  it.effect("uses an exact snapshot backend for grouped refresh snapshots", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "AAPL", price: 200 },
          { id: "o-3", symbol: "MSFT", price: 50 },
        ],
        snapshotBackend: groupedRefreshBackend("exact"),
        groupedRefreshDebounceMs: 250,
      });
      const events = yield* worker
        .subscribe("grouped-exact-backend-refresh", groupedOrdersQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      expect((yield* Queue.take(events)).type).toBe("snapshot");

      yield* worker.publish({ id: "o-4", symbol: "NVDA", price: 500 });
      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");

      yield* TestClock.adjust(250);
      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected grouped backend refresh snapshot");
      }
      expect(refreshed.meta.version).toBe("1");
      expect(refreshed.meta.totalRows).toBe(1);
      expect(refreshed.rows).toEqual([{ symbol: "BACKEND", orders: 999, totalPrice: 999 }]);
      yield* worker.unsubscribe("grouped-exact-backend-refresh");
    }).pipe(Effect.scoped),
  );

  it.effect("preserves snapshot backend method receivers for grouped refresh snapshots", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 50 },
        ],
        snapshotBackend: new MethodGroupedRefreshBackend(),
        groupedRefreshDebounceMs: 250,
      });
      const events = yield* worker
        .subscribe("grouped-method-backend-refresh", groupedOrdersQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      expect((yield* Queue.take(events)).type).toBe("snapshot");

      yield* worker.publish({ id: "o-3", symbol: "NVDA", price: 500 });
      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");

      yield* TestClock.adjust(250);
      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected grouped method backend refresh snapshot");
      }
      expect(refreshed.meta.version).toBe("1");
      expect(refreshed.rows).toEqual([{ symbol: "METHOD", orders: 7, totalPrice: 700 }]);
      yield* worker.unsubscribe("grouped-method-backend-refresh");
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to cooperative memory grouped refresh when backend is behind", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "AAPL", price: 200 },
          { id: "o-3", symbol: "MSFT", price: 50 },
        ],
        snapshotBackend: groupedRefreshBackend("behind"),
        groupedRefreshDebounceMs: 250,
      });
      const events = yield* worker
        .subscribe("grouped-behind-backend-refresh", groupedOrdersQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      expect((yield* Queue.take(events)).type).toBe("snapshot");

      yield* worker.publish({ id: "o-4", symbol: "NVDA", price: 500 });
      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");

      yield* TestClock.adjust(250);
      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected grouped memory fallback refresh snapshot");
      }
      expect(refreshed.meta.version).toBe("1");
      expect(refreshed.meta.totalRows).toBe(3);
      expect(refreshed.rows).toEqual([
        { symbol: "NVDA", orders: 1, totalPrice: 500 },
        { symbol: "AAPL", orders: 2, totalPrice: 300 },
        { symbol: "MSFT", orders: 1, totalPrice: 50 },
      ]);
      yield* worker.unsubscribe("grouped-behind-backend-refresh");
    }).pipe(Effect.scoped),
  );

  it.effect("deleteById preserves raw query id fallback order after swap removal", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
          { id: "o-3", symbol: "NVDA", price: 300 },
          { id: "o-4", symbol: "AMZN", price: 400 },
        ],
      });

      yield* worker.deleteById("o-2");

      const result = yield* worker.query(idFallbackQuery);
      expect(result.totalRows).toBe(3);
      expect(result.rows.map((row) => row.id)).toEqual(["o-1", "o-3", "o-4"]);
    }).pipe(Effect.scoped),
  );

  it.effect("deleteById keeps the id index valid across multiple deletes and inserts", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
          { id: "o-3", symbol: "NVDA", price: 300 },
          { id: "o-4", symbol: "AMZN", price: 400 },
        ],
      });

      yield* worker.deleteById("o-2");
      yield* worker.deltaPublish({ id: "o-4", price: 450 });
      yield* worker.deleteById("o-1");
      yield* worker.publish({ id: "o-5", symbol: "TSLA", price: 500 });
      yield* worker.deleteById("o-3");
      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 250 });

      const result = yield* worker.query(idFallbackQuery);
      expect(result.totalRows).toBe(3);
      expect(result.rows).toEqual([
        { id: "o-2", price: 250 },
        { id: "o-4", price: 450 },
        { id: "o-5", price: 500 },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("deleteById during pending active-plan build catches up without stale rows", () =>
    Effect.gen(function* () {
      const rowCount = 1_000;
      const visibleDeletedId = `o-${rowCount - 1}`;
      const nextVisibleId = `o-${rowCount - 2}`;
      const initialRows = Array.from({ length: rowCount }, (_, index) => ({
        id: `o-${index}`,
        symbol: `SYM-${index % 100}`,
        price: rowCount - index,
      }));
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows,
        activePlanBuildChunkSize: 1,
      });
      const events = yield* worker
        .subscribe("active-plan-delete-catch-up", coalesceQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      const initial = yield* Queue.take(events);
      expect(initial.type).toBe("snapshot");
      if (initial.type !== "snapshot") {
        throw new Error("Expected initial snapshot");
      }
      expect(initial.rows[0]?.id).toBe(visibleDeletedId);

      const buildingMetrics = yield* waitForWorkerMetrics(
        worker,
        (metrics) => metrics.activePlanBuildingCount === 1,
      );
      expect(buildingMetrics.activePlanPendingCount).toBe(1);

      yield* worker.deleteById(visibleDeletedId);

      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");
      if (stale.type !== "status") {
        throw new Error("Expected stale status");
      }
      expect(stale.status).toBe("stale");
      expect(stale.meta.version).toBe("1");
      expect(stale.meta.totalRows).toBe(initialRows.length - 1);

      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("5 seconds"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected catch-up snapshot");
      }
      expect(refreshed.meta.version).toBe("1");
      expect(refreshed.meta.totalRows).toBe(initialRows.length - 1);
      expect(refreshed.rows.some((row) => row.id === visibleDeletedId)).toBe(false);
      expect(refreshed.rows[0]?.id).toBe(nextVisibleId);

      yield* worker.unsubscribe("active-plan-delete-catch-up");
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to memory recompute when the active plan count limit is hit", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxActivePlans: 1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
            ],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstEvents = yield* client
        .Subscribe({
          requestId: "active-plan-limit-first",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));
      const fallbackEvents = yield* client
        .Subscribe({
          requestId: "active-plan-limit-fallback",
          topic: "orders",
          query: {
            ...coalesceQuery,
            orderBy: [{ field: "price", direction: "desc" }],
          },
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      expect((yield* Queue.take(firstEvents)).type).toBe("snapshot");
      expect((yield* Queue.take(fallbackEvents)).type).toBe("snapshot");

      const limitedTopicHealth = yield* waitForHealthRow(
        () =>
          client.Query({
            topic: VIEW_SERVER_HEALTH_TOPIC,
            query: healthQuery,
          }),
        "topic:orders",
        (row) =>
          row.activePlanCount === 1 &&
          row.activeViewCount === 1 &&
          row.activePlanFallbackCount === 1,
      );
      expect(limitedTopicHealth).toMatchObject({
        subscribers: 2,
        activePlanCount: 1,
        activeViewCount: 1,
        activePlanFallbackCount: 1,
        status: "degraded",
      });

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      const fallbackDelta = yield* Queue.take(fallbackEvents).pipe(Effect.timeout("1 second"));
      expect(fallbackDelta.type).toBe("delta");

      yield* client.Unsubscribe({ requestId: "active-plan-limit-fallback" });
      yield* client.Unsubscribe({ requestId: "active-plan-limit-first" });
      const releasedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(releasedHealth.rows, "topic:orders")).toMatchObject({
        activePlanCount: 0,
        activeViewCount: 0,
        activePlanFallbackCount: 0,
        status: "ready",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to memory recompute when the active plan byte limit is hit", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxActivePlanEstimatedBytes: 1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
            ],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const events = yield* client
        .Subscribe({
          requestId: "active-plan-byte-limit",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      expect((yield* Queue.take(events)).type).toBe("snapshot");

      const limitedTopicHealth = yield* waitForHealthRow(
        () =>
          client.Query({
            topic: VIEW_SERVER_HEALTH_TOPIC,
            query: healthQuery,
          }),
        "topic:orders",
        (row) => row.activePlanFallbackCount === 1 && row.activePlanPendingCount === 0,
      );
      expect(limitedTopicHealth).toMatchObject({
        activePlanCount: 0,
        activeViewCount: 0,
        activePlanFallbackCount: 1,
        status: "degraded",
      });

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(delta.type).toBe("delta");

      yield* client.Unsubscribe({ requestId: "active-plan-byte-limit" });
      const releasedHealth = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(releasedHealth.rows, "topic:orders")).toMatchObject({
        activePlanFallbackCount: 0,
        status: "ready",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("reports queued subscription deltas as health queue depth", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();

      yield* client
        .Subscribe({
          requestId: "health-queue-depth",
          topic: "orders",
          query,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            return Deferred.succeed(firstDelta, event).pipe(
              Effect.flatMap(() => Deferred.await(releaseDelta)),
            );
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const delta = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(delta.type).toBe("delta");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });

      const health = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(health.rows, "topic:orders")).toMatchObject({
        queueDepth: 1,
        maxSubscriptionLagVersions: 1,
        totalSubscriptionLagVersions: 1,
        status: "degraded",
      });

      yield* Deferred.succeed(releaseDelta, undefined);
    }).pipe(Effect.scoped),
  );

  it.effect("reports logical subscription lag without coalescing", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 4,
            deltaCoalescing: false,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let deltas = 0;

      yield* client
        .Subscribe({
          requestId: "health-logical-lag-no-coalescing",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            deltas += 1;
            if (deltas === 1) {
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Effect.void;
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "TSLA", price: 400 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-5", symbol: "AMZN", price: 500 },
      });

      const health = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(health.rows, "server")).toMatchObject({
        maxSubscriptionLagVersions: 3,
        totalSubscriptionLagVersions: 3,
      });
      expect(rowById(health.rows, "topic:orders")).toMatchObject({
        queueDepth: 3,
        maxSubscriptionLagVersions: 3,
        totalSubscriptionLagVersions: 3,
        status: "ready",
      });

      yield* Deferred.succeed(releaseDelta, undefined);
    }).pipe(Effect.scoped),
  );

  it("does not overreport non-coalesced lag when a queue partially drains", () => {
    expect(subscriptionLagVersionsForQueueDepth(2, 3n, false)).toBe(2n);
    expect(subscriptionLagVersionsForQueueDepth(3, 3n, false)).toBe(3n);
    expect(subscriptionLagVersionsForQueueDepth(0, 3n, false)).toBe(0n);
    expect(subscriptionLagVersionsForQueueDepth(1, 3n, true)).toBe(3n);
  });

  it.effect("coalesces queued subscription deltas while preserving version continuity", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 4,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const coalescedDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let deltas = 0;

      yield* client
        .Subscribe({
          requestId: "coalesced-deltas",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            deltas += 1;
            if (deltas === 1) {
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Deferred.succeed(coalescedDelta, event).pipe(Effect.asVoid);
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("0");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");
      if (first.type !== "delta") {
        throw new Error("Expected first delta");
      }
      expect(first.meta.fromVersion).toBe("0");
      expect(first.meta.toVersion).toBe("1");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "TSLA", price: 400 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-5", symbol: "AMZN", price: 500 },
      });

      const health = yield* client.Query({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        query: healthQuery,
      });
      expect(rowById(health.rows, "server")).toMatchObject({
        maxSubscriptionLagVersions: 3,
        totalSubscriptionLagVersions: 3,
      });
      expect(rowById(health.rows, "topic:orders")).toMatchObject({
        queueDepth: 1,
        maxSubscriptionLagVersions: 3,
        totalSubscriptionLagVersions: 3,
        status: "ready",
      });

      yield* Deferred.succeed(releaseDelta, undefined);
      const next = yield* Deferred.await(coalescedDelta).pipe(Effect.timeout("1 second"));
      expect(next.type).toBe("delta");
      if (next.type !== "delta") {
        throw new Error("Expected coalesced delta");
      }
      expect(next.meta.fromVersion).toBe("1");
      expect(next.meta.toVersion).toBe("4");
      expect(next.meta.totalRows).toBe(5);
      expect(
        next.ops
          .filter((operation) => operation.type === "upsert")
          .map((operation) => {
            if (operation.type !== "upsert") {
              throw new Error("Expected upsert");
            }
            return operation.row.id;
          }),
      ).toEqual(["o-3", "o-4", "o-5"]);
    }).pipe(Effect.scoped),
  );

  it.effect("coalesces sorted row movement into an ordered version-contiguous delta", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 5,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
              { id: "o-3", symbol: "NVDA", price: 300 },
            ],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const coalescedDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let deltas = 0;

      yield* client
        .Subscribe({
          requestId: "coalesced-sorted-move",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            deltas += 1;
            if (deltas === 1) {
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Deferred.succeed(coalescedDelta, event).pipe(Effect.asVoid);
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "AMZN", price: 400 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");
      if (first.type !== "delta") {
        throw new Error("Expected first delta");
      }

      yield* client.DeltaPublish({
        topic: "orders",
        patch: { id: "o-3", price: 50 },
      });
      yield* client.DeltaPublish({
        topic: "orders",
        patch: { id: "o-2", price: 25 },
      });

      yield* Deferred.succeed(releaseDelta, undefined);
      const next = yield* Deferred.await(coalescedDelta).pipe(Effect.timeout("1 second"));
      expect(next.type).toBe("delta");
      if (next.type !== "delta") {
        throw new Error("Expected coalesced delta");
      }
      expect(next.meta.fromVersion).toBe("1");
      expect(next.meta.toVersion).toBe("3");
      const afterFirst = applyDeltaOperations(snapshot.rows, first, "id");
      const afterCoalesced = applyDeltaOperations(afterFirst, next, "id");
      expect(afterCoalesced.map((row) => row.id)).toEqual(["o-2", "o-3", "o-1", "o-4"]);
      expect(
        next.ops.some(
          (operation) =>
            operation.type === "upsert" && operation.row.id === "o-2" && operation.index === 0,
        ),
      ).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("coalesces a visible delete without stale rows leaking", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 4,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
              { id: "o-3", symbol: "NVDA", price: 300 },
            ],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const coalescedDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let deltas = 0;

      yield* client
        .Subscribe({
          requestId: "coalesced-visible-delete",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            deltas += 1;
            if (deltas === 1) {
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Deferred.succeed(coalescedDelta, event).pipe(Effect.asVoid);
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "AMZN", price: 400 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");
      if (first.type !== "delta") {
        throw new Error("Expected first delta");
      }

      yield* runtime.deleteById("orders", "o-2");

      yield* Deferred.succeed(releaseDelta, undefined);
      const next = yield* Deferred.await(coalescedDelta).pipe(Effect.timeout("1 second"));
      expect(next.type).toBe("delta");
      if (next.type !== "delta") {
        throw new Error("Expected coalesced delta");
      }
      expect(next.meta.fromVersion).toBe("1");
      expect(next.meta.toVersion).toBe("2");
      expect(
        next.ops.some((operation) => operation.type === "remove" && operation.key === "o-2"),
      ).toBe(true);
      const afterFirst = applyDeltaOperations(snapshot.rows, first, "id");
      const afterCoalesced = applyDeltaOperations(afterFirst, next, "id");
      expect(afterCoalesced.map((row) => row.id)).toEqual(["o-1", "o-3", "o-4"]);
    }).pipe(Effect.scoped),
  );

  it.effect("deleteById on a visible row emits a correct delta without stale rows", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxActivePlans: 0,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
              { id: "o-3", symbol: "NVDA", price: 300 },
            ],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const events = yield* client
        .Subscribe({
          requestId: "visible-delete-delta",
          topic: "orders",
          query: {
            ...coalesceQuery,
            limit: 2,
          },
        })
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      expect(snapshot.type).toBe("snapshot");
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.rows.map((row) => row.id)).toEqual(["o-1", "o-2"]);

      yield* runtime.deleteById("orders", "o-1");

      const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(delta.type).toBe("delta");
      if (delta.type !== "delta") {
        throw new Error("Expected delta");
      }
      expect(
        delta.ops.some((operation) => operation.type === "remove" && operation.key === "o-1"),
      ).toBe(true);
      const afterDelete = applyDeltaOperations(snapshot.rows, delta, "id");
      expect(afterDelete.map((row) => row.id)).toEqual(["o-2", "o-3"]);
      expect(afterDelete.some((row) => row.id === "o-1")).toBe(false);
    }).pipe(Effect.scoped),
  );

  it.effect("coalesces totalRows-only deltas without visible row churn", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 4,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const coalescedDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let deltas = 0;

      yield* client
        .Subscribe({
          requestId: "coalesced-total-rows",
          topic: "orders",
          query: firstByPriceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            deltas += 1;
            if (deltas === 1) {
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Deferred.succeed(coalescedDelta, event).pipe(Effect.asVoid);
          }),
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");
      if (first.type !== "delta") {
        throw new Error("Expected first delta");
      }
      expect(first.ops).toEqual([]);
      expect(first.meta.totalRows).toBe(2);

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "AMZN", price: 400 },
      });

      yield* Deferred.succeed(releaseDelta, undefined);
      const next = yield* Deferred.await(coalescedDelta).pipe(Effect.timeout("1 second"));
      expect(next.type).toBe("delta");
      if (next.type !== "delta") {
        throw new Error("Expected coalesced delta");
      }
      expect(next.meta.fromVersion).toBe("1");
      expect(next.meta.toVersion).toBe("3");
      expect(next.meta.totalRows).toBe(4);
      expect(next.ops).toEqual([]);
    }).pipe(Effect.scoped),
  );

  it.effect("fails coalesced streams when version lag exceeds maxQueueDepth", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 2,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let blockedFirstDelta = false;

      const streamFiber = yield* client
        .Subscribe({
          requestId: "coalesced-version-lag",
          topic: "orders",
          query: coalesceQuery,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            if (!blockedFirstDelta) {
              blockedFirstDelta = true;
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Effect.void;
          }),
          Effect.exit,
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const first = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(first.type).toBe("delta");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "TSLA", price: 400 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-5", symbol: "AMZN", price: 500 },
      });
      yield* Deferred.succeed(releaseDelta, undefined);

      const exit = yield* Fiber.join(streamFiber).pipe(Effect.timeout("1 second"));
      const error = Option.getOrUndefined(Exit.findErrorOption(exit));
      expect(error?._tag).toBe("BackpressureExceeded");
    }).pipe(Effect.scoped),
  );

  it.effect("fails slow raw subscription streams with BackpressureExceeded", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          initialRows: {
            orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          },
        },
      );
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let blockedFirstDelta = false;

      const streamFiber = yield* client
        .Subscribe({
          requestId: "raw-backpressure",
          topic: "orders",
          query,
        })
        .pipe(
          Stream.runForEach((event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            if (!blockedFirstDelta) {
              blockedFirstDelta = true;
              return Deferred.succeed(firstDelta, event).pipe(
                Effect.flatMap(() => Deferred.await(releaseDelta)),
              );
            }
            return Effect.void;
          }),
          Effect.exit,
          Effect.forkScoped,
        );

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-2", symbol: "MSFT", price: 200 },
      });
      const delta = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(delta.type).toBe("delta");

      yield* client.Publish({
        topic: "orders",
        row: { id: "o-3", symbol: "NVDA", price: 300 },
      });
      yield* client.Publish({
        topic: "orders",
        row: { id: "o-4", symbol: "TSLA", price: 400 },
      });
      yield* Deferred.succeed(releaseDelta, undefined);

      const exit = yield* Fiber.join(streamFiber).pipe(Effect.timeout("1 second"));
      const error = Option.getOrUndefined(Exit.findErrorOption(exit));
      expect(error?._tag).toBe("BackpressureExceeded");
    }).pipe(Effect.scoped),
  );

  it.effect("generated clients resubscribe after server backpressure closes a stream", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(backpressureConfig, {
        initialRows: {
          orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
        },
      });
      const rpcClient = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const client = createViewServerClient<typeof backpressureConfig>(
        rpcClient,
        backpressureConfig,
      );
      const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const resubscribedSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
      const releaseDelta = yield* Deferred.make<void>();
      let snapshots = 0;
      let blockedFirstDelta = false;

      const subscription = yield* client.subscribe("orders", query, (event) => {
        if (event.type === "snapshot") {
          snapshots += 1;
          return snapshots === 1
            ? Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid)
            : Deferred.succeed(resubscribedSnapshot, event).pipe(Effect.asVoid);
        }
        if (!blockedFirstDelta) {
          blockedFirstDelta = true;
          return Deferred.succeed(firstDelta, event).pipe(
            Effect.flatMap(() => Deferred.await(releaseDelta)),
          );
        }
        return Effect.void;
      });

      const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
      expect(snapshot.type).toBe("snapshot");
      yield* Effect.yieldNow;

      yield* client
        .publish("orders", { id: "o-2", symbol: "MSFT", price: 200 })
        .pipe(Effect.timeout("1 second"));
      const delta = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
      expect(delta.type).toBe("delta");

      const overflowPublish = yield* Effect.forEach(
        Array.from({ length: 96 }, (_, index) => index + 3),
        (index) =>
          client.publish("orders", {
            id: `o-${index}`,
            symbol: `SYM-${index}`,
            price: index * 100,
          }),
        { discard: true },
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseDelta, undefined);
      yield* Fiber.join(overflowPublish).pipe(Effect.ignore);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(250);

      const nextSnapshot = yield* Deferred.await(resubscribedSnapshot).pipe(
        Effect.timeout("1 second"),
      );
      expect(nextSnapshot.type).toBe("snapshot");
      expect(nextSnapshot.meta.totalRows).toBeGreaterThanOrEqual(3);

      yield* subscription.close;
    }).pipe(Effect.scoped),
  );

  it.effect("exposes typed health through the generated client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config, {
        initialRows: {
          orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
        },
      });
      const rpcClient = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );
      const client = createViewServerClient<typeof config>(rpcClient, config);

      const health = yield* client.health();
      expect(health.ok).toBe(true);
      expect(health.topics.orders).toMatchObject({
        rows: 1,
        subscribers: 0,
        queueDepth: 0,
        maxSubscriptionLagVersions: 0,
        totalSubscriptionLagVersions: 0,
        status: "ready",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("returns typed Effect errors for invalid external publishes", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(config);
      const client = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
        Effect.provide(ViewServerHandlersLive),
        Effect.provideService(ViewServerRuntime, runtime),
      );

      const reservedTopicError = yield* client
        .Publish({
          topic: "__view_server_health",
          row: { id: "external", rows: 1 },
        })
        .pipe(Effect.flip);
      expect(reservedTopicError._tag).toBe("InvalidPublish");

      const schemaError = yield* client
        .Publish({
          topic: "orders",
          row: { id: "bad", symbol: "MSFT", price: "not-a-number" },
        })
        .pipe(Effect.flip);
      expect(schemaError._tag).toBe("SchemaDecodeFailed");
    }).pipe(Effect.scoped),
  );
});

function waitForHealthRow<TRow extends Readonly<Record<string, unknown>>, E>(
  queryHealth: () => Effect.Effect<{ readonly rows: readonly TRow[] }, E>,
  id: string,
  predicate: (row: TRow) => boolean,
): Effect.Effect<TRow, E> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      const health = yield* queryHealth();
      const row = rowById(health.rows, id);
      if (predicate(row)) {
        return row;
      }
      yield* yieldToHost;
    }
    const health = yield* queryHealth();
    const row = rowById(health.rows, id);
    throw new Error(`Timed out waiting for health row ${id}: ${JSON.stringify(row)}`);
  });
}

function waitForWorkerMetrics(
  worker: TopicWorkerCore,
  predicate: (metrics: TopicWorkerMetrics) => boolean,
): Effect.Effect<TopicWorkerMetrics, ViewServerError> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      const metrics = yield* worker.metrics;
      if (predicate(metrics)) {
        return metrics;
      }
      yield* yieldToHost;
    }
    const metrics = yield* worker.metrics;
    return yield* Effect.die(
      new Error(
        `Timed out waiting for worker metrics: version=${metrics.version.toString()} activePlanBuildingCount=${metrics.activePlanBuildingCount} activePlanPendingCount=${metrics.activePlanPendingCount}`,
      ),
    );
  });
}

const yieldToHost = Effect.promise<void>(() => new Promise((resolve) => setTimeout(resolve, 0)));

function groupedRefreshBackend(mode: "exact" | "behind"): SnapshotBackend {
  let backendVersion = 0n;
  return {
    supportsGroupedRefreshSnapshots: true,
    init: (args) =>
      Effect.sync(() => {
        backendVersion = args.version;
      }),
    applyBatch: (args) =>
      Effect.sync(() => {
        if (mode === "exact") {
          backendVersion = args.highestVersion;
        }
      }),
    snapshot: (args): Effect.Effect<SnapshotBackendResult> =>
      Effect.sync(() => {
        const exact = mode === "exact" || args.targetVersion === 0n;
        const resultVersion = exact ? args.targetVersion : backendVersion;
        return {
          backendVersion: resultVersion,
          rows:
            args.targetVersion === 0n
              ? [
                  { symbol: "AAPL", orders: 2, totalPrice: 300 },
                  { symbol: "MSFT", orders: 1, totalPrice: 50 },
                ]
              : [{ symbol: "BACKEND", orders: 999, totalPrice: 999 }],
          totalRows: args.targetVersion === 0n ? 2 : 1,
        };
      }),
    groupedRefreshSnapshot: (args): Effect.Effect<SnapshotBackendResult> =>
      Effect.sync(() => {
        const resultVersion = mode === "exact" ? args.targetVersion : backendVersion;
        return {
          backendVersion: resultVersion,
          rows: [{ symbol: "BACKEND", orders: 999, totalPrice: 999 }],
          totalRows: 1,
        };
      }),
    close: () => Effect.void,
  };
}

class MethodGroupedRefreshBackend implements SnapshotBackend {
  #backendVersion = 0n;

  get supportsGroupedRefreshSnapshots(): boolean {
    return true;
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void> {
    return Effect.sync(() => {
      this.#backendVersion = args.version;
    });
  }

  applyBatch(args: Parameters<SnapshotBackend["applyBatch"]>[0]): Effect.Effect<void> {
    return Effect.sync(() => {
      this.#backendVersion = args.highestVersion;
    });
  }

  snapshot(args: Parameters<SnapshotBackend["snapshot"]>[0]): Effect.Effect<SnapshotBackendResult> {
    return Effect.sync(() => ({
      backendVersion: args.targetVersion,
      rows:
        args.targetVersion === 0n
          ? [
              { symbol: "AAPL", orders: 1, totalPrice: 100 },
              { symbol: "MSFT", orders: 1, totalPrice: 50 },
            ]
          : [{ symbol: "SNAPSHOT", orders: 1, totalPrice: 1 }],
      totalRows: args.targetVersion === 0n ? 2 : 1,
    }));
  }

  groupedRefreshSnapshot(
    args: Parameters<NonNullable<SnapshotBackend["groupedRefreshSnapshot"]>>[0],
  ): Effect.Effect<SnapshotBackendResult> {
    return Effect.sync(() => ({
      backendVersion: this.#backendVersion === args.targetVersion ? args.targetVersion : 0n,
      rows: [{ symbol: "METHOD", orders: 7, totalPrice: 700 }],
      totalRows: 1,
    }));
  }

  close(): Effect.Effect<void> {
    return Effect.void;
  }
}

function rowById<TRow extends Readonly<Record<string, unknown>>>(
  rows: readonly TRow[],
  id: string,
): TRow {
  const row = rows.find((entry) => entry.id === id);
  expect(row).toBeDefined();
  if (row === undefined) {
    throw new Error(`Missing row ${id}`);
  }
  return row;
}
