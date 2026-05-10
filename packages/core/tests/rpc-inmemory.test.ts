import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as RpcTest from "effect/unstable/rpc/RpcTest";
import { createViewServerClient, type ViewServerRpcTransport } from "../src/client/index.ts";
import {
  defineConfig,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthRow,
} from "../src/config/index.ts";
import { transportError } from "../src/errors.ts";
import type { RawQuery, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { ViewServerHandlersLive, ViewServerRpcs } from "../src/rpc/index.ts";
import { makeViewServerRuntime, ViewServerRuntime } from "../src/server/index.ts";

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

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const healthQuery = {
  fields: {
    id: true,
    kind: true,
    topic: true,
    rows: true,
    subscribers: true,
    queueDepth: true,
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

function rowById(
  rows: readonly Readonly<Record<string, unknown>>[],
  id: string,
): Readonly<Record<string, unknown>> {
  const row = rows.find((entry) => entry.id === id);
  expect(row).toBeDefined();
  if (row === undefined) {
    throw new Error(`Missing row ${id}`);
  }
  return row;
}
