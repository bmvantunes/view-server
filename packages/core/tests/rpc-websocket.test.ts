import { NodeHttpServer, NodeSocket } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Deferred, Effect, Exit, Layer, Option, Queue, Schema, Stream } from "effect";
import type * as Cause from "effect/Cause";
import { HttpServer } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import { createViewServerClient } from "../src/client/index.ts";
import { defineConfig } from "../src/config/index.ts";
import { backpressureExceeded, type ViewServerError } from "../src/errors.ts";
import type { RawQuery, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { ViewServerRpcs } from "../src/rpc/index.ts";
import { layerViewServerWebsocketServer, makeNodeWebsocketClient } from "../src/rpc/websocket.ts";
import { layerViewServerRuntime } from "../src/server/index.ts";
import { createChdbSnapshotBackendFactory } from "../src/snapshot/chdb-backend.ts";
import { makeInProcessTopicWorkerHost, type TopicWorkerHost } from "../src/worker/index.ts";

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

const chdbConfig = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
      snapshot: {
        backend: "chdb",
      },
    },
  },
});

const TimedOrder = Schema.Struct({
  id: Schema.String,
  updatedAt: Schema.BigInt,
  price: Schema.Number,
});

const DecimalOrder = Schema.Struct({
  id: Schema.String,
  price: Schema.BigDecimal,
});

type TimedOrderRow = {
  readonly id: string;
  readonly updatedAt: bigint;
  readonly price: number;
};

type DecimalOrderRow = {
  readonly id: string;
  readonly price: BigDecimal.BigDecimal;
};

const timedConfig = defineConfig({
  topics: {
    timedOrders: {
      id: "id",
      schema: TimedOrder,
    },
  },
});

const decimalConfig = defineConfig({
  topics: {
    decimalOrders: {
      id: "id",
      schema: DecimalOrder,
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

const timedQuery = {
  fields: {
    id: true,
    updatedAt: true,
  },
  where: {
    field: "updatedAt",
    comparator: "greater_than_or_equal",
    value: 1_700_000_000_000_000_000n,
  },
  orderBy: [{ field: "updatedAt", direction: "asc" }],
  limit: 2,
} satisfies RawQuery<TimedOrderRow, { readonly id: true; readonly updatedAt: true }>;

const decimalQuery = {
  fields: {
    id: true,
    price: true,
  },
  where: {
    field: "price",
    comparator: "greater_than",
    value: BigDecimal.fromStringUnsafe("10.000000000000000001"),
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 5,
} satisfies RawQuery<DecimalOrderRow, { readonly id: true; readonly price: true }>;

const topOneQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 1,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

function makeRetryWorker(topic: string, idField: string): Effect.Effect<TopicWorkerHost> {
  return Effect.sync(() => {
    let rows: RuntimeRow[] = [{ id: "o-1", symbol: "AAPL", price: 100 }];
    let version = 0n;
    let attempts = 0;
    let active:
      | {
          readonly requestId: string;
          readonly queue: Queue.Queue<
            SubscriptionEvent<readonly RuntimeRow[]>,
            ViewServerError | Cause.Done
          >;
        }
      | undefined;

    const snapshot = (requestId: string): SubscriptionEvent<readonly RuntimeRow[]> => ({
      type: "snapshot",
      requestId,
      rows: rows.map((row) => ({ id: row.id, price: row.price })),
      meta: {
        version: version.toString(),
        totalRows: rows.length,
        serverTime: Date.now(),
      },
    });

    const publish = (input: unknown): Effect.Effect<void, ViewServerError> =>
      Effect.gen(function* () {
        if (!isRuntimeRow(input)) {
          return yield* Effect.die(new Error("Expected runtime row"));
        }
        const row = input;
        const id = row[idField];
        if (typeof id !== "string" && typeof id !== "number") {
          return yield* Effect.die(new Error(`Expected ${idField} to be a row key`));
        }
        const index = rows.findIndex((existing) => existing[idField] === id);
        const beforeVersion = version;
        version += 1n;
        if (index >= 0) {
          rows = rows.map((existing, existingIndex) => (existingIndex === index ? row : existing));
        } else {
          rows = [...rows, row];
        }
        const subscriber = active;
        if (subscriber !== undefined) {
          yield* Queue.offer(subscriber.queue, {
            type: "delta",
            requestId: subscriber.requestId,
            ops: [
              {
                type: "upsert",
                key: id,
                row: { id: row.id, price: row.price },
                index: rows.length - 1,
              },
            ],
            meta: {
              fromVersion: beforeVersion.toString(),
              toVersion: version.toString(),
              totalRows: rows.length,
              serverTime: Date.now(),
            },
          });
        }
      });

    const worker: TopicWorkerHost = {
      topic,
      idField,
      version: Effect.sync(() => version),
      metrics: Effect.sync(() => ({
        rows: rows.length,
        subscribers: active === undefined ? 0 : 1,
        version,
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
        status: "ready" as const,
      })),
      query: () =>
        Effect.succeed({
          rows: rows.map((row) => ({ id: row.id, price: row.price })),
          totalRows: rows.length,
          version: version.toString(),
        }),
      subscribe: (requestId) =>
        Stream.callback<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>((queue) =>
          Effect.gen(function* () {
            attempts += 1;
            active = { requestId, queue };
            yield* Queue.offer(queue, snapshot(requestId));
            if (attempts === 1) {
              yield* publish({ id: "o-2", symbol: "MSFT", price: 200 });
              active = undefined;
              yield* Queue.fail(
                queue,
                backpressureExceeded(requestId, "synthetic websocket retry backpressure"),
              );
              return;
            }
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                if (active?.requestId === requestId) {
                  active = undefined;
                }
              }),
            );
          }),
        ),
      unsubscribe: (requestId) =>
        Effect.gen(function* () {
          if (active?.requestId === requestId) {
            const queue = active.queue;
            active = undefined;
            yield* Queue.end(queue);
          }
        }),
      publish,
      deltaPublish: (patch) =>
        Effect.gen(function* () {
          const current = rows.find((row) => row[idField] === patch[idField]);
          yield* publish({ ...current, ...patch });
        }),
      deleteById: (id) =>
        Effect.sync(() => {
          rows = rows.filter((row) => row[idField] !== id);
          version += 1n;
        }),
      getRowsForTest: Effect.sync(() => rows.map((row) => ({ ...row }))),
      shutdown: Effect.sync(() => {
        active = undefined;
      }),
    };
    return worker;
  });
}

function isRuntimeRow(value: unknown): value is RuntimeRow {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

describe("Effect RPC websocket", () => {
  it.effect("serves health and readiness probes beside the websocket route", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(config, {
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
              ],
            },
          }),
        ),
      );
      const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

      yield* Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        const baseUrl = `http://127.0.0.1:${address.port}`;

        const healthResponse = yield* Effect.promise(() => fetch(`${baseUrl}/health`));
        expect(healthResponse.status).toBe(200);
        const health = yield* Effect.promise(() => healthResponse.json());
        expect(health).toMatchObject({
          ok: true,
          topics: {
            orders: {
              rows: 2,
              subscribers: 0,
              queueDepth: 0,
              status: "ready",
            },
            __view_server_health: {
              rows: 2,
              status: "ready",
            },
          },
        });

        const readinessResponse = yield* Effect.promise(() => fetch(`${baseUrl}/ready`));
        expect(readinessResponse.status).toBe(200);
        const readiness = yield* Effect.promise(() => readinessResponse.json());
        expect(readiness.ok).toBe(true);
        expect(readiness.topics.orders.version).toEqual(expect.any(String));
      }).pipe(Effect.provide(testServerLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("streams snapshots and deltas over websocket with NDJSON", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(config, {
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
              ],
            },
          }),
        ),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const initial = yield* rpcClient
          .Query({
            topic: "orders",
            query,
          })
          .pipe(Effect.timeout("1 second"));
        expect(initial.totalRows).toBe(2);

        const events = yield* rpcClient
          .Subscribe({
            requestId: "websocket-sub",
            topic: "orders",
            query,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const snapshot = yield* Queue.take(events);
        expect(snapshot.type).toBe("snapshot");
        expect(snapshot.meta.totalRows).toBe(2);

        yield* rpcClient.Publish({
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
        yield* rpcClient.Unsubscribe({ requestId: "websocket-sub" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it("makeNodeWebsocketClient runs queries over websocket NDJSON", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(config, {
              initialRows: {
                orders: [
                  { id: "o-1", symbol: "AAPL", price: 100 },
                  { id: "o-2", symbol: "MSFT", price: 200 },
                ],
              },
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        yield* Effect.gen(function* () {
          const server = yield* HttpServer.HttpServer;
          const address = server.address;
          if (address._tag !== "TcpAddress") {
            return yield* Effect.die(new Error("Expected test server to listen on TCP"));
          }
          const client = yield* makeNodeWebsocketClient<typeof config>(
            `ws://127.0.0.1:${address.port}/rpc`,
            config,
          );

          const result = yield* client.query("orders", query).pipe(Effect.timeout("1 second"));
          expect(result).toEqual({
            rows: [
              { id: "o-2", price: 200 },
              { id: "o-1", price: 100 },
            ],
            totalRows: 2,
          });
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    );
  });

  it("makeNodeWebsocketClient deletes rows over websocket NDJSON", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(config, {
              initialRows: {
                orders: [
                  { id: "o-1", symbol: "AAPL", price: 100 },
                  { id: "o-2", symbol: "MSFT", price: 200 },
                ],
              },
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        yield* Effect.gen(function* () {
          const server = yield* HttpServer.HttpServer;
          const address = server.address;
          if (address._tag !== "TcpAddress") {
            return yield* Effect.die(new Error("Expected test server to listen on TCP"));
          }
          const client = yield* makeNodeWebsocketClient<typeof config>(
            `ws://127.0.0.1:${address.port}/rpc`,
            config,
          );

          yield* client.deleteById("orders", "o-1").pipe(Effect.timeout("1 second"));
          const result = yield* client.query("orders", query).pipe(Effect.timeout("1 second"));

          expect(result).toEqual({
            rows: [{ id: "o-2", price: 200 }],
            totalRows: 1,
          });
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    );
  });

  it("makeNodeWebsocketClient subscribes over websocket NDJSON", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(config, {
              initialRows: {
                orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
              },
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        yield* Effect.gen(function* () {
          const server = yield* HttpServer.HttpServer;
          const address = server.address;
          if (address._tag !== "TcpAddress") {
            return yield* Effect.die(new Error("Expected test server to listen on TCP"));
          }
          const client = yield* makeNodeWebsocketClient<typeof config>(
            `ws://127.0.0.1:${address.port}/rpc`,
            config,
          );
          const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();

          const subscription = yield* client.subscribe("orders", query, (event) => {
            if (event.type === "snapshot") {
              return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
            }
            return Deferred.succeed(firstDelta, event).pipe(Effect.asVoid);
          });

          const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
          expect(snapshot.type).toBe("snapshot");
          expect(snapshot.requestId).toBe(subscription.requestId);
          expect(snapshot.meta.totalRows).toBe(1);

          yield* client
            .publish("orders", { id: "o-2", symbol: "MSFT", price: 200 })
            .pipe(Effect.timeout("1 second"));
          const delta = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
          expect(delta.type).toBe("delta");
          expect(delta.requestId).toBe(subscription.requestId);
          if (delta.type === "delta") {
            expect(delta.meta.totalRows).toBe(2);
            expect(
              delta.ops.some(
                (operation) => operation.type === "upsert" && operation.row.id === "o-2",
              ),
            ).toBe(true);
          }

          yield* subscription.close;
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    );
  });

  it.effect("multiplexes multiple subscriptions over one websocket client", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(config, {
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
              ],
            },
          }),
        ),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const pageEvents = yield* rpcClient
          .Subscribe({
            requestId: "websocket-page",
            topic: "orders",
            query,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));
        const topEvents = yield* rpcClient
          .Subscribe({
            requestId: "websocket-top",
            topic: "orders",
            query: topOneQuery,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const pageSnapshot = yield* Queue.take(pageEvents);
        const topSnapshot = yield* Queue.take(topEvents);
        expect(pageSnapshot.type).toBe("snapshot");
        expect(pageSnapshot.requestId).toBe("websocket-page");
        expect(pageSnapshot.meta.totalRows).toBe(2);
        expect(topSnapshot.type).toBe("snapshot");
        expect(topSnapshot.requestId).toBe("websocket-top");
        expect(topSnapshot.meta.totalRows).toBe(2);

        yield* rpcClient.Publish({
          topic: "orders",
          row: { id: "o-3", symbol: "NVDA", price: 300 },
        });

        const pageDelta = yield* Queue.take(pageEvents);
        const topDelta = yield* Queue.take(topEvents);
        if (pageDelta.type !== "delta" || topDelta.type !== "delta") {
          throw new Error("Expected deltas from both subscriptions");
        }
        expect(pageDelta.requestId).toBe("websocket-page");
        expect(pageDelta.meta.totalRows).toBe(3);
        expect(topDelta.requestId).toBe("websocket-top");
        expect(topDelta.meta.totalRows).toBe(3);
        expect(
          pageDelta.ops.some(
            (operation) => operation.type === "upsert" && operation.row.id === "o-3",
          ),
        ).toBe(true);
        expect(
          topDelta.ops.some(
            (operation) => operation.type === "upsert" && operation.row.id === "o-3",
          ),
        ).toBe(true);
        yield* rpcClient.Unsubscribe({ requestId: "websocket-page" });
        yield* rpcClient.Unsubscribe({ requestId: "websocket-top" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("propagates typed Effect errors over websocket", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(layerViewServerRuntime(config)),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const error = yield* rpcClient
          .Publish({
            topic: "orders",
            row: { id: "bad", symbol: "MSFT", price: "not-a-number" },
          })
          .pipe(Effect.flip);
        expect(error._tag).toBe("SchemaDecodeFailed");
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("propagates typed backpressure errors over websocket", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(backpressureConfig, {
            initialRows: {
              orders: [{ id: "o-1", symbol: "AAPL", price: 100 }],
            },
          }),
        ),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const events = yield* rpcClient
          .Subscribe({
            requestId: "websocket-backpressure",
            topic: "orders",
            query,
          })
          .pipe(Stream.toQueue({ capacity: 1 }));

        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        expect(snapshot.type).toBe("snapshot");
        expect(snapshot.requestId).toBe("websocket-backpressure");

        yield* Effect.forEach(
          Array.from({ length: 96 }, (_, index) => index + 2),
          (index) =>
            rpcClient.Publish({
              topic: "orders",
              row: { id: `o-${index}`, symbol: `SYM-${index}`, price: index * 100 },
            }),
          { discard: true },
        );

        const error = yield* Effect.gen(function* () {
          for (let index = 0; index < 96; index++) {
            const exit = yield* Queue.take(events).pipe(Effect.exit);
            if (Exit.isFailure(exit)) {
              return Option.getOrUndefined(Exit.findErrorOption(exit));
            }
          }
          return undefined;
        }).pipe(Effect.timeout("2 seconds"));
        expect(error).toMatchObject({
          _tag: "BackpressureExceeded",
          requestId: "websocket-backpressure",
        });

        const health = yield* rpcClient.Health({}).pipe(Effect.timeout("1 second"));
        expect(health.topics.orders).toMatchObject({
          subscribers: 0,
          queueDepth: 0,
        });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it("generated clients resubscribe after websocket backpressure", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(config, {
              topicWorkerFactory: (topic, topicConfig, options) =>
                topic === "orders"
                  ? makeRetryWorker(topic, topicConfig.id)
                  : makeInProcessTopicWorkerHost(topic, topicConfig, options),
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        yield* Effect.gen(function* () {
          const server = yield* HttpServer.HttpServer;
          const address = server.address;
          if (address._tag !== "TcpAddress") {
            return yield* Effect.die(new Error("Expected test server to listen on TCP"));
          }
          const websocketUrl = `ws://127.0.0.1:${address.port}/rpc`;
          const clientLayer = RpcClient.layerProtocolSocket().pipe(
            Layer.provide(NodeSocket.layerWebSocket(websocketUrl)),
            Layer.provide(RpcSerialization.layerNdjson),
          );

          yield* Effect.gen(function* () {
            const rpcClient = yield* RpcClient.make(ViewServerRpcs);
            const client = createViewServerClient<typeof config>(rpcClient, config);
            const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
            const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
            const resubscribedSnapshot =
              yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
            const postResubscribeDelta =
              yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
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
              return snapshots >= 2
                ? Deferred.succeed(postResubscribeDelta, event).pipe(Effect.asVoid)
                : Effect.void;
            });

            const snapshot = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
            expect(snapshot.type).toBe("snapshot");
            const firstRequestId = snapshot.requestId;
            expect(firstRequestId).toBe(subscription.requestId);

            const delta = yield* Deferred.await(firstDelta).pipe(Effect.timeout("1 second"));
            expect(delta.type).toBe("delta");
            expect(delta.requestId).toBe(firstRequestId);

            yield* Deferred.succeed(releaseDelta, undefined);

            const nextSnapshot = yield* Deferred.await(resubscribedSnapshot).pipe(
              Effect.timeout("2 seconds"),
            );
            expect(nextSnapshot.type).toBe("snapshot");
            expect(nextSnapshot.requestId).not.toBe(firstRequestId);
            expect(nextSnapshot.requestId).toBe(subscription.requestId);
            expect(nextSnapshot.meta.totalRows).toBe(2);

            const health = yield* client.health().pipe(Effect.timeout("1 second"));
            expect(health.topics.orders).toMatchObject({
              subscribers: 1,
              queueDepth: 0,
            });

            yield* client.publish("orders", { id: "o-200", symbol: "AMZN", price: 20_000 });
            const nextDelta = yield* Deferred.await(postResubscribeDelta).pipe(
              Effect.timeout("1 second"),
            );
            expect(nextDelta.type).toBe("delta");
            expect(nextDelta.requestId).toBe(nextSnapshot.requestId);
            if (nextDelta.type === "delta") {
              expect(nextDelta.meta.totalRows).toBe(3);
              expect(
                nextDelta.ops.some(
                  (operation) => operation.type === "upsert" && operation.row.id === "o-200",
                ),
              ).toBe(true);
            }

            yield* subscription.close;
          }).pipe(Effect.provide(clientLayer));
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    );
  });

  it.effect("round-trips BigInt row values and filters over websocket NDJSON", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(layerViewServerRuntime(timedConfig)),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const nanos = 1_700_000_000_000_000_123n;

        yield* rpcClient.Publish({
          topic: "timedOrders",
          row: { id: "nano-1", updatedAt: nanos, price: 123 },
        });

        const response = yield* rpcClient
          .Query({
            topic: "timedOrders",
            query: timedQuery,
          })
          .pipe(Effect.timeout("1 second"));
        expect(response.rows[0]?.updatedAt).toBe(nanos);

        const events = yield* rpcClient
          .Subscribe({
            requestId: "websocket-bigint",
            topic: "timedOrders",
            query: timedQuery,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        if (snapshot.type !== "snapshot") {
          throw new Error("Expected snapshot");
        }
        expect(snapshot.rows[0]?.updatedAt).toBe(nanos);
        yield* rpcClient.Unsubscribe({ requestId: "websocket-bigint" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("round-trips BigDecimal row values and filters over websocket NDJSON", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(layerViewServerRuntime(decimalConfig)),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const lower = BigDecimal.fromStringUnsafe("10.000000000000000001");
        const exact = BigDecimal.fromStringUnsafe("1234567890.123456789");

        yield* rpcClient.Publish({
          topic: "decimalOrders",
          row: { id: "decimal-low", price: lower },
        });
        yield* rpcClient.Publish({
          topic: "decimalOrders",
          row: { id: "decimal-high", price: exact },
        });

        const response = yield* rpcClient
          .Query({
            topic: "decimalOrders",
            query: decimalQuery,
          })
          .pipe(Effect.timeout("1 second"));
        expect(response.totalRows).toBe(1);
        expect(BigDecimal.equals(expectBigDecimal(response.rows[0]?.price), exact)).toBe(true);

        const events = yield* rpcClient
          .Subscribe({
            requestId: "websocket-bigdecimal",
            topic: "decimalOrders",
            query: decimalQuery,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        if (snapshot.type !== "snapshot") {
          throw new Error("Expected snapshot");
        }
        expect(snapshot.meta.totalRows).toBe(1);
        expect(BigDecimal.equals(expectBigDecimal(snapshot.rows[0]?.price), exact)).toBe(true);
        yield* rpcClient.Unsubscribe({ requestId: "websocket-bigdecimal" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it.effect("serves version-fenced chDB snapshots over websocket", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(chdbConfig, {
            snapshotBackendFactory: createChdbSnapshotBackendFactory(),
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
              ],
            },
          }),
        ),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const events = yield* rpcClient
          .Subscribe({
            requestId: "websocket-chdb",
            topic: "orders",
            query,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        if (snapshot.type !== "snapshot") {
          throw new Error("Expected snapshot");
        }
        expect(snapshot.meta.version).toBe("0");
        expect(snapshot.meta.backendVersion).toBe("0");
        expect(snapshot.rows).toEqual([
          { id: "o-2", price: 200 },
          { id: "o-1", price: 100 },
        ]);

        yield* rpcClient.Publish({
          topic: "orders",
          row: { id: "o-3", symbol: "NVDA", price: 300 },
        });

        const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        if (delta.type !== "delta") {
          throw new Error("Expected delta");
        }
        expect(delta.meta.fromVersion).toBe("0");
        expect(delta.meta.toVersion).toBe("1");
        yield* rpcClient.Unsubscribe({ requestId: "websocket-chdb" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );
});

function expectBigDecimal(value: unknown): BigDecimal.BigDecimal {
  if (BigDecimal.isBigDecimal(value)) {
    return value;
  }
  throw new Error(`Expected BigDecimal, got ${String(value)}`);
}
