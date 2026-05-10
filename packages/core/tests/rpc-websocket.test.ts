import { NodeHttpServer, NodeSocket } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Layer, Queue, Schema, Stream } from "effect";
import { HttpServer } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import { defineConfig } from "../src/config/index.ts";
import type { RawQuery } from "../src/protocol/index.ts";
import { ViewServerRpcs } from "../src/rpc/index.ts";
import { layerViewServerWebsocketServer } from "../src/rpc/websocket.ts";
import { layerViewServerRuntime } from "../src/server/index.ts";
import { createChdbSnapshotBackendFactory } from "../src/snapshot/chdb-backend.ts";

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
