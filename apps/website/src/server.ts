import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { createServer } from "node:http";
import {
  layerViewServerRuntime,
  ViewServerRuntime,
  type ViewServerError,
  type ViewServerRuntimeShape,
} from "@view-server/core";
import { layerViewServerWebsocketServer } from "@view-server/core/rpc/websocket";
import { initialOrders, makeOrder, ordersDemoConfig, symbols } from "./view-server.ts";

const host = process.env.VIEW_SERVER_HOST ?? "127.0.0.1";
const port = envNumber("VIEW_SERVER_PORT", 3000);
const seedRows = envNumber("VIEW_SERVER_DEMO_ROWS", 800);
const publishIntervalMs = envNumber("VIEW_SERVER_DEMO_PUBLISH_INTERVAL_MS", 350);

const RuntimeLayer = layerViewServerRuntime(ordersDemoConfig, {
  initialRows: {
    orders: initialOrders(seedRows),
  },
  useMemorySnapshotBackend: true,
});

const PublisherLayer = Layer.effectDiscard(
  Effect.fn("view-server.demo.publisher.layer")(function* () {
    const runtime = yield* ViewServerRuntime;
    yield* Effect.logInfo(
      `orders demo publisher started rows=${seedRows} intervalMs=${publishIntervalMs}`,
    );
    yield* Effect.forkScoped(publisherLoop(runtime));
  })(),
);

const ServerLayer = Layer.mergeAll(layerViewServerWebsocketServer("/rpc"), PublisherLayer).pipe(
  Layer.provide(RuntimeLayer),
  Layer.provide(NodeHttpServer.layer(createServer, { host, port })),
);

Layer.launch(ServerLayer).pipe(NodeRuntime.runMain);

function publisherLoop(runtime: ViewServerRuntimeShape): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.demo.publisher.loop")(function* () {
    let tick = 0;
    while (true) {
      const id = tick % seedRows;
      const symbol = symbols[tick % symbols.length] ?? "AAPL";
      yield* runtime.deltaPublish("orders", {
        id: `order-${id}`,
        symbol,
        price: 100 + ((tick * 17) % 120),
        quantity: 25 + ((tick * 11) % 300),
        updatedAt: 1_800_000_000_000 + tick,
      });

      if (tick % 7 === 0) {
        yield* runtime.publish("orders", makeOrder(seedRows + tick, tick));
      }

      tick += 1;
      yield* Effect.sleep(`${publishIntervalMs} millis`);
    }
  })();
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}
