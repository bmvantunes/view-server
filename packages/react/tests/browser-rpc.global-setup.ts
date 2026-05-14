import { NodeHttpServer } from "@effect/platform-node";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { HttpServer } from "effect/unstable/http";
import { defineConfig, layerViewServerRuntime, type RawQuery } from "@view-server/core";
import {
  layerViewServerWebsocketServer,
  makeNodeWebsocketClient,
} from "@view-server/core/rpc/websocket";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

type OrderRow = typeof Order.Type;

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const smokeQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const websocketLayer = layerViewServerWebsocketServer("/rpc").pipe(
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

const httpLayer = NodeHttpServer.layerTest;
const serverLayer = websocketLayer.pipe(Layer.provideMerge(httpLayer));

type BrowserRpcProject = {
  readonly provide: (key: "viewServerWsUrl", value: string) => void;
};

export default async function setup(project: BrowserRpcProject) {
  const scope = await Effect.runPromise(Scope.make());
  const context = await Effect.runPromise(Layer.buildWithScope(serverLayer, scope));
  const server = Context.get(context, HttpServer.HttpServer);
  const address = server.address;
  if (address._tag !== "TcpAddress") {
    throw new Error("Expected test server to listen on TCP");
  }

  const url = `ws://127.0.0.1:${address.port}/rpc`;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* makeNodeWebsocketClient<typeof config>(url, config);
        const result = yield* client.query("orders", smokeQuery).pipe(Effect.timeout("1 second"));
        if (result.rows[0]?.id !== "o-2") {
          return yield* Effect.die(new Error("Websocket smoke query returned unexpected rows"));
        }
      }),
    ),
  );

  project.provide("viewServerWsUrl", url);

  return () => Effect.runPromise(Scope.close(scope, Exit.void));
}

declare module "vitest" {
  export interface ProvidedContext {
    readonly viewServerWsUrl: string;
  }
}

declare module "vite-plus/test" {
  export interface ProvidedContext {
    readonly viewServerWsUrl: string;
  }
}
