import { NodeHttpServer } from "@effect/platform-node";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { HttpServer } from "effect/unstable/http";
import { layerViewServerRuntime } from "@view-server/core/runtime";
import { layerViewServerWebsocketServer } from "@view-server/core/rpc/websocket";
import { initialOrders, ordersDemoConfig } from "../src/view-server.ts";

const websocketLayer = layerViewServerWebsocketServer("/rpc").pipe(
  Layer.provide(
    layerViewServerRuntime(ordersDemoConfig, {
      initialRows: {
        orders: initialOrders(120),
      },
    }),
  ),
);

const serverLayer = websocketLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

type OrdersDemoProject = {
  readonly provide: (key: "ordersDemoWsUrl", value: string) => void;
};

export default async function setup(project: OrdersDemoProject) {
  const scope = await Effect.runPromise(Scope.make());
  const context = await Effect.runPromise(Layer.buildWithScope(serverLayer, scope));
  const server = Context.get(context, HttpServer.HttpServer);
  const address = server.address;
  if (address._tag !== "TcpAddress") {
    throw new Error("Expected orders demo test server to listen on TCP");
  }
  project.provide("ordersDemoWsUrl", `ws://127.0.0.1:${address.port}/rpc`);

  return () => Effect.runPromise(Scope.close(scope, Exit.void));
}

declare module "vitest" {
  export interface ProvidedContext {
    readonly ordersDemoWsUrl: string;
  }
}

declare module "vite-plus/test" {
  export interface ProvidedContext {
    readonly ordersDemoWsUrl: string;
  }
}
