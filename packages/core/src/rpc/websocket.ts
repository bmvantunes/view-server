import { NodeSocket } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { createViewServerClient, type ViewServerClient } from "../client/index.ts";
import type { ViewServerConfig } from "../config/index.ts";
import { layerViewServerHealthRoutes } from "../server/index.ts";
import { ViewServerRpcs } from "./rpcs.ts";
import { ViewServerHandlersLive } from "./server.ts";

export const layerViewServerWebsocketProtocolRoute = (path: HttpRouter.PathInput = "/rpc") =>
  RpcServer.layerProtocolWebsocket({ path });

export const layerViewServerWebsocketProtocol = (path: HttpRouter.PathInput = "/rpc") =>
  layerViewServerWebsocketProtocolRoute(path).pipe(Layer.provide(HttpRouter.layer));

export const layerViewServerWebsocketServer = (path: HttpRouter.PathInput = "/rpc") => {
  const routes = Layer.mergeAll(
    layerViewServerWebsocketProtocolRoute(path),
    layerViewServerHealthRoutes,
  ).pipe(Layer.provide(HttpRouter.layer));
  return RpcServer.layer(ViewServerRpcs).pipe(
    Layer.provide(ViewServerHandlersLive),
    Layer.provideMerge(routes),
    Layer.provide(HttpRouter.serve(routes, { disableListenLog: true, disableLogger: true })),
    Layer.provide(RpcSerialization.layerNdjson),
  );
};

export const layerNodeWebsocketRpcClient = (url: string) =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(NodeSocket.layerWebSocket(url)),
    Layer.provide(RpcSerialization.layerNdjson),
  );

export function makeNodeWebsocketClient<TConfig extends ViewServerConfig>(
  url: string,
  config: TConfig,
): Effect.Effect<ViewServerClient<TConfig>, never, import("effect/Scope").Scope> {
  return Effect.fn("view-server.rpc.websocket.node_client")(function* () {
    const scope = yield* Effect.scope;
    const context = yield* Layer.buildWithScope(layerNodeWebsocketRpcClient(url), scope);
    const rpcClient = yield* RpcClient.make(ViewServerRpcs).pipe(Effect.provide(context));
    return createViewServerClient<TConfig>(rpcClient, config);
  })();
}
