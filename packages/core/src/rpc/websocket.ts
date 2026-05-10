import { NodeSocket } from "@effect/platform-node";
import { Effect, Layer, Stream } from "effect";
import { HttpRouter } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import {
  createViewServerClient,
  type ViewServerClient,
  type ViewServerRpcTransport,
} from "../client/index.ts";
import type { ViewServerConfig } from "../config/index.ts";
import type { ViewServerError } from "../errors.ts";
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
): Effect.Effect<ViewServerClient<TConfig>> {
  return Effect.sync(() =>
    createViewServerClient<TConfig>(nodeWebsocketTransport(url), config),
  ).pipe(Effect.withSpan("view-server.rpc.websocket.node_client"));
}

function nodeWebsocketTransport(url: string): ViewServerRpcTransport {
  const clientLayer = layerNodeWebsocketRpcClient(url);
  const runRpc = <A>(
    run: (rpcClient: RpcClientForWebsocket) => Effect.Effect<A, ViewServerError | RpcClientError>,
  ): Effect.Effect<A, ViewServerError | RpcClientError> =>
    Effect.scoped(
      RpcClient.make(ViewServerRpcs).pipe(Effect.flatMap((rpcClient) => run(rpcClient))),
    ).pipe(Effect.provide(clientLayer));

  return {
    Query: (payload) => runRpc((rpcClient) => rpcClient.Query(payload)),
    Subscribe: (payload) =>
      RpcClient.make(ViewServerRpcs).pipe(
        Effect.map((rpcClient) => rpcClient.Subscribe(payload)),
        Stream.unwrap,
        Stream.provide(clientLayer),
      ),
    Unsubscribe: (payload) => runRpc((rpcClient) => rpcClient.Unsubscribe(payload)),
    Publish: (payload) => runRpc((rpcClient) => rpcClient.Publish(payload)),
    DeltaPublish: (payload) => runRpc((rpcClient) => rpcClient.DeltaPublish(payload)),
    Health: (payload) => runRpc((rpcClient) => rpcClient.Health(payload)),
  };
}

type RpcClientForWebsocket = RpcClient.RpcClient<
  import("effect/unstable/rpc/RpcGroup").Rpcs<typeof ViewServerRpcs>,
  RpcClientError
>;
