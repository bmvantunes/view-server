import { Effect, Layer, Stream } from "effect";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { ViewServerRuntime } from "../server/index.ts";
import { ViewServerRpcs } from "./rpcs.ts";
import { wireQueryResponse, wireSubscriptionEvent } from "./wire.ts";

export const ViewServerHandlersLive = ViewServerRpcs.toLayer(
  Effect.gen(function* () {
    const runtime = yield* ViewServerRuntime;
    return ViewServerRpcs.of({
      Subscribe: (payload) =>
        runtime
          .subscribe(payload.requestId, payload.topic, payload.query)
          .pipe(Stream.map(wireSubscriptionEvent))
          .pipe(Stream.toQueue({ capacity: 64 })),
      Unsubscribe: (payload) => runtime.unsubscribe(payload.requestId),
      Query: (payload) =>
        runtime.query(payload.topic, payload.query).pipe(Effect.map(wireQueryResponse)),
      Publish: (payload) => runtime.publish(payload.topic, payload.row),
      DeltaPublish: (payload) => runtime.deltaPublish(payload.topic, payload.patch),
      Health: () => runtime.health,
    });
  }),
);

export const ViewServerRpcServerLive = RpcServer.layer(ViewServerRpcs).pipe(
  Layer.provide(ViewServerHandlersLive),
);
