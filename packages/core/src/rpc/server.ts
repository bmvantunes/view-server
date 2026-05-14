import { Effect, Layer, Stream } from "effect";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { ViewServerRuntime } from "../server/index.ts";
import { ViewServerRpcs } from "./rpcs.ts";
import { wireQueryResponse, wireSubscriptionEvent } from "./wire.ts";

export const ViewServerHandlersLive = ViewServerRpcs.toLayer(
  Effect.fn("view-server.rpc.handlers.make")(function* () {
    const runtime = yield* ViewServerRuntime;
    return ViewServerRpcs.of({
      Subscribe: (payload) =>
        Effect.fn("view-server.rpc.subscribe")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": payload.requestId,
            "view_server.subscription_id": payload.requestId,
            "view_server.topic": payload.topic,
          });
          return yield* runtime
            .subscribe(payload.requestId, payload.topic, payload.query)
            .pipe(Stream.map(wireSubscriptionEvent))
            .pipe(Stream.toQueue({ capacity: 64 }));
        })(),
      Unsubscribe: (payload) =>
        Effect.fn("view-server.rpc.unsubscribe")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": payload.requestId,
            "view_server.subscription_id": payload.requestId,
          });
          yield* runtime.unsubscribe(payload.requestId);
        })(),
      Query: (payload) =>
        Effect.fn("view-server.rpc.query")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": payload.topic,
          });
          const response = yield* runtime.query(payload.topic, payload.query);
          yield* Effect.annotateCurrentSpan({
            "view_server.rows": response.rows.length,
            "view_server.total_rows": response.totalRows,
            "view_server.worker_version": response.version,
          });
          return wireQueryResponse(response);
        })(),
      Publish: (payload) =>
        Effect.fn("view-server.rpc.publish")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": payload.topic,
          });
          yield* runtime.publish(payload.topic, payload.row);
        })(),
      DeltaPublish: (payload) =>
        Effect.fn("view-server.rpc.delta_publish")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": payload.topic,
          });
          yield* runtime.deltaPublish(payload.topic, payload.patch);
        })(),
      DeleteById: (payload) =>
        Effect.fn("view-server.rpc.delete_by_id")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": payload.topic,
          });
          yield* runtime.deleteById(payload.topic, payload.id);
        })(),
      Health: () =>
        Effect.fn("view-server.rpc.health")(function* () {
          return yield* runtime.health;
        })(),
    });
  })(),
);

export const ViewServerRpcServerLive = RpcServer.layer(ViewServerRpcs).pipe(
  Layer.provide(ViewServerHandlersLive),
);
