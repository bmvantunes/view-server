import { Effect, Fiber, Queue, Stream } from "effect";
import type * as Scope from "effect/Scope";
import type * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ReadableTopicName,
  TopicIdFromConfig,
  TopicName,
  TopicPatchFromConfig,
  TopicRowFromConfig,
  ViewServerConfig,
} from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import { isViewServerError, transportError, type ViewServerError } from "../errors.ts";
import {
  type InferReadableQueryResult,
  type QueryForReadableTopic,
  type RuntimeRow,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import type {
  RpcDeltaPublishPayload,
  RpcDeleteByIdPayload,
  RpcHealthPayload,
  RpcHealthResponse,
  RpcPublishPayload,
  RpcQueryPayload,
  RpcQueryResponse,
  RpcSubscribePayload,
  RpcSubscriptionEvent,
  RpcUnsubscribePayload,
  ViewServerRpcs,
} from "../rpc/index.ts";
import type { HealthResponse } from "../server/index.ts";
import {
  queryResultToRuntimeRows,
  rowKeyForTypedQuery,
  rpcDeltaPublishPayload,
  rpcPublishPayload,
  rpcQueryPayload,
  rpcQueryRows,
  rpcSubscribePayload,
  rpcSubscriptionEvent,
} from "./rpc-boundary.ts";
import { LiveQueryStore, type LiveQueryInitialData } from "./live-query-store.ts";

export type RpcClientForViewServer = RpcClient.RpcClient<
  import("effect/unstable/rpc/RpcGroup").Rpcs<typeof ViewServerRpcs>,
  RpcClientError
>;

export type ViewServerRpcTransport = {
  readonly Query: (
    payload: RpcQueryPayload,
  ) => Effect.Effect<RpcQueryResponse, ViewServerError | RpcClientError>;
  readonly Subscribe: (
    payload: RpcSubscribePayload,
  ) => Stream.Stream<RpcSubscriptionEvent, ViewServerError | RpcClientError>;
  readonly Unsubscribe: (
    payload: RpcUnsubscribePayload,
  ) => Effect.Effect<void, ViewServerError | RpcClientError>;
  readonly Publish: (
    payload: RpcPublishPayload,
  ) => Effect.Effect<void, ViewServerError | RpcClientError>;
  readonly DeltaPublish: (
    payload: RpcDeltaPublishPayload,
  ) => Effect.Effect<void, ViewServerError | RpcClientError>;
  readonly DeleteById: (
    payload: RpcDeleteByIdPayload,
  ) => Effect.Effect<void, ViewServerError | RpcClientError>;
  readonly Health: (
    payload: RpcHealthPayload,
  ) => Effect.Effect<RpcHealthResponse, ViewServerError | RpcClientError>;
};

export type ActiveSubscription = {
  readonly requestId: string;
  readonly close: Effect.Effect<void>;
};

export type LiveQueryLifecycleEvent =
  | {
      readonly type: "attempt";
      readonly requestId: string;
      readonly attempt: number;
    }
  | {
      readonly type: "retry";
      readonly requestId: string;
      readonly attempt: number;
      readonly error: ViewServerError;
    };

export type LiveQueryLifecycleHandler = (event: LiveQueryLifecycleEvent) => Effect.Effect<void>;

export type ViewServerClient<TConfig extends ViewServerConfig> = {
  readonly query: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
  ) => Effect.Effect<
    LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
    ViewServerError
  >;
  readonly subscribe: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    onEvent: (event: SubscriptionEvent<readonly RuntimeRow[]>) => Effect.Effect<void>,
    onLifecycle?: LiveQueryLifecycleHandler,
  ) => Effect.Effect<ActiveSubscription, ViewServerError, Scope.Scope>;
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    row: TopicRowFromConfig<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchFromConfig<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    id: TopicIdFromConfig<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly health: () => Effect.Effect<HealthResponse, ViewServerError>;
  readonly createStore: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
  ) => Effect.Effect<LiveQueryStore, ViewServerError, Scope.Scope>;
};

export function createViewServerClient<TConfig extends ViewServerConfig>(
  rpcClient: ViewServerRpcTransport,
  config: TConfig,
): ViewServerClient<TConfig> {
  const idFieldForTopic = (topic: ReadableTopicName<TConfig>) =>
    topic === VIEW_SERVER_HEALTH_TOPIC ? "id" : String(config.topics[topic]?.id ?? "id");

  return {
    query: (topic, query) =>
      Effect.fn("view-server.client.query")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        const response = yield* rpcClient.Query(
          rpcQueryPayload<TConfig, typeof topic, typeof query>(topic, query),
        );
        yield* Effect.annotateCurrentSpan({
          "view_server.rows": response.rows.length,
          "view_server.total_rows": response.totalRows,
          "view_server.worker_version": response.version,
        });
        const rows = yield* rpcQueryRows<TConfig, typeof topic, typeof query>(
          response,
          query,
          config,
          topic,
        );
        return {
          rows,
          totalRows: response.totalRows,
        };
      })().pipe(Effect.mapError(toViewServerError)),

    subscribe: (topic, query, onEvent, onLifecycle) =>
      Effect.fn("view-server.client.subscribe")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        let closed = false;
        let currentRequestId = crypto.randomUUID();
        let attempt = 0;
        const runAttempt = Effect.fn("view-server.client.subscribe.attempt")(function* () {
          currentRequestId = crypto.randomUUID();
          attempt += 1;
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": currentRequestId,
            "view_server.subscription_id": currentRequestId,
            "view_server.topic": String(topic),
          });
          yield* (
            onLifecycle?.({
              type: "attempt",
              requestId: currentRequestId,
              attempt,
            }) ?? Effect.void
          );
          const stream = rpcClient
            .Subscribe(
              rpcSubscribePayload<TConfig, typeof topic, typeof query>(
                currentRequestId,
                topic,
                query,
              ),
            )
            .pipe(
              Stream.mapEffect((event) =>
                rpcSubscriptionEvent<TConfig, typeof topic, typeof query>(
                  event,
                  query,
                  config,
                  topic,
                ),
              ),
            );
          const events = yield* stream.pipe(Stream.toQueue({ capacity: 16 }));
          yield* Effect.whileLoop({
            while: () => !closed,
            body: () =>
              Queue.take(events).pipe(
                Effect.flatMap((event) =>
                  event.requestId === currentRequestId ? onEvent(event) : Effect.void,
                ),
              ),
            step: () => undefined,
          }).pipe(
            Effect.mapError(toViewServerError),
            Effect.flatMap(() => Effect.fail(transportError("subscription stream ended"))),
          );
        });
        const scope = yield* Effect.scope;
        const subscriptionLoop = Effect.whileLoop({
          while: () => !closed,
          body: () =>
            runAttempt().pipe(
              Effect.catchTags({
                TransportError: (error) =>
                  !closed
                    ? (
                        onLifecycle?.({
                          type: "retry",
                          requestId: currentRequestId,
                          attempt,
                          error,
                        }) ?? Effect.void
                      ).pipe(Effect.flatMap(() => Effect.sleep("250 millis")))
                    : Effect.void,
                BackpressureExceeded: (error) =>
                  !closed
                    ? (
                        onLifecycle?.({
                          type: "retry",
                          requestId: currentRequestId,
                          attempt,
                          error,
                        }) ?? Effect.void
                      ).pipe(
                        Effect.flatMap(() =>
                          rpcClient.Unsubscribe({ requestId: currentRequestId }),
                        ),
                        Effect.ignore,
                        Effect.flatMap(() => Effect.sleep("250 millis")),
                      )
                    : Effect.void,
              }),
            ),
          step: () => undefined,
        });
        const fiber = yield* Effect.forkIn(subscriptionLoop, scope, { startImmediately: true });
        return {
          get requestId() {
            return currentRequestId;
          },
          close: Effect.fn("view-server.client.unsubscribe")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.request_id": currentRequestId,
              "view_server.subscription_id": currentRequestId,
              "view_server.topic": String(topic),
            });
            closed = true;
            yield* rpcClient.Unsubscribe({ requestId: currentRequestId }).pipe(Effect.ignore);
            yield* Fiber.interrupt(fiber);
          })(),
        };
      })(),

    publish: (topic, row) =>
      Effect.fn("view-server.client.publish")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        const payload = yield* rpcPublishPayload<TConfig, typeof topic>(config, topic, row);
        yield* rpcClient.Publish(payload);
      })().pipe(Effect.mapError(toViewServerError)),

    deltaPublish: (topic, patch) =>
      Effect.fn("view-server.client.delta_publish")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        const payload = yield* rpcDeltaPublishPayload<TConfig, typeof topic>(config, topic, patch);
        yield* rpcClient.DeltaPublish(payload);
      })().pipe(Effect.mapError(toViewServerError)),

    deleteById: (topic, id) =>
      Effect.fn("view-server.client.delete_by_id")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        yield* rpcClient.DeleteById({
          topic,
          id,
        });
      })().pipe(Effect.mapError(toViewServerError)),

    health: () =>
      Effect.fn("view-server.client.health")(function* () {
        return yield* rpcClient.Health({});
      })().pipe(Effect.mapError(toViewServerError)),

    createStore: (topic, query, initialData) =>
      Effect.fn("view-server.client.store.create")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        const store = new LiveQueryStore(
          initialData === undefined
            ? undefined
            : {
                rows: queryResultToRuntimeRows(initialData.rows),
                totalRows: initialData.totalRows,
              },
          rowKeyForTypedQuery<TConfig, typeof topic, typeof query>(query, idFieldForTopic(topic)),
        );
        store.setStatus("syncing");
        const subscription = yield* createViewServerClient<TConfig>(rpcClient, config).subscribe(
          topic,
          query,
          (event) => Effect.sync(() => store.apply(event)),
          (event) =>
            Effect.sync(() => {
              if (event.type === "attempt") {
                store.beginAttempt(event.attempt);
              } else {
                store.retryAttempt(event.attempt);
              }
            }),
        );
        yield* Effect.addFinalizer(() => subscription.close);
        return store;
      })(),
  };
}

function toViewServerError(error: unknown): ViewServerError {
  return isViewServerError(error) ? error : transportError(error);
}
