import { Effect, Fiber, Stream } from "effect";
import type * as Scope from "effect/Scope";
import type * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ReadableTopicName,
  TopicName,
  TopicPatchFromConfig,
  TopicRowFromConfig,
  ViewServerConfig,
} from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import { isViewServerError, transportError, type ViewServerError } from "../errors.ts";
import {
  rowKeyForQuery,
  type InferReadableQueryResult,
  type QueryForReadableTopic,
  type RuntimeQuery,
  type RuntimeRow,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import type {
  RpcDeltaPublishPayload,
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
import { SubscriptionStore } from "./subscription-store.ts";

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
  readonly Health: (
    payload: RpcHealthPayload,
  ) => Effect.Effect<RpcHealthResponse, ViewServerError | RpcClientError>;
};

export type ActiveSubscription = {
  readonly requestId: string;
  readonly close: Effect.Effect<void>;
};

export type ViewServerClient<TConfig extends ViewServerConfig> = {
  readonly query: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
  ) => Effect.Effect<InferReadableQueryResult<TConfig, TTopic, TQuery>, ViewServerError>;
  readonly subscribe: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    onEvent: (event: SubscriptionEvent<readonly RuntimeRow[]>) => Effect.Effect<void>,
  ) => Effect.Effect<ActiveSubscription, ViewServerError, Scope.Scope>;
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    row: TopicRowFromConfig<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchFromConfig<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
  readonly health: () => Effect.Effect<HealthResponse, ViewServerError>;
  readonly createStore: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: InferReadableQueryResult<TConfig, TTopic, TQuery>,
  ) => Effect.Effect<SubscriptionStore<readonly RuntimeRow[]>, ViewServerError, Scope.Scope>;
};

export function createViewServerClient<TConfig extends ViewServerConfig>(
  rpcClient: ViewServerRpcTransport,
  config?: TConfig,
): ViewServerClient<TConfig> {
  const idFieldForTopic = (topic: ReadableTopicName<TConfig>) =>
    topic === VIEW_SERVER_HEALTH_TOPIC ? "id" : String(config?.topics[topic]?.id ?? "id");

  return {
    query: (topic, query) =>
      rpcClient.Query({ topic, query: query as never }).pipe(
        Effect.map((response) => response.rows as never),
        Effect.mapError(toViewServerError),
      ),

    subscribe: (topic, query, onEvent) =>
      Effect.gen(function* () {
        let closed = false;
        let currentRequestId = crypto.randomUUID();
        const runAttempt = Effect.gen(function* () {
          currentRequestId = crypto.randomUUID();
          const stream = rpcClient.Subscribe({
            requestId: currentRequestId,
            topic,
            query: query as never,
          });
          yield* stream.pipe(Stream.runForEach(onEvent), Effect.mapError(toViewServerError));
        });
        const fiber = yield* Effect.whileLoop({
          while: () => !closed,
          body: () =>
            runAttempt.pipe(
              Effect.catchTag("TransportError", () =>
                !closed ? Effect.sleep("250 millis") : Effect.void,
              ),
            ),
          step: () => undefined,
        }).pipe(Effect.forkScoped({ startImmediately: true }));
        return {
          get requestId() {
            return currentRequestId;
          },
          close: Effect.gen(function* () {
            closed = true;
            yield* rpcClient.Unsubscribe({ requestId: currentRequestId }).pipe(Effect.ignore);
            yield* Fiber.interrupt(fiber);
          }),
        };
      }),

    publish: (topic, row) =>
      rpcClient.Publish({ topic, row: row as never }).pipe(Effect.mapError(toViewServerError)),

    deltaPublish: (topic, patch) =>
      rpcClient
        .DeltaPublish({ topic, patch: patch as never })
        .pipe(Effect.mapError(toViewServerError)),

    health: () => rpcClient.Health({}).pipe(Effect.mapError(toViewServerError)),

    createStore: (topic, query, initialData) =>
      Effect.gen(function* () {
        const store = new SubscriptionStore<readonly RuntimeRow[]>(
          (initialData ?? []) as readonly RuntimeRow[],
          rowKeyForQuery(query as RuntimeQuery, idFieldForTopic(topic)),
        );
        store.setStatus("snapshot_loading");
        const subscription = yield* createViewServerClient<TConfig>(rpcClient, config).subscribe(
          topic,
          query,
          (event) => Effect.sync(() => store.apply(event)),
        );
        yield* Effect.addFinalizer(() => subscription.close);
        return store;
      }),
  };
}

function toViewServerError(error: unknown): ViewServerError {
  return isViewServerError(error) ? error : transportError(error);
}
