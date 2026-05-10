import { Effect } from "effect";
import * as RpcTest from "effect/unstable/rpc/RpcTest";
import {
  createViewServerClient,
  makeViewServerRuntime,
  type HealthResponse,
  type InferReadableQueryResult,
  type QueryForReadableTopic,
  type ReadableTopicName,
  type RuntimeRow,
  type TopicName,
  type TopicPatchFromConfig,
  type TopicRowFromConfig,
  type ViewServerClient,
  type ViewServerConfig,
  type ViewServerError,
  ViewServerRuntime,
  ViewServerRpcs,
  ViewServerHandlersLive,
} from "@view-server/core";
import { createViewServerHooks, type ViewServerHooks } from "@view-server/react";

export type InMemoryViewServer<TConfig extends ViewServerConfig> = {
  readonly client: ViewServerClient<TConfig>;
  readonly hooks: ViewServerHooks<TConfig>;
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    rows: TopicRowFromConfig<TConfig, TTopic> | readonly TopicRowFromConfig<TConfig, TTopic>[],
  ) => Promise<void>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchFromConfig<TConfig, TTopic>,
  ) => Promise<void>;
  readonly query: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
  ) => Promise<InferReadableQueryResult<TConfig, TTopic, TQuery>>;
  readonly health: () => Promise<HealthResponse>;
  readonly close: () => Promise<void>;
};

export type InMemoryViewServerOptions<TConfig extends ViewServerConfig> = {
  readonly initialRows?:
    | Partial<{
        readonly [TTopic in TopicName<TConfig>]: readonly TopicRowFromConfig<TConfig, TTopic>[];
      }>
    | undefined;
};

export function inMemoryViewServer<const TConfig extends ViewServerConfig>(
  config: TConfig,
  options: InMemoryViewServerOptions<TConfig> = {},
): Effect.Effect<InMemoryViewServer<TConfig>, ViewServerError, import("effect/Scope").Scope> {
  return Effect.gen(function* () {
    const runtime = yield* makeViewServerRuntime(config, {
      initialRows: options.initialRows as Readonly<Record<string, readonly RuntimeRow[]>>,
      useMemorySnapshotBackend: true,
    });
    const rpcClient = yield* RpcTest.makeClient(ViewServerRpcs).pipe(
      Effect.provide(ViewServerHandlersLive),
      Effect.provideService(ViewServerRuntime, runtime),
    );
    const client = createViewServerClient<TConfig>(rpcClient, config);
    const hooks = createViewServerHooks(client, config);

    return {
      client,
      hooks,
      publish: async (topic, rows) => {
        const batch = Array.isArray(rows) ? rows : [rows];
        for (const row of batch) {
          await Effect.runPromise(client.publish(topic, row));
        }
      },
      deltaPublish: (topic, patch) => Effect.runPromise(client.deltaPublish(topic, patch)),
      query: (topic, query) => Effect.runPromise(client.query(topic, query)),
      health: () => Effect.runPromise(client.health()),
      close: () => Effect.runPromise(runtime.close),
    };
  });
}
