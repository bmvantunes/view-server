import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import {
  createViewServerClient,
  type ActiveSubscription,
  type LiveQueryInitialData,
  type ViewServerClient,
  type ViewServerRpcTransport,
} from "@view-server/core/client";
import type {
  ReadableTopicName,
  TopicIdFromConfig,
  TopicName,
  TopicPatchFromConfig,
  TopicRowFromConfig,
  ViewServerConfig,
} from "@view-server/core/config";
import { VIEW_SERVER_HEALTH_TOPIC } from "@view-server/core/config";
import { isViewServerError, transportError, type ViewServerError } from "@view-server/core/errors";
import type {
  InferReadableQueryResult,
  QueryForReadableTopic,
  RuntimeRow,
  SubscriptionEvent,
} from "@view-server/core/query";
import {
  makeViewServerRuntime,
  type HealthResponse,
  type ViewServerRuntimeShape,
} from "@view-server/core/runtime";
import {
  fromWireRow,
  toWireRow,
  type RpcQueryPayload,
  ViewServerRpcs,
  wireQueryResponse,
  wireSubscriptionEvent,
} from "@view-server/core/rpc";
import {
  createViewServerHooks,
  layerBrowserWebsocketRpcClient,
  type ViewServerHooks,
} from "@view-server/react";
import {
  createContext,
  createElement,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

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
  readonly deleteById: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    id: TopicIdFromConfig<TConfig, TTopic>,
  ) => Promise<void>;
  readonly query: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
  ) => Promise<LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>>;
  readonly subscribe: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    onEvent: (event: SubscriptionEvent<readonly RuntimeRow[]>) => Effect.Effect<void>,
  ) => Effect.Effect<ActiveSubscription, ViewServerError, import("effect/Scope").Scope>;
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

type TopicRowWithoutIsolation<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Omit<TopicRowFromConfig<TConfig, TTopic>, "isolationId">;

type TopicPatchWithoutIsolation<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Omit<TopicPatchFromConfig<TConfig, TTopic>, "isolationId">;

type MissingIsolationTopics<TConfig extends ViewServerConfig> = {
  readonly [TTopic in TopicName<TConfig>]: TopicRowFromConfig<TConfig, TTopic> extends {
    readonly isolationId: string;
  }
    ? never
    : TTopic;
}[TopicName<TConfig>];

type RequireIsolationId<TConfig extends ViewServerConfig> =
  MissingIsolationTopics<TConfig> extends never
    ? unknown
    : {
        readonly "Each test-isolated topic schema must include isolationId": MissingIsolationTopics<TConfig>;
      };

export type TestingViewServerClient<TConfig extends ViewServerConfig> = {
  readonly query: ViewServerClient<TConfig>["query"];
  readonly subscribe: ViewServerClient<TConfig>["subscribe"];
  readonly health: ViewServerClient<TConfig>["health"];
  readonly createStore: ViewServerClient<TConfig>["createStore"];
  readonly deleteById: ViewServerClient<TConfig>["deleteById"];
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    rows:
      | TopicRowWithoutIsolation<TConfig, TTopic>
      | readonly TopicRowWithoutIsolation<TConfig, TTopic>[],
  ) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchWithoutIsolation<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
};

export type IsolatedInMemoryViewServer<TConfig extends ViewServerConfig> = {
  readonly client: TestingViewServerClient<TConfig>;
  readonly hooks: ViewServerHooks<TConfig>;
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    rows:
      | TopicRowWithoutIsolation<TConfig, TTopic>
      | readonly TopicRowWithoutIsolation<TConfig, TTopic>[],
  ) => Promise<void>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchWithoutIsolation<TConfig, TTopic>,
  ) => Promise<void>;
  readonly deleteById: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    id: TopicIdFromConfig<TConfig, TTopic>,
  ) => Promise<void>;
  readonly query: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
  ) => Promise<LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>>;
  readonly subscribe: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    onEvent: (event: SubscriptionEvent<readonly RuntimeRow[]>) => Effect.Effect<void>,
  ) => Effect.Effect<ActiveSubscription, ViewServerError, import("effect/Scope").Scope>;
  readonly health: () => Promise<HealthResponse>;
  readonly close: () => Promise<void>;
};

export type IsolatedInMemoryViewServerOptions<TConfig extends ViewServerConfig> = {
  readonly isolationId: string;
  readonly initialRows?:
    | Partial<{
        readonly [TTopic in TopicName<TConfig>]: readonly TopicRowWithoutIsolation<
          TConfig,
          TTopic
        >[];
      }>
    | undefined;
};

export function inMemoryViewServer<const TConfig extends ViewServerConfig>(
  config: TConfig,
  options: InMemoryViewServerOptions<TConfig> = {},
): Effect.Effect<InMemoryViewServer<TConfig>, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.testing.in_memory.make")(function* () {
    const runtime = yield* makeViewServerRuntime(config, {
      initialRows: normalizeInitialRows(options.initialRows),
      __testingUseMemorySnapshotBackend: true,
    });
    const client = createViewServerClient<TConfig>(runtimeTransport(runtime), config);
    const hooks = createViewServerHooks(client, config);

    const server: InMemoryViewServer<TConfig> = {
      client,
      hooks,
      publish: async (topic, rows) => {
        const batch = Array.isArray(rows) ? rows : [rows];
        for (const row of batch) {
          await Effect.runPromise(client.publish(topic, row));
        }
      },
      deltaPublish: (topic, patch) => Effect.runPromise(client.deltaPublish(topic, patch)),
      deleteById: (topic, id) => Effect.runPromise(client.deleteById(topic, id)),
      query: (topic, query) => Effect.runPromise(client.query(topic, query)),
      subscribe: (topic, query, onEvent) => client.subscribe(topic, query, onEvent),
      health: () => Effect.runPromise(client.health()),
      close: () => Effect.runPromise(runtime.close),
    };
    return server;
  })();
}

export function isolatedInMemoryViewServer<const TConfig extends ViewServerConfig>(
  config: TConfig,
  options: IsolatedInMemoryViewServerOptions<TConfig> & RequireIsolationId<TConfig>,
): Effect.Effect<
  IsolatedInMemoryViewServer<TConfig>,
  ViewServerError,
  import("effect/Scope").Scope
> {
  return Effect.fn("view-server.testing.in_memory.isolated.make")(function* () {
    const runtime = yield* makeViewServerRuntime(config, {
      initialRows: normalizeIsolatedInitialRows(options.initialRows, options.isolationId),
      __testingUseMemorySnapshotBackend: true,
    });
    const baseTransport = runtimeTransport(runtime);
    const { client, liveClient } = createTestingViewServerClientFromTransport(
      baseTransport,
      config,
      options.isolationId,
    );
    const hooks = createViewServerHooks(liveClient, config);

    const server: IsolatedInMemoryViewServer<TConfig> = {
      client,
      hooks,
      publish: (topic, rows) => Effect.runPromise(client.publish(topic, rows)),
      deltaPublish: (topic, patch) => Effect.runPromise(client.deltaPublish(topic, patch)),
      deleteById: (topic, id) => Effect.runPromise(client.deleteById(topic, id)),
      query: (topic, query) => Effect.runPromise(client.query(topic, query)),
      subscribe: (topic, query, onEvent) => client.subscribe(topic, query, onEvent),
      health: () => Effect.runPromise(client.health()),
      close: () => Effect.runPromise(runtime.close),
    };
    return server;
  })();
}

export function makeTestingBrowserWebsocketClient<const TConfig extends ViewServerConfig>(
  url: string,
  config: TConfig,
  options: { readonly isolationId: string } & RequireIsolationId<TConfig>,
): Effect.Effect<TestingViewServerClient<TConfig>> {
  return Effect.sync(() => {
    const { client } = createTestingViewServerClientFromTransport(
      browserWebsocketTransport(url),
      config,
      options.isolationId,
    );
    return client;
  }).pipe(Effect.withSpan("view-server.testing.websocket.browser_client"));
}

export function createTestingViewServerReact<const TConfig extends ViewServerConfig>(
  config: TConfig,
  ...requireIsolation: MissingIsolationTopics<TConfig> extends never
    ? []
    : [RequireIsolationId<TConfig>]
) {
  void requireIsolation;
  const TestingViewServerContext = createContext<ViewServerHooks<TConfig> | undefined>(undefined);

  function TestingViewServerProvider(props: {
    readonly url: string;
    readonly isolationId: string;
    readonly children: ReactNode;
  }) {
    const [hooks, setHooks] = useState<ViewServerHooks<TConfig> | undefined>(undefined);
    const [error, setError] = useState<unknown>();

    useEffect(() => {
      let disposed = false;
      setHooks(undefined);
      setError(undefined);

      Effect.runPromise(
        Effect.sync(() =>
          createTestingViewServerClientFromTransport(
            browserWebsocketTransport(props.url),
            config,
            props.isolationId,
          ),
        ),
      ).then(
        ({ liveClient }) => {
          if (!disposed) {
            setHooks(createViewServerHooks(liveClient, config));
          }
        },
        (cause: unknown) => {
          if (!disposed) {
            setError(cause);
          }
        },
      );

      return () => {
        disposed = true;
        setHooks(undefined);
      };
    }, [props.url, props.isolationId]);

    if (error !== undefined) {
      throw error;
    }
    if (hooks === undefined) {
      return null;
    }
    return createElement(TestingViewServerContext.Provider, { value: hooks }, props.children);
  }

  function useTestingViewServerHooks(): ViewServerHooks<TConfig> {
    const hooks = useContext(TestingViewServerContext);
    if (hooks === undefined) {
      throw new Error("TestingViewServerProvider is missing or not connected");
    }
    return hooks;
  }

  function useLiveQuery<
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
  ) {
    return useTestingViewServerHooks().useLiveQuery(topic, query, initialData);
  }

  return {
    TestingViewServerProvider,
    useTestingViewServerHooks,
    useLiveQuery,
  };
}

function normalizeInitialRows<TConfig extends ViewServerConfig>(
  initialRows: InMemoryViewServerOptions<TConfig>["initialRows"],
): Readonly<Record<string, readonly RuntimeRow[]>> | undefined {
  if (initialRows === undefined) {
    return undefined;
  }
  const normalized: Record<string, readonly RuntimeRow[]> = {};
  for (const [topic, rows] of Object.entries(initialRows)) {
    if (rows !== undefined) {
      normalized[topic] = rows.map((row) => Object.fromEntries(Object.entries(row)));
    }
  }
  return normalized;
}

function normalizeIsolatedInitialRows<TConfig extends ViewServerConfig>(
  initialRows: IsolatedInMemoryViewServerOptions<TConfig>["initialRows"],
  isolationId: string,
): Readonly<Record<string, readonly RuntimeRow[]>> | undefined {
  if (initialRows === undefined) {
    return undefined;
  }
  const normalized: Record<string, readonly RuntimeRow[]> = {};
  for (const [topic, rows] of Object.entries(initialRows)) {
    if (rows !== undefined) {
      normalized[topic] = rows.map((row) => ({ ...row, isolationId }));
    }
  }
  return normalized;
}

function runtimeTransport(runtime: ViewServerRuntimeShape): ViewServerRpcTransport {
  return {
    Query: (payload) =>
      runtime.query(payload.topic, payload.query).pipe(Effect.map(wireQueryResponse)),
    Subscribe: (payload) =>
      runtime
        .subscribe(payload.requestId, payload.topic, payload.query)
        .pipe(Stream.map(wireSubscriptionEvent)),
    Unsubscribe: (payload) => runtime.unsubscribe(payload.requestId),
    Publish: (payload) => runtime.publish(payload.topic, fromWireRow(payload.row)),
    DeltaPublish: (payload) => runtime.deltaPublish(payload.topic, fromWireRow(payload.patch)),
    DeleteById: (payload) => runtime.deleteById(payload.topic, payload.id),
    Health: () => runtime.health,
  };
}

function browserWebsocketTransport(url: string): ViewServerRpcTransport {
  const clientLayer = layerBrowserWebsocketRpcClient(url);
  const runRpc = <A>(
    run: (
      rpcClient: RpcClient.RpcClient<
        import("effect/unstable/rpc/RpcGroup").Rpcs<typeof ViewServerRpcs>,
        RpcClientError
      >,
    ) => Effect.Effect<A, ViewServerError | RpcClientError>,
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
    DeleteById: (payload) => runRpc((rpcClient) => rpcClient.DeleteById(payload)),
    Health: (payload) => runRpc((rpcClient) => rpcClient.Health(payload)),
  };
}

function createTestingViewServerClientFromTransport<TConfig extends ViewServerConfig>(
  transport: ViewServerRpcTransport,
  config: TConfig,
  isolationId: string,
): {
  readonly client: TestingViewServerClient<TConfig>;
  readonly liveClient: ViewServerClient<TConfig>;
} {
  const isolatedTransport = isolateTransport(transport, isolationId);
  const liveClient = createViewServerClient<TConfig>(isolatedTransport, config);
  const client: TestingViewServerClient<TConfig> = {
    query: liveClient.query,
    subscribe: liveClient.subscribe,
    health: liveClient.health,
    createStore: liveClient.createStore,
    deleteById: liveClient.deleteById,
    publish: (topic, rows) =>
      Effect.forEach(Array.isArray(rows) ? rows : [rows], (row) =>
        transport.Publish({
          topic,
          row: toWireRow({ ...row, isolationId }),
        }),
      ).pipe(Effect.asVoid, Effect.mapError(toViewServerError)),
    deltaPublish: (topic, patch) =>
      transport
        .DeltaPublish({
          topic,
          patch: toWireRow({ ...patch, isolationId }),
        })
        .pipe(Effect.mapError(toViewServerError)),
  };
  return { client, liveClient };
}

function isolateTransport(
  transport: ViewServerRpcTransport,
  isolationId: string,
): ViewServerRpcTransport {
  return {
    Query: (payload) =>
      transport.Query({
        ...payload,
        query: isolateQuery(payload.topic, payload.query, isolationId),
      }),
    Subscribe: (payload) =>
      transport.Subscribe({
        ...payload,
        query: isolateQuery(payload.topic, payload.query, isolationId),
      }),
    Unsubscribe: transport.Unsubscribe,
    Publish: (payload) =>
      transport.Publish({
        ...payload,
        row: { ...payload.row, isolationId },
      }),
    DeltaPublish: (payload) =>
      transport.DeltaPublish({
        ...payload,
        patch: { ...payload.patch, isolationId },
      }),
    DeleteById: transport.DeleteById,
    Health: transport.Health,
  };
}

function isolateQuery(
  topic: string,
  query: RpcQueryPayload["query"],
  isolationId: string,
): RpcQueryPayload["query"] {
  if (topic === VIEW_SERVER_HEALTH_TOPIC) {
    return query;
  }
  const isolationFilter = {
    field: "isolationId",
    comparator: "equals",
    value: isolationId,
  } satisfies RpcQueryPayload["query"]["where"];
  const where =
    query.where === undefined
      ? isolationFilter
      : ({
          op: "and",
          conditions: [isolationFilter, query.where],
        } satisfies RpcQueryPayload["query"]["where"]);
  return {
    ...query,
    where,
  };
}

function toViewServerError(error: unknown): ViewServerError {
  return isViewServerError(error) ? error : transportError(error);
}
