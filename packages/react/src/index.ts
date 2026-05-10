import { BrowserSocket } from "@effect/platform-browser";
import { Effect, Exit, Layer, Scope } from "effect";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import {
  createViewServerClient,
  queryResultToRuntimeRows,
  rowKeyForTypedQuery,
  runtimeRowsToQueryResult,
  SubscriptionStore,
  type InferReadableQueryResult,
  type QueryForReadableTopic,
  type ReadableTopicName,
  transportError,
  type ViewServerClient,
  type ViewServerConfig,
  ViewServerRpcs,
} from "@view-server/core";
import {
  createContext,
  createElement,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

export type ViewServerHooks<TConfig extends ViewServerConfig> = {
  readonly useSubscription: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: InferReadableQueryResult<TConfig, TTopic, TQuery>,
  ) => {
    readonly data: InferReadableQueryResult<TConfig, TTopic, TQuery>;
    readonly totalRows: number;
    readonly status: "connecting" | "snapshot_loading" | "live" | "error" | "closed";
    readonly error?: unknown;
  };
};

export const layerBrowserWebsocketRpcClient = (url: string) =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(BrowserSocket.layerWebSocket(url)),
    Layer.provide(RpcSerialization.layerNdjson),
  );

export function makeBrowserWebsocketClient<TConfig extends ViewServerConfig>(
  url: string,
  config: TConfig,
): Effect.Effect<ViewServerClient<TConfig>, never, Scope.Scope> {
  return Effect.fn("view-server.react.websocket.browser_client")(function* () {
    const scope = yield* Effect.scope;
    const context = yield* Layer.buildWithScope(layerBrowserWebsocketRpcClient(url), scope);
    const rpcClient = yield* RpcClient.make(ViewServerRpcs).pipe(Effect.provide(context));
    return createViewServerClient<TConfig>(rpcClient, config);
  })();
}

export function createViewServerHooks<TConfig extends ViewServerConfig>(
  client: ViewServerClient<TConfig>,
  config: TConfig,
): ViewServerHooks<TConfig> {
  return {
    useSubscription(topic, query, initialData) {
      return useSubscriptionWithClient(client, config, topic, query, initialData);
    },
  };
}

type ViewServerContextValue<TConfig extends ViewServerConfig = ViewServerConfig> = {
  readonly status: "connecting" | "ready" | "error";
  readonly config: TConfig;
  readonly client?: ViewServerClient<TConfig> | undefined;
  readonly error?: unknown;
};

export function createViewServerReact<const TConfig extends ViewServerConfig>(config: TConfig) {
  const ViewServerContext = createContext<ViewServerContextValue<TConfig> | undefined>(undefined);

  function ViewServerProvider(props: { readonly url: string; readonly children: ReactNode }) {
    const [state, setState] = useState<ViewServerContextValue<TConfig>>({
      status: "connecting",
      config,
    });

    useEffect(() => {
      let disposed = false;
      let scope: Scope.Closeable | undefined;
      setState({ status: "connecting", config });

      Effect.runPromise(
        Effect.fn("view-server.react.provider.connect")(function* () {
          scope = yield* Scope.make();
          return yield* Scope.provide(scope)(
            makeBrowserWebsocketClient<TConfig>(props.url, config),
          );
        })(),
      ).then(
        (client) => {
          if (!disposed) {
            setState({ status: "ready", config, client });
          }
        },
        (error: unknown) => {
          if (!disposed) {
            setState({ status: "error", config, error });
          }
        },
      );

      return () => {
        disposed = true;
        if (scope !== undefined) {
          void Effect.runPromise(Scope.close(scope, Exit.void));
        }
      };
    }, [props.url]);

    return createElement(ViewServerContext.Provider, { value: state }, props.children);
  }

  function ViewServerClientProvider(props: {
    readonly client: ViewServerClient<TConfig>;
    readonly children: ReactNode;
  }) {
    const value = useMemo<ViewServerContextValue<TConfig>>(
      () => ({
        status: "ready",
        config,
        client: props.client,
      }),
      [props.client],
    );
    return createElement(ViewServerContext.Provider, { value }, props.children);
  }

  function useViewServerContext(): ViewServerContextValue<TConfig> {
    const context = useContext(ViewServerContext);
    if (context === undefined) {
      throw new Error("ViewServerProvider is missing");
    }
    return context;
  }

  function useViewServerClient(): ViewServerClient<TConfig> {
    const context = useViewServerContext();
    if (context.client === undefined) {
      throw context.error ?? new Error("ViewServer client is not ready");
    }
    return context.client;
  }

  function useViewServerHooks(): ViewServerHooks<TConfig> {
    const context = useViewServerContext();
    return useMemo(
      () => ({
        useSubscription(topic, query, initialData) {
          return useSubscriptionWithClient(
            context.client,
            config,
            topic,
            query,
            initialData,
            context.status,
            context.error,
          );
        },
      }),
      [context],
    );
  }

  function useSubscription<
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(topic: TTopic, query: TQuery, initialData?: InferReadableQueryResult<TConfig, TTopic, TQuery>) {
    const context = useViewServerContext();
    return useSubscriptionWithClient(
      context.client,
      config,
      topic,
      query,
      initialData,
      context.status,
      context.error,
    );
  }

  return {
    ViewServerProvider,
    ViewServerClientProvider,
    useViewServerClient,
    useViewServerHooks,
    useSubscription,
    createHooks: (client: ViewServerClient<TConfig>) => createViewServerHooks(client, config),
  };
}

function useSubscriptionWithClient<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  client: ViewServerClient<TConfig> | undefined,
  config: TConfig,
  topic: TTopic,
  query: TQuery,
  initialData?: InferReadableQueryResult<TConfig, TTopic, TQuery>,
  connectionStatus: "connecting" | "ready" | "error" = "ready",
  connectionError?: unknown,
) {
  if (query === undefined) {
    throw new Error(`useSubscription query is missing for topic ${String(topic)}`);
  }
  const rowKey = useMemo(
    () => rowKeyForTypedQuery<TConfig, TTopic, TQuery>(query, idFieldForTopic(config, topic)),
    [config, query, topic],
  );
  const initialRows = useMemo(() => queryResultToRuntimeRows(initialData), [initialData]);
  const storeRef = useRef<SubscriptionStore | undefined>(undefined);
  if (storeRef.current === undefined) {
    storeRef.current = new SubscriptionStore(initialRows, rowKey);
  }
  const store = storeRef.current;

  useEffect(() => {
    let disposed = false;
    let scope: Scope.Closeable | undefined;
    store.setRowKey(rowKey);

    if (client === undefined) {
      if (connectionStatus === "error") {
        store.setError(transportError(connectionError));
      } else {
        store.setStatus("connecting");
      }
      return () => {
        disposed = true;
        store.setStatus("closed");
      };
    }

    store.setStatus("snapshot_loading");
    Effect.runPromise(
      Effect.fn("view-server.react.subscription.start")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        scope = yield* Scope.make();
        yield* Scope.provide(scope)(
          client.subscribe(topic, query, (event) => Effect.sync(() => store.apply(event))),
        );
      })(),
    ).catch((error) => {
      if (!disposed) {
        store.setError(error);
      }
    });

    return () => {
      disposed = true;
      store.setStatus("closed");
      if (scope !== undefined) {
        void Effect.runPromise(Scope.close(scope, Exit.void));
      }
    };
  }, [client, connectionError, connectionStatus, query, rowKey, store, topic]);

  const state = useSyncExternalStore(
    (listener) => store.subscribe(listener),
    () => store.snapshot,
    () => store.snapshot,
  );

  return {
    data: runtimeRowsToQueryResult(state.data, query, config, topic),
    totalRows: state.totalRows,
    status: state.status,
    ...(state.error === undefined ? {} : { error: state.error }),
  };
}

function idFieldForTopic<TConfig extends ViewServerConfig>(
  config: TConfig,
  topic: ReadableTopicName<TConfig>,
): string {
  return topic === "__view_server_health" ? "id" : String(config.topics[topic]?.id ?? "id");
}

export * from "./metrics-ui.tsx";
