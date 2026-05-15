import { BrowserSocket } from "@effect/platform-browser";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { AsyncResult } from "effect/unstable/reactivity";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import {
  createViewServerClient,
  LiveQueryStore,
  queryResultToRuntimeRows,
  rowKeyForTypedQuery,
  runtimeRowsToQueryResult,
  type LiveQueryInitialData,
  type LiveQueryResult,
  type ViewServerClient,
} from "@view-server/core/client";
import type { ReadableTopicName, ViewServerConfig } from "@view-server/core/config";
import { transportError } from "@view-server/core/errors";
import type { InferReadableQueryResult, QueryForReadableTopic } from "@view-server/core/query";
import { ViewServerRpcs } from "@view-server/core/rpc";
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
  readonly useLiveQuery: <
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
  ) => LiveQueryResult<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>;
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
    useLiveQuery(topic, query, initialData) {
      return useLiveQueryWithClient(client, config, topic, query, initialData);
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
        useLiveQuery(topic, query, initialData) {
          return useLiveQueryWithClient(
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

  function useLiveQuery<
    TTopic extends ReadableTopicName<TConfig>,
    TQuery extends QueryForReadableTopic<TConfig, TTopic>,
  >(
    topic: TTopic,
    query: TQuery,
    initialData?: LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
  ) {
    const context = useViewServerContext();
    return useLiveQueryWithClient(
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
    useLiveQuery,
    createHooks: (client: ViewServerClient<TConfig>) => createViewServerHooks(client, config),
  };
}

function useLiveQueryWithClient<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  client: ViewServerClient<TConfig> | undefined,
  config: TConfig,
  topic: TTopic,
  query: TQuery,
  initialData?: LiveQueryInitialData<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]>,
  connectionStatus: "connecting" | "ready" | "error" = "ready",
  connectionError?: unknown,
): LiveQueryResult<InferReadableQueryResult<TConfig, TTopic, TQuery>[number]> {
  if (query === undefined) {
    throw new Error(`useLiveQuery query is missing for topic ${String(topic)}`);
  }
  const rowKey = useMemo(
    () => rowKeyForTypedQuery<TConfig, TTopic, TQuery>(query, idFieldForTopic(config, topic)),
    [config, query, topic],
  );
  const initialValue = useMemo(
    () =>
      initialData === undefined
        ? undefined
        : {
            rows: queryResultToRuntimeRows(initialData.rows),
            totalRows: initialData.totalRows,
          },
    [initialData],
  );
  const storeRef = useRef<LiveQueryStore | undefined>(undefined);
  if (storeRef.current === undefined) {
    storeRef.current = new LiveQueryStore(initialValue, rowKey);
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

    store.setStatus("syncing");
    Effect.runPromise(
      Effect.fn("view-server.react.subscription.start")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": String(topic),
        });
        scope = yield* Scope.make();
        yield* Scope.provide(scope)(
          client.subscribe(
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
          ),
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

  return AsyncResult.map(state, (value) => ({
    rows: runtimeRowsToQueryResult(value.rows, query, config, topic),
    totalRows: value.totalRows,
    status: value.status,
    connection: value.connection,
  }));
}

function idFieldForTopic<TConfig extends ViewServerConfig>(
  config: TConfig,
  topic: ReadableTopicName<TConfig>,
): string {
  return topic === "__view_server_health" ? "id" : String(config.topics[topic]?.id ?? "id");
}

export * from "./metrics-ui.tsx";
