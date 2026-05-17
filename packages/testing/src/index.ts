import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import { type LiveQueryInitialData, type ViewServerRpcTransport } from "@view-server/core/client";
import type { ReadableTopicName, ViewServerConfig } from "@view-server/core/config";
import type { ViewServerError } from "@view-server/core/errors";
import type { InferReadableQueryResult, QueryForReadableTopic } from "@view-server/core/query";
import { ViewServerRpcs } from "@view-server/core/rpc";
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
import {
  createTestingViewServerClientFromTransport,
  validateTestingIsolationId,
  type MissingIsolationTopics,
  type RequireIsolationId,
  type TestingViewServerClient,
} from "./testing-isolation.ts";

export {
  readyUrlForRpcUrl,
  realViewServerTestHarness,
  type RealViewServerTestHarness,
  type RealViewServerTestHarnessOptions,
} from "./real-server-harness.ts";

export type {
  MissingIsolationTopics,
  RequireIsolationId,
  TestingViewServerClient,
  TopicPatchWithoutIsolation,
  TopicRowWithoutIsolation,
} from "./testing-isolation.ts";

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
            validateTestingIsolationId(props.isolationId),
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
