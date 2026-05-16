import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import {
  type ActiveSubscription,
  type LiveQueryInitialData,
  type ViewServerRpcTransport,
} from "@view-server/core/client";
import type {
  ReadableTopicName,
  TopicIdFromConfig,
  TopicName,
  ViewServerConfig,
} from "@view-server/core/config";
import { transportError, type ViewServerError } from "@view-server/core/errors";
import type {
  InferReadableQueryResult,
  QueryForReadableTopic,
  RuntimeRow,
  SubscriptionEvent,
} from "@view-server/core/query";
import { ViewServerRpcs } from "@view-server/core/rpc";
import { layerBrowserWebsocketRpcClient } from "@view-server/react";
import {
  createTestingViewServerClientFromTransport,
  validateTestingIsolationId,
  type RequireIsolationId,
  type TestingViewServerClient,
  type TopicPatchWithoutIsolation,
  type TopicRowWithoutIsolation,
} from "./testing-isolation.ts";

export type RealViewServerTestHarness<TConfig extends ViewServerConfig> = {
  readonly rpcUrl: string;
  readonly readyUrl: string | undefined;
  readonly isolationId: string;
  readonly client: TestingViewServerClient<TConfig>;
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
  readonly close: () => Promise<void>;
};

export type RealViewServerTestHarnessOptions = {
  readonly rpcUrl: string;
  readonly readyUrl?: string | undefined;
  readonly isolationId: string;
  readonly transport?: ViewServerRpcTransport | undefined;
  readonly start?: Effect.Effect<void, ViewServerError> | undefined;
  readonly stop?: Effect.Effect<void, ViewServerError> | undefined;
  readonly readyTimeoutMs?: number | undefined;
  readonly readyPollMs?: number | undefined;
};

export function realViewServerTestHarness<const TConfig extends ViewServerConfig>(
  config: TConfig,
  options: RealViewServerTestHarnessOptions & RequireIsolationId<TConfig>,
): Effect.Effect<
  RealViewServerTestHarness<TConfig>,
  ViewServerError,
  import("effect/Scope").Scope
> {
  return Effect.fn("view-server.testing.real_server_harness.make")(function* () {
    const isolationId = validateTestingIsolationId(options.isolationId);
    yield* options.start ?? Effect.void;
    if (options.readyUrl !== undefined) {
      yield* waitForReady(options.readyUrl, {
        timeoutMs: options.readyTimeoutMs ?? 5_000,
        pollMs: options.readyPollMs ?? 100,
      });
    }
    const stopOwnedServer = ownedServerStopper(options.stop);
    yield* Effect.addFinalizer(() => stopOwnedServer().pipe(Effect.ignore));
    const transport = options.transport ?? browserWebsocketTransport(options.rpcUrl);
    const { client } = createTestingViewServerClientFromTransport(transport, config, isolationId);
    const harness: RealViewServerTestHarness<TConfig> = {
      rpcUrl: options.rpcUrl,
      readyUrl: options.readyUrl,
      isolationId,
      client,
      publish: (topic, rows) => Effect.runPromise(client.publish(topic, rows)),
      deltaPublish: (topic, patch) => Effect.runPromise(client.deltaPublish(topic, patch)),
      deleteById: (topic, id) => Effect.runPromise(client.deleteById(topic, id)),
      query: (topic, query) => Effect.runPromise(client.query(topic, query)),
      subscribe: (topic, query, onEvent) => client.subscribe(topic, query, onEvent),
      close: () => Effect.runPromise(stopOwnedServer()),
    };
    return harness;
  })();
}

export function readyUrlForRpcUrl(rpcUrl: string, readyPath = "/ready"): string {
  const url = new URL(rpcUrl);
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname = readyPath;
  url.search = "";
  url.hash = "";
  return url.toString();
}

function waitForReady(
  readyUrl: string,
  options: {
    readonly timeoutMs: number;
    readonly pollMs: number;
  },
): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.testing.real_server_harness.ready")(function* () {
    const deadline = Date.now() + options.timeoutMs;
    while (Date.now() <= deadline) {
      const ready = yield* Effect.tryPromise({
        try: () => fetch(readyUrl),
        catch: (error) => transportError(error),
      }).pipe(
        Effect.match({
          onFailure: () => false,
          onSuccess: (response) => response.ok,
        }),
      );
      if (ready) {
        return;
      }
      yield* Effect.sleep(`${options.pollMs} millis`);
    }
    return yield* Effect.fail(
      transportError(new Error(`Timed out waiting for View Server readiness at ${readyUrl}`)),
    );
  })();
}

function ownedServerStopper(stop: Effect.Effect<void, ViewServerError> | undefined) {
  let stopped = false;
  return () =>
    Effect.suspend(() => {
      if (stopped || stop === undefined) {
        return Effect.void;
      }
      stopped = true;
      return stop;
    });
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
