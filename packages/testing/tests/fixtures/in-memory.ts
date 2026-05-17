import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
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
import type { ViewServerError } from "@view-server/core/errors";
import type {
  InferReadableQueryResult,
  QueryForReadableTopic,
  RuntimeRow,
  SubscriptionEvent,
} from "@view-server/core/query";
import type { HealthResponse, ViewServerRuntimeShape } from "@view-server/core/runtime";
import { fromWireRow, wireQueryResponse, wireSubscriptionEvent } from "@view-server/core/rpc";
import { createViewServerHooks, type ViewServerHooks } from "@view-server/react";
import { makeInternalTestingViewServerRuntime } from "../../../core/src/server/runtime.ts";
import {
  createTestingViewServerClientFromTransport,
  normalizeTestingInitialRows,
  validateTestingIsolationId,
  type RequireIsolationId,
  type TestingViewServerClient,
  type TopicPatchWithoutIsolation,
  type TopicRowWithoutIsolation,
} from "../../src/testing-isolation.ts";

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
    const runtime = yield* makeInternalTestingViewServerRuntime(config, {
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
    const isolationId = validateTestingIsolationId(options.isolationId);
    const runtime = yield* makeInternalTestingViewServerRuntime(config, {
      initialRows: normalizeTestingInitialRows(options.initialRows, isolationId),
      __testingUseMemorySnapshotBackend: true,
    });
    const baseTransport = runtimeTransport(runtime);
    const { client, liveClient } = createTestingViewServerClientFromTransport(
      baseTransport,
      config,
      isolationId,
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
