import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  columnCatalogForTopic,
  normalizeConfig,
  type NormalizedViewServerConfig,
  type ViewServerConfig,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import { invalidConfig, type ViewServerError } from "../errors.ts";
import type {
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../protocol/index.ts";
import { defaultAuthPolicy, type AuthPolicy } from "./auth-policy.ts";
import { QueryLimitPolicy } from "./query-limit-policy.ts";
import {
  healthRowsFromResponse,
  projectRuntimeHealth,
  type HealthResponse,
  type RuntimeHealthProjectionTopicInput,
} from "./runtime-health-projection.ts";
import { makeRuntimeOperations } from "./runtime-operations.ts";
import { RuntimeShutdownController } from "./runtime-shutdown-controller.ts";
import {
  createRuntimeSourceGraph,
  type RuntimeSourceGraphOptions,
} from "./runtime-source-graph.ts";

export type { HealthResponse } from "./runtime-health-projection.ts";

export type ViewServerRuntimeShape = {
  readonly config: NormalizedViewServerConfig;
  readonly query: (
    topic: string,
    query: RuntimeQuery,
  ) => Effect.Effect<QueryResponse<readonly RuntimeRow[]>, ViewServerError>;
  readonly subscribe: (
    requestId: string,
    topic: string,
    query: RuntimeQuery,
  ) => Stream.Stream<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>;
  readonly unsubscribe: (requestId: string) => Effect.Effect<void, ViewServerError>;
  readonly publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: (topic: string, patch: RuntimeRow) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: (topic: string, id: string | number) => Effect.Effect<void, ViewServerError>;
  readonly health: Effect.Effect<HealthResponse, ViewServerError>;
  readonly close: Effect.Effect<void, ViewServerError>;
};

export class ViewServerRuntime extends Context.Service<ViewServerRuntime, ViewServerRuntimeShape>()(
  "@view-server/core/ViewServerRuntime",
) {}

type InternalViewServerRuntimeOptions = RuntimeSourceGraphOptions & {
  readonly authPolicy?: AuthPolicy | undefined;
};

export type ViewServerRuntimeOptions = Omit<
  RuntimeSourceGraphOptions,
  | "__testingSnapshotBackends"
  | "__testingSnapshotBackendFactory"
  | "__testingUseMemorySnapshotBackend"
> & {
  readonly authPolicy?: AuthPolicy | undefined;
};

export function makeViewServerRuntime(
  config: ViewServerConfig,
  options: ViewServerRuntimeOptions = {},
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return makeViewServerRuntimeInternal(config, options);
}

export type InternalTestingViewServerRuntimeOptions = InternalViewServerRuntimeOptions;

export function makeInternalTestingViewServerRuntime(
  config: ViewServerConfig,
  options: InternalTestingViewServerRuntimeOptions = {},
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return makeViewServerRuntimeInternal(config, options);
}

export const layerInternalTestingViewServerRuntime = (
  config: ViewServerConfig,
  options?: InternalTestingViewServerRuntimeOptions,
): Layer.Layer<ViewServerRuntime, ViewServerError> =>
  Layer.effect(ViewServerRuntime, makeInternalTestingViewServerRuntime(config, options));

function makeViewServerRuntimeInternal(
  config: ViewServerConfig,
  options: InternalViewServerRuntimeOptions,
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.runtime.make")(function* () {
    const normalized = yield* Effect.try({
      try: () => normalizeConfig(config),
      catch: (error) => invalidConfig("Invalid view-server config", "config", error),
    });
    const sourceGraph = yield* createRuntimeSourceGraph(normalized, options);
    const sourceSupervisor = sourceGraph.sourceSupervisor;
    const workers = sourceGraph.workers;
    const columnCatalogs = new Map(
      Object.entries(normalized.topics).map(([topic, topicConfig]) => [
        topic,
        columnCatalogForTopic(topic, topicConfig),
      ]),
    );
    const queryLimitPolicy = QueryLimitPolicy.fromConfig(normalized);
    const authPolicy = options.authPolicy ?? defaultAuthPolicy(normalized);
    const shutdownController = new RuntimeShutdownController();

    const collectHealth = Effect.fn("view-server.runtime.health")(function* () {
      const topics: Record<string, RuntimeHealthProjectionTopicInput> = {};
      for (const [topic, worker] of workers) {
        const metrics = yield* worker.metrics;
        const sourceHealth = sourceSupervisor.topicHealth(topic);
        topics[topic] = {
          worker: metrics,
          kafka: sourceHealth.kafka,
          sourceFailed: sourceHealth.sourceFailed,
          queryRejectedCount: queryLimitPolicy.rejectedCount(topic),
        };
      }
      return projectRuntimeHealth({ closing: shutdownController.isClosing(), topics });
    });

    const syncHealthTopic = Effect.fn("view-server.runtime.health_topic.sync")(function* () {
      const healthWorker = workers.get(VIEW_SERVER_HEALTH_TOPIC);
      if (healthWorker === undefined) {
        return;
      }
      const health = yield* collectHealth();
      yield* Effect.forEach(healthRowsFromResponse(health), (row) => healthWorker.publish(row), {
        discard: true,
      });
    });

    const syncHealthTopicIgnoringErrors = syncHealthTopic().pipe(Effect.ignore);

    const operations = makeRuntimeOperations({
      workers,
      columnCatalogs,
      authPolicy,
      queryLimitPolicy,
      shutdownController,
      syncHealthTopic: syncHealthTopic(),
      syncHealthTopicIgnoringErrors,
    });

    const closeRuntime = Effect.fn("view-server.runtime.close")(function* () {
      yield* shutdownController.close({
        syncHealth: syncHealthTopicIgnoringErrors,
        stopSources: sourceSupervisor.shutdown(),
        workers: workers.values(),
      });
    });

    const runtime: ViewServerRuntimeShape = {
      config: normalized,

      query: operations.query,

      subscribe: (requestId, topic, query) =>
        Stream.unwrap(operations.subscribe(requestId, topic, query)),

      unsubscribe: operations.unsubscribe,

      publish: (topic, row) => operations.publishWithTransport(topic, row, "rpc"),

      deltaPublish: (topic, patch) => operations.deltaPublishWithTransport(topic, patch, "rpc"),

      deleteById: (topic, id) => operations.deleteByIdWithTransport(topic, id, "rpc"),

      health: collectHealth(),

      close: closeRuntime(),
    };

    yield* syncHealthTopic();
    yield* sourceSupervisor.start({
      publish: (topic, row) => operations.publishWithTransport(topic, row, "internal"),
      deltaPublish: (topic, patch) =>
        operations.deltaPublishWithTransport(topic, patch, "internal"),
      deleteById: (topic, id) => operations.deleteByIdWithTransport(topic, id, "internal"),
      mutateBatch: (topic, mutations) =>
        operations.mutateBatchWithTransport(topic, mutations, "internal"),
      syncHealth: syncHealthTopicIgnoringErrors,
    });

    return runtime;
  })();
}

export const layerViewServerRuntime = (
  config: ViewServerConfig,
  options?: ViewServerRuntimeOptions,
): Layer.Layer<ViewServerRuntime, ViewServerError> =>
  Layer.effect(ViewServerRuntime, makeViewServerRuntime(config, options));
