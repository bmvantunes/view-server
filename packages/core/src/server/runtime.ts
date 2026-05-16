import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  type AuthorizationContext,
  columnCatalogForTopic,
  type ColumnCatalog,
  type KafkaSourceConfig,
  isReservedTopic,
  normalizeConfig,
  type NormalizedViewServerConfig,
  type RowObject,
  type TopicConfig,
  type ViewServerConfig,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import {
  invalidPublish,
  invalidConfig,
  invalidQuery,
  missingTopic,
  snapshotBackendFailed,
  unauthorized,
  type ViewServerError,
} from "../errors.ts";
import type {
  RuntimeFilterNode,
  RuntimeGroupedQuery,
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../protocol/index.ts";
import { isRuntimeGroupedQuery } from "../protocol/index.ts";
import type { KafkaTopicConsumer, KafkaTopicVerifier } from "../kafka/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../snapshot/snapshot-backend.ts";
import {
  makeInProcessTopicWorkerHost,
  type TopicWorkerHost,
  type TopicWorkerHostFactory,
} from "../worker/index.ts";
import { KafkaSourceSupervisor } from "./kafka-source-supervisor.ts";
import {
  healthRowsFromResponse,
  projectRuntimeHealth,
  type HealthResponse,
  type RuntimeHealthProjectionTopicInput,
} from "./runtime-health-projection.ts";
import { RuntimeShutdownController } from "./runtime-shutdown-controller.ts";

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

export type ViewServerRuntimeOptions = {
  readonly initialRows?: Readonly<Record<string, readonly RuntimeRow[]>> | undefined;
  readonly kafkaConsumerFactory?:
    | ((source: KafkaSourceConfig<RowObject, string>) => KafkaTopicConsumer)
    | undefined;
  readonly kafkaTopicVerifier?: KafkaTopicVerifier | undefined;
  readonly topicWorkerFactory?: TopicWorkerHostFactory | undefined;
  /** @internal Test-only backend injection for fault and fallback coverage. */
  readonly __testingSnapshotBackends?: Readonly<Record<string, SnapshotBackend>> | undefined;
  /** @internal Test-only backend factory for fault and fallback coverage. */
  readonly __testingSnapshotBackendFactory?:
    | ((topic: string, config: TopicConfig) => SnapshotBackend)
    | undefined;
  /** @internal Browser/package tests only. Production runtime must use chDB. */
  readonly __testingUseMemorySnapshotBackend?: boolean | undefined;
};

export function makeViewServerRuntime(
  config: ViewServerConfig,
  options: ViewServerRuntimeOptions = {},
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.runtime.make")(function* () {
    const normalized = yield* Effect.try({
      try: () => normalizeConfig(config),
      catch: (error) => invalidConfig("Invalid view-server config", "config", error),
    });
    const sourceSupervisor = new KafkaSourceSupervisor(normalized, {
      kafkaConsumerFactory: options.kafkaConsumerFactory,
      kafkaTopicVerifier: options.kafkaTopicVerifier,
    });
    yield* sourceSupervisor.verifyTopics();
    const workers = new Map<string, TopicWorkerHost>();
    const columnCatalogs = new Map(
      Object.entries(normalized.topics).map(([topic, topicConfig]) => [
        topic,
        columnCatalogForTopic(topic, topicConfig),
      ]),
    );
    const shutdownController = new RuntimeShutdownController();
    const makeTopicWorker = options.topicWorkerFactory ?? makeInProcessTopicWorkerHost;

    for (const [topic, topicConfig] of Object.entries(normalized.topics)) {
      const backend = yield* shouldResolveSnapshotBackend(options)
        ? resolveSnapshotBackend(topic, topicConfig, options)
        : Effect.succeed(undefined);
      const worker = yield* makeTopicWorker(topic, topicConfig, {
        initialRows: options.initialRows?.[topic],
        snapshotBackend: backend,
        maxQueueDepth: normalized.worker.maxQueueDepth,
        mutationLogSize: normalized.worker.mutationLogSize,
        deltaCoalescing: normalized.worker.deltaCoalescing,
        maxActivePlans: normalized.worker.maxActivePlans,
        maxActivePlanEstimatedBytes: normalized.worker.maxActivePlanEstimatedBytes,
        activePlanAutoBuildMaxRows: normalized.worker.activePlanAutoBuildMaxRows,
        activePlanBuildConcurrency: normalized.worker.activePlanBuildConcurrency,
        groupedRefreshDebounceMs: normalized.worker.groupedRefreshDebounceMs,
      });
      workers.set(topic, worker);
    }

    const workerFor = (topic: string) => {
      const worker = workers.get(topic);
      return worker === undefined ? Effect.fail(missingTopic(topic)) : Effect.succeed(worker);
    };

    const ensureRuntimeOpen = (
      operation: "query" | "subscribe" | "publish" | "delta-publish" | "delete",
      topic: string,
      requestId?: string,
    ) => shutdownController.ensureOpen(operation, topic, requestId);

    const ensureReadableTopic = (topic: string, operation: "subscribe" | "query") =>
      isReservedTopic(topic) && topic !== VIEW_SERVER_HEALTH_TOPIC
        ? Effect.fail(unauthorized(topic, operation))
        : Effect.void;

    const authorizeQuery = (topic: string, operation: "subscribe" | "query", payload: unknown) =>
      normalized.auth
        .authorizeQuery({ topic, operation, payload, transport: "rpc" })
        .pipe(
          Effect.flatMap((allowed) =>
            allowed ? Effect.void : Effect.fail(unauthorized(topic, operation)),
          ),
        );

    const authorizePublish = (
      topic: string,
      operation: "publish" | "delta-publish" | "delete",
      payload: unknown,
      transport: AuthorizationContext["transport"],
    ) =>
      normalized.auth
        .authorizePublish({ topic, operation, payload, transport })
        .pipe(
          Effect.flatMap((allowed) =>
            allowed ? Effect.void : Effect.fail(unauthorized(topic, operation)),
          ),
        );

    const collectHealth = Effect.fn("view-server.runtime.health")(function* () {
      const topics: Record<string, RuntimeHealthProjectionTopicInput> = {};
      for (const [topic, worker] of workers) {
        const metrics = yield* worker.metrics;
        const sourceHealth = sourceSupervisor.topicHealth(topic);
        topics[topic] = {
          worker: metrics,
          kafka: sourceHealth.kafka,
          sourceFailed: sourceHealth.sourceFailed,
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

    const publishWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      row: unknown,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(invalidPublish(topic, "Cannot publish to reserved topics"));
      }
      yield* ensureRuntimeOpen("publish", topic);
      yield* authorizePublish(topic, "publish", row, transport);
      const worker = yield* workerFor(topic);
      yield* worker.publish(row);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const publishWithTransport = (
      topic: string,
      row: unknown,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? publishWithTransportUntraced(topic, row, transport)
        : Effect.fn("view-server.runtime.publish")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* publishWithTransportUntraced(topic, row, transport);
          })();

    const deltaPublishWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      patch: RuntimeRow,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(invalidPublish(topic, "Cannot publish to reserved topics"));
      }
      yield* ensureRuntimeOpen("delta-publish", topic);
      yield* authorizePublish(topic, "delta-publish", patch, transport);
      const worker = yield* workerFor(topic);
      yield* worker.deltaPublish(patch);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const deltaPublishWithTransport = (
      topic: string,
      patch: RuntimeRow,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? deltaPublishWithTransportUntraced(topic, patch, transport)
        : Effect.fn("view-server.runtime.delta_publish")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* deltaPublishWithTransportUntraced(topic, patch, transport);
          })();

    const deleteByIdWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      id: string | number,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(invalidPublish(topic, "Cannot publish to reserved topics"));
      }
      yield* ensureRuntimeOpen("delete", topic);
      yield* authorizePublish(topic, "delete", id, transport);
      const worker = yield* workerFor(topic);
      yield* worker.deleteById(id);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const deleteByIdWithTransport = (
      topic: string,
      id: string | number,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? deleteByIdWithTransportUntraced(topic, id, transport)
        : Effect.fn("view-server.runtime.delete")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* deleteByIdWithTransportUntraced(topic, id, transport);
          })();

    const queryRuntime = Effect.fn("view-server.runtime.query")(function* (
      topic: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      yield* ensureRuntimeOpen("query", topic);
      yield* ensureReadableTopic(topic, "query");
      const guardedQuery = yield* validateRuntimeQuery(
        topic,
        query,
        normalized.limits,
        columnCatalogs.get(topic),
      );
      yield* authorizeQuery(topic, "query", guardedQuery);
      if (topic === VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopicIgnoringErrors;
      }
      const worker = yield* workerFor(topic);
      const response = yield* worker.query(guardedQuery);
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": response.rows.length,
        "view_server.total_rows": response.totalRows,
        "view_server.worker_version": response.version,
      });
      return response;
    });

    const subscribeRuntime = Effect.fn("view-server.runtime.subscribe")(function* (
      requestId: string,
      topic: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": requestId,
        "view_server.subscription_id": requestId,
        "view_server.topic": topic,
      });
      yield* ensureRuntimeOpen("subscribe", topic, requestId);
      yield* ensureReadableTopic(topic, "subscribe");
      const guardedQuery = yield* validateRuntimeQuery(
        topic,
        query,
        normalized.limits,
        columnCatalogs.get(topic),
      );
      yield* authorizeQuery(topic, "subscribe", guardedQuery);
      const worker = yield* workerFor(topic);
      return worker.subscribe(requestId, guardedQuery).pipe(
        Stream.onFirst(() => syncHealthTopicIgnoringErrors),
        Stream.ensuring(syncHealthTopicIgnoringErrors),
      );
    });

    const unsubscribeRuntime = Effect.fn("view-server.runtime.unsubscribe")(function* (
      requestId: string,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": requestId,
        "view_server.subscription_id": requestId,
      });
      yield* Effect.forEach(workers.values(), (worker) => worker.unsubscribe(requestId), {
        discard: true,
      });
      yield* syncHealthTopicIgnoringErrors;
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

      query: queryRuntime,

      subscribe: (requestId, topic, query) =>
        Stream.unwrap(subscribeRuntime(requestId, topic, query)),

      unsubscribe: unsubscribeRuntime,

      publish: (topic, row) => publishWithTransport(topic, row, "rpc"),

      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "rpc"),

      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "rpc"),

      health: collectHealth(),

      close: closeRuntime(),
    };

    yield* syncHealthTopic();
    yield* sourceSupervisor.start({
      publish: (topic, row) => publishWithTransport(topic, row, "internal"),
      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "internal"),
      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "internal"),
      syncHealth: syncHealthTopicIgnoringErrors,
    });

    return runtime;
  })();
}

function resolveSnapshotBackend(
  topic: string,
  topicConfig: TopicConfig,
  options: ViewServerRuntimeOptions,
): Effect.Effect<SnapshotBackend, ViewServerError> {
  if (topic === VIEW_SERVER_HEALTH_TOPIC) {
    return Effect.succeed(createMemorySnapshotBackend());
  }
  if (options.__testingUseMemorySnapshotBackend === true) {
    return Effect.succeed(createMemorySnapshotBackend());
  }
  const injected = options.__testingSnapshotBackends?.[topic];
  if (injected !== undefined) {
    return Effect.succeed(injected);
  }
  if (options.__testingSnapshotBackendFactory !== undefined) {
    return Effect.succeed(options.__testingSnapshotBackendFactory(topic, topicConfig));
  }
  return Effect.tryPromise({
    try: async () => {
      const { createChdbSnapshotBackend } = await import("../snapshot/chdb-backend.ts");
      return createChdbSnapshotBackend();
    },
    catch: (error) => snapshotBackendFailed(topic, error),
  });
}

function shouldResolveSnapshotBackend(options: ViewServerRuntimeOptions): boolean {
  return (
    options.topicWorkerFactory === undefined ||
    options.__testingUseMemorySnapshotBackend === true ||
    options.__testingSnapshotBackends !== undefined ||
    options.__testingSnapshotBackendFactory !== undefined
  );
}

export const layerViewServerRuntime = (
  config: ViewServerConfig,
  options?: ViewServerRuntimeOptions,
): Layer.Layer<ViewServerRuntime, ViewServerError> =>
  Layer.effect(ViewServerRuntime, makeViewServerRuntime(config, options));

type RuntimeQueryLimits = NormalizedViewServerConfig["limits"];

function validateRuntimeQuery(
  topic: string,
  query: RuntimeQuery,
  limits: RuntimeQueryLimits,
  catalog: ColumnCatalog | undefined,
): Effect.Effect<RuntimeQuery, ViewServerError> {
  return Effect.fnUntraced(function* () {
    if (query.offset !== undefined && (!Number.isInteger(query.offset) || query.offset < 0)) {
      return yield* Effect.fail(invalidQuery(topic, "Query offset must be a non-negative integer"));
    }
    if (query.limit !== undefined && (!Number.isInteger(query.limit) || query.limit <= 0)) {
      return yield* Effect.fail(invalidQuery(topic, "Query limit must be a positive integer"));
    }
    const limitedQuery =
      query.limit === undefined ? { ...query, limit: limits.maxPageSize } : query;
    if (limitedQuery.limit !== undefined && limitedQuery.limit > limits.maxPageSize) {
      return yield* Effect.fail(
        invalidQuery(
          topic,
          `Query limit ${limitedQuery.limit} exceeds maxPageSize ${limits.maxPageSize}`,
        ),
      );
    }
    if (isRuntimeGroupedQuery(limitedQuery)) {
      yield* validateGroupedQueryLimits(topic, limitedQuery, limits);
    }
    const filterStats = runtimeFilterStats(limitedQuery.where);
    if (filterStats.depth > limits.maxFilterDepth) {
      return yield* Effect.fail(
        invalidQuery(
          topic,
          `Query filter depth ${filterStats.depth} exceeds maxFilterDepth ${limits.maxFilterDepth}`,
        ),
      );
    }
    if (filterStats.conditions > limits.maxFilterConditions) {
      return yield* Effect.fail(
        invalidQuery(
          topic,
          `Query filter conditions ${filterStats.conditions} exceeds maxFilterConditions ${limits.maxFilterConditions}`,
        ),
      );
    }
    if (catalog !== undefined) {
      return yield* catalog.validateQuery(limitedQuery);
    }
    return limitedQuery;
  })();
}

function validateGroupedQueryLimits(
  topic: string,
  query: RuntimeGroupedQuery,
  limits: RuntimeQueryLimits,
): Effect.Effect<void, ViewServerError> {
  return Effect.fnUntraced(function* () {
    if (query.groupBy.length > limits.maxGroupByFields) {
      return yield* Effect.fail(
        invalidQuery(
          topic,
          `Query groupBy field count ${query.groupBy.length} exceeds maxGroupByFields ${limits.maxGroupByFields}`,
        ),
      );
    }
    const aggregateCount = Object.keys(query.aggregates).length;
    if (aggregateCount > limits.maxAggregateCount) {
      return yield* Effect.fail(
        invalidQuery(
          topic,
          `Query aggregate count ${aggregateCount} exceeds maxAggregateCount ${limits.maxAggregateCount}`,
        ),
      );
    }
  })();
}

function runtimeFilterStats(node: RuntimeFilterNode | undefined): {
  readonly depth: number;
  readonly conditions: number;
} {
  if (node === undefined) {
    return { depth: 0, conditions: 0 };
  }
  if ("conditions" in node) {
    const childStats = node.conditions.map(runtimeFilterStats);
    return {
      depth: 1 + childStats.reduce((max, stats) => Math.max(max, stats.depth), 0),
      conditions: childStats.reduce((sum, stats) => sum + stats.conditions, 0),
    };
  }
  return { depth: 1, conditions: 1 };
}
