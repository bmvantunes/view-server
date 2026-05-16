import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  type AuthorizationContext,
  type EffectSourceContext,
  type KafkaSourceConfig,
  isReservedTopic,
  normalizeConfig,
  type NormalizedViewServerConfig,
  type RowObject,
  type TopicConfig,
  type ViewServerConfig,
  type ViewServerHealthRow,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import {
  invalidPublish,
  invalidConfig,
  invalidQuery,
  kafkaIngestFailed,
  missingTopic,
  serverShutdown,
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
import {
  runKafkaSource,
  type KafkaBatchMetrics,
  type KafkaTopicConsumer,
  type KafkaTopicVerifier,
} from "../kafka/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../snapshot/snapshot-backend.ts";
import {
  makeInProcessTopicWorkerHost,
  type TopicWorkerHost,
  type TopicWorkerHostFactory,
} from "../worker/index.ts";

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

export type HealthResponse = {
  readonly ok: boolean;
  readonly topics: Readonly<
    Record<
      string,
      {
        readonly rows: number;
        readonly subscribers: number;
        readonly queueDepth: number;
        readonly maxSubscriptionLagVersions: number;
        readonly totalSubscriptionLagVersions: number;
        readonly activePlanCount: number;
        readonly activeViewCount: number;
        readonly activePlanRows: number;
        readonly activePlanIndexEstimatedBytes: number;
        readonly activePlanBuildQueueDepth: number;
        readonly activePlanBuildingCount: number;
        readonly activePlanPendingCount: number;
        readonly activePlanBuildMs: number;
        readonly activePlanBuildMsTotal: number;
        readonly activePlanBuildMsMax: number;
        readonly activePlanFallbackCount: number;
        readonly activePlanAutoBuildSkippedCount: number;
        readonly chdbStatus: "ready" | "degraded" | "restarting" | "stopped";
        readonly chdbPid: number;
        readonly chdbRestarts: number;
        readonly chdbPendingRequests: number;
        readonly chdbLastError: string;
        readonly chdbBackendVersion: string;
        readonly version: string;
        readonly kafkaLagTotal: number;
        readonly kafkaLagMax: number;
        readonly kafkaPartitions: number;
        readonly lastKafkaOffset: number;
        readonly lastKafkaEndOffset: number;
        readonly status: "ready" | "degraded" | "stopping";
      }
    >
  >;
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
    yield* verifyKafkaSourceTopics(normalized, options);
    const workers = new Map<string, TopicWorkerHost>();
    const kafkaMetricsByTopic = new Map<string, KafkaRuntimeMetrics>();
    const sourceFailuresByTopic = new Map<string, string>();
    const sourceFibers: Fiber.Fiber<void, ViewServerError>[] = [];
    let closing = false;
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
    ) =>
      closing
        ? Effect.fail(
            serverShutdown(`Server is shutting down; refusing ${operation}`, topic, requestId),
          )
        : Effect.void;

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
      const topics: Record<
        string,
        {
          rows: number;
          subscribers: number;
          queueDepth: number;
          maxSubscriptionLagVersions: number;
          totalSubscriptionLagVersions: number;
          activePlanCount: number;
          activeViewCount: number;
          activePlanRows: number;
          activePlanIndexEstimatedBytes: number;
          activePlanBuildQueueDepth: number;
          activePlanBuildingCount: number;
          activePlanPendingCount: number;
          activePlanBuildMs: number;
          activePlanBuildMsTotal: number;
          activePlanBuildMsMax: number;
          activePlanFallbackCount: number;
          activePlanAutoBuildSkippedCount: number;
          chdbStatus: "ready" | "degraded" | "restarting" | "stopped";
          chdbPid: number;
          chdbRestarts: number;
          chdbPendingRequests: number;
          chdbLastError: string;
          chdbBackendVersion: string;
          version: string;
          kafkaLagTotal: number;
          kafkaLagMax: number;
          kafkaPartitions: number;
          lastKafkaOffset: number;
          lastKafkaEndOffset: number;
          status: "ready" | "degraded" | "stopping";
        }
      > = {};
      for (const [topic, worker] of workers) {
        const metrics = yield* worker.metrics;
        const kafkaMetrics = kafkaMetricsByTopic.get(topic) ?? emptyKafkaRuntimeMetrics;
        const sourceFailed = sourceFailuresByTopic.has(topic);
        topics[topic] = {
          rows: metrics.rows,
          subscribers: metrics.subscribers,
          queueDepth: metrics.queueDepth,
          maxSubscriptionLagVersions: metrics.maxSubscriptionLagVersions,
          totalSubscriptionLagVersions: metrics.totalSubscriptionLagVersions,
          activePlanCount: metrics.activePlanCount,
          activeViewCount: metrics.activeViewCount,
          activePlanRows: metrics.activePlanRows,
          activePlanIndexEstimatedBytes: metrics.activePlanIndexEstimatedBytes,
          activePlanBuildQueueDepth: metrics.activePlanBuildQueueDepth,
          activePlanBuildingCount: metrics.activePlanBuildingCount,
          activePlanPendingCount: metrics.activePlanPendingCount,
          activePlanBuildMs: metrics.activePlanBuildMs,
          activePlanBuildMsTotal: metrics.activePlanBuildMsTotal,
          activePlanBuildMsMax: metrics.activePlanBuildMsMax,
          activePlanFallbackCount: metrics.activePlanFallbackCount,
          activePlanAutoBuildSkippedCount: metrics.activePlanAutoBuildSkippedCount,
          chdbStatus: metrics.chdbStatus,
          chdbPid: metrics.chdbPid,
          chdbRestarts: metrics.chdbRestarts,
          chdbPendingRequests: metrics.chdbPendingRequests,
          chdbLastError: metrics.chdbLastError,
          chdbBackendVersion: metrics.chdbBackendVersion.toString(),
          version: metrics.version.toString(),
          kafkaLagTotal: kafkaMetrics.lagTotal,
          kafkaLagMax: kafkaMetrics.lagMax,
          kafkaPartitions: kafkaMetrics.partitions,
          lastKafkaOffset: kafkaMetrics.offset,
          lastKafkaEndOffset: kafkaMetrics.endOffset,
          status: closing ? "stopping" : sourceFailed ? "degraded" : metrics.status,
        };
      }
      return {
        ok: !closing && Object.values(topics).every((topic) => topic.status === "ready"),
        topics,
      };
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
      const guardedQuery = yield* validateRuntimeQueryLimits(topic, query, normalized.limits);
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
      const guardedQuery = yield* validateRuntimeQueryLimits(topic, query, normalized.limits);
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
      if (closing) {
        return;
      }
      closing = true;
      yield* syncHealthTopicIgnoringErrors;
      yield* Effect.forEach(sourceFibers, (fiber) => Fiber.interrupt(fiber), {
        discard: true,
      }).pipe(Effect.ignore);
      yield* Effect.forEach(workers.values(), (worker) => worker.shutdown, { discard: true });
    });

    const recordKafkaMetrics = Effect.fnUntraced(function* (
      topic: string,
      metrics: KafkaBatchMetrics,
    ) {
      kafkaMetricsByTopic.set(topic, kafkaRuntimeMetrics(metrics));
      yield* syncHealthTopic();
    });

    const recordSourceFailure = Effect.fn("view-server.runtime.source.failed")(function* (
      topic: string,
      message: string,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      sourceFailuresByTopic.set(topic, message);
      yield* Effect.logWarning(`view-server source degraded topic=${topic} reason=${message}`);
      yield* syncHealthTopicIgnoringErrors;
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
    const startedSourceFibers = yield* startTopicSources(normalized, options, {
      publish: (topic, row) => publishWithTransport(topic, row, "internal"),
      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "internal"),
      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "internal"),
      recordKafkaMetrics,
      recordSourceFailure,
    });
    sourceFibers.push(...startedSourceFibers);

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

function healthRowsFromResponse(health: HealthResponse): readonly ViewServerHealthRow[] {
  const updatedAt = BigInt(Date.now());
  const topicEntries = Object.entries(health.topics).filter(
    ([topic]) => topic !== VIEW_SERVER_HEALTH_TOPIC,
  );
  const serverStatus = topicEntries.some(([, topic]) => topic.status === "stopping")
    ? "stopping"
    : health.ok
      ? "ready"
      : "degraded";
  const serverRow = healthRow({
    id: "server",
    kind: "server",
    rows: sumTopicMetric(topicEntries, "rows"),
    subscribers: sumTopicMetric(topicEntries, "subscribers"),
    queueDepth: sumTopicMetric(topicEntries, "queueDepth"),
    maxSubscriptionLagVersions: maxTopicMetric(topicEntries, "maxSubscriptionLagVersions"),
    totalSubscriptionLagVersions: sumTopicMetric(topicEntries, "totalSubscriptionLagVersions"),
    activePlanCount: sumTopicMetric(topicEntries, "activePlanCount"),
    activeViewCount: sumTopicMetric(topicEntries, "activeViewCount"),
    activePlanRows: sumTopicMetric(topicEntries, "activePlanRows"),
    activePlanIndexEstimatedBytes: sumTopicMetric(topicEntries, "activePlanIndexEstimatedBytes"),
    activePlanBuildQueueDepth: sumTopicMetric(topicEntries, "activePlanBuildQueueDepth"),
    activePlanBuildingCount: sumTopicMetric(topicEntries, "activePlanBuildingCount"),
    activePlanPendingCount: sumTopicMetric(topicEntries, "activePlanPendingCount"),
    activePlanBuildMs: maxTopicMetric(topicEntries, "activePlanBuildMs"),
    activePlanBuildMsTotal: sumTopicMetric(topicEntries, "activePlanBuildMsTotal"),
    activePlanBuildMsMax: maxTopicMetric(topicEntries, "activePlanBuildMsMax"),
    activePlanFallbackCount: sumTopicMetric(topicEntries, "activePlanFallbackCount"),
    activePlanAutoBuildSkippedCount: sumTopicMetric(
      topicEntries,
      "activePlanAutoBuildSkippedCount",
    ),
    chdbStatus: aggregateChdbStatus(topicEntries),
    chdbPid: 0,
    chdbRestarts: sumTopicMetric(topicEntries, "chdbRestarts"),
    chdbPendingRequests: sumTopicMetric(topicEntries, "chdbPendingRequests"),
    chdbLastError: firstTopicTextMetric(topicEntries, "chdbLastError"),
    chdbBackendVersion: maxTopicVersionString(topicEntries, "chdbBackendVersion"),
    kafkaLagTotal: sumTopicMetric(topicEntries, "kafkaLagTotal"),
    kafkaLagMax: maxTopicMetric(topicEntries, "kafkaLagMax"),
    kafkaPartitions: sumTopicMetric(topicEntries, "kafkaPartitions"),
    lastKafkaOffset: maxTopicMetric(topicEntries, "lastKafkaOffset"),
    lastKafkaEndOffset: maxTopicMetric(topicEntries, "lastKafkaEndOffset"),
    status: serverStatus,
    updatedAt,
  });
  const topicRows = topicEntries.map(([topic, metrics]) =>
    healthRow({
      id: `topic:${topic}`,
      kind: "topic",
      topic,
      rows: metrics.rows,
      subscribers: metrics.subscribers,
      queueDepth: metrics.queueDepth,
      maxSubscriptionLagVersions: metrics.maxSubscriptionLagVersions,
      totalSubscriptionLagVersions: metrics.totalSubscriptionLagVersions,
      activePlanCount: metrics.activePlanCount,
      activeViewCount: metrics.activeViewCount,
      activePlanRows: metrics.activePlanRows,
      activePlanIndexEstimatedBytes: metrics.activePlanIndexEstimatedBytes,
      activePlanBuildQueueDepth: metrics.activePlanBuildQueueDepth,
      activePlanBuildingCount: metrics.activePlanBuildingCount,
      activePlanPendingCount: metrics.activePlanPendingCount,
      activePlanBuildMs: metrics.activePlanBuildMs,
      activePlanBuildMsTotal: metrics.activePlanBuildMsTotal,
      activePlanBuildMsMax: metrics.activePlanBuildMsMax,
      activePlanFallbackCount: metrics.activePlanFallbackCount,
      activePlanAutoBuildSkippedCount: metrics.activePlanAutoBuildSkippedCount,
      chdbStatus: metrics.chdbStatus,
      chdbPid: metrics.chdbPid,
      chdbRestarts: metrics.chdbRestarts,
      chdbPendingRequests: metrics.chdbPendingRequests,
      chdbLastError: metrics.chdbLastError,
      chdbBackendVersion: metrics.chdbBackendVersion,
      kafkaLagTotal: metrics.kafkaLagTotal,
      kafkaLagMax: metrics.kafkaLagMax,
      kafkaPartitions: metrics.kafkaPartitions,
      lastKafkaOffset: metrics.lastKafkaOffset,
      lastKafkaEndOffset: metrics.lastKafkaEndOffset,
      status: metrics.status,
      updatedAt,
    }),
  );
  return [serverRow, ...topicRows];
}

function healthRow(input: {
  readonly id: string;
  readonly kind: ViewServerHealthRow["kind"];
  readonly topic?: string | undefined;
  readonly rows: number;
  readonly subscribers: number;
  readonly queueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly totalSubscriptionLagVersions: number;
  readonly activePlanCount: number;
  readonly activeViewCount: number;
  readonly activePlanRows: number;
  readonly activePlanIndexEstimatedBytes: number;
  readonly activePlanBuildQueueDepth: number;
  readonly activePlanBuildingCount: number;
  readonly activePlanPendingCount: number;
  readonly activePlanBuildMs: number;
  readonly activePlanBuildMsTotal: number;
  readonly activePlanBuildMsMax: number;
  readonly activePlanFallbackCount: number;
  readonly activePlanAutoBuildSkippedCount: number;
  readonly chdbStatus: ViewServerHealthRow["chdbStatus"];
  readonly chdbPid: number;
  readonly chdbRestarts: number;
  readonly chdbPendingRequests: number;
  readonly chdbLastError: string;
  readonly chdbBackendVersion: string;
  readonly kafkaLagTotal: number;
  readonly kafkaLagMax: number;
  readonly kafkaPartitions: number;
  readonly lastKafkaOffset: number;
  readonly lastKafkaEndOffset: number;
  readonly status: ViewServerHealthRow["status"];
  readonly updatedAt: bigint;
}): ViewServerHealthRow {
  return {
    id: input.id,
    kind: input.kind,
    ...(input.topic === undefined ? {} : { topic: input.topic }),
    rows: input.rows,
    subscribers: input.subscribers,
    queueDepth: input.queueDepth,
    maxSubscriptionLagVersions: input.maxSubscriptionLagVersions,
    totalSubscriptionLagVersions: input.totalSubscriptionLagVersions,
    activePlanCount: input.activePlanCount,
    activeViewCount: input.activeViewCount,
    activePlanRows: input.activePlanRows,
    activePlanIndexEstimatedBytes: input.activePlanIndexEstimatedBytes,
    activePlanBuildQueueDepth: input.activePlanBuildQueueDepth,
    activePlanBuildingCount: input.activePlanBuildingCount,
    activePlanPendingCount: input.activePlanPendingCount,
    activePlanBuildMs: input.activePlanBuildMs,
    activePlanBuildMsTotal: input.activePlanBuildMsTotal,
    activePlanBuildMsMax: input.activePlanBuildMsMax,
    activePlanFallbackCount: input.activePlanFallbackCount,
    activePlanAutoBuildSkippedCount: input.activePlanAutoBuildSkippedCount,
    chdbStatus: input.chdbStatus,
    chdbPid: input.chdbPid,
    chdbRestarts: input.chdbRestarts,
    chdbPendingRequests: input.chdbPendingRequests,
    chdbLastError: input.chdbLastError,
    chdbBackendVersion: input.chdbBackendVersion,
    workerLagP95Ms: 0,
    deltaFanoutP95Ms: 0,
    publishLatencyP95Ms: 0,
    snapshotLatencyP95Ms: 0,
    chdbSnapshotLatencyP95Ms: 0,
    kafkaLagTotal: input.kafkaLagTotal,
    kafkaLagMax: input.kafkaLagMax,
    kafkaPartitions: input.kafkaPartitions,
    lastKafkaOffset: input.lastKafkaOffset,
    lastKafkaEndOffset: input.lastKafkaEndOffset,
    rssMb: 0,
    status: input.status,
    updatedAt: input.updatedAt,
  };
}

function sumTopicMetric(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
  field:
    | "rows"
    | "subscribers"
    | "queueDepth"
    | "totalSubscriptionLagVersions"
    | "activePlanCount"
    | "activeViewCount"
    | "activePlanRows"
    | "activePlanIndexEstimatedBytes"
    | "activePlanBuildQueueDepth"
    | "activePlanBuildingCount"
    | "activePlanPendingCount"
    | "activePlanBuildMsTotal"
    | "activePlanFallbackCount"
    | "activePlanAutoBuildSkippedCount"
    | "chdbRestarts"
    | "chdbPendingRequests"
    | "kafkaLagTotal"
    | "kafkaPartitions",
): number {
  return entries.reduce((sum, [, metrics]) => sum + metrics[field], 0);
}

function maxTopicMetric(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
  field:
    | "maxSubscriptionLagVersions"
    | "activePlanBuildMs"
    | "activePlanBuildMsMax"
    | "kafkaLagMax"
    | "lastKafkaOffset"
    | "lastKafkaEndOffset",
): number {
  return entries.reduce((max, [, metrics]) => Math.max(max, metrics[field]), 0);
}

function aggregateChdbStatus(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
): ViewServerHealthRow["chdbStatus"] {
  if (entries.some(([, metrics]) => metrics.chdbStatus === "degraded")) {
    return "degraded";
  }
  if (entries.some(([, metrics]) => metrics.chdbStatus === "restarting")) {
    return "restarting";
  }
  if (entries.length > 0 && entries.every(([, metrics]) => metrics.chdbStatus === "stopped")) {
    return "stopped";
  }
  return "ready";
}

function firstTopicTextMetric(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
  field: "chdbLastError",
): string {
  return entries.find(([, metrics]) => metrics[field].length > 0)?.[1][field] ?? "";
}

function maxTopicVersionString(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
  field: "chdbBackendVersion",
): string {
  let max = 0n;
  for (const [, metrics] of entries) {
    const value = BigInt(metrics[field]);
    if (value > max) {
      max = value;
    }
  }
  return max.toString();
}

type KafkaSourceForVerification = {
  readonly viewTopic: string;
  readonly brokers: readonly string[];
  readonly kafkaTopic: string;
};

type KafkaRuntimeMetrics = {
  readonly lagTotal: number;
  readonly lagMax: number;
  readonly partitions: number;
  readonly offset: number;
  readonly endOffset: number;
};

const emptyKafkaRuntimeMetrics: KafkaRuntimeMetrics = {
  lagTotal: 0,
  lagMax: 0,
  partitions: 0,
  offset: 0,
  endOffset: 0,
};

function kafkaRuntimeMetrics(metrics: KafkaBatchMetrics): KafkaRuntimeMetrics {
  return {
    lagTotal: metrics.lagTotal,
    lagMax: metrics.lagMax,
    partitions: metrics.partitions,
    offset: metrics.offset ?? 0,
    endOffset: metrics.endOffset ?? 0,
  };
}

function verifyKafkaSourceTopics(
  config: NormalizedViewServerConfig,
  options: ViewServerRuntimeOptions,
): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.kafka.verify_topics")(function* () {
    const sources = collectKafkaSources(config);
    yield* Effect.annotateCurrentSpan({
      "view_server.batch_size": sources.length,
    });
    if (sources.length === 0) {
      return;
    }
    const verifier = options.kafkaTopicVerifier;
    if (verifier === undefined) {
      return yield* Effect.fail(
        kafkaIngestFailed(
          sources[0].viewTopic,
          new Error("KafkaSource requires a kafkaTopicVerifier runtime option"),
        ),
      );
    }
    yield* Effect.forEach(
      kafkaVerificationGroups(sources),
      ({ brokers, topics }) => verifier.verifyTopics({ brokers, topics }),
      { discard: true },
    );
  })();
}

function collectKafkaSources(
  config: NormalizedViewServerConfig,
): readonly KafkaSourceForVerification[] {
  const sources: KafkaSourceForVerification[] = [];
  for (const [viewTopic, topicConfig] of Object.entries(config.topics)) {
    const source = topicConfig.source;
    if (viewTopic !== VIEW_SERVER_HEALTH_TOPIC && source?._tag === "KafkaSource") {
      sources.push({
        viewTopic,
        brokers: source.brokers,
        kafkaTopic: source.topic,
      });
    }
  }
  return sources;
}

function kafkaVerificationGroups(sources: readonly KafkaSourceForVerification[]) {
  const groups = new Map<
    string,
    {
      readonly brokers: readonly string[];
      readonly topicSet: Set<string>;
    }
  >();
  for (const source of sources) {
    const key = JSON.stringify(source.brokers);
    const group = groups.get(key);
    if (group === undefined) {
      groups.set(key, {
        brokers: source.brokers,
        topicSet: new Set([source.kafkaTopic]),
      });
    } else {
      group.topicSet.add(source.kafkaTopic);
    }
  }
  return Array.from(groups.values(), ({ brokers, topicSet }) => ({
    brokers,
    topics: Array.from(topicSet),
  }));
}

type RuntimeQueryLimits = NormalizedViewServerConfig["limits"];

function validateRuntimeQueryLimits(
  topic: string,
  query: RuntimeQuery,
  limits: RuntimeQueryLimits,
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

function startTopicSources(
  config: NormalizedViewServerConfig,
  options: ViewServerRuntimeOptions,
  runtime: {
    readonly publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerError>;
    readonly deltaPublish: (
      topic: string,
      patch: RuntimeRow,
    ) => Effect.Effect<void, ViewServerError>;
    readonly deleteById: (
      topic: string,
      id: string | number,
    ) => Effect.Effect<void, ViewServerError>;
    readonly recordKafkaMetrics: (
      topic: string,
      metrics: KafkaBatchMetrics,
    ) => Effect.Effect<void, ViewServerError>;
    readonly recordSourceFailure: (
      topic: string,
      message: string,
    ) => Effect.Effect<void, ViewServerError>;
  },
): Effect.Effect<
  readonly Fiber.Fiber<void, ViewServerError>[],
  ViewServerError,
  import("effect/Scope").Scope
> {
  return Effect.fn("view-server.runtime.sources.start")(function* () {
    const entries = Object.entries(config.topics);
    const fibers: Fiber.Fiber<void, ViewServerError>[] = [];
    yield* Effect.annotateCurrentSpan({
      "view_server.batch_size": entries.length,
    });
    yield* Effect.forEach(
      entries,
      ([topic, topicConfig]) =>
        Effect.gen(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": topic,
          });
          if (topic === VIEW_SERVER_HEALTH_TOPIC || topicConfig.source === undefined) {
            return;
          }
          const context: EffectSourceContext<RowObject, string> = {
            topic,
            idField: topicConfig.id,
            publish: (row) => runtime.publish(topic, row),
            deltaPublish: (patch) => runtime.deltaPublish(topic, patch),
            deleteById: (id) => runtime.deleteById(topic, id),
          };
          if (topicConfig.source._tag === "EffectSource") {
            const fiber = yield* monitorSource(
              topic,
              topicConfig.source.run(context),
              runtime.recordSourceFailure,
            ).pipe(Effect.forkScoped({ startImmediately: true }));
            fibers.push(fiber);
            return;
          }
          const source = topicConfig.source;
          const consumer = options.kafkaConsumerFactory?.(source);
          if (consumer === undefined) {
            return yield* Effect.fail(
              kafkaIngestFailed(
                topic,
                new Error("KafkaSource requires a kafkaConsumerFactory runtime option"),
              ),
            );
          }
          const fiber = yield* monitorSource(
            topic,
            runKafkaSource({
              viewTopic: topic,
              idField: topicConfig.id,
              source,
              consumer,
              runtime: context,
              onBatchMetrics: (metrics) => runtime.recordKafkaMetrics(topic, metrics),
            }),
            runtime.recordSourceFailure,
          ).pipe(Effect.forkScoped({ startImmediately: true }));
          fibers.push(fiber);
        }).pipe(Effect.withSpan("view-server.runtime.source.start")),
      { discard: true },
    );
    return fibers;
  })();
}

function monitorSource(
  topic: string,
  source: Effect.Effect<void, ViewServerError>,
  recordSourceFailure: (topic: string, message: string) => Effect.Effect<void, ViewServerError>,
): Effect.Effect<void, ViewServerError> {
  return source.pipe(
    Effect.exit,
    Effect.flatMap((exit) =>
      Exit.isSuccess(exit)
        ? recordSourceFailure(topic, "Source exited")
        : recordSourceFailure(topic, Cause.pretty(exit.cause)),
    ),
  );
}
