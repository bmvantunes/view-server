import { Context, Effect, Layer, Stream } from "effect";
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
  kafkaIngestFailed,
  missingTopic,
  schemaDecodeFailed,
  snapshotBackendFailed,
  unauthorized,
  type ViewServerError,
} from "../errors.ts";
import type {
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../protocol/index.ts";
import {
  runKafkaSource,
  type KafkaBatchMetrics,
  type KafkaTopicConsumer,
  type KafkaTopicVerifier,
} from "../kafka/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../snapshot/index.ts";
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
  readonly snapshotBackends?: Readonly<Record<string, SnapshotBackend>> | undefined;
  readonly useMemorySnapshotBackend?: boolean | undefined;
  readonly kafkaConsumerFactory?:
    | ((source: KafkaSourceConfig<RowObject, string>) => KafkaTopicConsumer)
    | undefined;
  readonly kafkaTopicVerifier?: KafkaTopicVerifier | undefined;
  readonly snapshotBackendFactory?:
    | ((topic: string, config: TopicConfig) => SnapshotBackend)
    | undefined;
  readonly topicWorkerFactory?: TopicWorkerHostFactory | undefined;
};

export function makeViewServerRuntime(
  config: ViewServerConfig,
  options: ViewServerRuntimeOptions = {},
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.runtime.make")(function* () {
    const normalized = yield* Effect.try({
      try: () => normalizeConfig(config),
      catch: (error) => schemaDecodeFailed("__config", error),
    });
    yield* verifyKafkaSourceTopics(normalized, options);
    const workers = new Map<string, TopicWorkerHost>();
    const kafkaMetricsByTopic = new Map<string, KafkaRuntimeMetrics>();
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
      });
      workers.set(topic, worker);
    }

    const workerFor = (topic: string) => {
      const worker = workers.get(topic);
      return worker === undefined ? Effect.fail(missingTopic(topic)) : Effect.succeed(worker);
    };

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
        topics[topic] = {
          rows: metrics.rows,
          subscribers: metrics.subscribers,
          queueDepth: metrics.queueDepth,
          version: metrics.version.toString(),
          kafkaLagTotal: kafkaMetrics.lagTotal,
          kafkaLagMax: kafkaMetrics.lagMax,
          kafkaPartitions: kafkaMetrics.partitions,
          lastKafkaOffset: kafkaMetrics.offset,
          lastKafkaEndOffset: kafkaMetrics.endOffset,
          status: metrics.status,
        };
      }
      return {
        ok: Object.values(topics).every((topic) => topic.status === "ready"),
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
            yield* publishWithTransportUntraced(topic, row, transport);
          })();

    const deltaPublishWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      patch: RuntimeRow,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(invalidPublish(topic, "Cannot publish to reserved topics"));
      }
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
            yield* deltaPublishWithTransportUntraced(topic, patch, transport);
          })();

    const deleteByIdWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      id: string | number,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(invalidPublish(topic, "Cannot publish to reserved topics"));
      }
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
            yield* deleteByIdWithTransportUntraced(topic, id, transport);
          })();

    const queryRuntime = Effect.fn("view-server.runtime.query")(function* (
      topic: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      yield* authorizeQuery(topic, "query", query);
      const worker = yield* workerFor(topic);
      const response = yield* worker.query(query);
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
      yield* authorizeQuery(topic, "subscribe", query);
      const worker = yield* workerFor(topic);
      return worker.subscribe(requestId, query).pipe(
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
      yield* Effect.forEach(workers.values(), (worker) => worker.shutdown, { discard: true });
    });

    const recordKafkaMetrics = Effect.fnUntraced(function* (
      topic: string,
      metrics: KafkaBatchMetrics,
    ) {
      kafkaMetricsByTopic.set(topic, kafkaRuntimeMetrics(metrics));
      yield* syncHealthTopic();
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
    yield* startTopicSources(normalized, options, {
      publish: (topic, row) => publishWithTransport(topic, row, "internal"),
      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "internal"),
      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "internal"),
      recordKafkaMetrics,
    });

    return runtime;
  })();
}

function resolveSnapshotBackend(
  topic: string,
  topicConfig: TopicConfig,
  options: ViewServerRuntimeOptions,
): Effect.Effect<SnapshotBackend, ViewServerError> {
  if (options.useMemorySnapshotBackend === true) {
    return Effect.succeed(createMemorySnapshotBackend());
  }
  const injected = options.snapshotBackends?.[topic];
  if (injected !== undefined) {
    return Effect.succeed(injected);
  }
  if (options.snapshotBackendFactory !== undefined) {
    return Effect.succeed(options.snapshotBackendFactory(topic, topicConfig));
  }
  if (topicConfig.snapshot?.backend === "chdb") {
    return Effect.fail(
      snapshotBackendFailed(
        topic,
        new Error("snapshot.backend chdb requires a snapshotBackendFactory runtime option"),
      ),
    );
  }
  return Effect.succeed(createMemorySnapshotBackend());
}

function shouldResolveSnapshotBackend(options: ViewServerRuntimeOptions): boolean {
  return (
    options.topicWorkerFactory === undefined ||
    options.useMemorySnapshotBackend === true ||
    options.snapshotBackends !== undefined ||
    options.snapshotBackendFactory !== undefined
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
  field: "rows" | "subscribers" | "queueDepth" | "kafkaLagTotal" | "kafkaPartitions",
): number {
  return entries.reduce((sum, [, metrics]) => sum + metrics[field], 0);
}

function maxTopicMetric(
  entries: readonly (readonly [string, HealthResponse["topics"][string]])[],
  field: "kafkaLagMax" | "lastKafkaOffset" | "lastKafkaEndOffset",
): number {
  return entries.reduce((max, [, metrics]) => Math.max(max, metrics[field]), 0);
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
  },
): Effect.Effect<void, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.runtime.sources.start")(function* () {
    const entries = Object.entries(config.topics);
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
            yield* topicConfig.source
              .run(context)
              .pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid);
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
          yield* runKafkaSource({
            viewTopic: topic,
            idField: topicConfig.id,
            source,
            consumer,
            runtime: context,
            onBatchMetrics: (metrics) => runtime.recordKafkaMetrics(topic, metrics),
          }).pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid);
        }).pipe(Effect.withSpan("view-server.runtime.source.start")),
      { discard: true },
    );
  })();
}
