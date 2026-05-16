import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { workerData } from "node:worker_threads";
import {
  type NormalizedViewServerConfig,
  normalizeConfig,
  readViewServerConfigExport,
  type TopicConfig,
  type ViewServerConfig,
} from "../config/index.ts";
import { missingTopic, schemaDecodeFailed, type ViewServerError } from "../errors.ts";
import { toWireRow, wireQueryResponse, wireSubscriptionEvent } from "../rpc/index.ts";
import { createChdbSnapshotBackend } from "../snapshot/chdb-backend.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../snapshot/index.ts";
import { makeTopicWorkerCore } from "./topic-worker-core.ts";
import {
  TopicWorkerInitialMessage,
  TopicWorkerRpcs,
  type TopicWorkerInitialMessage as TopicWorkerInitialMessageType,
} from "./topic-worker-rpcs.ts";

const TopicWorkerHandlersLive = TopicWorkerRpcs.toLayer(
  Effect.fn("view-server.worker.node.entry.handlers.make")(function* () {
    const initialMessage = yield* Schema.decodeUnknownEffect(TopicWorkerInitialMessage)(
      workerData,
    ).pipe(Effect.mapError((error) => schemaDecodeFailed("__worker_initial_message", error)));
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": initialMessage.topic,
      "view_server.rows": initialMessage.initialRows?.length ?? 0,
    });
    const config = yield* loadConfig(initialMessage.configModuleUrl);
    const topicConfig = config.topics[initialMessage.topic];
    if (topicConfig === undefined) {
      return yield* Effect.fail(missingTopic(initialMessage.topic));
    }
    const worker = yield* makeTopicWorkerCore(initialMessage.topic, topicConfig, {
      initialRows: initialMessage.initialRows,
      maxQueueDepth: initialMessage.maxQueueDepth,
      mutationLogSize: initialMessage.mutationLogSize,
      deltaCoalescing: initialMessage.deltaCoalescing,
      maxActivePlans: initialMessage.maxActivePlans,
      maxActivePlanEstimatedBytes: initialMessage.maxActivePlanEstimatedBytes,
      activePlanAutoBuildMaxRows: initialMessage.activePlanAutoBuildMaxRows,
      activePlanBuildConcurrency: initialMessage.activePlanBuildConcurrency,
      groupedRefreshDebounceMs: initialMessage.groupedRefreshDebounceMs,
      snapshotBackend: makeSnapshotBackend(initialMessage, topicConfig),
    });

    return TopicWorkerRpcs.of({
      Subscribe: (payload) =>
        Effect.fn("view-server.worker.node.entry.subscribe")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": payload.requestId,
            "view_server.subscription_id": payload.requestId,
            "view_server.topic": initialMessage.topic,
          });
          return yield* worker
            .subscribe(payload.requestId, payload.query)
            .pipe(Stream.map(wireSubscriptionEvent))
            .pipe(Stream.toQueue({ capacity: 64 }));
        })(),
      Unsubscribe: (payload) =>
        Effect.fn("view-server.worker.node.entry.unsubscribe")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": payload.requestId,
            "view_server.subscription_id": payload.requestId,
            "view_server.topic": initialMessage.topic,
          });
          yield* worker.unsubscribe(payload.requestId);
        })(),
      Query: (payload) =>
        Effect.fn("view-server.worker.node.entry.query")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": initialMessage.topic,
          });
          const response = yield* worker.query(payload.query);
          yield* Effect.annotateCurrentSpan({
            "view_server.rows": response.rows.length,
            "view_server.total_rows": response.totalRows,
            "view_server.worker_version": response.version,
          });
          return wireQueryResponse(response);
        })(),
      Publish: (payload) =>
        Effect.fnUntraced(function* () {
          yield* worker.publish(payload.row);
        })(),
      DeltaPublish: (payload) =>
        Effect.fnUntraced(function* () {
          yield* worker.deltaPublish(payload.patch);
        })(),
      DeleteById: (payload) =>
        Effect.fnUntraced(function* () {
          yield* worker.deleteById(payload.id);
        })(),
      RowsForTest: () => worker.getRowsForTest.pipe(Effect.map((rows) => rows.map(toWireRow))),
      Metrics: () =>
        worker.metrics.pipe(
          Effect.map((metrics) => ({
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
            status: metrics.status,
          })),
        ),
      Shutdown: () =>
        Effect.fn("view-server.worker.node.entry.shutdown")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": initialMessage.topic,
          });
          yield* worker.shutdown;
        })(),
    });
  })(),
);

const MainLive = RpcServer.layer(TopicWorkerRpcs).pipe(
  Layer.provide(TopicWorkerHandlersLive),
  Layer.provide(RpcServer.layerProtocolWorkerRunner),
  Layer.provide(NodeWorkerRunner.layer),
);

Effect.runFork(
  Layer.launch(MainLive).pipe(
    Effect.catchCause((cause) =>
      Effect.sync(() => {
        Effect.runFork(Effect.logError(Cause.pretty(cause)));
        process.exitCode = 1;
      }),
    ),
  ),
);

function makeSnapshotBackend(
  initialMessage: TopicWorkerInitialMessageType,
  _topicConfig: TopicConfig,
): SnapshotBackend {
  const mode = initialMessage.snapshotBackend ?? "chdb";
  if (mode === "memory") {
    return createMemorySnapshotBackend();
  }
  return createChdbSnapshotBackend();
}

function loadConfig(
  configModuleUrl: string,
): Effect.Effect<NormalizedViewServerConfig, ViewServerError> {
  return Effect.fn("view-server.worker.node.entry.load_config")(function* () {
    const moduleValue = yield* Effect.tryPromise({
      try: () => importConfigModule(configModuleUrl),
      catch: (error) => schemaDecodeFailed("__config", error),
    });
    const config = yield* Effect.try({
      try: () => readConfigExport(moduleValue),
      catch: (error) => schemaDecodeFailed("__config", error),
    });
    return yield* Effect.try({
      try: () => normalizeConfig(config),
      catch: (error) => schemaDecodeFailed("__config", error),
    });
  })();
}

async function importConfigModule(configModuleUrl: string): Promise<unknown> {
  return import(configModuleUrl);
}

function readConfigExport(moduleValue: unknown): ViewServerConfig {
  return readViewServerConfigExport(moduleValue);
}
