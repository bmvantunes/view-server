import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import * as Cause from "effect/Cause";
import { Effect, Layer, Schema, Stream } from "effect";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { workerData } from "node:worker_threads";
import {
  type NormalizedViewServerConfig,
  normalizeConfig,
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
  Effect.gen(function* () {
    const initialMessage = yield* Schema.decodeUnknownEffect(TopicWorkerInitialMessage)(
      workerData,
    ).pipe(Effect.mapError((error) => schemaDecodeFailed("__worker_initial_message", error)));
    const config = yield* loadConfig(initialMessage.configModuleUrl);
    const topicConfig = config.topics[initialMessage.topic];
    if (topicConfig === undefined) {
      return yield* Effect.fail(missingTopic(initialMessage.topic));
    }
    const worker = yield* makeTopicWorkerCore(initialMessage.topic, topicConfig, {
      initialRows: initialMessage.initialRows,
      mutationLogSize: initialMessage.mutationLogSize,
      snapshotBackend: makeSnapshotBackend(initialMessage, topicConfig),
    });

    return TopicWorkerRpcs.of({
      Subscribe: (payload) =>
        worker
          .subscribe(payload.requestId, payload.query)
          .pipe(Stream.map(wireSubscriptionEvent))
          .pipe(Stream.toQueue({ capacity: 64 })),
      Unsubscribe: (payload) => worker.unsubscribe(payload.requestId),
      Query: (payload) => worker.query(payload.query).pipe(Effect.map(wireQueryResponse)),
      Publish: (payload) => worker.publish(payload.row),
      DeltaPublish: (payload) => worker.deltaPublish(payload.patch),
      DeleteById: (payload) => worker.deleteById(payload.id),
      RowsForTest: () => worker.getRowsForTest.pipe(Effect.map((rows) => rows.map(toWireRow))),
      Metrics: () =>
        worker.metrics.pipe(
          Effect.map((metrics) => ({
            rows: metrics.rows,
            subscribers: metrics.subscribers,
            queueDepth: metrics.queueDepth,
            version: metrics.version.toString(),
            status: metrics.status,
          })),
        ),
      Shutdown: () => worker.shutdown,
    });
  }),
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
  topicConfig: TopicConfig,
): SnapshotBackend {
  const mode = initialMessage.snapshotBackend ?? "config";
  if (mode === "memory") {
    return createMemorySnapshotBackend();
  }
  if (mode === "chdb" || topicConfig.snapshot?.backend === "chdb") {
    return createChdbSnapshotBackend();
  }
  return createMemorySnapshotBackend();
}

function loadConfig(
  configModuleUrl: string,
): Effect.Effect<NormalizedViewServerConfig, ViewServerError> {
  return Effect.gen(function* () {
    const moduleValue = yield* Effect.tryPromise({
      try: () => import(configModuleUrl) as Promise<unknown>,
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
  });
}

function readConfigExport(moduleValue: unknown): ViewServerConfig {
  if (!isRecord(moduleValue)) {
    throw new Error("Config module did not resolve to an object");
  }
  const config = moduleValue.default ?? moduleValue.config ?? moduleValue.viewServerConfig;
  if (!isRecord(config) || !isRecord(config.topics)) {
    throw new Error("Config module must export a defineConfig result");
  }
  return config as ViewServerConfig;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
