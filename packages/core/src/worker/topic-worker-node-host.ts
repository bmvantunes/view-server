import * as NodeWorker from "@effect/platform-node/NodeWorker";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Worker as NodeThreadWorker, type WorkerOptions } from "node:worker_threads";
import {
  invalidPublish,
  isViewServerError,
  type ViewServerError,
  workerUnavailable,
} from "../errors.ts";
import type { RuntimeQuery } from "../protocol/index.ts";
import { fromWireRows, toWireRow, type RpcWireValue } from "../rpc/index.ts";
import {
  TopicWorkerRpcs,
  type TopicWorkerInitialMessage as TopicWorkerInitialMessageType,
} from "./topic-worker-rpcs.ts";
import type { TopicWorkerHost, TopicWorkerHostFactory } from "./topic-worker-host.ts";

export type TopicWorkerSnapshotBackendMode = "config" | "memory" | "chdb";

export type NodeThreadTopicWorkerHostFactoryOptions = {
  readonly configModuleUrl: string | URL;
  readonly workerEntryUrl?: string | URL | undefined;
  readonly snapshotBackend?: TopicWorkerSnapshotBackendMode | undefined;
  readonly rpcConcurrency?: number | undefined;
  readonly workerNamePrefix?: string | undefined;
  readonly workerOptions?: Omit<WorkerOptions, "name" | "execArgv" | "workerData"> | undefined;
  readonly execArgv?: readonly string[] | undefined;
};

type TopicWorkerRpcClient = RpcClient.RpcClient<
  import("effect/unstable/rpc/RpcGroup").Rpcs<typeof TopicWorkerRpcs>,
  RpcClientError
>;

export const makeNodeThreadTopicWorkerHostFactory = (
  options: NodeThreadTopicWorkerHostFactoryOptions,
): TopicWorkerHostFactory => {
  const configModuleUrl = toImportUrl(options.configModuleUrl);
  const snapshotBackend = options.snapshotBackend ?? "config";
  const rpcConcurrency = options.rpcConcurrency ?? 64;
  const workerNamePrefix = options.workerNamePrefix ?? "view-server-topic";
  return (topic, config, hostOptions) =>
    Effect.fn("view-server.worker.node.host.make")(function* () {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
        "view_server.rows": hostOptions.initialRows?.length ?? 0,
      });
      const workerEntryUrl = resolveWorkerEntryUrl(options.workerEntryUrl);
      const initialMessage: TopicWorkerInitialMessageType = {
        configModuleUrl,
        topic,
        ...(hostOptions.initialRows === undefined
          ? {}
          : { initialRows: hostOptions.initialRows.map(toWireRow) }),
        ...(hostOptions.maxQueueDepth === undefined
          ? {}
          : { maxQueueDepth: hostOptions.maxQueueDepth }),
        ...(hostOptions.mutationLogSize === undefined
          ? {}
          : { mutationLogSize: hostOptions.mutationLogSize }),
        ...(hostOptions.deltaCoalescing === undefined
          ? {}
          : { deltaCoalescing: hostOptions.deltaCoalescing }),
        ...(hostOptions.maxActivePlans === undefined
          ? {}
          : { maxActivePlans: hostOptions.maxActivePlans }),
        ...(hostOptions.maxActivePlanEstimatedBytes === undefined
          ? {}
          : { maxActivePlanEstimatedBytes: hostOptions.maxActivePlanEstimatedBytes }),
        ...(hostOptions.activePlanBuildConcurrency === undefined
          ? {}
          : { activePlanBuildConcurrency: hostOptions.activePlanBuildConcurrency }),
        ...(hostOptions.groupedRefreshDebounceMs === undefined
          ? {}
          : { groupedRefreshDebounceMs: hostOptions.groupedRefreshDebounceMs }),
        snapshotBackend,
      };
      const layer = RpcClient.layerProtocolWorker({ size: 1, concurrency: rpcConcurrency }).pipe(
        Layer.provide(
          NodeWorker.layer(
            (id) =>
              new NodeThreadWorker(workerEntryUrl, {
                ...options.workerOptions,
                name: `${workerNamePrefix}-${topic}-${id}`,
                execArgv: [...(options.execArgv ?? defaultExecArgv(workerEntryUrl) ?? [])],
                workerData: initialMessage,
              }),
          ),
        ),
      );
      const context = yield* Layer.buildWithScope(layer, yield* Effect.scope).pipe(
        Effect.mapError(() => workerUnavailable(topic)),
      );
      const client = yield* RpcClient.make(TopicWorkerRpcs).pipe(Effect.provide(context));
      return topicWorkerHostFromClient(topic, config.id, client);
    })();
};

function topicWorkerHostFromClient(
  topic: string,
  idField: string,
  client: TopicWorkerRpcClient,
): TopicWorkerHost {
  return {
    topic,
    idField,
    version: client.Metrics().pipe(
      Effect.map((metrics) => BigInt(metrics.version)),
      Effect.mapError((error) => toWorkerError(topic, error)),
    ),
    metrics: client.Metrics().pipe(
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
        version: BigInt(metrics.version),
        status: metrics.status,
      })),
      Effect.mapError((error) => toWorkerError(topic, error)),
    ),
    query: (query: RuntimeQuery) =>
      Effect.fn("view-server.worker.node.query")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
        });
        const response = yield* client.Query({ query });
        yield* Effect.annotateCurrentSpan({
          "view_server.rows": response.rows.length,
          "view_server.total_rows": response.totalRows,
          "view_server.worker_version": response.version,
        });
        return {
          rows: response.rows,
          totalRows: response.totalRows,
          version: response.version,
        };
      })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
    subscribe: (requestId, query) =>
      Stream.unwrap(
        Effect.fn("view-server.worker.node.subscribe")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.request_id": requestId,
            "view_server.subscription_id": requestId,
            "view_server.topic": topic,
          });
          return client.Subscribe({ requestId, query }).pipe(
            Stream.map((event) => event),
            Stream.mapError((error) => toWorkerError(topic, error)),
          );
        })(),
      ),
    unsubscribe: (requestId) =>
      Effect.fn("view-server.worker.node.unsubscribe")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.request_id": requestId,
          "view_server.subscription_id": requestId,
          "view_server.topic": topic,
        });
        yield* client.Unsubscribe({ requestId });
      })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
    publish: (row) =>
      Effect.fnUntraced(function* () {
        const rpcRow = yield* toRpcRow(topic, row);
        yield* client.Publish({ row: rpcRow });
      })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
    deltaPublish: (patch) =>
      Effect.fnUntraced(function* () {
        yield* client.DeltaPublish({ patch });
      })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
    deleteById: (id) =>
      Effect.fnUntraced(function* () {
        yield* client.DeleteById({ id });
      })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
    getRowsForTest: client.RowsForTest().pipe(
      Effect.map(fromWireRows),
      Effect.mapError((error) => toWorkerError(topic, error)),
    ),
    shutdown: Effect.fn("view-server.worker.node.shutdown")(function* () {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      yield* client.Shutdown();
    })().pipe(Effect.mapError((error) => toWorkerError(topic, error))),
  };
}

function resolveWorkerEntryUrl(workerEntryUrl: string | URL | undefined): string | URL {
  if (workerEntryUrl !== undefined) {
    return toWorkerSpecifier(workerEntryUrl);
  }
  const currentUrl = new URL(import.meta.url);
  if (currentUrl.pathname.endsWith("/src/worker/topic-worker-node-host.ts")) {
    return new URL("./topic-worker-node-entry.ts", import.meta.url);
  }
  return new URL("./worker/topic-worker-node-entry.mjs", import.meta.url);
}

function toImportUrl(value: string | URL): string {
  if (value instanceof URL) {
    return value.href;
  }
  if (hasUrlScheme(value)) {
    return value;
  }
  return pathToFileURL(isAbsolute(value) ? value : resolve(value)).href;
}

function toWorkerSpecifier(value: string | URL): string | URL {
  if (value instanceof URL) {
    return value;
  }
  if (hasUrlScheme(value)) {
    return new URL(value);
  }
  if (value.endsWith(".ts") || value.endsWith(".mjs") || value.endsWith(".js")) {
    return pathToFileURL(isAbsolute(value) ? value : resolve(value));
  }
  return value;
}

function defaultExecArgv(workerEntryUrl: string | URL): readonly string[] | undefined {
  const value = workerEntryUrl instanceof URL ? workerEntryUrl.pathname : workerEntryUrl;
  return value.endsWith(".ts") ? ["--experimental-strip-types"] : undefined;
}

function toWorkerError(topic: string, error: ViewServerError | RpcClientError): ViewServerError {
  return isViewServerError(error) ? error : workerUnavailable(topic);
}

function hasUrlScheme(value: string): boolean {
  return /^[a-z][a-z\d+.-]*:/i.test(value);
}

function toRpcRow(
  topic: string,
  row: unknown,
): Effect.Effect<Readonly<Record<string, RpcWireValue>>, ViewServerError> {
  if (row !== null && typeof row === "object" && !Array.isArray(row)) {
    return Effect.succeed(toWireRow(row));
  }
  return Effect.fail(invalidPublish(topic, "Worker publish requires an object row"));
}
