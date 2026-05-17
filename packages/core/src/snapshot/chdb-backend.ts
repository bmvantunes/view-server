import * as Effect from "effect/Effect";
import type { TopicConfig } from "../config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import { isStableKey } from "../protocol/stable-key.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
} from "./snapshot-backend.ts";
import { ChdbProcessClient } from "./chdb-process-client.ts";
import type { ChdbQueryWorkerRequest } from "./chdb-worker-protocol.ts";
import {
  decodeSnapshotBackendResult,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeVersionedRow,
} from "./row-wire-codec.ts";

const WORKER_INIT_CHUNK_SIZE = 25_000;

export type ChdbSnapshotBackendOptions = {
  readonly groupedRefreshWorkerEntryUrl?: string | URL | undefined;
  readonly restartWorkerOnUnexpectedExit?: boolean | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerSpawn?: ((pid: number | undefined) => void) | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerExit?:
    | ((event: { readonly pid: number | undefined; readonly message: string }) => void)
    | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerRequest?:
    | ((request: {
        readonly pid: number | undefined;
        readonly type: ChdbQueryWorkerRequest["type"];
      }) => void)
    | undefined;
};

export function createChdbSnapshotBackend(
  options: ChdbSnapshotBackendOptions = {},
): SnapshotBackend {
  return new WorkerChdbSnapshotBackend(options);
}

export function createChdbSnapshotBackendFactory(): (
  topic: string,
  config: TopicConfig,
) => SnapshotBackend {
  return () => createChdbSnapshotBackend();
}

class ChdbGroupedSnapshotWorkerClient {
  readonly #process: ChdbProcessClient;
  #topic = "chdb";

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#process = new ChdbProcessClient({
      workerEntryUrl: options.groupedRefreshWorkerEntryUrl,
      onWorkerExit: options.onWorkerExit,
      onWorkerRequest: options.onWorkerRequest,
      onWorkerSpawn: options.onWorkerSpawn,
    });
  }

  get pid(): number | undefined {
    return this.#process.pid;
  }

  get pendingRequests(): number {
    return this.#process.pendingRequests;
  }

  get health(): SnapshotBackendHealth {
    return this.#process.health;
  }

  get exitedUnexpectedly(): boolean {
    return this.#process.exitedUnexpectedly;
  }

  kill(signal: NodeJS.Signals = "SIGTERM"): void {
    this.#process.kill(signal);
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    this.#topic = args.topic;
    this.#process.setTopic(args.topic);
    if (args.rows.length > WORKER_INIT_CHUNK_SIZE) {
      return this.#chunkedInit(args);
    }
    return this.#process
      .request({
        id: this.#process.nextRequestId(),
        type: "init",
        args: {
          ...args,
          rows: args.rows.map(encodeVersionedRow),
        },
      })
      .pipe(Effect.asVoid);
  }

  applyBatch(
    args: Parameters<SnapshotBackend["applyBatch"]>[0],
  ): Effect.Effect<void, ViewServerError> {
    return this.#process
      .request({
        id: this.#process.nextRequestId(),
        type: "applyBatch",
        args: {
          mutations: args.mutations.map(encodeMutationLogEntry),
          highestVersion: args.highestVersion,
        },
      })
      .pipe(Effect.asVoid);
  }

  snapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return this.#process
      .request({
        id: this.#process.nextRequestId(),
        type: "snapshot",
        args: {
          query: encodeRuntimeQuery(args.query),
          targetVersion: args.targetVersion,
        },
      })
      .pipe(
        Effect.flatMap((response) =>
          response.result === undefined
            ? Effect.fail(snapshotBackendFailed(this.#topic, "chDB worker returned no snapshot"))
            : Effect.succeed(decodeSnapshotBackendResult(response.result)),
        ),
      );
  }

  groupedRefreshSnapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return this.#process
      .request({
        id: this.#process.nextRequestId(),
        type: "groupedRefreshSnapshot",
        args: {
          query: encodeRuntimeQuery(args.query),
          targetVersion: args.targetVersion,
        },
      })
      .pipe(
        Effect.flatMap((response) =>
          response.result === undefined
            ? Effect.fail(
                snapshotBackendFailed(this.#topic, "chDB worker returned no grouped snapshot"),
              )
            : Effect.succeed(decodeSnapshotBackendResult(response.result)),
        ),
      );
  }

  close(): Effect.Effect<void, ViewServerError> {
    return this.#process.shutdown();
  }

  #chunkedInit(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (worker: ChdbGroupedSnapshotWorkerClient) {
      yield* worker.#process.request({
        id: worker.#process.nextRequestId(),
        type: "initStart",
        args: {
          topic: args.topic,
          idField: args.idField,
          version: args.version,
          ...(args.literalStringFields === undefined
            ? {}
            : { literalStringFields: args.literalStringFields }),
        },
      });
      for (let offset = 0; offset < args.rows.length; offset += WORKER_INIT_CHUNK_SIZE) {
        yield* worker.#process.request({
          id: worker.#process.nextRequestId(),
          type: "initRows",
          rows: args.rows.slice(offset, offset + WORKER_INIT_CHUNK_SIZE).map(encodeVersionedRow),
        });
      }
      yield* worker.#process.request({
        id: worker.#process.nextRequestId(),
        type: "initCommit",
      });
    })(this);
  }
}

class WorkerChdbSnapshotBackend implements SnapshotBackend {
  readonly #options: ChdbSnapshotBackendOptions;
  #worker: ChdbGroupedSnapshotWorkerClient;
  #topic = "chdb";
  #idField = "id";
  #literalStringFields: ReadonlySet<string> | undefined;
  #mirrorRows = new Map<string | number, RuntimeRow>();
  #mirrorPendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #mirrorVersion: WorkerVersion = 0n;
  #knownBackendVersion: WorkerVersion = 0n;
  #restartCount = 0;
  #restarting = false;
  #lastError = "";
  #closed = false;

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#options = options;
    this.#worker = this.#makeWorker();
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return true;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => {
      const workerHealth = this.#worker.health;
      return {
        status: this.#restarting ? "restarting" : workerHealth.status,
        message: workerHealth.message,
        pid: workerHealth.pid ?? 0,
        restarts: this.#restartCount,
        pendingRequests: workerHealth.pendingRequests ?? this.#worker.pendingRequests,
        lastError: workerHealth.lastError ?? workerHealth.message ?? this.#lastError,
        backendVersion: this.#knownBackendVersion,
      };
    });
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.init")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      backend.#topic = args.topic;
      backend.#idField = args.idField;
      backend.#literalStringFields = args.literalStringFields;
      backend.#mirrorRows = new Map();
      backend.#mirrorPendingByVersion = new Map();
      backend.#mirrorVersion = args.version;
      backend.#knownBackendVersion = args.version;
      backend.#lastError = "";
      for (const entry of args.rows) {
        const id = entry.row[args.idField];
        if (isStableKey(id)) {
          backend.#mirrorRows.set(id, { ...entry.row });
        }
      }
      yield* backend.#worker.init(args);
    })(this);
  }

  applyBatch(
    args: Parameters<SnapshotBackend["applyBatch"]>[0],
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (backend: WorkerChdbSnapshotBackend) {
      backend.#applyBatchToMirror(args);
      const worker = yield* backend.#ensureWorker();
      yield* worker.applyBatch(args).pipe(
        Effect.tapError((error) =>
          Effect.sync(() => {
            backend.#lastError = error.message;
          }),
        ),
      );
    })(this);
  }

  snapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.snapshot")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      const worker = yield* backend.#ensureWorker();
      const result = yield* worker.snapshot(args).pipe(
        Effect.tapError((error) =>
          Effect.sync(() => {
            backend.#lastError = error.message;
          }),
        ),
      );
      backend.#knownBackendVersion = result.backendVersion;
      return result;
    })(this);
  }

  groupedRefreshSnapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.grouped_refresh_snapshot")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      const worker = yield* backend.#ensureWorker();
      const result = yield* worker.groupedRefreshSnapshot(args).pipe(
        Effect.tapError((error) =>
          Effect.sync(() => {
            backend.#lastError = error.message;
          }),
        ),
      );
      backend.#knownBackendVersion = result.backendVersion;
      return result;
    })(this);
  }

  close(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.worker_backend.close")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      backend.#closed = true;
      yield* backend.#worker.close().pipe(Effect.ignore);
    })(this);
  }

  #makeWorker(): ChdbGroupedSnapshotWorkerClient {
    return new ChdbGroupedSnapshotWorkerClient(this.#options);
  }

  #ensureWorker(): Effect.Effect<ChdbGroupedSnapshotWorkerClient, ViewServerError> {
    if (
      this.#closed ||
      !this.#worker.exitedUnexpectedly ||
      this.#options.restartWorkerOnUnexpectedExit !== true
    ) {
      return this.#worker.exitedUnexpectedly && !this.#closed
        ? Effect.sync(() => {
            this.#lastError = this.#worker.health.message ?? "chDB worker exited";
          }).pipe(
            Effect.flatMap(() => Effect.fail(snapshotBackendFailed(this.#topic, this.#lastError))),
          )
        : Effect.succeed(this.#worker);
    }
    return Effect.fn("view-server.chdb.worker_backend.restart")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      backend.#lastError = backend.#worker.health.message ?? "chDB worker exited";
      backend.#restarting = true;
      yield* Effect.logWarning(`view-server chDB worker restarting topic=${backend.#topic}`);
      const nextWorker = backend.#makeWorker();
      backend.#worker = nextWorker;
      backend.#restartCount++;
      yield* nextWorker
        .init({
          topic: backend.#topic,
          idField: backend.#idField,
          rows: Array.from(backend.#mirrorRows.values(), (row) => ({
            row,
            version: backend.#mirrorVersion,
          })),
          version: backend.#mirrorVersion,
          ...(backend.#literalStringFields === undefined
            ? {}
            : { literalStringFields: backend.#literalStringFields }),
        })
        .pipe(
          Effect.tapError((error) =>
            Effect.sync(() => {
              backend.#lastError = error.message;
            }),
          ),
        );
      backend.#knownBackendVersion = backend.#mirrorVersion;
      return nextWorker;
    })(this).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.#restarting = false;
        }),
      ),
    );
  }

  #applyBatchToMirror(args: Parameters<SnapshotBackend["applyBatch"]>[0]): void {
    for (const mutation of args.mutations) {
      if (mutation.version > this.#mirrorVersion) {
        this.#mirrorPendingByVersion.set(mutation.version, mutation);
      }
    }
    let nextVersion = this.#mirrorVersion + 1n;
    while (true) {
      const mutation = this.#mirrorPendingByVersion.get(nextVersion);
      if (mutation === undefined) {
        return;
      }
      this.#mirrorPendingByVersion.delete(nextVersion);
      this.#applyMutationToMirror(mutation);
      this.#mirrorVersion = mutation.version;
      nextVersion = this.#mirrorVersion + 1n;
    }
  }

  #applyMutationToMirror(mutation: MutationLogEntry): void {
    if (mutation.kind === "delete") {
      this.#mirrorRows.delete(mutation.id);
      return;
    }
    if (mutation.after === undefined) {
      return;
    }
    const id = mutation.after[this.#idField];
    if (isStableKey(id)) {
      this.#mirrorRows.set(id, { ...mutation.after });
    }
  }
}
