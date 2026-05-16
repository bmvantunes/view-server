import { Session } from "chdb";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import type { TopicConfig } from "../config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type {
  RuntimeAggregateDefinition,
  RuntimeGroupedQuery,
  RuntimeQuery,
  RuntimeRow,
} from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
  VersionedRow,
} from "./snapshot-backend.ts";
import { fork, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import type {
  ChdbQueryWorkerRequest,
  ChdbQueryWorkerResponse,
  ChdbQueryWorkerSuccessResponse,
} from "./chdb-query-worker-protocol.ts";
import {
  decodeSnapshotBackendResult,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeVersionedRow,
} from "./chdb-query-worker-codec.ts";

const BIG_DECIMAL_SCALE = 38;
type BigDecimalColumnType = "Decimal(76, 38)";
const BIG_DECIMAL_COLUMN_TYPE: BigDecimalColumnType = "Decimal(76, 38)";

type ColumnType = "String" | "Float64" | "Int64" | "UInt8" | BigDecimalColumnType;

type Column = {
  readonly name: string;
  readonly type: ColumnType;
};

const DELETED_COLUMN = "__view_server_deleted";
const VERSION_COLUMN = "__view_server_version";
const JSON_FORMAT_SETTINGS =
  "SETTINGS output_format_json_quote_decimals=1, output_format_json_quote_64bit_integers=1";
const WORKER_INIT_CHUNK_SIZE = 25_000;

let sharedSession: Session | undefined;
let sharedSessionReferences = 0;
let nextBackendTableId = 0;

export type ChdbSnapshotBackendOptions = {
  readonly groupedRefreshWorker?: boolean | undefined;
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
  return options.groupedRefreshWorker === false
    ? new ChdbSnapshotBackend(options)
    : new WorkerChdbSnapshotBackend(options);
}

export function createChdbSnapshotBackendFactory(): (
  topic: string,
  config: TopicConfig,
) => SnapshotBackend {
  return () => createChdbSnapshotBackend();
}

type ChdbWorkerPending = {
  readonly resolve: (response: ChdbQueryWorkerResponse) => void;
  readonly reject: (error: unknown) => void;
};

type ChdbWorkerClientOptions = Pick<
  ChdbSnapshotBackendOptions,
  "groupedRefreshWorkerEntryUrl" | "onWorkerExit" | "onWorkerRequest" | "onWorkerSpawn"
>;

class ChdbGroupedSnapshotWorkerClient {
  readonly #worker: ChildProcess;
  readonly #onWorkerExit: ChdbWorkerClientOptions["onWorkerExit"];
  readonly #onWorkerRequest: ChdbWorkerClientOptions["onWorkerRequest"];
  #topic = "chdb";
  #nextId = 0;
  #pending = new Map<number, ChdbWorkerPending>();
  #closed = false;
  #exitMessage: string | undefined;

  constructor(options: ChdbWorkerClientOptions = {}) {
    const entryUrl = resolveChdbQueryWorkerEntryUrl(options.groupedRefreshWorkerEntryUrl);
    this.#onWorkerExit = options.onWorkerExit;
    this.#onWorkerRequest = options.onWorkerRequest;
    this.#worker = fork(fileURLToPath(entryUrl), [], {
      execArgv: defaultExecArgv(entryUrl),
      serialization: "advanced",
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    options.onWorkerSpawn?.(this.#worker.pid);
    this.#worker.on("message", (response: unknown) => {
      if (isChdbQueryWorkerResponse(response)) {
        this.#handleResponse(response);
        return;
      }
      this.#failPending(new Error("chDB worker returned an invalid response"));
    });
    this.#worker.on("error", (error) => {
      this.#failPending(error);
    });
    this.#worker.on("exit", (code, signal) => {
      const message =
        signal === null
          ? `chDB worker exited with code ${String(code)}`
          : `chDB worker exited from signal ${signal}`;
      if (!this.#closed) {
        this.#exitMessage = message;
        this.#onWorkerExit?.({ pid: this.#worker.pid, message });
      }
      this.#failPending(new Error(message));
    });
  }

  get pid(): number | undefined {
    return this.#worker.pid;
  }

  get health(): SnapshotBackendHealth {
    return this.#exitMessage === undefined
      ? { status: "ready" }
      : { status: "degraded", message: this.#exitMessage };
  }

  get exitedUnexpectedly(): boolean {
    return this.#exitMessage !== undefined;
  }

  kill(signal: NodeJS.Signals = "SIGTERM"): void {
    this.#worker.kill(signal);
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    this.#topic = args.topic;
    if (args.rows.length > WORKER_INIT_CHUNK_SIZE) {
      return this.#chunkedInit(args);
    }
    return this.#request({
      id: this.#requestId(),
      type: "init",
      args: {
        ...args,
        rows: args.rows.map(encodeVersionedRow),
      },
    }).pipe(Effect.asVoid);
  }

  applyBatch(
    args: Parameters<SnapshotBackend["applyBatch"]>[0],
  ): Effect.Effect<void, ViewServerError> {
    return this.#request({
      id: this.#requestId(),
      type: "applyBatch",
      args: {
        mutations: args.mutations.map(encodeMutationLogEntry),
        highestVersion: args.highestVersion,
      },
    }).pipe(Effect.asVoid);
  }

  snapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return this.#request({
      id: this.#requestId(),
      type: "snapshot",
      args: {
        query: encodeRuntimeQuery(args.query),
        targetVersion: args.targetVersion,
      },
    }).pipe(
      Effect.flatMap((response) =>
        response.result === undefined
          ? Effect.fail(snapshotBackendFailed(this.#topic, "chDB worker returned no snapshot"))
          : Effect.succeed(decodeSnapshotBackendResult(response.result)),
      ),
    );
  }

  close(): Effect.Effect<void, ViewServerError> {
    if (this.#closed) {
      return this.#terminate();
    }
    this.#closed = true;
    if (this.#worker.exitCode !== null || this.#worker.killed || !this.#worker.connected) {
      return this.#terminate();
    }
    return this.#requestClose(250).pipe(
      Effect.flatMap(() => this.#terminate()),
      Effect.asVoid,
    );
  }

  #chunkedInit(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (worker: ChdbGroupedSnapshotWorkerClient) {
      yield* worker.#request({
        id: worker.#requestId(),
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
        yield* worker.#request({
          id: worker.#requestId(),
          type: "initRows",
          rows: args.rows.slice(offset, offset + WORKER_INIT_CHUNK_SIZE).map(encodeVersionedRow),
        });
      }
      yield* worker.#request({
        id: worker.#requestId(),
        type: "initCommit",
      });
    })(this);
  }

  #request(
    request: ChdbQueryWorkerRequest,
  ): Effect.Effect<ChdbQueryWorkerSuccessResponse, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<ChdbQueryWorkerResponse>((resolve, reject) => {
          if (this.#exitMessage !== undefined) {
            reject(new Error(this.#exitMessage));
            return;
          }
          if (this.#worker.exitCode !== null || this.#worker.killed || !this.#worker.connected) {
            reject(new Error("chDB worker IPC channel is closed"));
            return;
          }
          this.#pending.set(request.id, { resolve, reject });
          let sent = false;
          try {
            sent = this.#worker.send(request, (error) => {
              if (error === null) {
                return;
              }
              const pending = this.#pending.get(request.id);
              if (pending === undefined) {
                return;
              }
              this.#pending.delete(request.id);
              pending.reject(error);
            });
          } catch (error) {
            this.#pending.delete(request.id);
            reject(error);
            return;
          }
          if (!sent) {
            this.#pending.delete(request.id);
            reject(new Error("chDB worker IPC channel is not writable"));
            return;
          }
          this.#onWorkerRequest?.({ pid: this.#worker.pid, type: request.type });
        }).then((response) => {
          if (response.success) {
            return response;
          }
          throw new Error(response.error);
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
  }

  #requestId(): number {
    const id = this.#nextId;
    this.#nextId++;
    return id;
  }

  #requestClose(deadlineMs: number): Effect.Effect<void, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve) => {
          const id = this.#requestId();
          let settled = false;
          let timer: ReturnType<typeof setTimeout> | undefined;
          const settle = () => {
            if (settled) {
              return;
            }
            settled = true;
            if (timer !== undefined) {
              clearTimeout(timer);
            }
            this.#pending.delete(id);
            resolve();
          };
          timer = setTimeout(settle, deadlineMs);
          this.#pending.set(id, {
            resolve: settle,
            reject: settle,
          });
          try {
            const sent = this.#worker.send({ id, type: "close" });
            if (!sent) {
              settle();
            }
          } catch {
            settle();
          }
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
  }

  #handleResponse(response: ChdbQueryWorkerResponse): void {
    const pending = this.#pending.get(response.id);
    if (pending === undefined) {
      return;
    }
    this.#pending.delete(response.id);
    pending.resolve(response);
  }

  #failPending(error: unknown): void {
    for (const [id, pending] of this.#pending) {
      this.#pending.delete(id);
      pending.reject(error);
    }
  }

  #terminate(): Effect.Effect<void, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve) => {
          if (this.#worker.exitCode !== null || this.#worker.killed) {
            resolve();
            return;
          }
          let settled = false;
          let forceKillTimer: ReturnType<typeof setTimeout> | undefined;
          let giveUpTimer: ReturnType<typeof setTimeout> | undefined;
          const settle = () => {
            if (settled) {
              return;
            }
            settled = true;
            if (forceKillTimer !== undefined) {
              clearTimeout(forceKillTimer);
            }
            if (giveUpTimer !== undefined) {
              clearTimeout(giveUpTimer);
            }
            resolve();
          };
          this.#worker.once("exit", () => {
            settle();
          });
          forceKillTimer = setTimeout(() => {
            this.#worker.kill("SIGKILL");
          }, 250);
          giveUpTimer = setTimeout(settle, 2_000);
          this.#worker.kill("SIGTERM");
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
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
  #closed = false;

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#options = options;
    this.#worker = this.#makeWorker();
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return true;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => this.#worker.health);
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
      for (const entry of args.rows) {
        const id = entry.row[args.idField];
        if (typeof id === "string" || typeof id === "number") {
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
      yield* worker.applyBatch(args);
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
      return yield* worker.snapshot(args);
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
      return yield* worker.snapshot(args);
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
        ? Effect.fail(
            snapshotBackendFailed(this.#topic, this.#worker.health.message ?? "chDB worker exited"),
          )
        : Effect.succeed(this.#worker);
    }
    return Effect.fn("view-server.chdb.worker_backend.restart")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      yield* Effect.logWarning(`view-server chDB worker restarting topic=${backend.#topic}`);
      const nextWorker = backend.#makeWorker();
      backend.#worker = nextWorker;
      yield* nextWorker.init({
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
      });
      return nextWorker;
    })(this);
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
    if (typeof id === "string" || typeof id === "number") {
      this.#mirrorRows.set(id, { ...mutation.after });
    }
  }
}

class ChdbSnapshotBackend implements SnapshotBackend {
  readonly #session: Session;
  readonly #tableName: string;
  #groupedRefreshWorker: ChdbGroupedSnapshotWorkerClient | undefined;
  #topic = "";
  #idField = "id";
  #columns: readonly Column[] = [];
  #backendVersion = 0n;
  #literalStringFields: ReadonlySet<string> = new Set();
  #pendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #flushScheduled = false;
  #closed = false;
  #lastFlushError: unknown;
  #tableReady = false;
  #sessionReleased = false;

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#session = acquireSharedSession();
    this.#tableName = `topic_rows_${nextBackendTableId}`;
    nextBackendTableId++;
    if (options.groupedRefreshWorker !== false) {
      this.#groupedRefreshWorker = new ChdbGroupedSnapshotWorkerClient(options);
    }
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return this.#groupedRefreshWorker !== undefined;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => this.#groupedRefreshWorker?.health ?? { status: "ready" });
  }

  init(args: {
    readonly topic: string;
    readonly idField: string;
    readonly rows: readonly VersionedRow[];
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.init")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": args.topic,
        "view_server.rows": args.rows.length,
        "view_server.backend_version": args.version.toString(),
      });
      yield* Effect.try({
        try: () => {
          backend.#topic = args.topic;
          backend.#idField = args.idField;
          const initialRows = args.rows.map((entry) => ({ ...entry.row }));
          backend.#backendVersion = args.version;
          backend.#literalStringFields = args.literalStringFields ?? new Set();
          backend.#pendingByVersion = new Map();
          backend.#flushScheduled = false;
          backend.#closed = false;
          backend.#lastFlushError = undefined;
          backend.#tableReady = false;
          backend.#columns = inferColumns(initialRows, backend.#idField);
          backend.#session.query(`DROP TABLE IF EXISTS ${quoteIdentifier(backend.#tableName)}`);
          if (backend.#columns.length > 0) {
            backend.#createTable();
            backend.#insertEvents(
              initialRows.map((row) => ({
                row,
                deleted: false,
                version: args.version,
              })),
            );
          }
        },
        catch: (error) => snapshotBackendFailed(args.topic, error),
      });
      const groupedRefreshWorker = backend.#groupedRefreshWorker;
      if (groupedRefreshWorker !== undefined) {
        yield* groupedRefreshWorker.init(args).pipe(
          Effect.catchTag("SnapshotBackendFailed", () =>
            Effect.sync(() => {
              backend.#groupedRefreshWorker = undefined;
            }),
          ),
        );
      }
    })(this);
  }

  applyBatch(args: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.try({
        try: () => {
          if (backend.#closed) {
            return;
          }
          for (const mutation of args.mutations) {
            if (mutation.version > backend.#backendVersion) {
              backend.#pendingByVersion.set(mutation.version, mutation);
            }
          }
          backend.#scheduleFlush();
        },
        catch: (error) => snapshotBackendFailed(backend.#topic, error),
      });
      const groupedRefreshWorker = backend.#groupedRefreshWorker;
      if (groupedRefreshWorker !== undefined) {
        yield* groupedRefreshWorker.applyBatch(args).pipe(
          Effect.catchTag("SnapshotBackendFailed", () =>
            Effect.sync(() => {
              backend.#groupedRefreshWorker = undefined;
            }),
          ),
        );
      }
    })(this);
  }

  snapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.snapshot")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.worker_version": args.targetVersion.toString(),
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      yield* backend.#flushPendingEffect();
      const result = yield* Effect.try({
        try: () => {
          if (backend.#lastFlushError !== undefined) {
            throw backend.#lastFlushError;
          }
          if (!backend.#tableReady) {
            return {
              rows: [],
              totalRows: 0,
              backendVersion: backend.#backendVersion,
            };
          }
          const sql = compileQuerySql(
            args.query,
            backend.#idField,
            backend.#columns,
            backend.#literalStringFields,
            backend.#tableName,
          );
          const rows = parseJsonEachRow(
            backend.#session.query(withJsonSettings(sql.rowsSql), "JSONEachRow"),
            sql.decimalFields,
            sql.integerFields,
            sql.numberFields,
          );
          const totalRows = parseCount(
            backend.#session.query(withJsonSettings(sql.countSql), "JSONEachRow"),
          );
          return {
            rows,
            totalRows,
            backendVersion: backend.#backendVersion,
          };
        },
        catch: (error) => snapshotBackendFailed(backend.#topic, error),
      });
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": result.rows.length,
        "view_server.total_rows": result.totalRows,
        "view_server.backend_version": result.backendVersion.toString(),
      });
      return result;
    })(this);
  }

  groupedRefreshSnapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.grouped_refresh.snapshot")(function* (
      backend: ChdbSnapshotBackend,
    ) {
      const groupedRefreshWorker = backend.#groupedRefreshWorker;
      if (groupedRefreshWorker === undefined) {
        return yield* Effect.fail(
          snapshotBackendFailed(backend.#topic, "chDB grouped refresh worker unavailable"),
        );
      }
      return yield* groupedRefreshWorker.snapshot(args);
    })(this);
  }

  close(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.close")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      backend.#closed = true;
      yield* backend.#flushPendingEffect();
      const groupedRefreshWorker = backend.#groupedRefreshWorker;
      backend.#groupedRefreshWorker = undefined;
      if (groupedRefreshWorker !== undefined) {
        yield* groupedRefreshWorker.close().pipe(Effect.ignore);
      }
      yield* Effect.sync(() => {
        try {
          backend.#session.query(`DROP TABLE IF EXISTS ${quoteIdentifier(backend.#tableName)}`);
        } finally {
          backend.#releaseSession();
        }
      });
    })(this);
  }

  #scheduleFlush(): void {
    if (this.#flushScheduled) {
      return;
    }
    this.#flushScheduled = true;
    queueMicrotask(() => {
      Effect.runFork(this.#flushPendingEffect());
    });
  }

  #flushPendingEffect(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.flush")(function* (backend: ChdbSnapshotBackend) {
      backend.#flushScheduled = false;
      const mutations = backend.#contiguousPendingMutations();
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.batch_size": mutations.length,
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      if (mutations.length === 0) {
        return;
      }
      const highestVersion = mutations[mutations.length - 1]?.version;
      if (highestVersion === undefined) {
        return;
      }
      let flushedVersion: WorkerVersion | undefined;
      try {
        backend.#applyMutations(mutations);
        for (const mutation of mutations) {
          backend.#pendingByVersion.delete(mutation.version);
        }
        backend.#backendVersion = highestVersion;
        backend.#lastFlushError = undefined;
        flushedVersion = highestVersion;
      } catch (error) {
        backend.#lastFlushError = error;
      }
      if (flushedVersion !== undefined) {
        yield* Effect.annotateCurrentSpan({
          "view_server.backend_version": flushedVersion.toString(),
        });
      }
    })(this);
  }

  #contiguousPendingMutations(): readonly MutationLogEntry[] {
    const mutations: MutationLogEntry[] = [];
    let nextVersion = this.#backendVersion + 1n;
    while (true) {
      const mutation = this.#pendingByVersion.get(nextVersion);
      if (mutation === undefined) {
        return mutations;
      }
      mutations.push(mutation);
      nextVersion = nextVersion + 1n;
    }
  }

  #applyMutations(mutations: readonly MutationLogEntry[]): void {
    const events = mutations.flatMap((mutation) => mutationToEvent(mutation, this.#idField));
    if (events.length === 0) {
      return;
    }
    this.#ensureColumns(events.map((event) => event.row));
    this.#insertEvents(events);
  }

  #ensureColumns(rows: readonly RuntimeRow[]): void {
    const nextColumns = mergeColumns(this.#columns, inferColumns(rows, this.#idField));
    const addedColumns = nextColumns.filter(
      (column) => !this.#columns.some((existing) => existing.name === column.name),
    );
    if (!this.#tableReady) {
      this.#columns = nextColumns;
      this.#createTable();
      return;
    }
    for (const column of addedColumns) {
      this.#session.query(
        `ALTER TABLE ${quoteIdentifier(this.#tableName)} ADD COLUMN ${quoteIdentifier(column.name)} ${column.type}`,
      );
    }
    this.#columns = nextColumns;
  }

  #createTable(): void {
    this.#session.query(
      `CREATE TABLE ${quoteIdentifier(this.#tableName)} (${[
        ...this.#columns.map((column) => `${quoteIdentifier(column.name)} ${column.type}`),
        `${DELETED_COLUMN} UInt8`,
        `${VERSION_COLUMN} Int64`,
      ].join(", ")}) ENGINE = Memory`,
    );
    this.#tableReady = true;
  }

  #insertEvents(events: readonly ChdbEvent[]): void {
    if (events.length === 0) {
      return;
    }
    const payload = events
      .map((event) =>
        JSON.stringify({
          ...projectSerializableRow(event.row, this.#columns),
          [DELETED_COLUMN]: event.deleted ? 1 : 0,
          [VERSION_COLUMN]: event.version.toString(),
        }),
      )
      .join("\n");
    this.#session.query(
      `INSERT INTO ${quoteIdentifier(this.#tableName)} FORMAT JSONEachRow ${payload}`,
    );
  }

  #releaseSession(): void {
    if (this.#sessionReleased) {
      return;
    }
    this.#sessionReleased = true;
    releaseSharedSession(this.#session);
  }
}

function resolveChdbQueryWorkerEntryUrl(workerEntryUrl: string | URL | undefined): URL {
  if (workerEntryUrl !== undefined) {
    return toWorkerUrl(workerEntryUrl);
  }
  const currentUrl = new URL(import.meta.url);
  if (currentUrl.pathname.endsWith("/src/snapshot/chdb-backend.ts")) {
    return new URL("./chdb-query-worker-entry.ts", import.meta.url);
  }
  return new URL("./snapshot/chdb-query-worker-entry.mjs", import.meta.url);
}

function toWorkerUrl(value: string | URL): URL {
  return value instanceof URL ? value : new URL(value);
}

function defaultExecArgv(workerEntryUrl: URL): string[] {
  return workerEntryUrl.pathname.endsWith(".ts") ? ["--experimental-strip-types"] : [];
}

function acquireSharedSession(): Session {
  if (sharedSession === undefined) {
    sharedSession = new Session();
  }
  sharedSessionReferences++;
  return sharedSession;
}

function releaseSharedSession(session: Session): void {
  if (session !== sharedSession) {
    return;
  }
  sharedSessionReferences = Math.max(0, sharedSessionReferences - 1);
  if (sharedSessionReferences > 0) {
    return;
  }
  sharedSession = undefined;
  session.cleanup();
}

function isChdbQueryWorkerResponse(value: unknown): value is ChdbQueryWorkerResponse {
  if (!isReadonlyRecord(value)) {
    return false;
  }
  if (typeof value.id !== "number" || typeof value.success !== "boolean") {
    return false;
  }
  if (value.success) {
    return value.result === undefined || isReadonlyRecord(value.result);
  }
  return typeof value.error === "string";
}

function isReadonlyRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

type ChdbEvent = {
  readonly row: RuntimeRow;
  readonly deleted: boolean;
  readonly version: WorkerVersion;
};

function mutationToEvent(mutation: MutationLogEntry, idField: string): readonly ChdbEvent[] {
  if (mutation.kind === "delete") {
    return [
      {
        row: {
          ...mutation.before,
          [idField]: mutation.id,
        },
        deleted: true,
        version: mutation.version,
      },
    ];
  }
  if (mutation.after === undefined) {
    return [];
  }
  return [
    {
      row: mutation.after,
      deleted: false,
      version: mutation.version,
    },
  ];
}

function mergeColumns(
  existingColumns: readonly Column[],
  incomingColumns: readonly Column[],
): readonly Column[] {
  const columns = [...existingColumns];
  for (const column of incomingColumns) {
    if (!columns.some((existing) => existing.name === column.name)) {
      columns.push(column);
    }
  }
  return columns;
}

function inferColumns(rows: readonly RuntimeRow[], idField: string): readonly Column[] {
  const names = new Set<string>();
  for (const row of rows) {
    if (row[idField] !== undefined) {
      names.add(idField);
    }
    for (const [key, value] of Object.entries(row)) {
      if (isUserColumn(key) && isScalar(value)) {
        names.add(key);
      }
    }
  }
  return Array.from(names).map((name) => ({
    name,
    type: inferColumnType(rows.map((row) => row[name])),
  }));
}

function isUserColumn(name: string): boolean {
  return name !== DELETED_COLUMN && name !== VERSION_COLUMN;
}

function inferColumnType(values: readonly unknown[]): ColumnType {
  if (values.some((value) => typeof value === "string")) {
    return "String";
  }
  if (values.some(BigDecimal.isBigDecimal)) {
    return BIG_DECIMAL_COLUMN_TYPE;
  }
  if (values.some((value) => typeof value === "boolean")) {
    return "UInt8";
  }
  if (values.some((value) => typeof value === "number")) {
    return "Float64";
  }
  return "Int64";
}

function projectSerializableRow(row: RuntimeRow, columns: readonly Column[]): RuntimeRow {
  const projected: RuntimeRow = {};
  for (const column of columns) {
    const value = row[column.name];
    projected[column.name] = BigDecimal.isBigDecimal(value)
      ? BigDecimal.format(value)
      : typeof value === "bigint"
        ? value.toString()
        : typeof value === "boolean"
          ? value
            ? 1
            : 0
          : (value ?? defaultValue(column.type));
  }
  return projected;
}

function defaultValue(type: ColumnType): string | number {
  return type === "String" ? "" : 0;
}

function isScalar(value: unknown): boolean {
  return (
    value == null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    BigDecimal.isBigDecimal(value)
  );
}

function latestRowsSql(columns: readonly Column[], idField: string, tableName: string): string {
  const selected = columns.map((column) => quoteIdentifier(column.name)).join(", ");
  const id = quoteIdentifier(idField);
  return `SELECT ${selected} FROM (SELECT ${selected}, ${DELETED_COLUMN}, ${VERSION_COLUMN} FROM ${quoteIdentifier(tableName)} ORDER BY ${id}, ${VERSION_COLUMN} DESC LIMIT 1 BY ${id}) WHERE ${DELETED_COLUMN} = 0`;
}

function compileQuerySql(
  query: RuntimeQuery,
  idField: string,
  columns: readonly Column[],
  literalStringFields: ReadonlySet<string>,
  tableName: string,
): {
  readonly rowsSql: string;
  readonly countSql: string;
  readonly decimalFields: ReadonlySet<string>;
  readonly integerFields: ReadonlySet<string>;
  readonly numberFields: ReadonlySet<string>;
} {
  const source = latestRowsSql(columns, idField, tableName);
  if (isGroupedQuery(query)) {
    const where = query.where ? `WHERE ${compileFilter(query.where, literalStringFields)}` : "";
    const groupBy = query.groupBy.map(quoteIdentifier).join(", ");
    const select = [
      ...query.groupBy.map(quoteIdentifier),
      ...Object.entries(query.aggregates).map(
        ([alias, aggregate]) =>
          `${compileAggregate(aggregate, columns)} AS ${quoteIdentifier(alias)}`,
      ),
    ].join(", ");
    const grouped = `SELECT ${select} FROM (${source}) ${where} GROUP BY ${groupBy}`;
    return {
      rowsSql: `SELECT * FROM (${grouped}) ${compileOrderBy(query.orderBy ?? [], columns)} ${compileLimit(query)}`,
      countSql: `SELECT count() AS totalRows FROM (${grouped})`,
      decimalFields: groupedDecimalFields(query, columns),
      integerFields: groupedIntegerFields(query, columns),
      numberFields: groupedNumberFields(query),
    };
  }

  const selected = new Set(
    Object.entries(query.fields)
      .filter(([, enabled]) => enabled)
      .map(([field]) => field),
  );
  selected.add(idField);
  const where = query.where ? `WHERE ${compileFilter(query.where, literalStringFields)}` : "";
  const orderBy = [
    ...(query.orderBy ?? []),
    ...(query.orderBy?.some((order) => order.field === idField)
      ? []
      : [{ field: idField, direction: "asc" as const }]),
  ];
  return {
    rowsSql: `SELECT ${Array.from(selected).map(quoteIdentifier).join(", ")} FROM (${source}) ${where} ${compileOrderBy(orderBy, columns)} ${compileLimit(query)}`,
    countSql: `SELECT count() AS totalRows FROM (${source}) ${where}`,
    decimalFields: selectedDecimalFields(Array.from(selected), columns),
    integerFields: selectedIntegerFields(Array.from(selected), columns),
    numberFields: new Set(),
  };
}

function selectedDecimalFields(
  selected: readonly string[],
  columns: readonly Column[],
): ReadonlySet<string> {
  const decimalColumns = new Set(
    columns
      .filter((column) => column.type === BIG_DECIMAL_COLUMN_TYPE)
      .map((column) => column.name),
  );
  return new Set(selected.filter((field) => decimalColumns.has(field)));
}

function selectedIntegerFields(
  selected: readonly string[],
  columns: readonly Column[],
): ReadonlySet<string> {
  const integerColumns = new Set(
    columns.filter((column) => column.type === "Int64").map((column) => column.name),
  );
  return new Set(selected.filter((field) => integerColumns.has(field)));
}

function groupedDecimalFields(
  query: RuntimeGroupedQuery,
  columns: readonly Column[],
): ReadonlySet<string> {
  const decimalColumns = new Set(
    columns
      .filter((column) => column.type === BIG_DECIMAL_COLUMN_TYPE)
      .map((column) => column.name),
  );
  const fields = new Set(query.groupBy.filter((field) => decimalColumns.has(field)));
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (
      decimalColumns.has(aggregate.field) &&
      (aggregate.aggFunc === "sum" ||
        aggregate.aggFunc === "avg" ||
        aggregate.aggFunc === "min" ||
        aggregate.aggFunc === "max")
    ) {
      fields.add(alias);
    }
  }
  return fields;
}

function groupedIntegerFields(
  query: RuntimeGroupedQuery,
  columns: readonly Column[],
): ReadonlySet<string> {
  const integerColumns = new Set(
    columns.filter((column) => column.type === "Int64").map((column) => column.name),
  );
  const fields = new Set(query.groupBy.filter((field) => integerColumns.has(field)));
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (
      integerColumns.has(aggregate.field) &&
      (aggregate.aggFunc === "sum" || aggregate.aggFunc === "min" || aggregate.aggFunc === "max")
    ) {
      fields.add(alias);
    }
  }
  return fields;
}

function groupedNumberFields(query: RuntimeGroupedQuery): ReadonlySet<string> {
  const fields = new Set<string>();
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (aggregate.aggFunc === "count" || aggregate.aggFunc === "count_distinct") {
      fields.add(alias);
    }
  }
  return fields;
}

function compileLimit(query: {
  readonly limit?: number | undefined;
  readonly offset?: number | undefined;
}): string {
  const limit = Math.max(0, Math.min(50, Math.trunc(query.limit ?? 50)));
  const offset = Math.max(0, Math.trunc(query.offset ?? 0));
  return `LIMIT ${limit} OFFSET ${offset}`;
}

function compileOrderBy(
  orderBy: readonly { readonly field: string; readonly direction: "asc" | "desc" }[],
  columns: readonly Column[],
): string {
  if (orderBy.length === 0) {
    return "";
  }
  const columnTypes = new Map(columns.map((column) => [column.name, column.type]));
  return `ORDER BY ${orderBy
    .map((order) => {
      const field = quoteIdentifier(order.field);
      const expression =
        columnTypes.get(order.field) === "String" ? `lower(toString(${field}))` : field;
      return `${expression} ${order.direction.toUpperCase()}`;
    })
    .join(", ")}`;
}

function compileFilter(
  filter: RuntimeQuery["where"],
  literalStringFields: ReadonlySet<string>,
): string {
  if (filter === undefined) {
    return "1";
  }
  if ("op" in filter) {
    const joiner = filter.op === "and" ? " AND " : " OR ";
    return `(${filter.conditions.map((condition) => compileFilter(condition, literalStringFields)).join(joiner)})`;
  }
  const field = quoteIdentifier(filter.field);
  const value = filter.value;
  const strictStringEquality = literalStringFields.has(filter.field);
  if (typeof value === "string") {
    const lowered = `lower(toString(${field}))`;
    if (filter.comparator === "equals") {
      if (strictStringEquality) {
        return `${field} = ${literal(value)}`;
      }
      return `${lowered} = lower(${literal(value)})`;
    }
    if (filter.comparator === "not_equals") {
      if (strictStringEquality) {
        return `${field} != ${literal(value)}`;
      }
      return `${lowered} != lower(${literal(value)})`;
    }
    if (filter.comparator === "contains") {
      return `position(${lowered}, lower(${literal(value)})) > 0`;
    }
    if (filter.comparator === "starts_with") {
      return `startsWith(${lowered}, lower(${literal(value)}))`;
    }
  }
  if (filter.comparator === "one_of" && Array.isArray(value)) {
    if (value.length === 0) {
      return "0";
    }
    if (value.every((item) => typeof item === "string")) {
      if (strictStringEquality) {
        return `${field} IN (${value.map((item) => literal(String(item))).join(", ")})`;
      }
      return `lower(toString(${field})) IN (${value.map((item) => `lower(${literal(String(item))})`).join(", ")})`;
    }
    return `${field} IN (${value.map(compileValue).join(", ")})`;
  }
  switch (filter.comparator) {
    case "equals":
      return `${field} = ${compileValue(value)}`;
    case "not_equals":
      return `${field} != ${compileValue(value)}`;
    case "greater_than":
      return `${field} > ${compileValue(value)}`;
    case "greater_than_or_equal":
      return `${field} >= ${compileValue(value)}`;
    case "less_than":
      return `${field} < ${compileValue(value)}`;
    case "less_than_or_equal":
      return `${field} <= ${compileValue(value)}`;
    default:
      return "0";
  }
}

function compileAggregate(
  aggregate: RuntimeAggregateDefinition,
  columns: readonly Column[],
): string {
  const field = quoteIdentifier(aggregate.field);
  const column = columns.find((candidate) => candidate.name === aggregate.field);
  switch (aggregate.aggFunc) {
    case "count":
      return "count()";
    case "count_distinct":
      return `uniqExact(${field})`;
    case "sum":
      return `sum(${field})`;
    case "avg":
      return `avg(${field})`;
    case "min":
      return column?.type === "String"
        ? `argMin(${field}, lower(toString(${field})))`
        : `min(${field})`;
    case "max":
      return column?.type === "String"
        ? `argMax(${field}, lower(toString(${field})))`
        : `max(${field})`;
    case "string_concat": {
      const values =
        aggregate.sort === "desc"
          ? `arrayReverseSort(value -> lower(value), groupArray(toString(${field})))`
          : aggregate.sort === "asc"
            ? `arraySort(value -> lower(value), groupArray(toString(${field})))`
            : `groupArray(toString(${field}))`;
      return `arrayStringConcat(${values}, ${literal(aggregate.joiner)})`;
    }
    case "string_concat_distinct": {
      const values =
        aggregate.sort === "desc"
          ? `arrayReverseSort(value -> lower(value), groupUniqArray(toString(${field})))`
          : `arraySort(value -> lower(value), groupUniqArray(toString(${field})))`;
      return `arrayStringConcat(${values}, ${literal(aggregate.joiner)})`;
    }
  }
}

function isGroupedQuery(query: RuntimeQuery): query is RuntimeGroupedQuery {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

function compileValue(value: unknown): string {
  if (BigDecimal.isBigDecimal(value)) {
    return `toDecimal256(${literal(BigDecimal.format(value))}, ${BIG_DECIMAL_SCALE})`;
  }
  if (typeof value === "string") {
    return literal(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "0";
  }
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return "0";
}

function quoteIdentifier(identifier: string): string {
  return "`" + identifier.replaceAll("`", "``") + "`";
}

function literal(value: string): string {
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

function withJsonSettings(sql: string): string {
  return `${sql} ${JSON_FORMAT_SETTINGS}`;
}

function parseJsonEachRow(
  output: string,
  decimalFields: ReadonlySet<string> = new Set(),
  integerFields: ReadonlySet<string> = new Set(),
  numberFields: ReadonlySet<string> = new Set(),
): RuntimeRow[] {
  const trimmed = output.trim();
  if (trimmed.length === 0) {
    return [];
  }
  return trimmed
    .split("\n")
    .map((line) => parseJsonRow(line, decimalFields, integerFields, numberFields));
}

function parseJsonRow(
  line: string,
  decimalFields: ReadonlySet<string>,
  integerFields: ReadonlySet<string>,
  numberFields: ReadonlySet<string>,
): RuntimeRow {
  const row = JSON.parse(line);
  for (const field of numberFields) {
    const value = row[field];
    if (typeof value === "string") {
      row[field] = Number(value);
    }
  }
  for (const field of decimalFields) {
    const value = row[field];
    if (typeof value === "string") {
      row[field] = BigDecimal.fromStringUnsafe(value);
    }
  }
  for (const field of integerFields) {
    const value = row[field];
    if (typeof value === "string" || typeof value === "number") {
      row[field] = BigInt(value);
    }
  }
  return row;
}

function parseCount(output: string): number {
  const [row] = parseJsonEachRow(output);
  return Number(row?.totalRows ?? 0);
}
