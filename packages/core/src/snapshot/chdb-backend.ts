import { Session } from "chdb";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import type { TopicConfig } from "../config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
  VersionedRow,
} from "./snapshot-backend.ts";
import { ChdbProcessClient } from "./chdb-process-client.ts";
import type { ChdbQueryWorkerRequest } from "./chdb-query-worker-protocol.ts";
import {
  decodeSnapshotBackendResult,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeVersionedRow,
} from "./chdb-query-worker-codec.ts";
import { compileQuerySql } from "./chdb-sql-compiler.ts";
import { ChdbSqlMirror } from "./chdb-sql-mirror.ts";

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
    if (typeof id === "string" || typeof id === "number") {
      this.#mirrorRows.set(id, { ...mutation.after });
    }
  }
}

class ChdbSnapshotBackend implements SnapshotBackend {
  readonly #session: Session;
  readonly #tableName: string;
  readonly #mirror: ChdbSqlMirror;
  #groupedRefreshWorker: ChdbGroupedSnapshotWorkerClient | undefined;
  #topic = "";
  #idField = "id";
  #backendVersion = 0n;
  #literalStringFields: ReadonlySet<string> = new Set();
  #pendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #flushScheduled = false;
  #closed = false;
  #lastFlushError: unknown;
  #sessionReleased = false;

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#session = acquireSharedSession();
    this.#tableName = `topic_rows_${nextBackendTableId}`;
    nextBackendTableId++;
    this.#mirror = new ChdbSqlMirror(this.#session, this.#tableName);
    if (options.groupedRefreshWorker !== false) {
      this.#groupedRefreshWorker = new ChdbGroupedSnapshotWorkerClient(options);
    }
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return this.#groupedRefreshWorker !== undefined;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => {
      const workerHealth = this.#groupedRefreshWorker?.health;
      const lastError =
        workerHealth?.lastError ??
        workerHealth?.message ??
        (this.#lastFlushError === undefined ? "" : errorMessage(this.#lastFlushError));
      return {
        status: this.#closed ? "stopped" : (workerHealth?.status ?? "ready"),
        message: workerHealth?.message,
        pid: workerHealth?.pid ?? 0,
        restarts: workerHealth?.restarts ?? 0,
        pendingRequests: workerHealth?.pendingRequests ?? 0,
        lastError,
        backendVersion: this.#backendVersion,
      };
    });
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
          backend.#mirror.init({
            idField: backend.#idField,
            rows: initialRows,
            version: args.version,
          });
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
          if (!backend.#mirror.tableReady) {
            return {
              rows: [],
              totalRows: 0,
              backendVersion: backend.#backendVersion,
            };
          }
          const sql = compileQuerySql(
            args.query,
            backend.#idField,
            backend.#mirror.columns,
            backend.#literalStringFields,
            backend.#mirror.tableName,
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
          backend.#mirror.drop();
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
    this.#mirror.applyMutations(mutations);
  }

  #releaseSession(): void {
    if (this.#sessionReleased) {
      return;
    }
    this.#sessionReleased = true;
    releaseSharedSession(this.#session);
  }
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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
