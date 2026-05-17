import { Session } from "chdb";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import { compileQuerySql } from "./chdb-sql-compiler.ts";
import { ChdbSqlMirror } from "./chdb-sql-mirror.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
  VersionedRow,
} from "./snapshot-backend.ts";

const JSON_FORMAT_SETTINGS =
  "SETTINGS output_format_json_quote_decimals=1, output_format_json_quote_64bit_integers=1";

let sharedSession: Session | undefined;
let sharedSessionReferences = 0;
let nextBackendTableId = 0;

export function createInProcessChdbSnapshotBackend(): SnapshotBackend {
  return new InProcessChdbSnapshotBackend();
}

class InProcessChdbSnapshotBackend implements SnapshotBackend {
  readonly #session: Session;
  readonly #tableName: string;
  readonly #mirror: ChdbSqlMirror;
  #topic = "";
  #idField = "id";
  #backendVersion = 0n;
  #literalStringFields: ReadonlySet<string> = new Set();
  #pendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #flushScheduled = false;
  #closed = false;
  #lastFlushError: unknown;
  #sessionReleased = false;

  constructor() {
    this.#session = acquireSharedSession();
    this.#tableName = `topic_rows_${nextBackendTableId}`;
    nextBackendTableId++;
    this.#mirror = new ChdbSqlMirror(this.#session, this.#tableName);
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return true;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => ({
      status: this.#closed ? "stopped" : "ready",
      pid: 0,
      restarts: 0,
      pendingRequests: 0,
      lastError: this.#lastFlushError === undefined ? "" : errorMessage(this.#lastFlushError),
      backendVersion: this.#backendVersion,
    }));
  }

  init(args: {
    readonly topic: string;
    readonly idField: string;
    readonly rows: readonly VersionedRow[];
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.in_process.init")(function* (
      backend: InProcessChdbSnapshotBackend,
    ) {
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
    })(this);
  }

  applyBatch(args: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (backend: InProcessChdbSnapshotBackend) {
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
    })(this);
  }

  snapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.in_process.snapshot")(function* (
      backend: InProcessChdbSnapshotBackend,
    ) {
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
    return this.snapshot(args);
  }

  close(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.in_process.close")(function* (
      backend: InProcessChdbSnapshotBackend,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      backend.#closed = true;
      yield* backend.#flushPendingEffect();
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
    return Effect.fn("view-server.chdb.in_process.flush")(function* (
      backend: InProcessChdbSnapshotBackend,
    ) {
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
        backend.#mirror.applyMutations(mutations);
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
