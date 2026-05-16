import * as Effect from "effect/Effect";
import type { ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import { executeMemoryQuery, type QueryExecutionOptions } from "../worker/query-engine.ts";

export type VersionedRow = {
  readonly row: RuntimeRow;
  readonly version: WorkerVersion;
};

export type SnapshotBackendResult = {
  readonly rows: readonly RuntimeRow[];
  readonly totalRows: number;
  readonly backendVersion: WorkerVersion;
  readonly replayRows?: readonly RuntimeRow[] | undefined;
};

export type SnapshotBackendHealth = {
  readonly status: "ready" | "degraded";
  readonly message?: string | undefined;
};

export interface SnapshotBackend {
  readonly supportsGroupedRefreshSnapshots?: boolean | undefined;

  readonly health?: Effect.Effect<SnapshotBackendHealth> | undefined;

  readonly init: (args: {
    readonly topic: string;
    readonly idField: string;
    readonly rows: readonly VersionedRow[];
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  }) => Effect.Effect<void, ViewServerError>;

  readonly applyBatch: (args: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }) => Effect.Effect<void, ViewServerError>;

  readonly snapshot: (args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }) => Effect.Effect<SnapshotBackendResult, ViewServerError>;

  readonly groupedRefreshSnapshot?: (args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }) => Effect.Effect<SnapshotBackendResult, ViewServerError>;

  readonly close: () => Effect.Effect<void>;
}

export function createMemorySnapshotBackend(): SnapshotBackend {
  let topic = "";
  let idField = "id";
  let rows: RuntimeRow[] = [];
  let backendVersion = 0n;
  let queryOptions: QueryExecutionOptions = {};

  return {
    init: (args) =>
      Effect.fn("view-server.snapshot.memory.init")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": args.topic,
          "view_server.rows": args.rows.length,
          "view_server.backend_version": args.version.toString(),
        });
        topic = args.topic;
        idField = args.idField;
        rows = args.rows.map((entry) => ({ ...entry.row }));
        backendVersion = args.version;
        queryOptions = { literalStringFields: args.literalStringFields };
      })(),

    applyBatch: (args) =>
      Effect.sync(() => {
        for (const mutation of args.mutations) {
          if (mutation.kind === "delete") {
            rows = rows.filter((row) => row[idField] !== mutation.id);
            continue;
          }
          if (mutation.after === undefined) {
            continue;
          }
          const index = rows.findIndex((row) => row[idField] === mutation.id);
          if (index >= 0) {
            rows[index] = { ...mutation.after };
          } else {
            rows.push({ ...mutation.after });
          }
        }
        backendVersion = args.highestVersion;
      }),

    snapshot: (args) =>
      Effect.fn("view-server.snapshot.memory.query")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
          "view_server.worker_version": args.targetVersion.toString(),
          "view_server.backend_version": backendVersion.toString(),
        });
        const result = executeMemoryQuery(rows, args.query, idField, queryOptions);
        yield* Effect.annotateCurrentSpan({
          "view_server.rows": result.rows.length,
          "view_server.total_rows": result.totalRows,
        });
        return {
          rows: result.rows,
          totalRows: result.totalRows,
          backendVersion,
          replayRows: rows.map((row) => ({ ...row })),
        };
      })(),

    close: () =>
      Effect.fn("view-server.snapshot.memory.close")(function* () {
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
          "view_server.backend_version": backendVersion.toString(),
        });
      })(),
  };
}
