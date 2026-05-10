import { Effect } from "effect";
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

export interface SnapshotBackend {
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

  readonly close: () => Effect.Effect<void>;
}

export function createMemorySnapshotBackend(): SnapshotBackend {
  let idField = "id";
  let rows: RuntimeRow[] = [];
  let backendVersion = 0n;
  let queryOptions: QueryExecutionOptions = {};

  return {
    init: (args) =>
      Effect.sync(() => {
        idField = args.idField;
        rows = args.rows.map((entry) => ({ ...entry.row }));
        backendVersion = args.version;
        queryOptions = { literalStringFields: args.literalStringFields };
      }),

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
      Effect.sync(() => {
        const result = executeMemoryQuery(rows, args.query, idField, queryOptions);
        return {
          rows: result.rows,
          totalRows: result.totalRows,
          backendVersion,
          replayRows: rows.map((row) => ({ ...row })),
        };
      }),

    close: () => Effect.void,
  };
}
