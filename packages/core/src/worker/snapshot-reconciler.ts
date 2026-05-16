import * as Effect from "effect/Effect";
import type { ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../snapshot/index.ts";
import {
  executeMemoryQuery,
  type QueryExecutionOptions,
  type QueryExecutionResult,
} from "./query-engine.ts";
import type { WorkerVersion } from "./mutation-log.ts";

export type SnapshotReconcilerSource = "backend" | "replay" | "memory";

export type ReconciledSnapshot = QueryExecutionResult & {
  readonly source: SnapshotReconcilerSource;
  readonly targetVersion: WorkerVersion;
  readonly backendVersion?: WorkerVersion | undefined;
  readonly backendFailed: boolean;
};

export type SnapshotReconciler = {
  readonly query: (args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }) => Effect.Effect<ReconciledSnapshot, ViewServerError>;
};

export function makeSnapshotReconciler(options: {
  readonly topic: string;
  readonly idField: string;
  readonly backend: SnapshotBackend;
  readonly rows: () => readonly RuntimeRow[];
  readonly canReplay: (fromVersion: WorkerVersion, toVersion: WorkerVersion) => boolean;
  readonly replayRowsFrom: (
    baseRows: readonly RuntimeRow[],
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ) => readonly RuntimeRow[];
  readonly queryOptions?: QueryExecutionOptions | undefined;
}): SnapshotReconciler {
  const memorySnapshot = (
    query: RuntimeQuery,
    targetVersion: WorkerVersion,
    backendFailed: boolean,
  ): ReconciledSnapshot => {
    const result = executeMemoryQuery(options.rows(), query, options.idField, options.queryOptions);
    return {
      ...result,
      source: "memory",
      targetVersion,
      backendFailed,
    };
  };

  return {
    query: Effect.fn("view-server.worker.snapshot.reconcile")(function* (args) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": options.topic,
        "view_server.worker_version": args.targetVersion.toString(),
      });
      return yield* options.backend
        .snapshot({
          query: args.query,
          targetVersion: args.targetVersion,
        })
        .pipe(
          Effect.map(
            (candidate) =>
              reconcileSnapshotCandidate({
                candidate,
                query: args.query,
                targetVersion: args.targetVersion,
                idField: options.idField,
                canReplay: options.canReplay,
                replayRowsFrom: options.replayRowsFrom,
                queryOptions: options.queryOptions,
              }) ?? memorySnapshot(args.query, args.targetVersion, false),
          ),
          Effect.catchTag("SnapshotBackendFailed", () =>
            Effect.succeed(memorySnapshot(args.query, args.targetVersion, true)),
          ),
        );
    }),
  };
}

export function reconcileSnapshotCandidate(args: {
  readonly candidate: SnapshotBackendResult;
  readonly query: RuntimeQuery;
  readonly targetVersion: WorkerVersion;
  readonly idField: string;
  readonly canReplay: (fromVersion: WorkerVersion, toVersion: WorkerVersion) => boolean;
  readonly replayRowsFrom: (
    baseRows: readonly RuntimeRow[],
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ) => readonly RuntimeRow[];
  readonly queryOptions?: QueryExecutionOptions | undefined;
}): ReconciledSnapshot | undefined {
  if (args.candidate.backendVersion === args.targetVersion) {
    return {
      rows: args.candidate.rows,
      totalRows: args.candidate.totalRows,
      backendVersion: args.candidate.backendVersion,
      source: "backend",
      targetVersion: args.targetVersion,
      backendFailed: false,
    };
  }
  if (args.candidate.backendVersion > args.targetVersion) {
    return undefined;
  }
  if (
    args.candidate.replayRows === undefined ||
    !args.canReplay(args.candidate.backendVersion, args.targetVersion)
  ) {
    return undefined;
  }
  const replayedRows = args.replayRowsFrom(
    args.candidate.replayRows,
    args.candidate.backendVersion,
    args.targetVersion,
  );
  const replayed = executeMemoryQuery(replayedRows, args.query, args.idField, args.queryOptions);
  return {
    ...replayed,
    backendVersion: args.candidate.backendVersion,
    source: "replay",
    targetVersion: args.targetVersion,
    backendFailed: false,
  };
}
