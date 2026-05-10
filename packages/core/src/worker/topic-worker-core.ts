import { Effect, Queue, Schema, Semaphore, Stream } from "effect";
import type * as Cause from "effect/Cause";
import type * as Scope from "effect/Scope";
import type { TopicConfig } from "../config/index.ts";
import { literalStringFieldsForSchema } from "../config/index.ts";
import {
  invalidPublish,
  missingTopicId,
  schemaDecodeFailed,
  type ViewServerError,
} from "../errors.ts";
import type {
  DeltaEvent,
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SnapshotEvent,
  SubscriptionEvent,
} from "../protocol/index.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../snapshot/index.ts";
import { createMemorySnapshotBackend } from "../snapshot/index.ts";
import { MutationLog, type MutationLogEntry, type WorkerVersion } from "./mutation-log.ts";
import {
  changedFields,
  collectDependencyFields,
  diffVisibleRows,
  executeMemoryQuery,
  type QueryExecutionResult,
  rowKeyForMemoryQuery,
  rowId,
} from "./query-engine.ts";

export type TopicWorkerMetrics = {
  readonly rows: number;
  readonly subscribers: number;
  readonly version: WorkerVersion;
  readonly queueDepth: number;
  readonly status: "ready" | "degraded" | "stopping";
};

export type TopicWorkerCore = {
  readonly topic: string;
  readonly idField: string;
  readonly version: Effect.Effect<WorkerVersion, ViewServerError>;
  readonly metrics: Effect.Effect<TopicWorkerMetrics, ViewServerError>;
  readonly query: (
    query: RuntimeQuery,
  ) => Effect.Effect<QueryResponse<readonly RuntimeRow[]>, ViewServerError>;
  readonly subscribe: (
    requestId: string,
    query: RuntimeQuery,
  ) => Stream.Stream<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>;
  readonly unsubscribe: (requestId: string) => Effect.Effect<void, ViewServerError>;
  readonly publish: (row: unknown) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: (patch: RuntimeRow) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: (id: string | number) => Effect.Effect<void, ViewServerError>;
  readonly getRowsForTest: Effect.Effect<readonly RuntimeRow[], ViewServerError>;
  readonly shutdown: Effect.Effect<void, ViewServerError>;
};

type ActiveSubscription = {
  readonly requestId: string;
  readonly query: RuntimeQuery;
  readonly dependencyFields: ReadonlySet<string>;
  readonly queue: Queue.Queue<
    SubscriptionEvent<readonly RuntimeRow[]>,
    ViewServerError | Cause.Done
  >;
  lastRows: readonly RuntimeRow[];
  lastTotalRows: number;
  lastVersion: WorkerVersion;
};

export type TopicWorkerCoreOptions = {
  readonly initialRows?: readonly RuntimeRow[] | undefined;
  readonly snapshotBackend?: SnapshotBackend | undefined;
  readonly mutationLogSize?: number | undefined;
};

export function makeTopicWorkerCore(
  topic: string,
  config: TopicConfig,
  options: TopicWorkerCoreOptions = {},
): Effect.Effect<TopicWorkerCore, ViewServerError, Scope.Scope> {
  return Effect.fn("view-server.worker.make")(function* () {
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": topic,
      "view_server.rows": options.initialRows?.length ?? 0,
    });
    const idField = config.id;
    const literalStringFields = literalStringFieldsForSchema(config.schema);
    const backend = options.snapshotBackend ?? createMemorySnapshotBackend();
    const mutationLog = new MutationLog(options.mutationLogSize ?? 10_000);
    const gate = yield* Semaphore.make(1);
    const scope = yield* Effect.scope;
    const subscriptions = new Map<string, ActiveSubscription>();
    let rows: RuntimeRow[] = [];
    let idIndex = new Map<string | number, number>();
    let version: WorkerVersion = 0n;
    let status: TopicWorkerMetrics["status"] = "ready";

    const decodeRow = (input: unknown) =>
      Schema.decodeUnknownEffect(config.schema)(input).pipe(
        Effect.mapError((error) => schemaDecodeFailed(topic, error)),
      );

    const ensureId = (row: RuntimeRow) => {
      const id = row[idField];
      return typeof id === "string" || typeof id === "number"
        ? Effect.succeed(id)
        : Effect.fail(missingTopicId(topic, idField));
    };

    const replaceIndexes = () => {
      idIndex = new Map();
      rows.forEach((row, index) => {
        const id = row[idField];
        if (typeof id === "string" || typeof id === "number") {
          idIndex.set(id, index);
        }
      });
    };

    const memoryQuery = (query: RuntimeQuery) =>
      executeMemoryQuery(rows, query, idField, { literalStringFields });

    const fencedQuery = Effect.fn("view-server.worker.snapshot.query")(function* (
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
        "view_server.worker_version": version.toString(),
      });
      const targetVersion = version;
      const memorySnapshot = () => ({
        ...memoryQuery(query),
        backendVersion: undefined,
      });
      const result = yield* backend.snapshot({ query, targetVersion }).pipe(
        Effect.map(
          (candidate) => reconcileSnapshot(candidate, query, targetVersion) ?? memorySnapshot(),
        ),
        Effect.catchTag("SnapshotBackendFailed", () => Effect.succeed(memorySnapshot())),
      );
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": result.rows.length,
        "view_server.total_rows": result.totalRows,
        "view_server.worker_version": targetVersion.toString(),
        ...(result.backendVersion === undefined
          ? {}
          : { "view_server.backend_version": result.backendVersion.toString() }),
      });
      return { result, targetVersion };
    });

    const reconcileSnapshot = (
      candidate: SnapshotBackendResult,
      query: RuntimeQuery,
      targetVersion: WorkerVersion,
    ): (QueryExecutionResult & { readonly backendVersion: WorkerVersion }) | undefined => {
      if (candidate.backendVersion === targetVersion) {
        return candidate;
      }
      if (candidate.backendVersion > targetVersion) {
        return undefined;
      }
      if (
        candidate.replayRows === undefined ||
        !mutationLog.coversExclusive(candidate.backendVersion, targetVersion)
      ) {
        return undefined;
      }
      const replayedRows = replayMutations(
        candidate.replayRows,
        mutationLog.entriesExclusive(candidate.backendVersion, targetVersion),
        idField,
      );
      const replayed = executeMemoryQuery(replayedRows, query, idField, { literalStringFields });
      return {
        ...replayed,
        backendVersion: candidate.backendVersion,
      };
    };

    const fencedSnapshot = Effect.fn("view-server.worker.snapshot.emit")(function* (
      requestId: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": requestId,
        "view_server.subscription_id": requestId,
        "view_server.topic": topic,
      });
      const { result, targetVersion } = yield* fencedQuery(query);
      const snapshot: SnapshotEvent<readonly RuntimeRow[]> = {
        type: "snapshot",
        requestId,
        rows: result.rows,
        meta: {
          version: targetVersion.toString(),
          totalRows: result.totalRows,
          ...(result.backendVersion === undefined
            ? {}
            : { backendVersion: result.backendVersion.toString() }),
          serverTime: Date.now(),
        },
      };
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": result.rows.length,
        "view_server.total_rows": result.totalRows,
        "view_server.worker_version": targetVersion.toString(),
      });
      return { snapshot, targetVersion, totalRows: result.totalRows };
    });

    const fanoutUntraced = Effect.fnUntraced(function* (
      fromVersion: WorkerVersion,
      toVersion: WorkerVersion,
      mutation: MutationLogEntry,
    ) {
      yield* Effect.forEach(
        subscriptions.values(),
        (subscription) => {
          if (
            mutation.kind === "update" &&
            !hasDependency(subscription.dependencyFields, mutation.changedFields)
          ) {
            return Effect.void;
          }
          const next = memoryQuery(subscription.query);
          const operations = diffVisibleRows(
            subscription.lastRows,
            next.rows,
            rowKeyForMemoryQuery(subscription.query, idField),
          );
          if (operations.length === 0 && subscription.lastTotalRows === next.totalRows) {
            subscription.lastVersion = toVersion;
            return Effect.void;
          }
          const event: DeltaEvent<readonly RuntimeRow[]> = {
            type: "delta",
            requestId: subscription.requestId,
            ops: operations,
            meta: {
              fromVersion: fromVersion.toString(),
              toVersion: toVersion.toString(),
              totalRows: next.totalRows,
              serverTime: Date.now(),
            },
          };
          subscription.lastRows = next.rows;
          subscription.lastTotalRows = next.totalRows;
          subscription.lastVersion = toVersion;
          return Queue.offer(subscription.queue, event);
        },
        { discard: true },
      );
    });

    const fanout = (
      fromVersion: WorkerVersion,
      toVersion: WorkerVersion,
      mutation: MutationLogEntry,
    ) =>
      subscriptions.size === 0
        ? fanoutUntraced(fromVersion, toVersion, mutation)
        : Effect.fn("view-server.worker.fanout.delta")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
              "view_server.worker_version": toVersion.toString(),
              "view_server.batch_size": subscriptions.size,
            });
            yield* fanoutUntraced(fromVersion, toVersion, mutation);
          })();

    const appendMutation = Effect.fnUntraced(function* (
      mutation: Omit<MutationLogEntry, "version">,
    ) {
      const fromVersion = version;
      version = version + 1n;
      const entry: MutationLogEntry = { ...mutation, version };
      mutationLog.append(entry);
      yield* backend
        .applyBatch({
          mutations: [entry],
          highestVersion: version,
        })
        .pipe(
          Effect.catchTag("SnapshotBackendFailed", () =>
            Effect.sync(() => {
              status = "degraded";
            }),
          ),
          Effect.forkIn(scope),
        );
      yield* fanout(fromVersion, version, entry);
    });

    const publishDecoded = Effect.fnUntraced(function* (decoded: RuntimeRow) {
      const id = yield* ensureId(decoded);
      const index = idIndex.get(id);
      if (index === undefined) {
        rows = [...rows, { ...decoded }];
        replaceIndexes();
        yield* appendMutation({
          kind: "insert",
          id,
          after: { ...decoded },
          changedFields: new Set(Object.keys(decoded)),
        });
        return;
      }

      const before = rows[index];
      const after = { ...decoded };
      rows[index] = after;
      yield* appendMutation({
        kind: "update",
        id,
        before,
        after,
        changedFields: changedFields(before, after),
      });
    });

    const worker: TopicWorkerCore = {
      topic,
      idField,
      version: Effect.sync(() => version),
      metrics: Effect.sync(() => ({
        rows: rows.length,
        subscribers: subscriptions.size,
        version,
        queueDepth: 0,
        status,
      })),

      query: (query) =>
        gate.withPermit(
          Effect.fn("view-server.worker.query")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
              "view_server.worker_version": version.toString(),
            });
            const { result, targetVersion } = yield* fencedQuery(query);
            yield* Effect.annotateCurrentSpan({
              "view_server.rows": result.rows.length,
              "view_server.total_rows": result.totalRows,
              "view_server.worker_version": targetVersion.toString(),
            });
            return {
              rows: result.rows,
              totalRows: result.totalRows,
              version: targetVersion.toString(),
            };
          })(),
        ),

      subscribe: (requestId, query) =>
        Stream.callback<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>((queue) =>
          gate.withPermit(
            Effect.fn("view-server.worker.subscribe")(function* () {
              yield* Effect.annotateCurrentSpan({
                "view_server.request_id": requestId,
                "view_server.subscription_id": requestId,
                "view_server.topic": topic,
                "view_server.worker_version": version.toString(),
              });
              const { snapshot, targetVersion, totalRows } = yield* fencedSnapshot(
                requestId,
                query,
              );
              const active: ActiveSubscription = {
                requestId,
                query,
                dependencyFields: collectDependencyFields(query, idField),
                queue,
                lastRows: snapshot.rows,
                lastTotalRows: totalRows,
                lastVersion: targetVersion,
              };
              subscriptions.set(requestId, active);
              yield* Queue.offer(queue, snapshot);
              yield* Effect.addFinalizer(() =>
                Effect.fn("view-server.worker.subscribe.finalize")(function* () {
                  yield* Effect.annotateCurrentSpan({
                    "view_server.request_id": requestId,
                    "view_server.subscription_id": requestId,
                    "view_server.topic": topic,
                  });
                  subscriptions.delete(requestId);
                })(),
              );
            })(),
          ),
        ),

      unsubscribe: (requestId) =>
        gate.withPermit(
          Effect.fn("view-server.worker.unsubscribe")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.request_id": requestId,
              "view_server.subscription_id": requestId,
              "view_server.topic": topic,
            });
            const subscription = subscriptions.get(requestId);
            if (subscription === undefined) {
              return;
            }
            subscriptions.delete(requestId);
            yield* Queue.end(subscription.queue);
          })(),
        ),

      publish: (input) =>
        gate.withPermit(
          Effect.fnUntraced(function* () {
            const decoded = yield* decodeRow(input);
            yield* publishDecoded(decoded);
          })(),
        ),

      deltaPublish: (patch) =>
        gate.withPermit(
          Effect.fnUntraced(function* () {
            const id = patch[idField];
            if (typeof id !== "string" && typeof id !== "number") {
              return yield* Effect.fail(missingTopicId(topic, idField));
            }
            const index = idIndex.get(id);
            if (index === undefined) {
              return yield* Effect.fail(
                invalidPublish(topic, `Cannot deltaPublish missing row ${String(id)}`),
              );
            }
            const before = rows[index];
            const merged = { ...before, ...patch };
            const decoded = yield* decodeRow(merged);
            rows[index] = decoded;
            yield* appendMutation({
              kind: "update",
              id,
              before,
              after: decoded,
              changedFields: changedFields(before, decoded),
            });
          })(),
        ),

      deleteById: (id) =>
        gate.withPermit(
          Effect.fnUntraced(function* () {
            const index = idIndex.get(id);
            if (index === undefined) {
              return;
            }
            const before = rows[index];
            rows = rows.filter((row) => rowId(row, idField) !== id);
            replaceIndexes();
            yield* appendMutation({
              kind: "delete",
              id,
              before,
              changedFields: new Set(Object.keys(before)),
            });
          })(),
        ),

      getRowsForTest: Effect.sync(() => rows.map((row) => ({ ...row }))),

      shutdown: gate.withPermit(
        Effect.fn("view-server.worker.shutdown")(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": topic,
            "view_server.worker_version": version.toString(),
          });
          status = "stopping";
          yield* backend.close();
          yield* Effect.forEach(
            subscriptions.values(),
            (subscription) => Queue.end(subscription.queue),
            { discard: true },
          );
          subscriptions.clear();
        })(),
      ),
    };

    for (const row of options.initialRows ?? []) {
      const decoded = yield* decodeRow(row);
      rows = [...rows, decoded];
    }
    replaceIndexes();
    yield* backend.init({
      topic,
      idField,
      rows: rows.map((row) => ({ row, version })),
      version,
      literalStringFields,
    });

    yield* Effect.addFinalizer(() => worker.shutdown.pipe(Effect.ignore));

    return worker;
  })();
}

function hasDependency(
  dependencyFields: ReadonlySet<string>,
  changed: ReadonlySet<string>,
): boolean {
  for (const field of changed) {
    if (dependencyFields.has(field)) {
      return true;
    }
  }
  return false;
}

function replayMutations(
  baseRows: readonly RuntimeRow[],
  mutations: readonly MutationLogEntry[],
  idField: string,
): readonly RuntimeRow[] {
  let rows = baseRows.map((row) => ({ ...row }));
  for (const mutation of mutations) {
    if (mutation.kind === "delete") {
      rows = rows.filter((row) => rowId(row, idField) !== mutation.id);
      continue;
    }
    if (mutation.after === undefined) {
      continue;
    }
    const index = rows.findIndex((row) => rowId(row, idField) === mutation.id);
    if (index >= 0) {
      rows[index] = { ...mutation.after };
    } else {
      rows = [...rows, { ...mutation.after }];
    }
  }
  return rows;
}
