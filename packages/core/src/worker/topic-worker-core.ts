import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Cause from "effect/Cause";
import type * as Scope from "effect/Scope";
import type { TopicConfig } from "../config/index.ts";
import { literalStringFieldsForSchema } from "../config/index.ts";
import {
  backpressureExceeded,
  invalidPublish,
  missingTopicId,
  schemaDecodeFailed,
  serverShutdown,
  type ViewServerError,
} from "../errors.ts";
import type {
  DeltaOperation,
  DeltaEvent,
  QueryResponse,
  RuntimeQuery,
  RuntimeGroupedQuery,
  RuntimeRawQuery,
  RuntimeRow,
  LiveQueryStatusEvent,
  SnapshotEvent,
  SubscriptionEvent,
} from "../protocol/index.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../snapshot/index.ts";
import { createMemorySnapshotBackend } from "../snapshot/index.ts";
import {
  activeRawPlanKey,
  estimateActiveRawPlanIndexBytes,
  estimateActiveRawPlanIndexBytesEffect,
  makeActiveRawPlanEffect,
  makeActiveRawViewFromPlan,
  stableStringify,
  type ActiveRawPlan,
  type ActiveRawView,
  type ActiveRawViewChange,
} from "./active-view.ts";
import { MutationLog, type MutationLogEntry, type WorkerVersion } from "./mutation-log.ts";
import {
  changedFields,
  collectDependencyFields,
  diffVisibleRows,
  executeGroupedQueryEffect,
  executeMemoryQuery,
  isGroupedQuery,
  matchesFilter,
  type QueryExecutionResult,
  rowKeyForMemoryQuery,
  rowId,
} from "./query-engine.ts";

export type TopicWorkerMetrics = {
  readonly rows: number;
  readonly subscribers: number;
  readonly version: WorkerVersion;
  readonly queueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly totalSubscriptionLagVersions: number;
  readonly activePlanCount: number;
  readonly activeViewCount: number;
  readonly activePlanRows: number;
  readonly activePlanIndexEstimatedBytes: number;
  readonly activePlanBuildQueueDepth: number;
  readonly activePlanBuildingCount: number;
  readonly activePlanPendingCount: number;
  readonly activePlanBuildMs: number;
  readonly activePlanBuildMsTotal: number;
  readonly activePlanBuildMsMax: number;
  readonly activePlanFallbackCount: number;
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
  pendingLagVersions: bigint;
  activeView?: ActiveRawView | undefined;
  activePlanKey?: string | undefined;
  activePlanBuildKey?: string | undefined;
  activePlanFallback?: boolean | undefined;
  dirtyTargetVersion?: WorkerVersion | undefined;
  groupedRefreshScheduled?: boolean | undefined;
  groupedRefreshInFlight?: boolean | undefined;
};

type MaterializedSubscriptionChange = {
  readonly operations: readonly DeltaOperation<RuntimeRow>[];
  readonly nextRows?: readonly RuntimeRow[] | undefined;
  readonly totalRows: number;
};

type ActiveRawPlanEntry = {
  readonly plan: ActiveRawPlan;
  readonly buildMs: number;
  subscribers: number;
};

type ActivePlanBuildEntry = {
  readonly key: string;
  readonly query: RuntimeRawQuery;
  readonly requestIds: Set<string>;
  state: "queued" | "building";
};

type ActivePlanBuildSnapshot = {
  readonly key: string;
  readonly query: RuntimeRawQuery;
  readonly rows: readonly RuntimeRow[];
  readonly version: WorkerVersion;
  readonly remainingEstimatedBytes: number | undefined;
};

type GroupedRefreshSnapshot = {
  readonly key: string;
  readonly requestId: string;
  readonly requestIds: readonly string[];
  readonly query: RuntimeGroupedQuery;
  readonly rows: readonly RuntimeRow[];
  readonly version: WorkerVersion;
};

type GroupedRefreshEntry = {
  readonly key: string;
  readonly query: RuntimeGroupedQuery;
  readonly requestIds: Set<string>;
  readonly pendingRequestIds: Set<string>;
  state: "queued" | "running";
};

type ShutdownSubscription = {
  readonly requestId: string;
  readonly queue: ActiveSubscription["queue"];
};

type ShutdownState = {
  readonly subscriptions: readonly ShutdownSubscription[];
  readonly backgroundFibers: readonly Fiber.Fiber<void, unknown>[];
};

export type TopicWorkerCoreOptions = {
  readonly initialRows?: readonly RuntimeRow[] | undefined;
  readonly snapshotBackend?: SnapshotBackend | undefined;
  readonly maxQueueDepth?: number | undefined;
  readonly mutationLogSize?: number | undefined;
  readonly deltaCoalescing?: boolean | undefined;
  readonly maxActivePlans?: number | undefined;
  readonly maxActivePlanEstimatedBytes?: number | undefined;
  readonly activePlanBuildConcurrency?: number | undefined;
  readonly activePlanBuildChunkSize?: number | undefined;
  readonly groupedRefreshDebounceMs?: number | undefined;
  readonly groupedRefreshChunkSize?: number | undefined;
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
    const maxQueueDepth = options.maxQueueDepth ?? 100_000;
    const deltaCoalescing = options.deltaCoalescing ?? true;
    const maxActivePlans = options.maxActivePlans;
    const maxActivePlanEstimatedBytes = options.maxActivePlanEstimatedBytes;
    const activePlanBuildConcurrency = Math.max(1, options.activePlanBuildConcurrency ?? 1);
    const activePlanBuildChunkSize = options.activePlanBuildChunkSize;
    const groupedRefreshDebounceMs = Math.max(0, options.groupedRefreshDebounceMs ?? 50);
    const groupedRefreshChunkSize = options.groupedRefreshChunkSize;
    const mutationLog = new MutationLog(options.mutationLogSize ?? 10_000);
    const gate = yield* Semaphore.make(1);
    const activePlanBuildQueue = yield* Queue.unbounded<string>();
    const scope = yield* Effect.scope;
    const activePlanBuildFibers: Fiber.Fiber<void, unknown>[] = [];
    const groupedRefreshFibers = new Set<Fiber.Fiber<void, unknown>>();
    const subscriptions = new Map<string, ActiveSubscription>();
    const activePlans = new Map<string, ActiveRawPlanEntry>();
    const activePlanBuilds = new Map<string, ActivePlanBuildEntry>();
    const groupedRefreshes = new Map<string, GroupedRefreshEntry>();
    let rows: RuntimeRow[] = [];
    let idIndex = new Map<string | number, number>();
    let version: WorkerVersion = 0n;
    let lastActivePlanBuildMs = 0;
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
      for (const entry of activePlans.values()) {
        entry.plan.applyMutation(mutation);
      }
      yield* Effect.forEach(
        subscriptions.values(),
        (subscription) => {
          if (
            mutation.kind === "update" &&
            !hasDependency(subscription.dependencyFields, mutation.changedFields)
          ) {
            return Effect.void;
          }
          if (isGroupedQuery(subscription.query)) {
            return markGroupedSubscriptionDirty(subscription, toVersion);
          }
          if (isPendingActivePlanSubscription(subscription)) {
            return markPendingActivePlanSubscriptionDirty(subscription, toVersion, mutation);
          }
          const materialized =
            subscription.activeView === undefined
              ? materializeMemorySubscriptionChange(subscription)
              : materializeActiveViewSubscriptionChange(
                  subscription,
                  subscription.activeView.applyMutation(mutation),
                );
          if (materialized === undefined) {
            subscription.lastVersion = toVersion;
            return Effect.void;
          }
          return Effect.gen(function* () {
            const event: DeltaEvent<readonly RuntimeRow[]> = {
              type: "delta",
              requestId: subscription.requestId,
              ops: materialized.operations,
              meta: {
                fromVersion: fromVersion.toString(),
                toVersion: toVersion.toString(),
                totalRows: materialized.totalRows,
                serverTime: Date.now(),
              },
            };
            if (materialized.nextRows !== undefined) {
              subscription.lastRows = materialized.nextRows;
            }
            subscription.lastTotalRows = materialized.totalRows;
            subscription.lastVersion = toVersion;
            const offered = yield* offerDelta(subscription, event);
            if (!offered) {
              removeSubscription(subscription.requestId);
              yield* Queue.fail(
                subscription.queue,
                backpressureExceeded(
                  subscription.requestId,
                  `Subscription ${subscription.requestId} exceeded maxQueueDepth ${maxQueueDepth}`,
                ),
              );
              return;
            }
          });
        },
        { discard: true },
      );
    });

    const materializeMemorySubscriptionChange = (
      subscription: ActiveSubscription,
    ): MaterializedSubscriptionChange | undefined => {
      const next = memoryQuery(subscription.query);
      const operations = diffVisibleRows(
        subscription.lastRows,
        next.rows,
        rowKeyForMemoryQuery(subscription.query, idField),
      );
      return operations.length === 0 && subscription.lastTotalRows === next.totalRows
        ? undefined
        : {
            operations,
            nextRows: next.rows,
            totalRows: next.totalRows,
          };
    };

    const materializeActiveViewSubscriptionChange = (
      subscription: ActiveSubscription,
      change: ActiveRawViewChange,
    ): MaterializedSubscriptionChange | undefined => {
      switch (change.type) {
        case "noop":
          return undefined;
        case "totalRowsOnly":
          return {
            operations: [],
            totalRows: change.totalRows,
          };
        case "changed": {
          const operations = diffVisibleRows(
            subscription.lastRows,
            change.result.rows,
            rowKeyForMemoryQuery(subscription.query, idField),
          );
          return operations.length === 0 && subscription.lastTotalRows === change.result.totalRows
            ? undefined
            : {
                operations,
                nextRows: change.result.rows,
                totalRows: change.result.totalRows,
              };
        }
      }
    };

    const isPendingActivePlanSubscription = (subscription: ActiveSubscription): boolean =>
      subscription.activePlanBuildKey !== undefined &&
      subscription.activePlanFallback !== true &&
      subscription.activeView === undefined &&
      !isGroupedQuery(subscription.query);

    const markPendingActivePlanSubscriptionDirty = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      targetVersion: WorkerVersion,
      mutation: MutationLogEntry,
    ) {
      if (isGroupedQuery(subscription.query)) {
        return;
      }
      subscription.dirtyTargetVersion = targetVersion;
      subscription.lastTotalRows = pendingActivePlanTotalRows(
        subscription.lastTotalRows,
        subscription.query,
        mutation,
      );
      const offered = yield* offerStatusEvent(subscription, {
        type: "status",
        requestId: subscription.requestId,
        status: "stale",
        meta: {
          version: targetVersion.toString(),
          totalRows: subscription.lastTotalRows,
          serverTime: Date.now(),
        },
      });
      if (!offered) {
        removeSubscription(subscription.requestId);
        yield* Queue.fail(
          subscription.queue,
          backpressureExceeded(
            subscription.requestId,
            `Subscription ${subscription.requestId} exceeded maxQueueDepth ${maxQueueDepth}`,
          ),
        );
      }
    });

    const markGroupedSubscriptionDirty = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      targetVersion: WorkerVersion,
    ) {
      subscription.dirtyTargetVersion = targetVersion;
      const offered = yield* offerStatusEvent(subscription, {
        type: "status",
        requestId: subscription.requestId,
        status: "stale",
        meta: {
          version: targetVersion.toString(),
          totalRows: subscription.lastTotalRows,
          serverTime: Date.now(),
        },
      });
      if (!offered) {
        removeSubscription(subscription.requestId);
        yield* Queue.fail(
          subscription.queue,
          backpressureExceeded(
            subscription.requestId,
            `Subscription ${subscription.requestId} exceeded maxQueueDepth ${maxQueueDepth}`,
          ),
        );
        return;
      }
      if (subscription.groupedRefreshInFlight !== true) {
        yield* scheduleGroupedSubscriptionRefresh(subscription.requestId);
      }
    });

    function scheduleGroupedSubscriptionRefresh(requestId: string): Effect.Effect<void> {
      return Effect.gen(function* () {
        const subscription = subscriptions.get(requestId);
        if (
          subscription === undefined ||
          subscription.groupedRefreshScheduled === true ||
          subscription.groupedRefreshInFlight === true ||
          !isGroupedQuery(subscription.query)
        ) {
          return;
        }
        const key = groupedRefreshKey(subscription.query);
        subscription.groupedRefreshScheduled = true;
        const existing = groupedRefreshes.get(key);
        if (existing !== undefined) {
          if (existing.state === "queued") {
            existing.requestIds.add(requestId);
          } else {
            existing.pendingRequestIds.add(requestId);
          }
          return;
        }
        groupedRefreshes.set(key, {
          key,
          query: subscription.query,
          requestIds: new Set([requestId]),
          pendingRequestIds: new Set(),
          state: "queued",
        });
        let fiber: Fiber.Fiber<void, unknown> | undefined;
        const trackedRefresh = runGroupedRefresh(key).pipe(
          Effect.catchCause(() => gate.withPermit(resetGroupedRefresh(key))),
          Effect.ensuring(
            Effect.sync(() => {
              if (fiber !== undefined) {
                groupedRefreshFibers.delete(fiber);
              }
            }),
          ),
        );
        fiber = yield* Effect.forkIn(trackedRefresh, scope, { startImmediately: true });
        groupedRefreshFibers.add(fiber);
      });
    }

    const runGroupedRefresh = Effect.fn("view-server.worker.grouped.refresh")(function* (
      key: string,
    ) {
      if (groupedRefreshDebounceMs > 0) {
        yield* Effect.sleep(`${groupedRefreshDebounceMs} millis`);
      }
      const snapshot = yield* gate.withPermit(Effect.sync(() => beginGroupedRefresh(key)));
      if (snapshot === undefined) {
        return;
      }
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
        "view_server.worker_version": snapshot.version.toString(),
        "view_server.batch_size": snapshot.requestIds.length,
        "view_server.rows": snapshot.rows.length,
      });
      const result = yield* groupedRefreshQuery(snapshot);
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": result.rows.length,
        "view_server.total_rows": result.totalRows,
      });
      yield* gate.withPermit(installGroupedRefresh(snapshot, result));
    });

    const groupedRefreshQuery = Effect.fn("view-server.worker.grouped.snapshot")(function* (
      snapshot: GroupedRefreshSnapshot,
    ) {
      if (
        backend.supportsGroupedRefreshSnapshots === true &&
        backend.groupedRefreshSnapshot !== undefined
      ) {
        const accelerated = yield* backend
          .groupedRefreshSnapshot({
            query: snapshot.query,
            targetVersion: snapshot.version,
          })
          .pipe(
            Effect.map((candidate) =>
              candidate.backendVersion === snapshot.version
                ? Option.some({
                    rows: candidate.rows,
                    totalRows: candidate.totalRows,
                  })
                : Option.none<QueryExecutionResult>(),
            ),
            Effect.catchTag("SnapshotBackendFailed", () =>
              Effect.succeed(Option.none<QueryExecutionResult>()),
            ),
          );
        if (Option.isSome(accelerated)) {
          yield* Effect.annotateCurrentSpan({
            "view_server.backend_version": snapshot.version.toString(),
          });
          return accelerated.value;
        }
      }
      return yield* executeGroupedQueryEffect(snapshot.rows, snapshot.query, {
        literalStringFields,
        chunkSize: groupedRefreshChunkSize,
      });
    });

    function beginGroupedRefresh(key: string): GroupedRefreshSnapshot | undefined {
      const entry = groupedRefreshes.get(key);
      if (entry === undefined || entry.state === "running") {
        return undefined;
      }
      const requestIds = Array.from(entry.requestIds).filter((requestId) => {
        const subscription = subscriptions.get(requestId);
        return (
          subscription !== undefined &&
          isGroupedQuery(subscription.query) &&
          subscription.dirtyTargetVersion !== undefined
        );
      });
      entry.requestIds.clear();
      if (requestIds.length === 0) {
        groupedRefreshes.delete(key);
        return undefined;
      }
      entry.state = "running";
      for (const requestId of requestIds) {
        const subscription = subscriptions.get(requestId);
        if (subscription !== undefined) {
          subscription.groupedRefreshScheduled = false;
          subscription.groupedRefreshInFlight = true;
        }
      }
      return {
        key,
        requestId: requestIds[0] ?? key,
        requestIds,
        query: entry.query,
        rows: rows.slice(),
        version,
      };
    }

    function installGroupedRefresh(
      snapshot: GroupedRefreshSnapshot,
      result: QueryExecutionResult,
    ): Effect.Effect<void, ViewServerError> {
      return Effect.gen(function* () {
        const entry = groupedRefreshes.get(snapshot.key);
        groupedRefreshes.delete(snapshot.key);
        const reschedule = new Set(entry?.pendingRequestIds ?? []);
        for (const requestId of snapshot.requestIds) {
          const subscription = subscriptions.get(requestId);
          if (subscription === undefined) {
            continue;
          }
          subscription.groupedRefreshInFlight = false;
          if (!isGroupedQuery(subscription.query)) {
            continue;
          }
          if (
            subscription.dirtyTargetVersion !== undefined &&
            subscription.dirtyTargetVersion > snapshot.version
          ) {
            reschedule.add(subscription.requestId);
            continue;
          }
          yield* refreshSubscriptionSnapshot(subscription, result, snapshot.version);
        }
        yield* Effect.forEach(
          reschedule,
          (requestId) => {
            const subscription = subscriptions.get(requestId);
            if (subscription === undefined) {
              return Effect.void;
            }
            subscription.groupedRefreshScheduled = false;
            subscription.groupedRefreshInFlight = false;
            return subscription.dirtyTargetVersion === undefined
              ? Effect.void
              : scheduleGroupedSubscriptionRefresh(requestId);
          },
          { discard: true },
        );
      });
    }

    function resetGroupedRefresh(key: string): Effect.Effect<void> {
      return Effect.gen(function* () {
        const entry = groupedRefreshes.get(key);
        groupedRefreshes.delete(key);
        const requestIds = new Set([
          ...(entry?.requestIds ?? []),
          ...(entry?.pendingRequestIds ?? []),
        ]);
        for (const requestId of requestIds) {
          const subscription = subscriptions.get(requestId);
          if (subscription === undefined) {
            continue;
          }
          subscription.groupedRefreshScheduled = false;
          subscription.groupedRefreshInFlight = false;
          if (subscription.dirtyTargetVersion !== undefined) {
            yield* scheduleGroupedSubscriptionRefresh(requestId);
          }
        }
      });
    }

    const pendingActivePlanTotalRows = (
      previousTotalRows: number,
      query: RuntimeRawQuery,
      mutation: MutationLogEntry,
    ): number => {
      const beforeMatches =
        "before" in mutation && mutation.before !== undefined
          ? matchesFilter(mutation.before, query.where, { literalStringFields })
          : false;
      const afterMatches =
        "after" in mutation && mutation.after !== undefined
          ? matchesFilter(mutation.after, query.where, { literalStringFields })
          : false;
      if (beforeMatches === afterMatches) {
        return previousTotalRows;
      }
      return previousTotalRows + (afterMatches ? 1 : -1);
    };

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

    const queueDepth = Effect.fnUntraced(function* () {
      let total = 0;
      for (const subscription of subscriptions.values()) {
        total += yield* Queue.size(subscription.queue);
      }
      return total;
    });

    const activePlanStats = (): Pick<
      TopicWorkerMetrics,
      | "activePlanCount"
      | "activeViewCount"
      | "activePlanRows"
      | "activePlanIndexEstimatedBytes"
      | "activePlanBuildQueueDepth"
      | "activePlanBuildingCount"
      | "activePlanPendingCount"
      | "activePlanBuildMs"
      | "activePlanBuildMsTotal"
      | "activePlanBuildMsMax"
      | "activePlanFallbackCount"
    > => {
      let activeViewCount = 0;
      let activePlanRows = 0;
      let activePlanIndexEstimatedBytes = 0;
      let activePlanBuildMsTotal = 0;
      let activePlanBuildMsMax = 0;
      let activePlanFallbackCount = 0;
      let activePlanBuildQueueDepth = 0;
      let activePlanBuildingCount = 0;
      let activePlanPendingCount = 0;
      for (const entry of activePlans.values()) {
        activeViewCount += entry.subscribers;
        activePlanRows += entry.plan.totalRows();
        activePlanIndexEstimatedBytes += entry.plan.estimatedIndexBytes();
        activePlanBuildMsTotal += entry.buildMs;
        activePlanBuildMsMax = Math.max(activePlanBuildMsMax, entry.buildMs);
      }
      for (const build of activePlanBuilds.values()) {
        if (build.state === "queued") {
          activePlanBuildQueueDepth++;
        } else {
          activePlanBuildingCount++;
        }
        activePlanPendingCount += build.requestIds.size;
      }
      for (const subscription of subscriptions.values()) {
        if (subscription.activePlanFallback === true) {
          activePlanFallbackCount++;
        }
      }
      return {
        activePlanCount: activePlans.size,
        activeViewCount,
        activePlanRows,
        activePlanIndexEstimatedBytes,
        activePlanBuildQueueDepth,
        activePlanBuildingCount,
        activePlanPendingCount,
        activePlanBuildMs: lastActivePlanBuildMs,
        activePlanBuildMsTotal,
        activePlanBuildMsMax,
        activePlanFallbackCount,
      };
    };

    const subscriptionLagStats = Effect.fnUntraced(function* () {
      let maxLag = 0n;
      let totalLag = 0n;
      for (const subscription of subscriptions.values()) {
        const depth = yield* Queue.size(subscription.queue);
        const queuedLag = subscriptionLagVersionsForQueueDepth(
          depth,
          subscription.pendingLagVersions,
          deltaCoalescing,
        );
        const dirtyLag =
          subscription.dirtyTargetVersion !== undefined &&
          subscription.dirtyTargetVersion > subscription.lastVersion
            ? subscription.dirtyTargetVersion - subscription.lastVersion
            : 0n;
        const lag = queuedLag > dirtyLag ? queuedLag : dirtyLag;
        if (lag > maxLag) {
          maxLag = lag;
        }
        totalLag += lag;
      }
      return {
        maxSubscriptionLagVersions: bigintMetricNumber(maxLag),
        totalSubscriptionLagVersions: bigintMetricNumber(totalLag),
      };
    });

    const wouldExceedQueueLimit = (depth: number): boolean =>
      maxQueueDepth <= 0 ? depth >= 0 : depth >= maxQueueDepth;

    const isQueueAtLimit = (depth: number): boolean =>
      maxQueueDepth <= 0 ? depth > 0 : depth >= maxQueueDepth;

    const statusForPressure = (
      depth: number,
      planStats: ReturnType<typeof activePlanStats>,
    ): TopicWorkerMetrics["status"] =>
      status === "ready" &&
      (isQueueAtLimit(depth) ||
        planStats.activePlanFallbackCount > 0 ||
        isActivePlanLimitNear(planStats))
        ? "degraded"
        : status;

    const offerDelta = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      event: DeltaEvent<readonly RuntimeRow[]>,
    ) {
      if (!deltaCoalescing) {
        const depth = yield* Queue.size(subscription.queue);
        if (wouldExceedQueueLimit(depth)) {
          return false;
        }
        yield* Queue.offer(subscription.queue, event);
        return true;
      }
      const queued = yield* drainQueuedEvents(subscription.queue);
      const queuedPrefix = queued.filter((queuedEvent) => queuedEvent.type !== "delta");
      const queuedDeltas = queued.filter((queuedEvent) => queuedEvent.type === "delta");
      const nextQueued = coalescedQueueEvents(queuedPrefix, queuedDeltas, event);
      if (
        nextQueued.length > maxQueueDepth ||
        (queuedDeltas.length === 0 && wouldExceedQueueLimit(queuedPrefix.length))
      ) {
        yield* offerQueuedEvents(subscription.queue, queued);
        return false;
      }
      const coalesced = nextQueued[nextQueued.length - 1];
      if (
        coalesced?.type === "delta" &&
        maxQueueDepth > 0 &&
        deltaVersionSpan(coalesced) > BigInt(maxQueueDepth)
      ) {
        yield* offerQueuedEvents(subscription.queue, queued);
        return false;
      }
      yield* offerQueuedEvents(subscription.queue, nextQueued);
      subscription.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    });

    const offerStatusEvent = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      event: LiveQueryStatusEvent,
    ) {
      const queued = yield* drainQueuedEvents(subscription.queue);
      const nextQueued = [...queued.filter((queuedEvent) => queuedEvent.type !== "status"), event];
      if (nextQueued.length > maxQueueDepth) {
        yield* offerQueuedEvents(subscription.queue, queued);
        return false;
      }
      yield* offerQueuedEvents(subscription.queue, nextQueued);
      subscription.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    });

    const offerSnapshotEvent = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      event: SnapshotEvent<readonly RuntimeRow[]>,
    ) {
      const queued = yield* drainQueuedEvents(subscription.queue);
      const nextQueued = [...queued.filter((queuedEvent) => queuedEvent.type !== "status"), event];
      if (nextQueued.length > maxQueueDepth) {
        yield* offerQueuedEvents(subscription.queue, queued);
        return false;
      }
      yield* offerQueuedEvents(subscription.queue, nextQueued);
      subscription.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    });

    const drainQueuedEvents = Effect.fnUntraced(function* (
      queue: Queue.Queue<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError | Cause.Done>,
    ) {
      const events: SubscriptionEvent<readonly RuntimeRow[]>[] = [];
      let polling = true;
      while (polling) {
        const next = yield* Queue.poll(queue);
        if (Option.isSome(next)) {
          events.push(next.value);
        } else {
          polling = false;
        }
      }
      return events;
    });

    const offerQueuedEvents = Effect.fnUntraced(function* (
      queue: Queue.Queue<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError | Cause.Done>,
      events: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
    ) {
      yield* Effect.forEach(events, (event) => Queue.offer(queue, event), { discard: true });
    });

    const prepareActivePlan = Effect.fnUntraced(function* (subscription: ActiveSubscription) {
      if (isGroupedQuery(subscription.query)) {
        return;
      }
      const rawQuery = subscription.query;
      const key = activeRawPlanKey(rawQuery, idField);
      const existing = activePlans.get(key);
      if (existing !== undefined) {
        activateSubscriptionWithPlan(subscription, key, rawQuery, existing);
        return;
      }
      subscription.activePlanFallback = false;
      const pending = activePlanBuilds.get(key);
      if (pending !== undefined) {
        pending.requestIds.add(subscription.requestId);
        subscription.activePlanBuildKey = key;
        return;
      }
      if (wouldExceedActivePlanCountLimitForNewBuild()) {
        subscription.activePlanFallback = true;
        subscription.activePlanBuildKey = undefined;
        return;
      }
      const remainingBytes = activePlanEstimatedBytesRemaining();
      if (
        remainingBytes !== undefined &&
        estimateActiveRawPlanIndexBytes([], rawQuery, { literalStringFields }) > remainingBytes
      ) {
        subscription.activePlanFallback = true;
        subscription.activePlanBuildKey = undefined;
        return;
      }
      const build: ActivePlanBuildEntry = {
        key,
        query: rawQuery,
        requestIds: new Set([subscription.requestId]),
        state: "queued",
      };
      activePlanBuilds.set(key, build);
      subscription.activePlanBuildKey = key;
      yield* Queue.offer(activePlanBuildQueue, key);
    });

    const activePlanEstimatedBytes = (): number => {
      let total = 0;
      for (const entry of activePlans.values()) {
        total += entry.plan.estimatedIndexBytes();
      }
      return total;
    };

    const activePlanEstimatedBytesRemaining = (): number | undefined =>
      maxActivePlanEstimatedBytes === undefined
        ? undefined
        : maxActivePlanEstimatedBytes - activePlanEstimatedBytes();

    const wouldExceedActivePlanCountLimitForNewBuild = (): boolean =>
      maxActivePlans !== undefined && activePlans.size + activePlanBuilds.size >= maxActivePlans;

    const wouldExceedActivePlanCountLimitOnInstall = (): boolean =>
      maxActivePlans !== undefined && activePlans.size + activePlanBuilds.size > maxActivePlans;

    const wouldExceedActivePlanEstimatedBytesLimit = (newPlanBytes: number): boolean =>
      maxActivePlanEstimatedBytes !== undefined &&
      activePlanEstimatedBytes() + newPlanBytes > maxActivePlanEstimatedBytes;

    const isActivePlanLimitNear = (planStats: ReturnType<typeof activePlanStats>): boolean =>
      isNearLimit(
        planStats.activePlanCount +
          planStats.activePlanBuildQueueDepth +
          planStats.activePlanBuildingCount,
        maxActivePlans,
      ) || isNearLimit(planStats.activePlanIndexEstimatedBytes, maxActivePlanEstimatedBytes);

    const activePlanBuildSnapshot = (key: string): ActivePlanBuildSnapshot | undefined => {
      const build = activePlanBuilds.get(key);
      if (build === undefined || build.state === "building") {
        return undefined;
      }
      if (build.requestIds.size === 0) {
        activePlanBuilds.delete(key);
        return undefined;
      }
      build.state = "building";
      return {
        key: build.key,
        query: build.query,
        rows: rows.slice(),
        version,
        remainingEstimatedBytes: activePlanEstimatedBytesRemaining(),
      };
    };

    const activateSubscriptionWithPlan = (
      subscription: ActiveSubscription,
      key: string,
      query: RuntimeRawQuery,
      entry: ActiveRawPlanEntry,
    ): void => {
      if (subscription.activePlanKey === key) {
        return;
      }
      entry.subscribers++;
      subscription.activePlanKey = key;
      subscription.activePlanBuildKey = undefined;
      subscription.activePlanFallback = false;
      subscription.activeView = makeActiveRawViewFromPlan(entry.plan, query, idField);
    };

    const refreshSubscriptionSnapshot = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      result: QueryExecutionResult,
      targetVersion: WorkerVersion,
    ) {
      const event: SnapshotEvent<readonly RuntimeRow[]> = {
        type: "snapshot",
        requestId: subscription.requestId,
        rows: result.rows,
        meta: {
          version: targetVersion.toString(),
          totalRows: result.totalRows,
          serverTime: Date.now(),
        },
      };
      subscription.lastRows = result.rows;
      subscription.lastTotalRows = result.totalRows;
      subscription.lastVersion = targetVersion;
      subscription.dirtyTargetVersion = undefined;
      const offered = yield* offerSnapshotEvent(subscription, event);
      if (!offered) {
        removeSubscription(subscription.requestId);
        yield* Queue.fail(
          subscription.queue,
          backpressureExceeded(
            subscription.requestId,
            `Subscription ${subscription.requestId} exceeded maxQueueDepth ${maxQueueDepth}`,
          ),
        );
      }
    });

    const discardActivePlanBuild = Effect.fnUntraced(function* (key: string) {
      const build = activePlanBuilds.get(key);
      if (build === undefined) {
        return;
      }
      activePlanBuilds.delete(key);
      for (const requestId of build.requestIds) {
        const subscription = subscriptions.get(requestId);
        if (subscription?.activePlanBuildKey === key) {
          subscription.activePlanBuildKey = undefined;
          subscription.activePlanFallback = true;
          if (subscription.dirtyTargetVersion !== undefined) {
            yield* refreshSubscriptionSnapshot(
              subscription,
              memoryQuery(subscription.query),
              version,
            );
          }
        }
      }
    });

    const installActivePlanBuild = Effect.fnUntraced(function* (
      snapshot: ActivePlanBuildSnapshot,
      plan: ActiveRawPlan,
      buildMs: number,
    ) {
      const build = activePlanBuilds.get(snapshot.key);
      if (build === undefined || build.requestIds.size === 0) {
        activePlanBuilds.delete(snapshot.key);
        return;
      }
      if (!catchUpActivePlan(plan, snapshot.version)) {
        yield* discardActivePlanBuild(snapshot.key);
        return;
      }
      if (
        wouldExceedActivePlanCountLimitOnInstall() ||
        wouldExceedActivePlanEstimatedBytesLimit(plan.estimatedIndexBytes())
      ) {
        yield* discardActivePlanBuild(snapshot.key);
        return;
      }
      const entry: ActiveRawPlanEntry = {
        plan,
        buildMs,
        subscribers: 0,
      };
      activePlans.set(snapshot.key, entry);
      lastActivePlanBuildMs = buildMs;
      activePlanBuilds.delete(snapshot.key);
      for (const requestId of build.requestIds) {
        const subscription = subscriptions.get(requestId);
        if (
          subscription === undefined ||
          subscription.activePlanBuildKey !== snapshot.key ||
          isGroupedQuery(subscription.query)
        ) {
          continue;
        }
        activateSubscriptionWithPlan(subscription, snapshot.key, subscription.query, entry);
        if (
          subscription.dirtyTargetVersion !== undefined &&
          subscription.activeView !== undefined
        ) {
          yield* refreshSubscriptionSnapshot(
            subscription,
            subscription.activeView.snapshot(),
            version,
          );
        }
      }
      if (entry.subscribers <= 0) {
        activePlans.delete(snapshot.key);
      }
    });

    const catchUpActivePlan = (plan: ActiveRawPlan, builtVersion: WorkerVersion): boolean => {
      if (builtVersion === version) {
        return true;
      }
      if (builtVersion > version || !mutationLog.coversExclusive(builtVersion, version)) {
        return false;
      }
      for (const entry of mutationLog.entriesExclusive(builtVersion, version)) {
        plan.applyMutation(entry);
      }
      return true;
    };

    const runActivePlanBuild = Effect.fn("view-server.worker.active_plan.build")(function* (
      key: string,
    ) {
      const snapshot = yield* gate.withPermit(Effect.sync(() => activePlanBuildSnapshot(key)));
      if (snapshot === undefined) {
        return;
      }
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
        "view_server.worker_version": snapshot.version.toString(),
        "view_server.rows": snapshot.rows.length,
      });
      if (snapshot.remainingEstimatedBytes !== undefined) {
        const estimatedBytes = yield* estimateActiveRawPlanIndexBytesEffect(
          snapshot.rows,
          snapshot.query,
          { literalStringFields, buildChunkSize: activePlanBuildChunkSize },
          snapshot.remainingEstimatedBytes,
        );
        if (estimatedBytes > snapshot.remainingEstimatedBytes) {
          yield* gate.withPermit(discardActivePlanBuild(snapshot.key));
          return;
        }
      }
      const started = Date.now();
      const plan = yield* makeActiveRawPlanEffect(snapshot.rows, snapshot.query, idField, {
        literalStringFields,
        buildChunkSize: activePlanBuildChunkSize,
      });
      const buildMs = Date.now() - started;
      yield* Effect.annotateCurrentSpan({
        "view_server.active_plan_build_ms": buildMs,
        "view_server.rows": plan.totalRows(),
      });
      yield* gate.withPermit(installActivePlanBuild(snapshot, plan, buildMs));
    });

    const activePlanBuildLoop = Effect.fn("view-server.worker.active_plan.build_loop")(
      function* () {
        while (true) {
          const key = yield* Queue.take(activePlanBuildQueue);
          yield* runActivePlanBuild(key).pipe(
            Effect.catchCause(() => gate.withPermit(discardActivePlanBuild(key))),
          );
        }
      },
    );

    const releaseActivePlan = (key: string | undefined): void => {
      if (key === undefined) {
        return;
      }
      const entry = activePlans.get(key);
      if (entry === undefined) {
        return;
      }
      entry.subscribers--;
      if (entry.subscribers <= 0) {
        activePlans.delete(key);
      }
    };

    const releaseActivePlanBuild = (key: string | undefined, requestId: string): void => {
      if (key === undefined) {
        return;
      }
      const build = activePlanBuilds.get(key);
      if (build === undefined) {
        return;
      }
      build.requestIds.delete(requestId);
      if (build.requestIds.size === 0) {
        activePlanBuilds.delete(key);
      }
    };

    const releaseGroupedRefresh = (requestId: string): void => {
      for (const [key, entry] of groupedRefreshes) {
        entry.requestIds.delete(requestId);
        entry.pendingRequestIds.delete(requestId);
        if (
          entry.state === "queued" &&
          entry.requestIds.size === 0 &&
          entry.pendingRequestIds.size === 0
        ) {
          groupedRefreshes.delete(key);
        }
      }
    };

    const removeSubscription = (requestId: string): ActiveSubscription | undefined => {
      const subscription = subscriptions.get(requestId);
      if (subscription === undefined) {
        return undefined;
      }
      subscriptions.delete(requestId);
      releaseActivePlan(subscription.activePlanKey);
      releaseActivePlanBuild(subscription.activePlanBuildKey, requestId);
      releaseGroupedRefresh(requestId);
      subscription.pendingLagVersions = 0n;
      subscription.dirtyTargetVersion = undefined;
      subscription.groupedRefreshScheduled = false;
      subscription.groupedRefreshInFlight = false;
      return subscription;
    };

    const removeSubscriptionForQueue = (
      requestId: string,
      queue: ActiveSubscription["queue"],
    ): ActiveSubscription | undefined => {
      const subscription = subscriptions.get(requestId);
      return subscription?.queue === queue ? removeSubscription(requestId) : undefined;
    };

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
        rows.push({ ...decoded });
        idIndex.set(id, rows.length - 1);
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
      metrics: Effect.fn("view-server.worker.metrics")(function* () {
        const depth = yield* queueDepth();
        const lagStats = yield* subscriptionLagStats();
        const planStats = activePlanStats();
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
          "view_server.rows": rows.length,
        });
        return {
          rows: rows.length,
          subscribers: subscriptions.size,
          version,
          queueDepth: depth,
          maxSubscriptionLagVersions: lagStats.maxSubscriptionLagVersions,
          totalSubscriptionLagVersions: lagStats.totalSubscriptionLagVersions,
          activePlanCount: planStats.activePlanCount,
          activeViewCount: planStats.activeViewCount,
          activePlanRows: planStats.activePlanRows,
          activePlanIndexEstimatedBytes: planStats.activePlanIndexEstimatedBytes,
          activePlanBuildQueueDepth: planStats.activePlanBuildQueueDepth,
          activePlanBuildingCount: planStats.activePlanBuildingCount,
          activePlanPendingCount: planStats.activePlanPendingCount,
          activePlanBuildMs: planStats.activePlanBuildMs,
          activePlanBuildMsTotal: planStats.activePlanBuildMsTotal,
          activePlanBuildMsMax: planStats.activePlanBuildMsMax,
          activePlanFallbackCount: planStats.activePlanFallbackCount,
          status: statusForPressure(depth, planStats),
        };
      })(),

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

      subscribe: (requestId, query) => {
        return Stream.callback<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>((queue) =>
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
                pendingLagVersions: 0n,
              };
              const previous = removeSubscription(requestId);
              if (previous !== undefined) {
                yield* Queue.end(previous.queue);
              }
              subscriptions.set(requestId, active);
              yield* Queue.offer(queue, snapshot);
              yield* prepareActivePlan(active);
              yield* Effect.addFinalizer(() =>
                Effect.fn("view-server.worker.subscribe.finalize")(function* () {
                  yield* Effect.annotateCurrentSpan({
                    "view_server.request_id": requestId,
                    "view_server.subscription_id": requestId,
                    "view_server.topic": topic,
                  });
                  removeSubscriptionForQueue(requestId, queue);
                })(),
              );
            })(),
          ),
        );
      },

      unsubscribe: (requestId) =>
        gate.withPermit(
          Effect.fn("view-server.worker.unsubscribe")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.request_id": requestId,
              "view_server.subscription_id": requestId,
              "view_server.topic": topic,
            });
            const subscription = removeSubscription(requestId);
            if (subscription === undefined) {
              return;
            }
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
            const lastIndex = rows.length - 1;
            const last = rows[lastIndex];
            rows.pop();
            idIndex.delete(id);
            if (index !== lastIndex && last !== undefined) {
              rows[index] = last;
              const lastId = last[idField];
              if (typeof lastId === "string" || typeof lastId === "number") {
                idIndex.set(lastId, index);
              }
            }
            yield* appendMutation({
              kind: "delete",
              id,
              before,
              changedFields: new Set(Object.keys(before)),
            });
          })(),
        ),

      getRowsForTest: Effect.sync(() => rows.map((row) => ({ ...row }))),

      shutdown: Effect.fn("view-server.worker.shutdown")(function* () {
        const shutdownState = yield* gate.withPermit(
          Effect.sync((): ShutdownState => {
            status = "stopping";
            const shutdownSubscriptions = Array.from(
              subscriptions.values(),
              (subscription): ShutdownSubscription => ({
                requestId: subscription.requestId,
                queue: subscription.queue,
              }),
            );
            const backgroundFibers = [...activePlanBuildFibers, ...groupedRefreshFibers];
            subscriptions.clear();
            activePlans.clear();
            activePlanBuilds.clear();
            groupedRefreshes.clear();
            groupedRefreshFibers.clear();
            return {
              subscriptions: shutdownSubscriptions,
              backgroundFibers,
            };
          }),
        );
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
          "view_server.worker_version": version.toString(),
        });
        yield* Effect.forEach(
          shutdownState.subscriptions,
          (subscription) =>
            Queue.fail(
              subscription.queue,
              serverShutdown("Topic worker is shutting down", topic, subscription.requestId),
            ),
          { discard: true },
        );
        yield* Effect.forEach(shutdownState.backgroundFibers, (fiber) => Fiber.interrupt(fiber), {
          discard: true,
        }).pipe(Effect.ignore);
        yield* backend.close();
      })(),
    };

    for (const row of options.initialRows ?? []) {
      const decoded = yield* decodeRow(row);
      rows.push(decoded);
    }
    replaceIndexes();
    yield* backend.init({
      topic,
      idField,
      rows: rows.map((row) => ({ row, version })),
      version,
      literalStringFields,
    });
    const buildFibers = yield* Effect.forEach(
      Array.from({ length: activePlanBuildConcurrency }, (_, index) => index),
      () => Effect.forkIn(activePlanBuildLoop(), scope, { startImmediately: true }),
    );
    activePlanBuildFibers.push(...buildFibers);

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

function groupedRefreshKey(query: RuntimeGroupedQuery): string {
  return stableStringify(query);
}

function coalescedQueueEvents(
  prefix: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
  queuedDeltas: readonly DeltaEvent<readonly RuntimeRow[]>[],
  nextDelta: DeltaEvent<readonly RuntimeRow[]>,
): readonly SubscriptionEvent<readonly RuntimeRow[]>[] {
  return [...prefix, coalesceDeltas([...queuedDeltas, nextDelta])];
}

function coalesceDeltas(
  deltas: readonly DeltaEvent<readonly RuntimeRow[]>[],
): DeltaEvent<readonly RuntimeRow[]> {
  const first = deltas[0];
  const last = deltas[deltas.length - 1];
  if (first === undefined || last === undefined) {
    throw new Error("Cannot coalesce an empty delta list");
  }
  return {
    type: "delta",
    requestId: last.requestId,
    ops: deltas.flatMap((delta) => delta.ops),
    meta: {
      fromVersion: first.meta.fromVersion,
      toVersion: last.meta.toVersion,
      totalRows: last.meta.totalRows,
      serverTime: last.meta.serverTime,
    },
  };
}

function deltaVersionSpan(delta: DeltaEvent<readonly RuntimeRow[]>): bigint {
  return BigInt(delta.meta.toVersion) - BigInt(delta.meta.fromVersion);
}

function queueEventsVersionLag(
  events: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
): bigint {
  return events.reduce(
    (total, event) => (event.type === "delta" ? total + deltaVersionSpan(event) : total),
    0n,
  );
}

export function subscriptionLagVersionsForQueueDepth(
  queueDepth: number,
  pendingLagVersions: bigint,
  deltaCoalescing: boolean,
): bigint {
  if (queueDepth <= 0) {
    return 0n;
  }
  return deltaCoalescing ? pendingLagVersions : BigInt(queueDepth);
}

function bigintMetricNumber(value: bigint): number {
  const max = BigInt(Number.MAX_SAFE_INTEGER);
  return value > max ? Number.MAX_SAFE_INTEGER : Number(value);
}

function isNearLimit(value: number, limit: number | undefined): boolean {
  return limit !== undefined && limit > 0 && value / limit >= 0.8;
}

function replayMutations(
  baseRows: readonly RuntimeRow[],
  entries: readonly MutationLogEntry[],
  idField: string,
): readonly RuntimeRow[] {
  let replayRows = baseRows.map((row) => ({ ...row }));
  let replayIndex = new Map<string | number, number>();
  const rebuildIndex = () => {
    replayIndex = new Map();
    replayRows.forEach((row, index) => {
      const id = rowId(row, idField);
      if (id !== undefined) {
        replayIndex.set(id, index);
      }
    });
  };
  rebuildIndex();
  for (const entry of entries) {
    if (entry.kind === "delete") {
      replayRows = replayRows.filter((row) => rowId(row, idField) !== entry.id);
      rebuildIndex();
      continue;
    }
    const next = { ...entry.after };
    const index = replayIndex.get(entry.id);
    if (index === undefined) {
      replayRows = [...replayRows, next];
      rebuildIndex();
    } else {
      replayRows[index] = next;
    }
  }
  return replayRows;
}
