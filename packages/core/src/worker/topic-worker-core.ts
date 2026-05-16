import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
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
  RuntimeRawQuery,
  RuntimeRow,
  SnapshotEvent,
  SubscriptionEvent,
} from "../protocol/index.ts";
import { rowKeyByField } from "../protocol/index.ts";
import type { SnapshotBackend } from "../snapshot/index.ts";
import { createMemorySnapshotBackend } from "../snapshot/index.ts";
import {
  estimateActiveRawPlanIndexBytesEffect,
  makeActiveRawPlanEffect,
  type ActiveRawPlan,
} from "./active-view.ts";
import { ActivePlanCoordinator, type ActivePlanBuildSnapshot } from "./active-plan-coordinator.ts";
import type { ActiveRawViewChange } from "./active-view.ts";
import { makeFanoutQueue } from "./fanout-queue.ts";
import {
  GroupedRefreshCoordinator,
  type GroupedRefreshSnapshot,
} from "./grouped-refresh-coordinator.ts";
import { type MutationLogEntry, type WorkerVersion } from "./mutation-log.ts";
import { MutationStore, type MutationStoreChange } from "./mutation-store.ts";
import {
  collectDependencyFields,
  diffVisibleRows,
  executeGroupedQueryEffect,
  executeMemoryQuery,
  groupedQueryOrderBy,
  isGroupedQuery,
  matchesFilter,
  normalizeLimit,
  normalizeOffset,
  RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT,
  type QueryExecutionResult,
  rowKeyForMemoryQuery,
  stableSortRows,
} from "./query-engine.ts";
import { makeIncrementalGroupedAccumulator } from "./grouped-accumulator.ts";
import { planQuery, type QueryPlan, type QueryOperation } from "./query-planner.ts";
import { makeSnapshotReconciler } from "./snapshot-reconciler.ts";
import {
  SubscriptionRegistry,
  type ActiveSubscription,
  type ShutdownSubscription,
} from "./subscription-registry.ts";
import { WorkerHealthProjection, type TopicWorkerMetrics } from "./worker-health-projection.ts";

export type { TopicWorkerMetrics } from "./worker-health-projection.ts";

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

type MaterializedSubscriptionChange = {
  readonly operations: readonly DeltaOperation<RuntimeRow>[];
  readonly nextRows?: readonly RuntimeRow[] | undefined;
  readonly totalRows: number;
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
  readonly activePlanAutoBuildMaxRows?: number | undefined;
  readonly activePlanBuildConcurrency?: number | undefined;
  readonly activePlanBuildChunkSize?: number | undefined;
  readonly groupedAccumulatorMaxRows?: number | undefined;
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
    const fanoutQueue = makeFanoutQueue({ maxQueueDepth, deltaCoalescing });
    const maxActivePlans = options.maxActivePlans;
    const maxActivePlanEstimatedBytes = options.maxActivePlanEstimatedBytes;
    const activePlanAutoBuildMaxRows = options.activePlanAutoBuildMaxRows ?? 1_000_000;
    const activePlanBuildConcurrency = Math.max(1, options.activePlanBuildConcurrency ?? 1);
    const activePlanBuildChunkSize = options.activePlanBuildChunkSize;
    const groupedAccumulatorMaxRows = Math.max(0, options.groupedAccumulatorMaxRows ?? 0);
    const groupedRefreshDebounceMs = Math.max(0, options.groupedRefreshDebounceMs ?? 50);
    const groupedRefreshChunkSize = options.groupedRefreshChunkSize;
    const mutationStore = new MutationStore({
      idField,
      mutationLogSize: options.mutationLogSize ?? 10_000,
    });
    const gate = yield* Semaphore.make(1);
    const activePlanBuildQueue = yield* Queue.unbounded<string>();
    const scope = yield* Effect.scope;
    const activePlanBuildFibers: Fiber.Fiber<void, unknown>[] = [];
    const groupedRefreshFibers = new Set<Fiber.Fiber<void, unknown>>();
    const subscriptions = new SubscriptionRegistry({
      releaseActivePlan: (key) => releaseActivePlan(key),
      releaseActivePlanBuild: (key, requestId) => releaseActivePlanBuild(key, requestId),
      releaseGroupedRefresh: (requestId) => releaseGroupedRefresh(requestId),
    });
    const activePlanCoordinator = new ActivePlanCoordinator({
      idField,
      literalStringFields,
      maxActivePlans,
      maxActivePlanEstimatedBytes,
      activePlanAutoBuildMaxRows,
    });
    const groupedRefreshCoordinator = new GroupedRefreshCoordinator();
    const healthProjection = new WorkerHealthProjection({
      topic,
      rows: () => mutationStore.rows().length,
      version: () => mutationStore.version(),
      subscriptionCount: () => subscriptions.size,
      subscriptions: () => subscriptions.values(),
      activePlanMetrics: () => activePlanCoordinator.metrics(subscriptions.values()),
      activePlanLimitNear: (metrics) => activePlanCoordinator.isLimitNear(metrics),
      queueAtLimit: (depth) => fanoutQueue.isQueueAtLimit(depth),
      lagForDepth: (depth, pendingLagVersions) =>
        fanoutQueue.lagForDepth(depth, pendingLagVersions),
      backendHealth: () => backend.health ?? Effect.succeed({ status: "stopped" }),
    });

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

    const memoryQuery = (query: RuntimeQuery) =>
      executeMemoryQuery(mutationStore.rows(), query, idField, { literalStringFields });

    const snapshotReconciler = makeSnapshotReconciler({
      topic,
      idField,
      backend,
      rows: () => mutationStore.rows(),
      canReplay: (fromVersion, toVersion) => mutationStore.canReplay(fromVersion, toVersion),
      replayRowsFrom: (baseRows, fromVersion, toVersion) =>
        mutationStore.replayRowsFrom(baseRows, fromVersion, toVersion),
      queryOptions: { literalStringFields },
    });

    const planWorkerQuery = (operation: QueryOperation, query: RuntimeQuery): QueryPlan =>
      planQuery({
        operation,
        query,
        rowCount: mutationStore.rows().length,
        rawWindowOptimizationLimit: RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT,
        activePlanAutoBuildMaxRows,
        maxActivePlans,
        groupedAccumulatorMaxRows,
        supportsGroupedRefreshSnapshots:
          backend.supportsGroupedRefreshSnapshots === true &&
          backend.groupedRefreshSnapshot !== undefined,
      });

    const fencedQuery = Effect.fn("view-server.worker.snapshot.query")(function* (
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
        "view_server.worker_version": mutationStore.version().toString(),
      });
      const targetVersion = mutationStore.version();
      const plan = planWorkerQuery("query", query);
      yield* Effect.annotateCurrentSpan({
        "view_server.query_strategy": plan.strategy,
      });
      const result = yield* snapshotReconciler.query({ query, targetVersion });
      if (result.backendFailed) {
        healthProjection.markDegraded();
      } else {
        healthProjection.markReadyIfDegraded();
      }
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
      activePlanCoordinator.applyMutation(mutation);
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
            if (subscription.groupedAccumulator !== undefined) {
              return fanoutGroupedAccumulatorSubscription(
                subscription,
                fromVersion,
                toVersion,
                mutation,
              );
            }
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
            const offered = yield* fanoutQueue.offerDelta(subscription.queue, subscription, event);
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
      (subscription.activePlanBuildKey !== undefined ||
        subscription.activePlanAutoBuildSkipped === true) &&
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
      const offered = yield* fanoutQueue.offerStatus(subscription.queue, subscription, {
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
      const offered = yield* fanoutQueue.offerStatus(subscription.queue, subscription, {
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

    const fanoutGroupedAccumulatorSubscription = Effect.fnUntraced(function* (
      subscription: ActiveSubscription,
      fromVersion: WorkerVersion,
      toVersion: WorkerVersion,
      mutation: MutationLogEntry,
    ) {
      if (!isGroupedQuery(subscription.query) || subscription.groupedAccumulator === undefined) {
        return;
      }
      const beforeMatches =
        mutation.before !== undefined &&
        matchesFilter(mutation.before, subscription.query.where, { literalStringFields });
      const afterMatches =
        mutation.after !== undefined &&
        matchesFilter(mutation.after, subscription.query.where, { literalStringFields });
      if (!beforeMatches && !afterMatches) {
        subscription.lastVersion = toVersion;
        return;
      }
      if (beforeMatches && afterMatches) {
        subscription.groupedAccumulator.applyMutation(mutation);
      } else if (beforeMatches && mutation.before !== undefined) {
        subscription.groupedAccumulator.applyMutation({
          version: mutation.version,
          kind: "delete",
          id: mutation.id,
          before: mutation.before,
          changedFields: mutation.changedFields,
        });
      } else if (afterMatches && mutation.after !== undefined) {
        subscription.groupedAccumulator.applyMutation({
          version: mutation.version,
          kind: "insert",
          id: mutation.id,
          after: mutation.after,
          changedFields: mutation.changedFields,
        });
      }
      const next = groupedAccumulatorQueryResult(subscription);
      const operations = diffVisibleRows(
        subscription.lastRows,
        next.rows,
        rowKeyForMemoryQuery(subscription.query, idField),
      );
      if (operations.length === 0 && subscription.lastTotalRows === next.totalRows) {
        subscription.lastVersion = toVersion;
        return;
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
      const offered = yield* fanoutQueue.offerDelta(subscription.queue, subscription, event);
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

    const groupedAccumulatorQueryResult = (
      subscription: ActiveSubscription,
    ): QueryExecutionResult => {
      if (!isGroupedQuery(subscription.query) || subscription.groupedAccumulator === undefined) {
        return memoryQuery(subscription.query);
      }
      const sorted = stableSortRows(
        subscription.groupedAccumulator.groupedRows(),
        groupedQueryOrderBy(subscription.query),
      );
      const offset = normalizeOffset(subscription.query.offset);
      const limit = normalizeLimit(subscription.query.limit);
      return {
        rows: sorted.slice(offset, offset + limit),
        totalRows: sorted.length,
      };
    };

    function scheduleGroupedSubscriptionRefresh(requestId: string): Effect.Effect<void> {
      return Effect.gen(function* () {
        const subscription = subscriptions.get(requestId);
        if (subscription === undefined) {
          return;
        }
        const decision = groupedRefreshCoordinator.schedule(subscription);
        if (decision.type === "none") {
          return;
        }
        let fiber: Fiber.Fiber<void, unknown> | undefined;
        const trackedRefresh = runGroupedRefresh(decision.key).pipe(
          Effect.catchCause(() => gate.withPermit(resetGroupedRefresh(decision.key))),
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
      const snapshot = yield* gate.withPermit(
        Effect.sync(() =>
          groupedRefreshCoordinator.begin({
            key,
            subscriptions,
            rows: mutationStore.rows().slice(),
            version: mutationStore.version(),
          }),
        ),
      );
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
            Effect.tap(() =>
              Effect.sync(() => {
                healthProjection.markReadyIfDegraded();
              }),
            ),
            Effect.map((candidate) =>
              candidate.backendVersion === snapshot.version
                ? Option.some({
                    rows: candidate.rows,
                    totalRows: candidate.totalRows,
                  })
                : Option.none<QueryExecutionResult>(),
            ),
            Effect.catchTag("SnapshotBackendFailed", () =>
              Effect.sync(() => {
                healthProjection.markDegraded();
                return Option.none<QueryExecutionResult>();
              }),
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

    function installGroupedRefresh(
      snapshot: GroupedRefreshSnapshot,
      result: QueryExecutionResult,
    ): Effect.Effect<void, ViewServerError> {
      return Effect.gen(function* () {
        const install = groupedRefreshCoordinator.install({
          snapshot,
          result,
          subscriptions,
        });
        for (const refresh of install.refreshes) {
          yield* refreshSubscriptionSnapshot(refresh.subscription, refresh.result, refresh.version);
        }
        yield* Effect.forEach(
          install.rescheduleRequestIds,
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
        for (const requestId of groupedRefreshCoordinator.reset({ key, subscriptions })) {
          yield* scheduleGroupedSubscriptionRefresh(requestId);
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

    const prepareActivePlan = Effect.fnUntraced(function* (subscription: ActiveSubscription) {
      if (isGroupedQuery(subscription.query)) {
        prepareGroupedAccumulator(subscription);
        return;
      }
      const decision = activePlanCoordinator.prepareSubscription(
        subscription,
        subscription.query,
        mutationStore.rows().length,
      );
      if (decision.type === "queued") {
        yield* Queue.offer(activePlanBuildQueue, decision.key);
      }
    });

    const prepareGroupedAccumulator = (subscription: ActiveSubscription): void => {
      if (!isGroupedQuery(subscription.query)) {
        return;
      }
      const query = subscription.query;
      const plan = planWorkerQuery("subscription", query);
      if (plan.strategy !== "grouped_incremental_accumulator_eligible") {
        return;
      }
      const accumulator = makeIncrementalGroupedAccumulator({
        rows: mutationStore
          .rows()
          .filter((row) => matchesFilter(row, query.where, { literalStringFields })),
        query,
        idOf: (row) => rowKeyByField(row, idField),
      });
      if (accumulator !== undefined) {
        subscription.groupedAccumulator = accumulator;
      }
    };

    const activePlanBuildSnapshot = (key: string): ActivePlanBuildSnapshot | undefined => {
      return activePlanCoordinator.beginBuildSnapshot({
        key,
        rows: mutationStore.rows().slice(),
        version: mutationStore.version(),
      });
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
      const offered = yield* fanoutQueue.offerSnapshot(subscription.queue, subscription, event);
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
      const dirtySubscriptions = activePlanCoordinator.discardBuild(key, subscriptions.values());
      for (const subscription of dirtySubscriptions) {
        yield* refreshSubscriptionSnapshot(
          subscription,
          memoryQuery(subscription.query),
          mutationStore.version(),
        );
      }
    });

    const installActivePlanBuild = Effect.fnUntraced(function* (
      snapshot: ActivePlanBuildSnapshot,
      plan: ActiveRawPlan,
      buildMs: number,
    ) {
      if (!catchUpActivePlan(plan, snapshot.version)) {
        yield* discardActivePlanBuild(snapshot.key);
        return;
      }
      if (!activePlanCoordinator.canInstallPlan(plan)) {
        yield* discardActivePlanBuild(snapshot.key);
        return;
      }
      const dirtySubscriptions = activePlanCoordinator.installBuild({
        snapshot,
        plan,
        buildMs,
        subscriptions: subscriptions.values(),
        isGrouped: isGroupedQuery,
      });
      for (const subscription of dirtySubscriptions) {
        if (subscription.activeView !== undefined) {
          yield* refreshSubscriptionSnapshot(
            subscription,
            subscription.activeView.snapshot(),
            mutationStore.version(),
          );
        }
      }
    });

    const catchUpActivePlan = (plan: ActiveRawPlan, builtVersion: WorkerVersion): boolean => {
      if (builtVersion === mutationStore.version()) {
        return true;
      }
      if (
        builtVersion > mutationStore.version() ||
        !mutationStore.canReplay(builtVersion, mutationStore.version())
      ) {
        return false;
      }
      for (const entry of mutationStore.entriesExclusive(builtVersion, mutationStore.version())) {
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
      activePlanCoordinator.releasePlan(key);
    };

    const releaseActivePlanBuild = (key: string | undefined, requestId: string): void => {
      activePlanCoordinator.releaseBuild(key, requestId);
    };

    const releaseGroupedRefresh = (requestId: string): void => {
      groupedRefreshCoordinator.release(requestId);
    };

    const removeSubscription = (requestId: string): ActiveSubscription | undefined => {
      return subscriptions.remove(requestId);
    };

    const removeSubscriptionForQueue = (
      requestId: string,
      queue: ActiveSubscription["queue"],
    ): ActiveSubscription | undefined => {
      return subscriptions.removeForQueue(requestId, queue);
    };

    const persistAndFanoutMutation = Effect.fnUntraced(function* (change: MutationStoreChange) {
      yield* backend
        .applyBatch({
          mutations: [change.entry],
          highestVersion: change.toVersion,
        })
        .pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              healthProjection.markReadyIfDegraded();
            }),
          ),
          Effect.catchTag("SnapshotBackendFailed", () =>
            Effect.sync(() => {
              healthProjection.markDegraded();
            }),
          ),
          Effect.forkIn(scope),
        );
      yield* fanout(change.fromVersion, change.toVersion, change.entry);
    });

    const publishDecoded = Effect.fnUntraced(function* (decoded: RuntimeRow) {
      const id = yield* ensureId(decoded);
      yield* persistAndFanoutMutation(mutationStore.publish(decoded, id));
    });

    const worker: TopicWorkerCore = {
      topic,
      idField,
      version: Effect.sync(() => mutationStore.version()),
      metrics: healthProjection.metrics(),

      query: (query) =>
        gate.withPermit(
          Effect.fn("view-server.worker.query")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
              "view_server.worker_version": mutationStore.version().toString(),
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
                "view_server.worker_version": mutationStore.version().toString(),
              });
              const { snapshot, targetVersion, totalRows } = yield* fencedSnapshot(
                requestId,
                query,
              );
              const plan = planWorkerQuery("subscription", query);
              yield* Effect.annotateCurrentSpan({
                "view_server.query_strategy": plan.strategy,
              });
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
              subscriptions.replace(active);
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
            const before = mutationStore.rowById(id);
            if (before === undefined) {
              return yield* Effect.fail(
                invalidPublish(topic, `Cannot deltaPublish missing row ${String(id)}`),
              );
            }
            const merged = { ...before, ...patch };
            const decoded = yield* decodeRow(merged);
            const change = mutationStore.updateExisting(id, decoded);
            if (change !== undefined) {
              yield* persistAndFanoutMutation(change);
            }
          })(),
        ),

      deleteById: (id) =>
        gate.withPermit(
          Effect.fnUntraced(function* () {
            const change = mutationStore.deleteById(id);
            if (change !== undefined) {
              yield* persistAndFanoutMutation(change);
            }
          })(),
        ),

      getRowsForTest: Effect.sync(() => mutationStore.snapshotRows()),

      shutdown: Effect.fn("view-server.worker.shutdown")(function* () {
        const shutdownState = yield* gate.withPermit(
          Effect.sync((): ShutdownState => {
            healthProjection.markStopping();
            const shutdownSubscriptions = subscriptions.clearForShutdown();
            const backgroundFibers = [...activePlanBuildFibers, ...groupedRefreshFibers];
            activePlanCoordinator.clear();
            groupedRefreshCoordinator.clear();
            groupedRefreshFibers.clear();
            return {
              subscriptions: shutdownSubscriptions,
              backgroundFibers,
            };
          }),
        );
        yield* Effect.annotateCurrentSpan({
          "view_server.topic": topic,
          "view_server.worker_version": mutationStore.version().toString(),
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

    const initialRows: RuntimeRow[] = [];
    for (const row of options.initialRows ?? []) {
      const decoded = yield* decodeRow(row);
      initialRows.push(decoded);
    }
    mutationStore.loadInitialRows(initialRows);
    yield* backend.init({
      topic,
      idField,
      rows: mutationStore.rows().map((row) => ({ row, version: mutationStore.version() })),
      version: mutationStore.version(),
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
