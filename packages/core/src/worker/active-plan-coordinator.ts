import type { RuntimeQuery, RuntimeRawQuery, RuntimeRow } from "../protocol/index.ts";
import { activeRawPlanKey } from "./active-raw-plan-key.ts";
import {
  estimateActiveRawPlanIndexBytes,
  makeActiveRawViewFromPlan,
  type ActiveRawPlan,
} from "./active-view.ts";
import type { MutationLogEntry, WorkerVersion } from "./mutation-log.ts";
import type { ActiveSubscription } from "./subscription-registry.ts";

export type ActiveRawPlanEntry = {
  readonly plan: ActiveRawPlan;
  readonly buildMs: number;
  subscribers: number;
};

export type ActivePlanBuildEntry = {
  readonly key: string;
  readonly query: RuntimeRawQuery;
  readonly requestIds: Set<string>;
  state: "queued" | "building";
};

export type ActivePlanBuildSnapshot = {
  readonly key: string;
  readonly query: RuntimeRawQuery;
  readonly rows: readonly RuntimeRow[];
  readonly version: WorkerVersion;
  readonly remainingEstimatedBytes: number | undefined;
};

export type ActivePlanCoordinatorMetrics = {
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
  readonly activePlanAutoBuildSkippedCount: number;
};

export type ActivePlanPrepareDecision =
  | { readonly type: "activated"; readonly key: string }
  | { readonly type: "joined"; readonly key: string }
  | { readonly type: "queued"; readonly key: string }
  | { readonly type: "fallback" }
  | { readonly type: "skipped" };

export type ActivePlanSubscriptionLifecycle = {
  readonly resetActivePlanAdmission: (subscription: ActiveSubscription) => void;
  readonly markActivePlanBuildQueued: (subscription: ActiveSubscription, key: string) => void;
  readonly markActivePlanBuildCleared: (subscription: ActiveSubscription) => void;
  readonly markActivePlanFallback: (subscription: ActiveSubscription) => void;
  readonly markActivePlanAutoBuildSkipped: (subscription: ActiveSubscription) => void;
  readonly activateActivePlan: (
    subscription: ActiveSubscription,
    key: string,
    activeView: ReturnType<typeof makeActiveRawViewFromPlan>,
  ) => void;
  readonly isDirty: (subscription: ActiveSubscription) => boolean;
};

export class ActivePlanCoordinator {
  readonly #idField: string;
  readonly #literalStringFields: ReadonlySet<string>;
  readonly #maxActivePlans: number | undefined;
  readonly #maxActivePlanEstimatedBytes: number | undefined;
  readonly #activePlanAutoBuildMaxRows: number;
  readonly #lifecycle: ActivePlanSubscriptionLifecycle;
  readonly #plans = new Map<string, ActiveRawPlanEntry>();
  readonly #builds = new Map<string, ActivePlanBuildEntry>();
  #lastBuildMs = 0;

  constructor(options: {
    readonly idField: string;
    readonly literalStringFields: ReadonlySet<string>;
    readonly maxActivePlans?: number | undefined;
    readonly maxActivePlanEstimatedBytes?: number | undefined;
    readonly activePlanAutoBuildMaxRows: number;
    readonly lifecycle: ActivePlanSubscriptionLifecycle;
  }) {
    this.#idField = options.idField;
    this.#literalStringFields = options.literalStringFields;
    this.#maxActivePlans = options.maxActivePlans;
    this.#maxActivePlanEstimatedBytes = options.maxActivePlanEstimatedBytes;
    this.#activePlanAutoBuildMaxRows = options.activePlanAutoBuildMaxRows;
    this.#lifecycle = options.lifecycle;
  }

  prepareSubscription(
    subscription: ActiveSubscription,
    query: RuntimeRawQuery,
    rowCount: number,
  ): ActivePlanPrepareDecision {
    const key = activeRawPlanKey(query, this.#idField);
    const existing = this.#plans.get(key);
    if (existing !== undefined) {
      this.activateSubscriptionWithPlan(subscription, key, query, existing);
      return { type: "activated", key };
    }
    this.#lifecycle.resetActivePlanAdmission(subscription);
    const pending = this.#builds.get(key);
    if (pending !== undefined) {
      pending.requestIds.add(subscription.requestId);
      this.#lifecycle.markActivePlanBuildQueued(subscription, key);
      return { type: "joined", key };
    }
    if (rowCount > this.#activePlanAutoBuildMaxRows) {
      this.#lifecycle.markActivePlanAutoBuildSkipped(subscription);
      return { type: "skipped" };
    }
    if (this.wouldExceedCountLimitForNewBuild()) {
      this.#lifecycle.markActivePlanFallback(subscription);
      return { type: "fallback" };
    }
    const remainingBytes = this.estimatedBytesRemaining();
    if (
      remainingBytes !== undefined &&
      estimateActiveRawPlanIndexBytes([], query, {
        literalStringFields: this.#literalStringFields,
      }) > remainingBytes
    ) {
      this.#lifecycle.markActivePlanFallback(subscription);
      return { type: "fallback" };
    }
    this.#builds.set(key, {
      key,
      query,
      requestIds: new Set([subscription.requestId]),
      state: "queued",
    });
    this.#lifecycle.markActivePlanBuildQueued(subscription, key);
    return { type: "queued", key };
  }

  applyMutation(mutation: MutationLogEntry): void {
    for (const entry of this.#plans.values()) {
      entry.plan.applyMutation(mutation);
    }
  }

  beginBuildSnapshot(args: {
    readonly key: string;
    readonly rows: readonly RuntimeRow[];
    readonly version: WorkerVersion;
  }): ActivePlanBuildSnapshot | undefined {
    const build = this.#builds.get(args.key);
    if (build === undefined || build.state === "building") {
      return undefined;
    }
    if (build.requestIds.size === 0) {
      this.#builds.delete(args.key);
      return undefined;
    }
    build.state = "building";
    return {
      key: build.key,
      query: build.query,
      rows: args.rows,
      version: args.version,
      remainingEstimatedBytes: this.estimatedBytesRemaining(),
    };
  }

  discardBuild(
    key: string,
    subscriptions: Iterable<ActiveSubscription>,
  ): readonly ActiveSubscription[] {
    const build = this.#builds.get(key);
    if (build === undefined) {
      return [];
    }
    this.#builds.delete(key);
    const activeSubscriptions = Array.from(subscriptions);
    const dirtySubscriptions: ActiveSubscription[] = [];
    for (const requestId of build.requestIds) {
      for (const subscription of activeSubscriptions) {
        if (subscription.requestId !== requestId || subscription.activePlanBuildKey !== key) {
          continue;
        }
        this.#lifecycle.markActivePlanFallback(subscription);
        if (this.#lifecycle.isDirty(subscription)) {
          dirtySubscriptions.push(subscription);
        }
      }
    }
    return dirtySubscriptions;
  }

  canInstallPlan(plan: ActiveRawPlan): boolean {
    return (
      !this.wouldExceedCountLimitOnInstall() &&
      !this.wouldExceedEstimatedBytesLimit(plan.estimatedIndexBytes())
    );
  }

  installBuild(args: {
    readonly snapshot: ActivePlanBuildSnapshot;
    readonly plan: ActiveRawPlan;
    readonly buildMs: number;
    readonly subscriptions: Iterable<ActiveSubscription>;
    readonly isGrouped: (query: RuntimeQuery) => boolean;
  }): readonly ActiveSubscription[] {
    const build = this.#builds.get(args.snapshot.key);
    if (build === undefined || build.requestIds.size === 0) {
      this.#builds.delete(args.snapshot.key);
      return [];
    }
    const entry: ActiveRawPlanEntry = {
      plan: args.plan,
      buildMs: args.buildMs,
      subscribers: 0,
    };
    this.#plans.set(args.snapshot.key, entry);
    this.#lastBuildMs = args.buildMs;
    this.#builds.delete(args.snapshot.key);
    const activeSubscriptions = Array.from(args.subscriptions);
    const dirtySubscriptions: ActiveSubscription[] = [];
    for (const requestId of build.requestIds) {
      for (const subscription of activeSubscriptions) {
        if (
          subscription.requestId !== requestId ||
          subscription.activePlanBuildKey !== args.snapshot.key ||
          args.isGrouped(subscription.query)
        ) {
          continue;
        }
        this.activateSubscriptionWithPlan(
          subscription,
          args.snapshot.key,
          args.snapshot.query,
          entry,
        );
        if (this.#lifecycle.isDirty(subscription) && subscription.activeView !== undefined) {
          dirtySubscriptions.push(subscription);
        }
      }
    }
    if (entry.subscribers <= 0) {
      this.#plans.delete(args.snapshot.key);
    }
    return dirtySubscriptions;
  }

  releasePlan(key: string | undefined): void {
    if (key === undefined) {
      return;
    }
    const entry = this.#plans.get(key);
    if (entry === undefined) {
      return;
    }
    entry.subscribers--;
    if (entry.subscribers <= 0) {
      this.#plans.delete(key);
    }
  }

  releaseBuild(key: string | undefined, requestId: string): void {
    if (key === undefined) {
      return;
    }
    const build = this.#builds.get(key);
    if (build === undefined) {
      return;
    }
    build.requestIds.delete(requestId);
    if (build.requestIds.size === 0) {
      this.#builds.delete(key);
    }
  }

  metrics(subscriptions: Iterable<ActiveSubscription>): ActivePlanCoordinatorMetrics {
    let activeViewCount = 0;
    let activePlanRows = 0;
    let activePlanIndexEstimatedBytes = 0;
    let activePlanBuildMsTotal = 0;
    let activePlanBuildMsMax = 0;
    let activePlanFallbackCount = 0;
    let activePlanBuildQueueDepth = 0;
    let activePlanBuildingCount = 0;
    let activePlanPendingCount = 0;
    let activePlanAutoBuildSkippedCount = 0;
    for (const entry of this.#plans.values()) {
      activeViewCount += entry.subscribers;
      activePlanRows += entry.plan.totalRows();
      activePlanIndexEstimatedBytes += entry.plan.estimatedIndexBytes();
      activePlanBuildMsTotal += entry.buildMs;
      activePlanBuildMsMax = Math.max(activePlanBuildMsMax, entry.buildMs);
    }
    for (const build of this.#builds.values()) {
      if (build.state === "queued") {
        activePlanBuildQueueDepth++;
      } else {
        activePlanBuildingCount++;
      }
      activePlanPendingCount += build.requestIds.size;
    }
    for (const subscription of subscriptions) {
      if (subscription.activePlanFallback === true) {
        activePlanFallbackCount++;
      }
      if (subscription.activePlanAutoBuildSkipped === true) {
        activePlanAutoBuildSkippedCount++;
      }
    }
    return {
      activePlanCount: this.#plans.size,
      activeViewCount,
      activePlanRows,
      activePlanIndexEstimatedBytes,
      activePlanBuildQueueDepth,
      activePlanBuildingCount,
      activePlanPendingCount,
      activePlanBuildMs: this.#lastBuildMs,
      activePlanBuildMsTotal,
      activePlanBuildMsMax,
      activePlanFallbackCount,
      activePlanAutoBuildSkippedCount,
    };
  }

  isLimitNear(metrics: ActivePlanCoordinatorMetrics): boolean {
    return (
      isNearLimit(
        metrics.activePlanCount +
          metrics.activePlanBuildQueueDepth +
          metrics.activePlanBuildingCount,
        this.#maxActivePlans,
      ) || isNearLimit(metrics.activePlanIndexEstimatedBytes, this.#maxActivePlanEstimatedBytes)
    );
  }

  clear(): void {
    this.#plans.clear();
    this.#builds.clear();
  }

  private activateSubscriptionWithPlan(
    subscription: ActiveSubscription,
    key: string,
    query: RuntimeRawQuery,
    entry: ActiveRawPlanEntry,
  ): void {
    if (subscription.activePlanKey === key) {
      return;
    }
    entry.subscribers++;
    this.#lifecycle.activateActivePlan(
      subscription,
      key,
      makeActiveRawViewFromPlan(entry.plan, query, this.#idField),
    );
  }

  private estimatedBytes(): number {
    let total = 0;
    for (const entry of this.#plans.values()) {
      total += entry.plan.estimatedIndexBytes();
    }
    return total;
  }

  private estimatedBytesRemaining(): number | undefined {
    return this.#maxActivePlanEstimatedBytes === undefined
      ? undefined
      : this.#maxActivePlanEstimatedBytes - this.estimatedBytes();
  }

  private wouldExceedCountLimitForNewBuild(): boolean {
    return (
      this.#maxActivePlans !== undefined &&
      this.#plans.size + this.#builds.size >= this.#maxActivePlans
    );
  }

  private wouldExceedCountLimitOnInstall(): boolean {
    return (
      this.#maxActivePlans !== undefined &&
      this.#plans.size + this.#builds.size > this.#maxActivePlans
    );
  }

  private wouldExceedEstimatedBytesLimit(newPlanBytes: number): boolean {
    return (
      this.#maxActivePlanEstimatedBytes !== undefined &&
      this.estimatedBytes() + newPlanBytes > this.#maxActivePlanEstimatedBytes
    );
  }
}

function isNearLimit(value: number, limit: number | undefined): boolean {
  return limit !== undefined && limit > 0 && value / limit >= 0.8;
}
