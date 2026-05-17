import type { RuntimeGroupedQuery, RuntimeRow } from "../protocol/index.ts";
import { stableStringify } from "./active-raw-plan-key.ts";
import type { WorkerVersion } from "./mutation-log.ts";
import { isGroupedQuery, type QueryExecutionResult } from "./query-engine.ts";
import type { ActiveSubscription } from "./subscription-registry.ts";

export type GroupedRefreshSnapshot = {
  readonly key: string;
  readonly requestId: string;
  readonly requestIds: readonly string[];
  readonly query: RuntimeGroupedQuery;
  readonly version: WorkerVersion;
};

export type GroupedRefreshMemorySnapshot = GroupedRefreshSnapshot & {
  readonly rows: readonly RuntimeRow[];
};

export type GroupedRefreshInstall = {
  readonly subscription: ActiveSubscription;
  readonly result: QueryExecutionResult;
  readonly version: WorkerVersion;
};

export type GroupedRefreshSubscriptionLifecycle = {
  readonly markGroupedRefreshScheduled: (subscription: ActiveSubscription) => void;
  readonly markGroupedRefreshInFlight: (subscription: ActiveSubscription) => void;
  readonly markGroupedRefreshIdle: (subscription: ActiveSubscription) => void;
  readonly isGroupedRefreshScheduled: (subscription: ActiveSubscription) => boolean;
  readonly isGroupedRefreshInFlight: (subscription: ActiveSubscription) => boolean;
  readonly dirtyTargetVersion: (subscription: ActiveSubscription) => WorkerVersion | undefined;
};

type GroupedRefreshEntry = {
  readonly key: string;
  readonly query: RuntimeGroupedQuery;
  readonly requestIds: Set<string>;
  readonly pendingRequestIds: Set<string>;
  readonly runningRequestIds: Set<string>;
  state: "queued" | "running";
};

export class GroupedRefreshCoordinator {
  readonly #entries = new Map<string, GroupedRefreshEntry>();
  readonly #lifecycle: GroupedRefreshSubscriptionLifecycle;

  constructor(options: { readonly lifecycle: GroupedRefreshSubscriptionLifecycle }) {
    this.#lifecycle = options.lifecycle;
  }

  schedule(subscription: ActiveSubscription):
    | { readonly type: "new"; readonly key: string }
    | {
        readonly type: "none";
      } {
    if (
      this.#lifecycle.isGroupedRefreshScheduled(subscription) ||
      this.#lifecycle.isGroupedRefreshInFlight(subscription) ||
      !isGroupedQuery(subscription.query)
    ) {
      return { type: "none" };
    }
    const key = groupedRefreshKey(subscription.query);
    this.#lifecycle.markGroupedRefreshScheduled(subscription);
    const existing = this.#entries.get(key);
    if (existing !== undefined) {
      if (existing.state === "queued") {
        existing.requestIds.add(subscription.requestId);
      } else {
        existing.pendingRequestIds.add(subscription.requestId);
      }
      return { type: "none" };
    }
    this.#entries.set(key, {
      key,
      query: subscription.query,
      requestIds: new Set([subscription.requestId]),
      pendingRequestIds: new Set(),
      runningRequestIds: new Set(),
      state: "queued",
    });
    return { type: "new", key };
  }

  begin(args: {
    readonly key: string;
    readonly subscriptions: { readonly get: (requestId: string) => ActiveSubscription | undefined };
    readonly version: WorkerVersion;
  }): GroupedRefreshSnapshot | undefined {
    const entry = this.#entries.get(args.key);
    if (entry === undefined || entry.state === "running") {
      return undefined;
    }
    const requestIds = Array.from(entry.requestIds).filter((requestId) => {
      const subscription = args.subscriptions.get(requestId);
      return (
        subscription !== undefined &&
        isGroupedQuery(subscription.query) &&
        this.#lifecycle.dirtyTargetVersion(subscription) !== undefined
      );
    });
    entry.requestIds.clear();
    if (requestIds.length === 0) {
      this.#entries.delete(args.key);
      return undefined;
    }
    entry.state = "running";
    entry.runningRequestIds.clear();
    for (const requestId of requestIds) {
      entry.runningRequestIds.add(requestId);
      const subscription = args.subscriptions.get(requestId);
      if (subscription !== undefined) {
        this.#lifecycle.markGroupedRefreshInFlight(subscription);
      }
    }
    return {
      key: entry.key,
      requestId: requestIds[0] ?? entry.key,
      requestIds,
      query: entry.query,
      version: args.version,
    };
  }

  install(args: {
    readonly snapshot: GroupedRefreshSnapshot;
    readonly result: QueryExecutionResult;
    readonly subscriptions: { readonly get: (requestId: string) => ActiveSubscription | undefined };
  }): {
    readonly refreshes: readonly GroupedRefreshInstall[];
    readonly rescheduleRequestIds: readonly string[];
  } {
    const entry = this.#entries.get(args.snapshot.key);
    this.#entries.delete(args.snapshot.key);
    const reschedule = new Set(entry?.pendingRequestIds ?? []);
    const refreshes: GroupedRefreshInstall[] = [];
    for (const requestId of args.snapshot.requestIds) {
      const subscription = args.subscriptions.get(requestId);
      if (subscription === undefined) {
        continue;
      }
      this.#lifecycle.markGroupedRefreshIdle(subscription);
      if (!isGroupedQuery(subscription.query)) {
        continue;
      }
      const dirtyTargetVersion = this.#lifecycle.dirtyTargetVersion(subscription);
      if (dirtyTargetVersion !== undefined && dirtyTargetVersion > args.snapshot.version) {
        reschedule.add(subscription.requestId);
        continue;
      }
      refreshes.push({
        subscription,
        result: args.result,
        version: args.snapshot.version,
      });
    }
    return {
      refreshes,
      rescheduleRequestIds: [...reschedule],
    };
  }

  reset(args: {
    readonly key: string;
    readonly subscriptions: { readonly get: (requestId: string) => ActiveSubscription | undefined };
  }): readonly string[] {
    const entry = this.#entries.get(args.key);
    this.#entries.delete(args.key);
    const requestIds = new Set([
      ...(entry?.requestIds ?? []),
      ...(entry?.pendingRequestIds ?? []),
      ...(entry?.runningRequestIds ?? []),
    ]);
    const reschedule: string[] = [];
    for (const requestId of requestIds) {
      const subscription = args.subscriptions.get(requestId);
      if (subscription === undefined) {
        continue;
      }
      this.#lifecycle.markGroupedRefreshIdle(subscription);
      if (this.#lifecycle.dirtyTargetVersion(subscription) !== undefined) {
        reschedule.push(requestId);
      }
    }
    return reschedule;
  }

  release(requestId: string): void {
    for (const [key, entry] of this.#entries) {
      entry.requestIds.delete(requestId);
      entry.pendingRequestIds.delete(requestId);
      entry.runningRequestIds.delete(requestId);
      if (
        entry.state === "queued" &&
        entry.requestIds.size === 0 &&
        entry.pendingRequestIds.size === 0
      ) {
        this.#entries.delete(key);
      }
    }
  }

  clear(): void {
    this.#entries.clear();
  }
}

export function groupedRefreshKey(query: RuntimeGroupedQuery): string {
  return stableStringify(query);
}
