import type * as Cause from "effect/Cause";
import type * as Queue from "effect/Queue";
import type { ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow, SubscriptionEvent } from "../protocol/index.ts";
import type { ActiveRawView } from "./active-view.ts";
import type { WorkerVersion } from "./mutation-log.ts";

export type ActiveSubscription = {
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
  activePlanAutoBuildSkipped?: boolean | undefined;
  dirtyTargetVersion?: WorkerVersion | undefined;
  groupedRefreshScheduled?: boolean | undefined;
  groupedRefreshInFlight?: boolean | undefined;
};

export type ShutdownSubscription = {
  readonly requestId: string;
  readonly queue: ActiveSubscription["queue"];
};

export class SubscriptionRegistry {
  readonly #subscriptions = new Map<string, ActiveSubscription>();
  readonly #hooks: {
    readonly releaseActivePlan: (key: string | undefined) => void;
    readonly releaseActivePlanBuild: (key: string | undefined, requestId: string) => void;
    readonly releaseGroupedRefresh: (requestId: string) => void;
  };

  constructor(hooks: {
    readonly releaseActivePlan: (key: string | undefined) => void;
    readonly releaseActivePlanBuild: (key: string | undefined, requestId: string) => void;
    readonly releaseGroupedRefresh: (requestId: string) => void;
  }) {
    this.#hooks = hooks;
  }

  get size(): number {
    return this.#subscriptions.size;
  }

  get(requestId: string): ActiveSubscription | undefined {
    return this.#subscriptions.get(requestId);
  }

  values(): IterableIterator<ActiveSubscription> {
    return this.#subscriptions.values();
  }

  replace(subscription: ActiveSubscription): ActiveSubscription | undefined {
    const previous = this.remove(subscription.requestId);
    this.#subscriptions.set(subscription.requestId, subscription);
    return previous;
  }

  remove(requestId: string): ActiveSubscription | undefined {
    const subscription = this.#subscriptions.get(requestId);
    if (subscription === undefined) {
      return undefined;
    }
    this.#subscriptions.delete(requestId);
    this.#hooks.releaseActivePlan(subscription.activePlanKey);
    this.#hooks.releaseActivePlanBuild(subscription.activePlanBuildKey, requestId);
    this.#hooks.releaseGroupedRefresh(requestId);
    resetSubscriptionState(subscription);
    return subscription;
  }

  removeForQueue(
    requestId: string,
    queue: ActiveSubscription["queue"],
  ): ActiveSubscription | undefined {
    const subscription = this.#subscriptions.get(requestId);
    return subscription?.queue === queue ? this.remove(requestId) : undefined;
  }

  clearForShutdown(): readonly ShutdownSubscription[] {
    const shutdownSubscriptions = Array.from(
      this.#subscriptions.values(),
      (subscription): ShutdownSubscription => ({
        requestId: subscription.requestId,
        queue: subscription.queue,
      }),
    );
    this.#subscriptions.clear();
    return shutdownSubscriptions;
  }
}

function resetSubscriptionState(subscription: ActiveSubscription): void {
  subscription.pendingLagVersions = 0n;
  subscription.dirtyTargetVersion = undefined;
  subscription.activePlanAutoBuildSkipped = false;
  subscription.groupedRefreshScheduled = false;
  subscription.groupedRefreshInFlight = false;
}
