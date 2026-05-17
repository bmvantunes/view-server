import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { makeActiveRawPlan, makeActiveRawViewFromPlan } from "../src/worker/active-view.ts";
import {
  SubscriptionRegistry,
  type ActiveSubscription,
} from "../src/worker/subscription-registry.ts";

describe("SubscriptionRegistry", () => {
  it.effect("replaces duplicate request ids and releases previous ownership", () =>
    Effect.gen(function* () {
      const releases: string[] = [];
      const registry = new SubscriptionRegistry({
        releaseActivePlan: (key) => {
          releases.push(`plan:${key ?? "none"}`);
        },
        releaseActivePlanBuild: (key, requestId) => {
          releases.push(`build:${key ?? "none"}:${requestId}`);
        },
        releaseGroupedRefresh: (requestId) => {
          releases.push(`grouped:${requestId}`);
        },
      });
      const firstQueue = yield* subscriptionQueue();
      const secondQueue = yield* subscriptionQueue();

      registry.replace(
        subscription({
          queue: firstQueue,
          activePlanKey: "plan-a",
          activePlanBuildKey: "build-a",
        }),
      );
      const previous = registry.replace(subscription({ queue: secondQueue }));

      expect(previous?.queue).toBe(firstQueue);
      expect(registry.size).toBe(1);
      expect(releases).toEqual(["plan:plan-a", "build:build-a:request", "grouped:request"]);
    }),
  );

  it.effect("removes only the matching queue from finalizers", () =>
    Effect.gen(function* () {
      const releases: string[] = [];
      const registry = new SubscriptionRegistry({
        releaseActivePlan: (key) => {
          releases.push(`plan:${key ?? "none"}`);
        },
        releaseActivePlanBuild: () => undefined,
        releaseGroupedRefresh: () => undefined,
      });
      const activeQueue = yield* subscriptionQueue();
      const staleQueue = yield* subscriptionQueue();
      registry.replace(subscription({ queue: activeQueue, activePlanKey: "plan-a" }));

      expect(registry.removeForQueue("request", staleQueue)).toBeUndefined();
      expect(registry.size).toBe(1);
      expect(registry.removeForQueue("request", activeQueue)?.requestId).toBe("request");
      expect(registry.size).toBe(0);
      expect(releases).toEqual(["plan:plan-a"]);
    }),
  );

  it.effect("clears shutdown subscriptions without running release hooks twice", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const registry = new SubscriptionRegistry({
        releaseActivePlan: () => {
          releaseCount += 1;
        },
        releaseActivePlanBuild: () => {
          releaseCount += 1;
        },
        releaseGroupedRefresh: () => {
          releaseCount += 1;
        },
      });
      const queue = yield* subscriptionQueue();
      registry.replace(subscription({ queue, activePlanKey: "plan-a" }));

      const shutdown = registry.clearForShutdown();

      expect(shutdown).toEqual([{ requestId: "request", queue }]);
      expect(registry.size).toBe(0);
      expect(releaseCount).toBe(0);
    }),
  );

  it.effect("owns live-query lifecycle transitions", () =>
    Effect.gen(function* () {
      const registry = new SubscriptionRegistry({
        releaseActivePlan: () => undefined,
        releaseActivePlanBuild: () => undefined,
        releaseGroupedRefresh: () => undefined,
      });
      const queue = yield* subscriptionQueue();
      const active = subscription({ queue });
      registry.replace(active);

      registry.markDirty(active, 2n, 3);
      expect(registry.dirtyTargetVersion(active)).toBe(2n);
      expect(active.lastTotalRows).toBe(3);

      registry.applyDelta(
        active,
        {
          operations: [{ type: "upsert", key: "a", row: { id: "a" }, index: 0 }],
          nextRows: [{ id: "a" }],
          totalRows: 1,
        },
        3n,
      );
      expect(active.lastRows).toEqual([{ id: "a" }]);
      expect(active.lastTotalRows).toBe(1);
      expect(active.lastVersion).toBe(3n);

      registry.applySnapshot(active, { rows: [{ id: "b" }], totalRows: 1 }, 4n);
      expect(active.lastRows).toEqual([{ id: "b" }]);
      expect(registry.isDirty(active)).toBe(false);

      registry.markActivePlanBuildQueued(active, "plan-a");
      expect(active.activePlanBuildKey).toBe("plan-a");
      registry.activateActivePlan(
        active,
        "plan-a",
        makeActiveRawViewFromPlan(
          makeActiveRawPlan([{ id: "b" }], { fields: { id: true } }, "id"),
          { fields: { id: true } },
          "id",
        ),
      );
      expect(active.activePlanKey).toBe("plan-a");
      expect(active.activePlanBuildKey).toBeUndefined();
      expect(active.activeView?.snapshot().rows).toEqual([{ id: "b" }]);

      registry.markGroupedRefreshScheduled(active);
      expect(registry.isGroupedRefreshScheduled(active)).toBe(true);
      registry.markGroupedRefreshInFlight(active);
      expect(registry.isGroupedRefreshScheduled(active)).toBe(false);
      expect(registry.isGroupedRefreshInFlight(active)).toBe(true);
      registry.markGroupedRefreshIdle(active);
      expect(registry.isGroupedRefreshInFlight(active)).toBe(false);
    }),
  );
});

function subscription(args: {
  readonly queue: ActiveSubscription["queue"];
  readonly activePlanKey?: string | undefined;
  readonly activePlanBuildKey?: string | undefined;
}): ActiveSubscription {
  return {
    requestId: "request",
    query: { fields: { id: true } },
    dependencyFields: new Set(["id"]),
    queue: args.queue,
    lastRows: [],
    lastTotalRows: 0,
    lastVersion: 0n,
    pendingLagVersions: 1n,
    activePlanKey: args.activePlanKey,
    activePlanBuildKey: args.activePlanBuildKey,
  };
}

function subscriptionQueue() {
  return Queue.unbounded<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError | Cause.Done>();
}
