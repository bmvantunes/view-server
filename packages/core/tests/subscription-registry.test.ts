import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
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
