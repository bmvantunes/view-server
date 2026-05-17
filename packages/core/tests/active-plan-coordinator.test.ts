import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeRawQuery, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { makeActiveRawPlan } from "../src/worker/active-view.ts";
import { ActivePlanCoordinator } from "../src/worker/active-plan-coordinator.ts";
import {
  SubscriptionRegistry,
  type ActiveSubscription,
} from "../src/worker/subscription-registry.ts";

const query = {
  fields: { id: true, price: true },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 2,
} satisfies RuntimeRawQuery;

describe("ActivePlanCoordinator", () => {
  it.effect("skips active-plan auto-build admission above the configured row threshold", () =>
    Effect.gen(function* () {
      const coordinator = new ActivePlanCoordinator({
        idField: "id",
        literalStringFields: new Set(),
        activePlanAutoBuildMaxRows: 1,
        lifecycle: lifecycle(),
      });
      const active = yield* subscription("request-1");

      const decision = coordinator.prepareSubscription(active, query, 2);

      expect(decision.type).toBe("skipped");
      expect(active.activePlanAutoBuildSkipped).toBe(true);
      expect(coordinator.metrics([active]).activePlanAutoBuildSkippedCount).toBe(1);
      expect(coordinator.metrics([active]).activePlanBuildQueueDepth).toBe(0);
    }),
  );

  it.effect("queues one shared build and joins later subscriptions for the same plan", () =>
    Effect.gen(function* () {
      const coordinator = new ActivePlanCoordinator({
        idField: "id",
        literalStringFields: new Set(),
        activePlanAutoBuildMaxRows: 10,
        lifecycle: lifecycle(),
      });
      const first = yield* subscription("request-1");
      const second = yield* subscription("request-2");

      expect(coordinator.prepareSubscription(first, query, 2).type).toBe("queued");
      expect(coordinator.prepareSubscription(second, query, 2).type).toBe("joined");

      const metrics = coordinator.metrics([first, second]);
      expect(metrics.activePlanBuildQueueDepth).toBe(1);
      expect(metrics.activePlanPendingCount).toBe(2);
    }),
  );

  it.effect("installs a built plan, activates subscribers, and releases capacity", () =>
    Effect.gen(function* () {
      const rows = [
        { id: "a", price: 2 },
        { id: "b", price: 1 },
      ];
      const coordinator = new ActivePlanCoordinator({
        idField: "id",
        literalStringFields: new Set(),
        activePlanAutoBuildMaxRows: 10,
        lifecycle: lifecycle(),
      });
      const active = yield* subscription("request-1");
      const decision = coordinator.prepareSubscription(active, query, rows.length);
      expect(decision.type).toBe("queued");
      if (decision.type !== "queued") {
        throw new Error("Expected queued active plan");
      }

      const snapshot = coordinator.beginBuildSnapshot({
        key: decision.key,
        rows,
        version: 0n,
      });
      expect(snapshot?.key).toBe(decision.key);
      if (snapshot === undefined) {
        throw new Error("Expected active-plan build snapshot");
      }
      const plan = makeActiveRawPlan(rows, query, "id");
      const dirty = coordinator.installBuild({
        snapshot,
        plan,
        buildMs: 12,
        subscriptions: [active],
        isGrouped: () => false,
      });

      expect(dirty).toEqual([]);
      expect(active.activeView?.snapshot().rows).toEqual([
        { id: "b", price: 1 },
        { id: "a", price: 2 },
      ]);
      expect(coordinator.metrics([active]).activePlanCount).toBe(1);

      coordinator.releasePlan(active.activePlanKey);
      expect(coordinator.metrics([active]).activePlanCount).toBe(0);
    }),
  );
});

function lifecycle(): SubscriptionRegistry {
  return new SubscriptionRegistry({
    releaseActivePlan: () => undefined,
    releaseActivePlanBuild: () => undefined,
    releaseGroupedRefresh: () => undefined,
  });
}

function subscription(requestId: string): Effect.Effect<ActiveSubscription> {
  return Effect.gen(function* () {
    const queue = yield* Queue.unbounded<
      SubscriptionEvent<readonly RuntimeRow[]>,
      ViewServerError | Cause.Done
    >();
    return {
      requestId,
      query,
      dependencyFields: new Set(["id", "price"]),
      queue,
      lastRows: [],
      lastTotalRows: 0,
      lastVersion: 0n,
      pendingLagVersions: 0n,
    };
  });
}
