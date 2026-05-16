import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeGroupedQuery, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import {
  GroupedRefreshCoordinator,
  groupedRefreshKey,
} from "../src/worker/grouped-refresh-coordinator.ts";
import type { ActiveSubscription } from "../src/worker/subscription-registry.ts";

const groupedQuery = {
  groupBy: ["symbol"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
  },
  orderBy: [{ field: "symbol", direction: "asc" }],
} satisfies RuntimeGroupedQuery;

describe("GroupedRefreshCoordinator", () => {
  it.effect("shares queued refresh work by grouped query key", () =>
    Effect.gen(function* () {
      const coordinator = new GroupedRefreshCoordinator();
      const first = yield* subscription("request-1");
      const second = yield* subscription("request-2");

      expect(coordinator.schedule(first)).toEqual({ type: "new", key: expect.any(String) });
      expect(coordinator.schedule(second)).toEqual({ type: "none" });

      const snapshot = coordinator.begin({
        key: groupedRefreshKey(groupedQuery),
        subscriptions: registry([first, second]),
        rows: [{ id: "a", symbol: "AAPL" }],
        version: 1n,
      });

      expect(snapshot?.requestIds).toEqual(["request-1", "request-2"]);
      expect(first.groupedRefreshInFlight).toBe(true);
      expect(second.groupedRefreshInFlight).toBe(true);
    }),
  );

  it.effect("reschedules subscriptions dirtied beyond the computed snapshot version", () =>
    Effect.gen(function* () {
      const coordinator = new GroupedRefreshCoordinator();
      const active = yield* subscription("request-1");
      coordinator.schedule(active);
      const snapshot = coordinator.begin({
        key: groupedRefreshKey(groupedQuery),
        subscriptions: registry([active]),
        rows: [],
        version: 1n,
      });
      if (snapshot === undefined) {
        throw new Error("Expected grouped refresh snapshot");
      }
      active.dirtyTargetVersion = 2n;

      const installed = coordinator.install({
        snapshot,
        result: { rows: [], totalRows: 0 },
        subscriptions: registry([active]),
      });

      expect(installed.refreshes).toEqual([]);
      expect(installed.rescheduleRequestIds).toEqual(["request-1"]);
    }),
  );

  it.effect("releases queued request ids on unsubscribe and resets in-flight entries", () =>
    Effect.gen(function* () {
      const coordinator = new GroupedRefreshCoordinator();
      const first = yield* subscription("request-1");
      const second = yield* subscription("request-2");
      coordinator.schedule(first);
      coordinator.schedule(second);

      coordinator.release("request-1");
      const snapshot = coordinator.begin({
        key: groupedRefreshKey(groupedQuery),
        subscriptions: registry([first, second]),
        rows: [],
        version: 1n,
      });
      expect(snapshot?.requestIds).toEqual(["request-2"]);

      const reset = coordinator.reset({
        key: groupedRefreshKey(groupedQuery),
        subscriptions: registry([second]),
      });
      expect(reset).toEqual(["request-2"]);
      expect(second.groupedRefreshInFlight).toBe(false);
    }),
  );
});

function registry(subscriptions: readonly ActiveSubscription[]) {
  const byId = new Map(subscriptions.map((subscription) => [subscription.requestId, subscription]));
  return {
    get: (requestId: string) => byId.get(requestId),
  };
}

function subscription(requestId: string): Effect.Effect<ActiveSubscription> {
  return Effect.gen(function* () {
    const queue = yield* Queue.unbounded<
      SubscriptionEvent<readonly RuntimeRow[]>,
      ViewServerError | Cause.Done
    >();
    return {
      requestId,
      query: groupedQuery,
      dependencyFields: new Set(["id", "symbol"]),
      queue,
      lastRows: [],
      lastTotalRows: 0,
      lastVersion: 0n,
      pendingLagVersions: 0n,
      dirtyTargetVersion: 1n,
    };
  });
}
