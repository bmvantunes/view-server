import { describe, expect, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type { ViewServerError } from "../src/errors.ts";
import type { DeltaEvent, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import {
  makeFanoutQueue,
  queueEventsVersionLag,
  subscriptionLagVersionsForQueueDepth,
} from "../src/worker/fanout-queue.ts";

describe("FanoutQueue", () => {
  it.effect("coalesces queued deltas while preserving logical version lag", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<
        SubscriptionEvent<readonly RuntimeRow[]>,
        ViewServerError | Cause.Done
      >();
      const state = { pendingLagVersions: 0n };
      const fanout = makeFanoutQueue({ maxQueueDepth: 10, deltaCoalescing: true });

      expect(yield* fanout.offerDelta(queue, state, delta("1", "2"))).toBe(true);
      expect(yield* fanout.offerDelta(queue, state, delta("2", "5"))).toBe(true);

      expect(yield* Queue.size(queue)).toBe(1);
      expect(state.pendingLagVersions).toBe(4n);
      const event = yield* Queue.take(queue);
      expect(event.type).toBe("delta");
      if (event.type === "delta") {
        expect(event.meta.fromVersion).toBe("1");
        expect(event.meta.toVersion).toBe("5");
      }
    }),
  );

  it.effect("keeps physical queue depth semantics when coalescing is disabled", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<
        SubscriptionEvent<readonly RuntimeRow[]>,
        ViewServerError | Cause.Done
      >();
      const state = { pendingLagVersions: 0n };
      const fanout = makeFanoutQueue({ maxQueueDepth: 3, deltaCoalescing: false });

      expect(yield* fanout.offerDelta(queue, state, delta("0", "1"))).toBe(true);
      expect(yield* fanout.offerDelta(queue, state, delta("1", "2"))).toBe(true);

      expect(yield* Queue.size(queue)).toBe(2);
      expect(fanout.lagForDepth(2, state.pendingLagVersions)).toBe(2n);
      yield* Queue.take(queue);
      expect(fanout.lagForDepth(1, state.pendingLagVersions)).toBe(1n);
    }),
  );

  it.effect("fails offers before exceeding max queue depth", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<
        SubscriptionEvent<readonly RuntimeRow[]>,
        ViewServerError | Cause.Done
      >();
      const state = { pendingLagVersions: 0n };
      const fanout = makeFanoutQueue({ maxQueueDepth: 1, deltaCoalescing: false });

      expect(yield* fanout.offerDelta(queue, state, delta("0", "1"))).toBe(true);
      expect(yield* fanout.offerDelta(queue, state, delta("1", "2"))).toBe(false);
      expect(yield* Queue.size(queue)).toBe(1);
    }),
  );

  it("reports logical lag from queued events and depth", () => {
    expect(queueEventsVersionLag([delta("1", "4"), status("4")])).toBe(3n);
    expect(subscriptionLagVersionsForQueueDepth(0, 3n, true)).toBe(0n);
    expect(subscriptionLagVersionsForQueueDepth(1, 3n, true)).toBe(3n);
    expect(subscriptionLagVersionsForQueueDepth(2, 3n, false)).toBe(2n);
  });
});

function delta(fromVersion: string, toVersion: string): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId: "request",
    ops: [],
    meta: {
      fromVersion,
      toVersion,
      totalRows: 0,
      serverTime: 0,
    },
  };
}

function status(version: string): SubscriptionEvent<readonly RuntimeRow[]> {
  return {
    type: "status",
    requestId: "request",
    status: "stale",
    meta: {
      version,
      totalRows: 0,
      serverTime: 0,
    },
  };
}
