import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { TestClock } from "effect/testing";
import { RuntimeHealthSyncScheduler } from "../src/server/runtime-health-sync-scheduler.ts";

describe("RuntimeHealthSyncScheduler", () => {
  it.effect("coalesces repeated health sync requests", () =>
    Effect.gen(function* () {
      let syncs = 0;
      const scope = yield* Effect.scope;
      const scheduler = new RuntimeHealthSyncScheduler({
        scope,
        delayMs: 10,
        syncNow: Effect.sync(() => {
          syncs += 1;
        }),
      });

      yield* scheduler.request;
      yield* scheduler.request;
      yield* scheduler.request;
      expect(syncs).toBe(0);

      yield* TestClock.adjust("10 millis");
      yield* Effect.yieldNow;

      expect(syncs).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect("flushes pending health synchronously for health-topic reads", () =>
    Effect.gen(function* () {
      let syncs = 0;
      const scope = yield* Effect.scope;
      const scheduler = new RuntimeHealthSyncScheduler({
        scope,
        delayMs: 1_000,
        syncNow: Effect.sync(() => {
          syncs += 1;
        }),
      });

      yield* scheduler.request;
      yield* scheduler.flush;

      expect(syncs).toBe(1);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      expect(syncs).toBe(1);
    }).pipe(Effect.scoped),
  );
});
