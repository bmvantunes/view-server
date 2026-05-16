import { describe, expect, it } from "@effect/vitest";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { transportError } from "../src/errors.ts";
import {
  initialLiveQueryLifecycle,
  transitionBeginAttempt,
  transitionDelta,
  transitionError,
  transitionRetryAttempt,
  transitionSnapshot,
  transitionStatusEvent,
} from "../src/client/live-query-lifecycle.ts";

describe("LiveQueryLifecycle", () => {
  it("represents initial empty and initial stale data states", () => {
    const empty = initialLiveQueryLifecycle(undefined);
    expect(empty.result.waiting).toBe(true);
    expect(
      AsyncResult.match(empty.result, {
        onInitial: () => "initial",
        onFailure: () => "failure",
        onSuccess: () => "success",
      }),
    ).toBe("initial");

    const hydrated = initialLiveQueryLifecycle({
      rows: [{ id: "a", price: 10 }],
      totalRows: 1,
    });
    expect(hydrated.result.waiting).toBe(true);
    expect(valueOf(hydrated.result)?.status).toBe("stale");
    expect(valueOf(hydrated.result)?.totalRows).toBe(1);
  });

  it("maps snapshot, status, and delta transitions to AsyncResult success states", () => {
    const initial = initialLiveQueryLifecycle<{ readonly id: string; readonly price: number }>(
      undefined,
    );
    const snapshot = transitionSnapshot(initial.lifecycle, [{ id: "a", price: 10 }], 1, 100);
    expect(snapshot.result.waiting).toBe(false);
    expect(valueOf(snapshot.result)?.status).toBe("live");
    expect(valueOf(snapshot.result)?.connection.connected).toBe(true);
    expect(valueOf(snapshot.result)?.connection.lastConnectedAt).toBe(100);

    const stale = transitionStatusEvent(
      snapshot.lifecycle,
      snapshot.lifecycle.value.rows,
      2,
      "stale",
    );
    expect(stale.result.waiting).toBe(true);
    expect(valueOf(stale.result)?.status).toBe("stale");
    expect(valueOf(stale.result)?.totalRows).toBe(2);

    const delta = transitionDelta(stale.lifecycle, [{ id: "b", price: 20 }], 1, 200);
    expect(delta.result.waiting).toBe(false);
    expect(valueOf(delta.result)?.status).toBe("live");
    expect(valueOf(delta.result)?.rows).toEqual([{ id: "b", price: 20 }]);
    expect(valueOf(delta.result)?.connection.lastConnectedAt).toBe(200);
  });

  it("keeps previous success data visible during reconnecting failure states", () => {
    const initial = initialLiveQueryLifecycle({
      rows: [{ id: "a", price: 10 }],
      totalRows: 1,
    });
    const attempt = transitionBeginAttempt(initial.lifecycle, 1, 100);
    expect(attempt.result.waiting).toBe(true);
    expect(valueOf(attempt.result)?.status).toBe("stale");

    const retry = transitionRetryAttempt(attempt.lifecycle, 2, 200);
    expect(retry.result.waiting).toBe(true);
    expect(valueOf(retry.result)?.status).toBe("reconnecting");
    expect(valueOf(retry.result)?.connection.lastDisconnectedAt).toBe(200);

    const failure = transitionError(retry.lifecycle, transportError("socket closed"), 300);
    expect(failure.result.waiting).toBe(true);
    expect(
      AsyncResult.match(failure.result, {
        onInitial: () => "initial",
        onSuccess: () => "success",
        onFailure: (error) => {
          const previous = Option.getOrUndefined(AsyncResult.value(error));
          expect(previous?.status).toBe("reconnecting");
          expect(previous?.rows).toEqual([{ id: "a", price: 10 }]);
          return "failure";
        },
      }),
    ).toBe("failure");
  });
});

function valueOf<TRow>(result: AsyncResult.AsyncResult<TRow, unknown>): TRow | undefined {
  return AsyncResult.match(result, {
    onInitial: () => undefined,
    onFailure: (failure) => Option.getOrUndefined(AsyncResult.value(failure)),
    onSuccess: ({ value }) => value,
  });
}
