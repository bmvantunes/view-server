import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeNodeThreadTopicWorkerHostFactory } from "@view-server/core/worker/node";

describe("node worker public export", () => {
  it.effect("exposes the Node thread worker host factory from the public subpath", () =>
    Effect.sync(() => {
      expect(typeof makeNodeThreadTopicWorkerHostFactory).toBe("function");
    }),
  );
});
