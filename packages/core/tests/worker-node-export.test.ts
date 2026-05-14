import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeNodeThreadTopicWorkerHostFactory } from "@view-server/core/worker/node";

describe("node worker public export", () => {
  it.effect("exposes the Node thread worker host factory from the public subpath", () =>
    Effect.sync(() => {
      expect(typeof makeNodeThreadTopicWorkerHostFactory).toBe("function");
    }),
  );
});
