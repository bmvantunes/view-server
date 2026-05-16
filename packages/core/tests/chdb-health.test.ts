import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import {
  ChdbHealthSchema,
  chdbHealthFromSnapshotBackendHealth,
} from "../src/snapshot/chdb-health.ts";

describe("ChdbHealth", () => {
  it("normalizes optional backend health fields into the public contract", () => {
    expect(
      chdbHealthFromSnapshotBackendHealth({
        status: "degraded",
        message: "child exited",
      }),
    ).toEqual({
      status: "degraded",
      pid: 0,
      restarts: 0,
      pendingRequests: 0,
      lastError: "child exited",
      backendVersion: 0n,
    });
  });

  it("serializes and deserializes every public field", () => {
    const health = {
      status: "ready",
      pid: 123,
      restarts: 2,
      pendingRequests: 3,
      lastError: "",
      backendVersion: 42n,
    } satisfies typeof ChdbHealthSchema.Type;

    const encoded = Schema.encodeUnknownSync(ChdbHealthSchema)(health);
    const decoded = Schema.decodeUnknownSync(ChdbHealthSchema)(encoded);

    expect(decoded).toEqual(health);
  });
});
