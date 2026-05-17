import { describe, expect, it } from "@effect/vitest";
import { MutationLog, type MutationLogEntry } from "../src/worker/mutation-log.ts";

describe("MutationLog", () => {
  it("keeps a contiguous ordered ring after capacity rollover", () => {
    const log = new MutationLog(3);
    for (let version = 1; version <= 5; version++) {
      log.append(entry(BigInt(version)));
    }

    expect(log.coversExclusive(2n, 5n)).toBe(true);
    expect(log.coversExclusive(1n, 5n)).toBe(false);
    expect(log.entriesExclusive(2n, 5n).map((item) => item.version)).toEqual([3n, 4n, 5n]);
    expect(log.entriesExclusive(3n, 4n).map((item) => item.version)).toEqual([4n]);
  });

  it("reports empty coverage only for no-op ranges", () => {
    const log = new MutationLog(2);

    expect(log.coversExclusive(7n, 7n)).toBe(true);
    expect(log.coversExclusive(0n, 1n)).toBe(false);
    expect(log.entriesExclusive(0n, 1n)).toEqual([]);
  });
});

function entry(version: bigint): MutationLogEntry {
  return {
    version,
    kind: "insert",
    id: `row-${version.toString()}`,
    after: {
      id: `row-${version.toString()}`,
    },
    changedFields: new Set(["id"]),
  };
}
