import { describe, expect, it } from "@effect/vitest";
import { applyDeltaOperations } from "../src/client/live-query-store.ts";
import type { DeltaEvent, DeltaOperation, RuntimeRow } from "../src/protocol/index.ts";

describe("applyDeltaOperations", () => {
  it("matches legacy operations for remove, insert, move, patch, and update cases", () => {
    const rows = [
      { id: "a", price: 10, status: "open" },
      { id: "b", price: 20, status: "open" },
      { id: "c", price: 30, status: "open" },
      { id: "d", price: 40, status: "open" },
    ];
    const ops: readonly DeltaOperation<RuntimeRow>[] = [
      { type: "remove", key: "b" },
      { type: "upsert", key: "d", row: { id: "d", price: 41, status: "open" }, index: 0 },
      { type: "patch", key: "a", changes: { status: "closed" }, index: 2 },
      { type: "upsert", key: "e", row: { id: "e", price: 50, status: "open" }, index: 1 },
      { type: "remove", key: "missing" },
      { type: "patch", key: "missing", changes: { status: "ignored" }, index: 0 },
    ];
    const event = deltaEvent(ops);

    expect(applyDeltaOperations(rows, event, "id")).toEqual(
      legacyApplyDeltaOperations(rows, event, "id"),
    );
  });
});

function deltaEvent(ops: readonly DeltaOperation<RuntimeRow>[]): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId: "test",
    ops,
    meta: {
      fromVersion: "0",
      toVersion: "1",
      totalRows: 0,
      serverTime: 0,
    },
  };
}

function legacyApplyDeltaOperations(
  rows: readonly RuntimeRow[],
  event: DeltaEvent<readonly RuntimeRow[]>,
  idField: string,
): readonly RuntimeRow[] {
  const next = rows.map((row) => ({ ...row }));
  for (const operation of event.ops) {
    if (operation.type === "remove") {
      const index = next.findIndex((row) => row[idField] === operation.key);
      if (index >= 0) {
        next.splice(index, 1);
      }
      continue;
    }

    if (operation.type === "patch") {
      const index = next.findIndex((row) => row[idField] === operation.key);
      if (index >= 0) {
        const patched = { ...next[index], ...operation.changes };
        next.splice(index, 1);
        next.splice(normalizeIndex(operation.index, next.length, index), 0, patched);
      }
      continue;
    }

    const key = operation.key ?? operation.row[idField];
    const index = next.findIndex((row) => row[idField] === key);
    if (index >= 0) {
      next.splice(index, 1);
    }
    next.splice(
      normalizeIndex(operation.index, next.length, index >= 0 ? index : next.length),
      0,
      operation.row,
    );
  }
  return next;
}

function normalizeIndex(index: number | undefined, length: number, fallback: number): number {
  if (index === undefined || !Number.isFinite(index)) {
    return Math.max(0, Math.min(length, fallback));
  }
  return Math.max(0, Math.min(length, Math.trunc(index)));
}
