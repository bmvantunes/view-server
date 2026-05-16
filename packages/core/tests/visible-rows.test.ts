import { describe, expect, it } from "@effect/vitest";
import {
  applyDeltaOperations,
  applySnapshot,
  applyStatus,
  isCurrentSubscriptionEvent,
} from "../src/client/visible-rows.ts";
import type { DeltaEvent, DeltaOperation, RuntimeRow } from "../src/protocol/index.ts";

describe("visible rows", () => {
  it("applies snapshots and status events", () => {
    const snapshot = applySnapshot({
      type: "snapshot",
      requestId: "request-1",
      rows: [{ id: "a", price: 1 }],
      meta: {
        version: "42",
        totalRows: 10,
        serverTime: 1,
      },
    });

    expect(snapshot).toEqual({
      rows: [{ id: "a", price: 1 }],
      totalRows: 10,
      version: 42n,
    });

    expect(
      applyStatus(snapshot.rows, {
        type: "status",
        requestId: "request-1",
        status: "stale",
        meta: {
          version: "43",
          totalRows: 11,
          serverTime: 2,
        },
      }),
    ).toEqual({
      rows: [{ id: "a", price: 1 }],
      totalRows: 11,
      status: "stale",
    });
  });

  it("guards events by request id", () => {
    const event = deltaEvent("current", []);

    expect(isCurrentSubscriptionEvent(event, "current")).toBe(true);
    expect(isCurrentSubscriptionEvent(event, "stale")).toBe(false);
  });

  it("returns no visible changes for an empty delta", () => {
    const rows = sampleRows();

    expect(applyDeltaOperations(rows, deltaEvent("request", []), "id")).toEqual(rows);
  });

  it("applies removed rows", () => {
    expect(
      applyDeltaOperations(
        sampleRows(),
        deltaEvent("request", [{ type: "remove", key: "b" }]),
        "id",
      ),
    ).toEqual([
      { id: "a", price: 10, status: "open" },
      { id: "c", price: 30, status: "open" },
      { id: "d", price: 40, status: "open" },
    ]);
  });

  it("applies inserted rows", () => {
    expect(
      applyDeltaOperations(
        sampleRows(),
        deltaEvent("request", [
          {
            type: "upsert",
            key: "e",
            row: { id: "e", price: 50, status: "open" },
            index: 1,
          },
        ]),
        "id",
      ),
    ).toEqual([
      { id: "a", price: 10, status: "open" },
      { id: "e", price: 50, status: "open" },
      { id: "b", price: 20, status: "closed" },
      { id: "c", price: 30, status: "open" },
      { id: "d", price: 40, status: "open" },
    ]);
  });

  it("applies moved rows", () => {
    expect(
      applyDeltaOperations(
        sampleRows(),
        deltaEvent("request", [
          {
            type: "upsert",
            key: "d",
            row: { id: "d", price: 40, status: "open" },
            index: 0,
          },
        ]),
        "id",
      ),
    ).toEqual([
      { id: "d", price: 40, status: "open" },
      { id: "a", price: 10, status: "open" },
      { id: "b", price: 20, status: "closed" },
      { id: "c", price: 30, status: "open" },
    ]);
  });

  it("applies changed rows at the same index", () => {
    expect(
      applyDeltaOperations(
        sampleRows(),
        deltaEvent("request", [
          {
            type: "upsert",
            key: "b",
            row: { id: "b", price: 21, status: "closed" },
            index: 1,
          },
        ]),
        "id",
      ),
    ).toEqual([
      { id: "a", price: 10, status: "open" },
      { id: "b", price: 21, status: "closed" },
      { id: "c", price: 30, status: "open" },
      { id: "d", price: 40, status: "open" },
    ]);
  });

  it("keeps unchanged rows with the same key and index stable", () => {
    expect(
      applyDeltaOperations(
        sampleRows(),
        deltaEvent("request", [
          {
            type: "upsert",
            key: "b",
            row: { id: "b", price: 20, status: "closed" },
            index: 1,
          },
        ]),
        "id",
      ),
    ).toEqual(sampleRows());
  });

  it("preserves mixed remove, move, and update operation order", () => {
    const rows = sampleRows();
    const operations: readonly DeltaOperation<RuntimeRow>[] = [
      { type: "remove", key: "b" },
      {
        type: "upsert",
        key: "d",
        row: { id: "d", price: 41, status: "open" },
        index: 0,
      },
      {
        type: "upsert",
        key: "e",
        row: { id: "e", price: 50, status: "open" },
        index: 1,
      },
    ];

    expect(applyDeltaOperations(rows, deltaEvent("request", operations), "id")).toEqual([
      { id: "d", price: 41, status: "open" },
      { id: "e", price: 50, status: "open" },
      { id: "a", price: 10, status: "open" },
      { id: "c", price: 30, status: "open" },
    ]);
  });

  it("uses indexed sequential application for interleaved operations", () => {
    const operations: readonly DeltaOperation<RuntimeRow>[] = [
      {
        type: "upsert",
        key: "d",
        row: { id: "d", price: 41, status: "open" },
        index: 0,
      },
      { type: "remove", key: "b" },
      {
        type: "patch",
        key: "a",
        changes: { status: "closed" },
        index: 2,
      },
    ];

    expect(applyDeltaOperations(sampleRows(), deltaEvent("request", operations), "id")).toEqual([
      { id: "d", price: 41, status: "open" },
      { id: "c", price: 30, status: "open" },
      { id: "a", price: 10, status: "closed" },
    ]);
  });

  it("handles a 10k-row snapshot to delta transition without changing semantics", () => {
    const rows = Array.from({ length: 10_000 }, (_, index) => ({
      id: `row-${index}`,
      price: index,
      status: index % 2 === 0 ? "open" : "closed",
    }));
    const removals = Array.from(
      { length: 50 },
      (_, index): DeltaOperation<RuntimeRow> => ({
        type: "remove",
        key: `row-${index}`,
      }),
    );
    const upserts = Array.from(
      { length: 50 },
      (_, index): DeltaOperation<RuntimeRow> => ({
        type: "upsert",
        key: `row-${9_999 - index}`,
        row: {
          id: `row-${9_999 - index}`,
          price: 20_000 + index,
          status: "open",
        },
        index,
      }),
    );

    const next = applyDeltaOperations(rows, deltaEvent("request", [...removals, ...upserts]), "id");

    expect(next).toHaveLength(9_950);
    expect(next.slice(0, 3)).toEqual([
      { id: "row-9999", price: 20_000, status: "open" },
      { id: "row-9998", price: 20_001, status: "open" },
      { id: "row-9997", price: 20_002, status: "open" },
    ]);
  });
});

function sampleRows(): readonly RuntimeRow[] {
  return [
    { id: "a", price: 10, status: "open" },
    { id: "b", price: 20, status: "closed" },
    { id: "c", price: 30, status: "open" },
    { id: "d", price: 40, status: "open" },
  ];
}

function deltaEvent(
  requestId: string,
  ops: readonly DeltaOperation<RuntimeRow>[],
): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId,
    ops,
    meta: {
      fromVersion: "0",
      toVersion: "1",
      totalRows: 0,
      serverTime: 0,
    },
  };
}
