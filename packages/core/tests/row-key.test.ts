import { describe, expect, it } from "@effect/vitest";
import { applyDeltaOperations } from "../src/client/visible-rows.ts";
import type { DeltaEvent, RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import { makeRowKey, rowKeyFromTopicConfig } from "../src/protocol/row-key.ts";
import { makeActiveRawView } from "../src/worker/active-view.ts";
import { MutationStore } from "../src/worker/mutation-store.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";
import { diffVisibleRows } from "../src/worker/query-engine.ts";

describe("RowKey", () => {
  it("creates a topic row-key policy with get, equals, encodeForWire, and decodeFromWire", () => {
    const rowKey = rowKeyFromTopicConfig({ id: "orderId" });
    const numericRow = { orderId: 1, price: 10 } satisfies RuntimeRow;
    const stringRow = { orderId: "1", price: 20 } satisfies RuntimeRow;

    expect(rowKey.idField).toBe("orderId");
    expect(rowKey.get(numericRow)).toBe(1);
    expect(rowKey.get(stringRow)).toBe("1");
    expect(rowKey.equals(1, "1")).toBe(false);
    expect(rowKey.encodeForWire(1)).toBe(1);
    expect(rowKey.decodeFromWire("1")).toBe("1");
  });

  it("rejects missing ids through the same row-key policy used by startup/publish guards", () => {
    const rowKey = makeRowKey("id");

    expect(() => rowKey.get({ price: 10 })).toThrow(
      "id must be a string or finite number, got undefined",
    );
    expect(() => rowKey.get({ id: Number.NaN })).toThrow(
      "id must be a string or finite number, got number",
    );
  });

  it("uses the same key semantics for deleteById and diffVisibleRows", () => {
    const rowKey = makeRowKey("id");
    const store = new MutationStore({ idField: "id", mutationLogSize: 10 });
    store.loadInitialRows([
      { id: 1, price: 10 },
      { id: "1", price: 20 },
    ]);
    const before = store.snapshotRows();

    const change = store.deleteById(1);
    const after = store.snapshotRows();
    const operations = diffVisibleRows(before, after, rowKey.get);

    expect(change?.entry.id).toBe(1);
    expect(store.rowById(1)).toBeUndefined();
    expect(store.rowById("1")).toEqual({ id: "1", price: 20 });
    expect(operations).toEqual([
      { type: "remove", key: 1 },
      { type: "upsert", key: "1", row: { id: "1", price: 20 }, index: 0 },
    ]);
  });

  it("keeps active raw view and client visible-row updates aligned", () => {
    const rowKey = makeRowKey("id");
    const rows: readonly RuntimeRow[] = [
      { id: 1, price: 10 },
      { id: "1", price: 20 },
      { id: "2", price: 30 },
    ];
    const query = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    } satisfies RuntimeRawQuery;
    const view = makeActiveRawView(rows, query, "id");
    const before = view.snapshot();
    const mutation = remove(1n, 1, rows[0]);
    const change = view.applyMutation(mutation);
    if (change.type !== "changed") {
      throw new Error("Expected active view to emit a changed snapshot");
    }
    const operations = diffVisibleRows(before.rows, change.result.rows, rowKey.get);
    const event = {
      type: "delta",
      requestId: "request-1",
      ops: operations,
      meta: {
        fromVersion: "0",
        toVersion: "1",
        totalRows: change.result.totalRows,
        serverTime: 123,
      },
    } satisfies DeltaEvent<readonly RuntimeRow[]>;

    expect(applyDeltaOperations(before.rows, event, rowKey.get)).toEqual(change.result.rows);
    expect(change.result.rows).toEqual([
      { id: "1", price: 20 },
      { id: "2", price: 30 },
    ]);
  });
});

function remove(
  version: bigint,
  id: string | number,
  before: RuntimeRow | undefined,
): MutationLogEntry {
  return {
    version,
    kind: "delete",
    id,
    before,
    changedFields: new Set(Object.keys(before ?? {})),
  };
}
