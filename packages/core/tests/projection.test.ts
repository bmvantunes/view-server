import { describe, expect, it } from "@effect/vitest";
import type { RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import { makeActiveRawView } from "../src/worker/active-view.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";
import {
  projectRow,
  projectedFieldsMayHaveChanged,
  projectedRowsEqual,
  visibleNonProjectedUpdateNoop,
} from "../src/worker/projection.ts";
import { executeRawQuery } from "../src/worker/query-engine.ts";

const query = {
  fields: { id: true, price: true },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 10,
} satisfies RuntimeRawQuery;

describe("Projection", () => {
  it("projects requested fields and always includes the id field", () => {
    expect(projectRow({ id: "a", price: 10, hidden: "x" }, { price: true }, "id")).toEqual({
      id: "a",
      price: 10,
    });
  });

  it("compares projected rows by exact visible values", () => {
    expect(projectedRowsEqual({ id: "a", price: 10 }, { id: "a", price: 10 })).toBe(true);
    expect(projectedRowsEqual({ id: "a", price: 10 }, { id: "a", price: 11 })).toBe(false);
    expect(projectedRowsEqual(undefined, { id: "a", price: 10 })).toBe(false);
  });

  it("classifies hidden-field updates as visible no-ops for visible rows", () => {
    const mutation = update(
      1n,
      "a",
      { id: "a", price: 10, hidden: "old" },
      { id: "a", price: 10, hidden: "new" },
    );

    expect(
      projectedFieldsMayHaveChanged({
        fields: query.fields,
        idField: "id",
        changedFields: mutation.changedFields,
      }),
    ).toBe(false);
    expect(
      visibleNonProjectedUpdateNoop({
        mutation,
        fields: query.fields,
        idField: "id",
        visibleIds: ["a"],
      }),
    ).toBe(true);
  });

  it("does not classify visible-field updates as projection no-ops", () => {
    const mutation = update(1n, "a", { id: "a", price: 10 }, { id: "a", price: 11 });

    expect(
      projectedFieldsMayHaveChanged({
        fields: query.fields,
        idField: "id",
        changedFields: mutation.changedFields,
      }),
    ).toBe(true);
    expect(
      visibleNonProjectedUpdateNoop({
        mutation,
        fields: query.fields,
        idField: "id",
        visibleIds: ["a"],
      }),
    ).toBe(false);
  });

  it("keeps memory snapshots and active raw view snapshots on the same projection semantics", () => {
    const rows: readonly RuntimeRow[] = [
      { id: "a", price: 10, hidden: "x" },
      { id: "b", price: 5, hidden: "y" },
    ];
    const activeView = makeActiveRawView(rows, query, "id");

    expect(activeView.snapshot()).toEqual(executeRawQuery(rows, query, "id"));
    expect(activeView.snapshot().rows).toEqual([
      { id: "b", price: 5 },
      { id: "a", price: 10 },
    ]);
  });
});

function update(
  version: bigint,
  id: string | number,
  before: RuntimeRow,
  after: RuntimeRow,
): MutationLogEntry {
  return {
    version,
    kind: "update",
    id,
    before,
    after,
    changedFields: changedFields(before, after),
  };
}

function changedFields(before: RuntimeRow, after: RuntimeRow): ReadonlySet<string> {
  const fields = new Set([...Object.keys(before), ...Object.keys(after)]);
  const changed = new Set<string>();
  for (const field of fields) {
    if (!Object.is(before[field], after[field])) {
      changed.add(field);
    }
  }
  return changed;
}
