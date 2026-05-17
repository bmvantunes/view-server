import { describe, expect, it } from "@effect/vitest";
import type { RuntimeGroupedQuery, RuntimeRow, RuntimeRowKey } from "../src/protocol/index.ts";
import {
  materializeGroupedAccumulatorChange,
  groupedAccumulatorQueryResult,
} from "../src/worker/grouped-accumulator-fanout.ts";
import { makeIncrementalGroupedAccumulator } from "../src/worker/grouped-accumulator.ts";

type TradeRow = {
  readonly id: string;
  readonly desk: "cash" | "rates";
  readonly region: "ny" | "ldn";
  readonly qty: number;
};

const query = {
  groupBy: ["region"],
  where: {
    field: "desk",
    comparator: "equals",
    value: "cash",
  },
  aggregates: {
    trades: { aggFunc: "count", field: "id" },
    totalQty: { aggFunc: "sum", field: "qty" },
  },
  orderBy: [{ field: "totalQty", direction: "desc" }],
  limit: 5,
} satisfies RuntimeGroupedQuery;

describe("grouped accumulator fanout", () => {
  it("materializes grouped accumulator updates with stable group keys", () => {
    const { accumulator, initialRows } = makeFixture();
    const change = materializeGroupedAccumulatorChange({
      query,
      groupedAccumulator: accumulator,
      lastRows: initialRows,
      lastTotalRows: initialRows.length,
      idField: "id",
      mutation: {
        version: 3n,
        kind: "insert",
        id: "t-3",
        after: trade({ id: "t-3", region: "ny", qty: 7 }),
        changedFields: new Set(["id", "region", "qty", "desk"]),
      },
    });

    expect(change?.totalRows).toBe(2);
    expect(change?.nextRows).toEqual([
      { region: "ny", trades: 2, totalQty: 17 },
      { region: "ldn", trades: 1, totalQty: 5 },
    ]);
    expect(change?.operations).toEqual([
      {
        type: "upsert",
        key: '{"region":"ny"}',
        row: { region: "ny", trades: 2, totalQty: 17 },
        index: 0,
      },
    ]);
  });

  it("returns no materialized change for mutations outside the grouped filter", () => {
    const { accumulator, initialRows } = makeFixture();
    const change = materializeGroupedAccumulatorChange({
      query,
      groupedAccumulator: accumulator,
      lastRows: initialRows,
      lastTotalRows: initialRows.length,
      idField: "id",
      mutation: {
        version: 3n,
        kind: "insert",
        id: "t-3",
        after: trade({ id: "t-3", desk: "rates", region: "ny", qty: 100 }),
        changedFields: new Set(["id", "region", "qty", "desk"]),
      },
    });

    expect(change).toBeUndefined();
    expect(groupedAccumulatorQueryResult({ query, groupedAccumulator: accumulator }).rows).toEqual(
      initialRows,
    );
  });

  it("applies filter enter and leave mutations as insert/delete against the accumulator", () => {
    const { accumulator, initialRows } = makeFixture();
    const enterChange = materializeGroupedAccumulatorChange({
      query,
      groupedAccumulator: accumulator,
      lastRows: initialRows,
      lastTotalRows: initialRows.length,
      idField: "id",
      mutation: {
        version: 3n,
        kind: "update",
        id: "t-3",
        before: trade({ id: "t-3", desk: "rates", region: "ny", qty: 100 }),
        after: trade({ id: "t-3", desk: "cash", region: "ny", qty: 100 }),
        changedFields: new Set(["desk"]),
      },
    });

    expect(enterChange?.nextRows?.[0]).toEqual({ region: "ny", trades: 2, totalQty: 110 });

    const leaveChange = materializeGroupedAccumulatorChange({
      query,
      groupedAccumulator: accumulator,
      lastRows: enterChange?.nextRows ?? [],
      lastTotalRows: enterChange?.totalRows ?? 0,
      idField: "id",
      mutation: {
        version: 4n,
        kind: "update",
        id: "t-3",
        before: trade({ id: "t-3", desk: "cash", region: "ny", qty: 100 }),
        after: trade({ id: "t-3", desk: "rates", region: "ny", qty: 100 }),
        changedFields: new Set(["desk"]),
      },
    });

    expect(leaveChange?.nextRows).toEqual(initialRows);
  });
});

function makeFixture(): {
  readonly accumulator: NonNullable<ReturnType<typeof makeIncrementalGroupedAccumulator>>;
  readonly initialRows: readonly RuntimeRow[];
} {
  const rows = [
    trade({ id: "t-1", region: "ny", qty: 10 }),
    trade({ id: "t-2", region: "ldn", qty: 5 }),
  ];
  const accumulator = makeIncrementalGroupedAccumulator({
    rows,
    query,
    idOf,
  });
  if (accumulator === undefined) {
    throw new Error("Expected grouped accumulator");
  }
  return {
    accumulator,
    initialRows: groupedAccumulatorQueryResult({ query, groupedAccumulator: accumulator }).rows,
  };
}

function trade(row: {
  readonly id: string;
  readonly desk?: TradeRow["desk"] | undefined;
  readonly region: TradeRow["region"];
  readonly qty: number;
}): TradeRow {
  return {
    id: row.id,
    desk: row.desk ?? "cash",
    region: row.region,
    qty: row.qty,
  };
}

function idOf(row: RuntimeRow): RuntimeRowKey {
  const id = row.id;
  if (typeof id !== "string" && typeof id !== "number") {
    throw new Error("Expected stable row id");
  }
  return id;
}
