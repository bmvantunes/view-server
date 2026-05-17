import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import type { RuntimeGroupedQuery, RuntimeRow } from "../src/protocol/index.ts";
import {
  isIncrementalGroupedAccumulatorSupported,
  makeIncrementalGroupedAccumulator,
} from "../src/worker/grouped-accumulator.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";
import { executeGroupedQuery, stableSortRows } from "../src/worker/query-engine.ts";

describe("GroupedAccumulator", () => {
  it("keeps count, sum, min, and max state incrementally", () => {
    const rows = [
      { id: "a", desk: "ny", quantity: 10, price: BigDecimal.fromStringUnsafe("1.10") },
      { id: "b", desk: "ny", quantity: 20, price: BigDecimal.fromStringUnsafe("2.20") },
      { id: "c", desk: "ldn", quantity: 5, price: BigDecimal.fromStringUnsafe("3.30") },
    ];
    const accumulator = expectAccumulator(rows, numericGroupedQuery);

    accumulator.applyMutation({
      version: 2n,
      kind: "update",
      id: "b",
      before: rows[1],
      after: { id: "b", desk: "ldn", quantity: 40, price: BigDecimal.fromStringUnsafe("4.40") },
      changedFields: new Set(["desk", "quantity", "price"]),
    });
    accumulator.applyMutation({
      version: 3n,
      kind: "delete",
      id: "a",
      before: rows[0],
      changedFields: new Set(["id"]),
    });
    accumulator.applyMutation({
      version: 4n,
      kind: "insert",
      id: "d",
      after: { id: "d", desk: null, quantity: 7, price: BigDecimal.fromStringUnsafe("5.50") },
      changedFields: new Set(["id"]),
    });

    const expectedRows = [
      { id: "c", desk: "ldn", quantity: 5, price: BigDecimal.fromStringUnsafe("3.30") },
      { id: "b", desk: "ldn", quantity: 40, price: BigDecimal.fromStringUnsafe("4.40") },
      { id: "d", desk: null, quantity: 7, price: BigDecimal.fromStringUnsafe("5.50") },
    ];
    expectGroupedRows(accumulator.groupedRows(), expectedRows, numericGroupedQuery);
  });

  it("handles filter enter and leave by applying only matching row transitions", () => {
    const rows = [
      { id: "a", desk: "ny", status: "open", quantity: 10 },
      { id: "b", desk: "ny", status: "closed", quantity: 20 },
    ];
    const accumulator = expectAccumulator(
      rows.filter((row) => row.status === "open"),
      filteredGroupedQuery,
    );

    applyFilteredMutation(accumulator, filteredGroupedQuery, {
      version: 2n,
      kind: "update",
      id: "b",
      before: rows[1],
      after: { id: "b", desk: "ny", status: "open", quantity: 20 },
      changedFields: new Set(["status"]),
    });
    applyFilteredMutation(accumulator, filteredGroupedQuery, {
      version: 3n,
      kind: "update",
      id: "a",
      before: rows[0],
      after: { id: "a", desk: "ny", status: "closed", quantity: 10 },
      changedFields: new Set(["status"]),
    });

    expectGroupedRows(
      accumulator.groupedRows(),
      [{ id: "b", desk: "ny", status: "open", quantity: 20 }],
      filteredGroupedQuery,
    );
  });

  it("keeps min and max correct when the current extrema has duplicates", () => {
    const rows = [
      { id: "a", desk: "ny", quantity: 10, price: 50 },
      { id: "b", desk: "ny", quantity: 10, price: 60 },
      { id: "c", desk: "ny", quantity: 30, price: 60 },
    ];
    const accumulator = expectAccumulator(rows, numericGroupedQuery);

    accumulator.applyMutation({
      version: 2n,
      kind: "delete",
      id: "a",
      before: rows[0],
      changedFields: new Set(["quantity", "price"]),
    });
    expectGroupedRows(accumulator.groupedRows(), rows.slice(1), numericGroupedQuery);

    accumulator.applyMutation({
      version: 3n,
      kind: "delete",
      id: "b",
      before: rows[1],
      changedFields: new Set(["quantity", "price"]),
    });
    expectGroupedRows(accumulator.groupedRows(), rows.slice(2), numericGroupedQuery);
  });

  it("does not claim unsupported grouped aggregates as incremental", () => {
    expect(isIncrementalGroupedAccumulatorSupported(unsupportedGroupedQuery)).toBe(false);
    expect(
      makeIncrementalGroupedAccumulator({
        rows: [],
        query: unsupportedGroupedQuery,
        idOf: (row) => String(row.id),
      }),
    ).toBeUndefined();
  });
});

const numericGroupedQuery = {
  groupBy: ["desk"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    quantity: { aggFunc: "sum", field: "quantity" },
    priceTotal: { aggFunc: "sum", field: "price" },
    minQuantity: { aggFunc: "min", field: "quantity" },
    maxPrice: { aggFunc: "max", field: "price" },
  },
  orderBy: [{ field: "desk", direction: "asc" }],
  limit: 10,
} satisfies RuntimeGroupedQuery;

const filteredGroupedQuery = {
  groupBy: ["desk"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    quantity: { aggFunc: "sum", field: "quantity" },
  },
  where: { field: "status", comparator: "equals", value: "open" },
  orderBy: [{ field: "desk", direction: "asc" }],
  limit: 10,
} satisfies RuntimeGroupedQuery;

const unsupportedGroupedQuery = {
  groupBy: ["desk"],
  aggregates: {
    averageQuantity: { aggFunc: "avg", field: "quantity" },
  },
  limit: 10,
} satisfies RuntimeGroupedQuery;

function expectAccumulator(rows: readonly RuntimeRow[], query: RuntimeGroupedQuery) {
  const accumulator = makeIncrementalGroupedAccumulator({
    rows,
    query,
    idOf: (row) => String(row.id),
  });
  if (accumulator === undefined) {
    throw new Error("Expected grouped accumulator");
  }
  expectGroupedRows(accumulator.groupedRows(), rows, query);
  return accumulator;
}

function expectGroupedRows(
  actualRows: readonly RuntimeRow[],
  sourceRows: readonly RuntimeRow[],
  query: RuntimeGroupedQuery,
): void {
  const expected = executeGroupedQuery(sourceRows, query);
  const offset = query.offset ?? 0;
  const limit = query.limit ?? 50;
  const sortedActual = stableSortRows(actualRows, groupedOrder(query)).slice(
    offset,
    offset + limit,
  );
  expect(sortedActual.map(normalizeRow)).toEqual(expected.rows.map(normalizeRow));
}

function applyFilteredMutation(
  accumulator: ReturnType<typeof expectAccumulator>,
  query: RuntimeGroupedQuery,
  mutation: MutationLogEntry,
): void {
  const beforeMatches = mutation.before !== undefined && matchesOpenStatus(mutation.before, query);
  const afterMatches = mutation.after !== undefined && matchesOpenStatus(mutation.after, query);
  if (beforeMatches && mutation.before !== undefined) {
    accumulator.applyMutation({ ...mutation, kind: "delete", after: undefined });
  }
  if (afterMatches && mutation.after !== undefined) {
    accumulator.applyMutation({ ...mutation, kind: "insert", before: undefined });
  }
}

function matchesOpenStatus(row: RuntimeRow, query: RuntimeGroupedQuery): boolean {
  return query.where !== undefined && row.status === "open";
}

function groupedOrder(query: RuntimeGroupedQuery) {
  return [
    ...(query.orderBy ?? []),
    ...query.groupBy
      .filter((field) => !query.orderBy?.some((order) => order.field === field))
      .map((field) => ({ field, direction: "asc" as const })),
  ];
}

function normalizeRow(row: RuntimeRow): RuntimeRow {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      BigDecimal.isBigDecimal(value) ? BigDecimal.format(value) : value,
    ]),
  );
}
