import { describe, expect, it } from "@effect/vitest";
import type {
  DeltaOperation,
  RuntimeRawQuery,
  RuntimeRow,
  RuntimeRowKey,
  RuntimeRowKeyFn,
} from "../src/protocol/index.ts";
import {
  diffVisibleRows,
  executeRawQuery,
  projectRawRow,
  rawQueryOrderBy,
  rowsEqual,
  stableSortRows,
} from "../src/worker/query-engine.ts";

describe("query-engine visible row diff", () => {
  it("emits no operations when rows keep the same key, index, and values", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
    ];

    expect(diffVisibleRows(previousRows, [...previousRows], keyById)).toEqual([]);
  });

  it("emits remove operations before upserts when rows leave the visible window", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
    ];
    const nextRows = [
      { id: "a", price: 10 },
      { id: "c", price: 30 },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual([
      { type: "remove", key: "b" },
      { type: "upsert", key: "c", row: nextRows[1], index: 1 },
    ]);
  });

  it("emits upserts for inserted rows and shifted existing rows", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "c", price: 30 },
    ];
    const nextRows = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual([
      { type: "upsert", key: "b", row: nextRows[1], index: 1 },
      { type: "upsert", key: "c", row: nextRows[2], index: 2 },
    ]);
  });

  it("emits upserts with new indexes for moved rows", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
    ];
    const nextRows = [
      { id: "c", price: 30 },
      { id: "a", price: 10 },
      { id: "b", price: 20 },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual([
      { type: "upsert", key: "c", row: nextRows[0], index: 0 },
      { type: "upsert", key: "a", row: nextRows[1], index: 1 },
      { type: "upsert", key: "b", row: nextRows[2], index: 2 },
    ]);
  });

  it("emits an upsert when a row changes at the same index", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
    ];
    const nextRows = [
      { id: "a", price: 10 },
      { id: "b", price: 25 },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual([
      { type: "upsert", key: "b", row: nextRows[1], index: 1 },
    ]);
  });

  it("keeps stable operation order for mixed remove, move, and update changes", () => {
    const previousRows = [
      { id: "a", price: 10, status: "open" },
      { id: "b", price: 20, status: "open" },
      { id: "c", price: 30, status: "open" },
      { id: "d", price: 40, status: "open" },
    ];
    const nextRows = [
      { id: "d", price: 40, status: "open" },
      { id: "a", price: 15, status: "open" },
      { id: "e", price: 50, status: "open" },
      { id: "c", price: 30, status: "open" },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual([
      { type: "remove", key: "b" },
      { type: "upsert", key: "d", row: nextRows[0], index: 0 },
      { type: "upsert", key: "a", row: nextRows[1], index: 1 },
      { type: "upsert", key: "e", row: nextRows[2], index: 2 },
      { type: "upsert", key: "c", row: nextRows[3], index: 3 },
    ]);
  });

  it("matches the legacy diff algorithm for representative changes", () => {
    const previousRows = Array.from({ length: 100 }, (_, index) => ({
      id: `row-${index}`,
      price: index,
      status: index % 2 === 0 ? "open" : "closed",
    }));
    const nextRows = previousRows
      .filter((row) => row.id !== "row-10" && row.id !== "row-42")
      .map((row) => (row.id === "row-25" ? { ...row, price: 999 } : row))
      .toReversed()
      .slice(0, 80);

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual(
      legacyDiffVisibleRows(previousRows, nextRows, keyById),
    );
  });

  it("preserves legacy first-match semantics if duplicate keys are present", () => {
    const previousRows = [
      { id: "a", price: 10 },
      { id: "a", price: 20 },
      { id: "b", price: 30 },
    ];
    const nextRows = [
      { id: "a", price: 20 },
      { id: "b", price: 30 },
    ];

    expect(diffVisibleRows(previousRows, nextRows, keyById)).toEqual(
      legacyDiffVisibleRows(previousRows, nextRows, keyById),
    );
  });
});

describe("query-engine raw query execution", () => {
  it("matches full-sort raw query semantics for small sorted windows", () => {
    const rows = Array.from({ length: 1_000 }, (_, index) => ({
      id: `row-${index.toString().padStart(4, "0")}`,
      price: (index * 97) % 211,
      bucket: index % 13 === 0 ? null : `bucket-${index % 9}`,
      status: index % 3 === 0 ? "open" : "closed",
    }));
    const query = {
      fields: {
        id: true,
        price: true,
        bucket: true,
      },
      where: {
        field: "status",
        comparator: "equals",
        value: "open",
      },
      orderBy: [
        { field: "bucket", direction: "asc" },
        { field: "price", direction: "desc" },
      ],
      offset: 40,
      limit: 50,
    } satisfies RuntimeRawQuery;

    expect(executeRawQuery(rows, query, "id")).toEqual(legacyExecuteRawQuery(rows, query, "id"));
  });

  it("preserves stable tiebreak semantics inside the windowed raw query path", () => {
    const rows: RuntimeRow[] = [
      { id: "b", price: 10, group: "same" },
      { id: "a", price: 10, group: "same" },
      { id: "d", price: 10, group: "same" },
      { id: "c", price: 10, group: "same" },
    ];
    const query = {
      fields: {
        id: true,
        price: true,
        group: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 4,
    } satisfies RuntimeRawQuery;

    expect(executeRawQuery(rows, query, "id")).toEqual(legacyExecuteRawQuery(rows, query, "id"));
  });

  it("returns exact totalRows when the requested raw window is empty", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
    ];
    const query = {
      fields: {
        id: true,
        price: true,
      },
      limit: 0,
    } satisfies RuntimeRawQuery;

    expect(executeRawQuery(rows, query, "id")).toEqual({
      rows: [],
      totalRows: 2,
    });
  });
});

function keyById(row: RuntimeRow): RuntimeRowKey {
  const id = row.id;
  if (typeof id !== "string" && typeof id !== "number") {
    throw new Error("Expected string or number id");
  }
  return id;
}

function legacyDiffVisibleRows(
  previousRows: readonly RuntimeRow[],
  nextRows: readonly RuntimeRow[],
  rowKey: RuntimeRowKeyFn,
): readonly DeltaOperation<RuntimeRow>[] {
  const previousKeys = new Set(previousRows.map(rowKey));
  const nextKeys = new Set(nextRows.map(rowKey));
  const operations: DeltaOperation<RuntimeRow>[] = [];

  for (const row of previousRows) {
    const key = rowKey(row);
    if (!nextKeys.has(key)) {
      operations.push({ type: "remove", key });
    }
  }

  nextRows.forEach((row, index) => {
    const key = rowKey(row);
    const previousIndex = previousRows.findIndex((previous) => rowKey(previous) === key);
    const previous = previousIndex >= 0 ? previousRows[previousIndex] : undefined;
    if (!previousKeys.has(key) || previousIndex !== index || !rowsEqual(previous, row)) {
      operations.push({ type: "upsert", key, row, index });
    }
  });

  return operations;
}

function legacyExecuteRawQuery(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
): ReturnType<typeof executeRawQuery> {
  const filtered = rows.filter((row) => legacyMatchesFilter(row, query.where));
  const sorted = stableSortRows(filtered, rawQueryOrderBy(query, idField));
  const offset =
    query.offset === undefined || !Number.isFinite(query.offset)
      ? 0
      : Math.max(0, Math.trunc(query.offset));
  const limit =
    query.limit === undefined || !Number.isFinite(query.limit)
      ? 50
      : Math.max(0, Math.min(50, Math.trunc(query.limit)));
  return {
    rows: sorted
      .slice(offset, offset + limit)
      .map((row) => projectRawRow(row, query.fields, idField)),
    totalRows: sorted.length,
  };
}

function legacyMatchesFilter(row: RuntimeRow, filter: RuntimeRawQuery["where"]): boolean {
  if (filter === undefined) {
    return true;
  }
  if ("op" in filter) {
    const matches = filter.conditions.map((condition) => legacyMatchesFilter(row, condition));
    return filter.op === "and" ? matches.every(Boolean) : matches.some(Boolean);
  }
  const rowValue = row[filter.field];
  switch (filter.comparator) {
    case "equals":
      return typeof rowValue === "string" && typeof filter.value === "string"
        ? rowValue.toLocaleLowerCase() === filter.value.toLocaleLowerCase()
        : Object.is(rowValue, filter.value);
    case "not_equals":
      return typeof rowValue === "string" && typeof filter.value === "string"
        ? rowValue.toLocaleLowerCase() !== filter.value.toLocaleLowerCase()
        : !Object.is(rowValue, filter.value);
    case "greater_than":
      return Number(rowValue) > Number(filter.value);
    case "greater_than_or_equal":
      return Number(rowValue) >= Number(filter.value);
    case "less_than":
      return Number(rowValue) < Number(filter.value);
    case "less_than_or_equal":
      return Number(rowValue) <= Number(filter.value);
    case "contains":
      return (
        typeof rowValue === "string" &&
        typeof filter.value === "string" &&
        rowValue.toLocaleLowerCase().includes(filter.value.toLocaleLowerCase())
      );
    case "starts_with":
      return (
        typeof rowValue === "string" &&
        typeof filter.value === "string" &&
        rowValue.toLocaleLowerCase().startsWith(filter.value.toLocaleLowerCase())
      );
    case "one_of":
      return (
        Array.isArray(filter.value) && filter.value.some((value) => Object.is(rowValue, value))
      );
  }
}
