import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import { createChdbSnapshotBackend } from "../src/snapshot/chdb-backend.ts";
import { compileQuerySql, type Column } from "../src/snapshot/chdb-sql-compiler.ts";
import type { VersionedRow } from "../src/snapshot/index.ts";
import { executeMemoryQuery } from "../src/worker/query-engine.ts";

describe("chDB SQL compiler", () => {
  it("compiles raw rows and count SQL with WHERE, order, limit, and escaped identifiers", () => {
    const sql = compileQuerySql(
      rawContractQuery,
      "id",
      contractColumns,
      new Set(["status"]),
      "topic`rows",
    );

    expect(sql.rowsSql).toContain("FROM `topic``rows`");
    expect(sql.rowsSql).toContain("WHERE (`status` = 'open' AND `weird``field` > 10)");
    expect(sql.rowsSql).toContain("ORDER BY isNull(`weird``field`) DESC, `weird``field` ASC");
    expect(sql.rowsSql).toContain("lower(toString(`symbol`)) DESC");
    expect(sql.rowsSql).toContain("lower(toString(`id`)) ASC");
    expect(sql.rowsSql).toContain("LIMIT 7 OFFSET 3");
    expect(sql.countSql).toContain("WHERE (`status` = 'open' AND `weird``field` > 10)");
    expect(sql.decimalFields).toEqual(new Set(["price"]));
  });

  it("compiles grouped rows and count SQL over the filtered grouped result", () => {
    const sql = compileQuerySql(
      groupedContractQuery,
      "id",
      contractColumns,
      new Set(["status"]),
      "topic_rows",
    );

    expect(sql.rowsSql).toContain("SELECT * FROM (SELECT `symbol`, sum(`price`) AS `totalPrice`");
    expect(sql.rowsSql).toContain("WHERE `status` = 'open' GROUP BY `symbol`");
    expect(sql.rowsSql).toContain("ORDER BY `totalPrice` DESC");
    expect(sql.rowsSql).toContain("LIMIT 5 OFFSET 1");
    expect(sql.countSql).toBe(
      "SELECT count() AS totalRows FROM (SELECT `symbol`, sum(`price`) AS `totalPrice`, count() AS `trades` FROM (SELECT `id`, `symbol`, `status`, `price`, `weird``field` FROM (SELECT `id`, `symbol`, `status`, `price`, `weird``field`, __view_server_deleted, __view_server_version FROM `topic_rows` ORDER BY `id`, __view_server_version DESC LIMIT 1 BY `id`) WHERE __view_server_deleted = 0) WHERE `status` = 'open' GROUP BY `symbol`)",
    );
    expect(sql.decimalFields).toEqual(new Set(["totalPrice"]));
    expect(sql.numberFields).toEqual(new Set(["trades"]));
  });

  it.effect("executes compiled raw and grouped count semantics through chDB", () =>
    Effect.gen(function* () {
      const rows = [
        { id: "a", symbol: "AAPL", status: "open", price: 10, quantity: 4 },
        { id: "b", symbol: "AAPL", status: "OPEN", price: 20, quantity: 6 },
        { id: "c", symbol: "MSFT", status: "open", price: 30, quantity: 8 },
        { id: "d", symbol: "NVDA", status: "closed", price: 40, quantity: 10 },
      ];
      const backend = createChdbSnapshotBackend({ groupedRefreshWorker: false });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows),
        literalStringFields: new Set(["status"]),
      });

      for (const query of executionQueries) {
        const memory = executeMemoryQuery(rows, query, "id", {
          literalStringFields: new Set(["status"]),
        });
        const chdb = yield* backend.snapshot({ query, targetVersion: 1n });
        expect(chdb.totalRows).toBe(memory.totalRows);
        expect(chdb.rows).toEqual(memory.rows);
      }
    }).pipe(Effect.scoped),
  );
});

const contractColumns: readonly Column[] = [
  { name: "id", type: "String", nullable: false },
  { name: "symbol", type: "String", nullable: false },
  { name: "status", type: "String", nullable: false },
  { name: "price", type: "Decimal(76, 38)", nullable: false },
  { name: "weird`field", type: "Int64", nullable: true },
];

const rawContractQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
    "weird`field": true,
  },
  where: {
    op: "and",
    conditions: [
      {
        field: "status",
        comparator: "equals",
        value: "open",
      },
      {
        field: "weird`field",
        comparator: "greater_than",
        value: 10,
      },
    ],
  },
  orderBy: [
    { field: "weird`field", direction: "asc" },
    { field: "symbol", direction: "desc" },
  ],
  offset: 3,
  limit: 7,
} satisfies RuntimeQuery;

const groupedContractQuery = {
  groupBy: ["symbol"],
  aggregates: {
    totalPrice: {
      aggFunc: "sum",
      field: "price",
    },
    trades: {
      aggFunc: "count",
      field: "id",
    },
  },
  where: {
    field: "status",
    comparator: "equals",
    value: "open",
  },
  orderBy: [{ field: "totalPrice", direction: "desc" }],
  offset: 1,
  limit: 5,
} satisfies RuntimeQuery;

const executionQueries: readonly RuntimeQuery[] = [
  {
    fields: {
      id: true,
      symbol: true,
      quantity: true,
    },
    where: {
      field: "status",
      comparator: "equals",
      value: "open",
    },
    orderBy: [
      { field: "quantity", direction: "asc" },
      { field: "id", direction: "asc" },
    ],
    limit: 10,
  },
  {
    groupBy: ["symbol"],
    aggregates: {
      totalQuantity: {
        aggFunc: "sum",
        field: "quantity",
      },
      trades: {
        aggFunc: "count",
        field: "id",
      },
    },
    where: {
      field: "status",
      comparator: "equals",
      value: "open",
    },
    orderBy: [{ field: "symbol", direction: "asc" }],
    limit: 10,
  },
];

function versionedRows(rows: readonly RuntimeRow[]): readonly VersionedRow[] {
  return rows.map((row) => ({ row, version: 1n }));
}
