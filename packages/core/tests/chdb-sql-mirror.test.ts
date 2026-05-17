import { Session } from "chdb";
import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import { compileQuerySql } from "../src/snapshot/chdb-sql-compiler.ts";
import { ChdbSqlMirror } from "../src/snapshot/chdb-sql-mirror.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";

const JSON_FORMAT_SETTINGS =
  "SETTINGS output_format_json_quote_decimals=1, output_format_json_quote_64bit_integers=1";

describe("ChdbSqlMirror", () => {
  it.effect("owns table creation, column evolution, tombstones, and escaped identifiers", () =>
    Effect.gen(function* () {
      const session = new Session();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          session.cleanup();
        }),
      );
      const mirror = new ChdbSqlMirror(session, `mirror\`orders_${Date.now()}`);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          mirror.drop();
        }).pipe(Effect.ignore),
      );

      mirror.init({
        idField: "order`id",
        version: 1n,
        rows: [
          {
            "order`id": "o-1",
            symbol: "AAPL",
            price: BigDecimal.fromStringUnsafe("10.000000000000000001"),
          },
          {
            "order`id": "o-2",
            symbol: "MSFT",
            price: BigDecimal.fromStringUnsafe("20.000000000000000002"),
          },
        ],
      });

      expect(mirror.tableReady).toBe(true);
      expect(mirror.columns).toContainEqual({
        name: "price",
        type: "Decimal(76, 38)",
        nullable: false,
      });

      mirror.applyMutations([
        updateRow(2n, "o-1", {
          "order`id": "o-1",
          symbol: "AAPL",
          price: BigDecimal.fromStringUnsafe("11.000000000000000001"),
          venue: null,
        }),
        deleteRow(3n, "o-2", {
          "order`id": "o-2",
          symbol: "MSFT",
          price: BigDecimal.fromStringUnsafe("20.000000000000000002"),
        }),
      ]);

      expect(mirror.columns).toContainEqual({
        name: "venue",
        type: "Int64",
        nullable: true,
      });

      const sql = compileQuerySql(
        latestRowsQuery,
        "order`id",
        mirror.columns,
        new Set(),
        mirror.tableName,
      );
      expect(sql.rowsSql).toContain("FROM `mirror``orders_");
      const rows = queryRows(session, sql.rowsSql);

      expect(rows).toEqual([
        {
          "order`id": "o-1",
          symbol: "AAPL",
          venue: null,
        },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("creates the table lazily from the first mutation batch after empty init", () =>
    Effect.gen(function* () {
      const session = new Session();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          session.cleanup();
        }),
      );
      const mirror = new ChdbSqlMirror(session, `mirror_empty_${Date.now()}`);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          mirror.drop();
        }).pipe(Effect.ignore),
      );

      mirror.init({
        idField: "id",
        rows: [],
        version: 0n,
      });
      expect(mirror.tableReady).toBe(false);

      mirror.applyMutations([
        {
          version: 1n,
          kind: "insert",
          id: "o-1",
          after: { id: "o-1", symbol: "AAPL" },
          changedFields: new Set(["id", "symbol"]),
        },
      ]);
      expect(mirror.tableReady).toBe(true);

      const sql = compileQuerySql(
        emptyInitQuery,
        "id",
        mirror.columns,
        new Set(),
        mirror.tableName,
      );
      expect(queryRows(session, sql.rowsSql)).toEqual([{ id: "o-1", symbol: "AAPL" }]);
    }).pipe(Effect.scoped),
  );

  it.effect("materializes missing initial fields and added mutation columns as nullable", () =>
    Effect.gen(function* () {
      const session = new Session();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          session.cleanup();
        }),
      );
      const mirror = new ChdbSqlMirror(session, `mirror_nullable_${Date.now()}`);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          mirror.drop();
        }).pipe(Effect.ignore),
      );

      mirror.init({
        idField: "id",
        rows: [{ id: "a", nullableRank: 1 }, { id: "b" }, { id: "c", nullableRank: null }],
        version: 1n,
      });

      expect(mirror.columns).toContainEqual({
        name: "nullableRank",
        type: "Float64",
        nullable: true,
      });

      mirror.applyMutations([
        updateRow(2n, "a", {
          id: "a",
          nullableRank: 1,
          lateVenue: "xnas",
        }),
      ]);

      expect(mirror.columns).toContainEqual({
        name: "lateVenue",
        type: "String",
        nullable: true,
      });
    }).pipe(Effect.scoped),
  );
});

const latestRowsQuery = {
  fields: {
    "order`id": true,
    symbol: true,
    venue: true,
  },
  orderBy: [{ field: "order`id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

const emptyInitQuery = {
  fields: {
    id: true,
    symbol: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

function updateRow(version: bigint, id: string, after: RuntimeRow): MutationLogEntry {
  return {
    version,
    kind: "update",
    id,
    after,
    changedFields: new Set(Object.keys(after)),
  };
}

function deleteRow(version: bigint, id: string, before: RuntimeRow): MutationLogEntry {
  return {
    version,
    kind: "delete",
    id,
    before,
    changedFields: new Set(Object.keys(before)),
  };
}

function queryRows(session: Session, sql: string): readonly Readonly<Record<string, unknown>>[] {
  const output = session.query(`${sql} ${JSON_FORMAT_SETTINGS}`, "JSONEachRow").trim();
  return output.length === 0 ? [] : output.split("\n").map(parseRow);
}

function parseRow(line: string): Readonly<Record<string, unknown>> {
  const value: unknown = JSON.parse(line);
  if (!isReadonlyRecord(value)) {
    throw new Error("Expected JSONEachRow output to contain an object row");
  }
  return value;
}

function isReadonlyRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
