import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import type { RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import { createInProcessChdbSnapshotBackend } from "../src/snapshot/chdb-in-process-backend.ts";
import type { VersionedRow } from "../src/snapshot/index.ts";
import {
  compareRowsForOrder,
  compareValues,
  QUERY_SEMANTICS_CONTRACT,
  rawQueryOrderBy,
  stableSortRows,
  valuesEqual,
} from "../src/protocol/query-semantics.ts";
import { executeRawQuery } from "../src/worker/query-engine.ts";

describe("CompareSemantics", () => {
  it("documents the shared query semantics seam and parity guard", () => {
    expect(QUERY_SEMANTICS_CONTRACT.owner).toBe("protocol/query-semantics");
    expect(QUERY_SEMANTICS_CONTRACT.parityTest).toBe("tests/query-semantics-parity.test.ts");
    expect(QUERY_SEMANTICS_CONTRACT.rules).toEqual(
      expect.arrayContaining([
        "raw queries append the topic id field as the stable ascending tiebreak unless already ordered",
        "filter string equality is case-insensitive except schema literal strings",
        "BigDecimal values compare by Effect BigDecimal semantics",
      ]),
    );
  });

  it("orders nulls first ascending and last descending", () => {
    const rows: readonly RuntimeRow[] = [
      { id: "value", rank: 1 },
      { id: "null", rank: null },
    ];

    expect(
      stableSortRows(rows, [{ field: "rank", direction: "asc" }]).map((row) => row.id),
    ).toEqual(["null", "value"]);
    expect(
      stableSortRows(rows, [{ field: "rank", direction: "desc" }]).map((row) => row.id),
    ).toEqual(["value", "null"]);
  });

  it("uses broad case-insensitive string equality unless literal strictness is requested", () => {
    expect(compareValues("bruno", "Bruno")).toBe(0);
    expect(valuesEqual("OPEN", "open")).toBe(true);
    expect(valuesEqual("OPEN", "open", true)).toBe(false);
  });

  it("orders BigDecimal values exactly", () => {
    const low = BigDecimal.fromStringUnsafe("1.000000000000000001");
    const high = BigDecimal.fromStringUnsafe("1.000000000000000002");
    const rows: readonly RuntimeRow[] = [
      { id: "high", amount: high },
      { id: "low", amount: low },
    ];

    expect(compareValues(low, high)).toBeLessThan(0);
    expect(
      stableSortRows(rows, [{ field: "amount", direction: "asc" }]).map((row) => row.id),
    ).toEqual(["low", "high"]);
  });

  it("appends a stable id tiebreak to raw query ordering", () => {
    const query = {
      fields: { id: true, score: true },
      orderBy: [{ field: "score", direction: "asc" }],
      limit: 10,
    } satisfies RuntimeRawQuery;
    const rows: readonly RuntimeRow[] = [
      { id: "b", score: 1 },
      { id: "a", score: 1 },
    ];

    expect(rawQueryOrderBy(query, "id")).toEqual([
      { field: "score", direction: "asc" },
      { field: "id", direction: "asc" },
    ]);
    expect(stableSortRows(rows, rawQueryOrderBy(query, "id")).map((row) => row.id)).toEqual([
      "a",
      "b",
    ]);
  });

  it("applies sort direction through row comparison", () => {
    expect(
      compareRowsForOrder({ id: "a", price: 10 }, { id: "b", price: 20 }, [
        { field: "price", direction: "desc" },
      ]),
    ).toBeGreaterThan(0);
  });

  it.effect("matches chDB for representative null, strict string, and BigDecimal ordering", () =>
    Effect.gen(function* () {
      const rows: readonly RuntimeRow[] = [
        {
          id: "a",
          status: "OPEN",
          nullableRank: null,
          amount: BigDecimal.fromStringUnsafe("2.000000000000000001"),
        },
        {
          id: "b",
          status: "open",
          nullableRank: 1,
          amount: BigDecimal.fromStringUnsafe("1.000000000000000001"),
        },
        {
          id: "c",
          status: "open",
          nullableRank: null,
          amount: BigDecimal.fromStringUnsafe("3.000000000000000001"),
        },
      ];
      const query = {
        fields: { id: true, status: true, nullableRank: true, amount: true },
        where: { field: "status", comparator: "equals", value: "open" },
        orderBy: [
          { field: "nullableRank", direction: "asc" },
          { field: "amount", direction: "desc" },
        ],
        limit: 10,
      } satisfies RuntimeRawQuery;
      const queryOptions = { literalStringFields: new Set(["status"]) };
      const backend = createInProcessChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows),
        literalStringFields: queryOptions.literalStringFields,
      });

      const memory = executeRawQuery(rows, query, "id", queryOptions);
      const chdb = yield* backend.snapshot({ query, targetVersion: 1n });

      expect(chdb.totalRows).toBe(memory.totalRows);
      expect(normalizeRows(chdb.rows)).toEqual(normalizeRows(memory.rows));
      expect(chdb.rows.map((row) => row.id)).toEqual(["c", "b"]);
    }).pipe(Effect.scoped),
  );
});

function versionedRows(rows: readonly RuntimeRow[]): readonly VersionedRow[] {
  return rows.map((row) => ({ row, version: 1n }));
}

function normalizeRows(rows: readonly RuntimeRow[]): readonly RuntimeRow[] {
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        BigDecimal.isBigDecimal(value) ? BigDecimal.format(value) : value,
      ]),
    ),
  );
}
