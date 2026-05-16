import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import type {
  RuntimeGroupedQuery,
  RuntimeQuery,
  RuntimeRawQuery,
  RuntimeRow,
} from "../src/protocol/index.ts";
import { stableStringify } from "../src/protocol/index.ts";
import { createChdbSnapshotBackend } from "../src/snapshot/chdb-backend.ts";
import type { VersionedRow } from "../src/snapshot/index.ts";
import { makeActiveRawView } from "../src/worker/active-view.ts";
import type { MutationLogEntry, WorkerVersion } from "../src/worker/mutation-log.ts";
import {
  executeGroupedQuery,
  executeRawQuery,
  type QueryExecutionOptions,
  type QueryExecutionResult,
} from "../src/worker/query-engine.ts";

const seed = 0x5_16_20_26;
const queryOptions: QueryExecutionOptions = {
  literalStringFields: new Set(["status"]),
};

describe("query semantics parity", () => {
  it.effect("matches raw query semantics across memory, active raw view, and chDB", () =>
    Effect.gen(function* () {
      const rows = deterministicRows(seed, 64);
      const backend = createChdbSnapshotBackend({ groupedRefreshWorker: false });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows, 1n),
        literalStringFields: queryOptions.literalStringFields,
      });

      for (const entry of rawParityQueries) {
        const memory = executeRawQuery(rows, entry.query, "id", queryOptions);
        const active = makeActiveRawView(rows, entry.query, "id", queryOptions).snapshot();
        const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });

        expectParity(`${entry.name}: active`, active, memory, entry.query);
        expectParity(`${entry.name}: chDB`, chdb, memory, entry.query);
      }
    }).pipe(Effect.scoped),
  );

  it.effect("matches grouped aggregate semantics across memory and chDB", () =>
    Effect.gen(function* () {
      const rows = deterministicRows(seed, 64);
      const backend = createChdbSnapshotBackend({ groupedRefreshWorker: false });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows, 1n),
        literalStringFields: queryOptions.literalStringFields,
      });

      for (const entry of groupedParityQueries) {
        const memory = executeGroupedQuery(rows, entry.query, queryOptions);
        const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });

        expectParity(`${entry.name}: chDB`, chdb, memory, entry.query);
      }
    }).pipe(Effect.scoped),
  );

  it.effect("matches raw query semantics after deletes and row-moving updates", () =>
    Effect.gen(function* () {
      let rows = deterministicRows(seed, 64);
      const query = movingRawQuery;
      const view = makeActiveRawView(rows, query, "id", queryOptions);
      const backend = createChdbSnapshotBackend({ groupedRefreshWorker: false });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows, 1n),
        literalStringFields: queryOptions.literalStringFields,
      });

      expectParity(
        "initial active",
        view.snapshot(),
        executeRawQuery(rows, query, "id", queryOptions),
        query,
      );

      const mutations = deterministicMutations(rows);
      for (const mutation of mutations) {
        rows = applyMutation(rows, mutation);
        view.applyMutation(mutation);
        const memory = executeRawQuery(rows, query, "id", queryOptions);
        expectParity(
          `active after ${mutation.kind} ${mutation.version}`,
          view.snapshot(),
          memory,
          query,
        );
      }

      yield* backend.applyBatch({
        mutations,
        highestVersion: mutations[mutations.length - 1]?.version ?? 1n,
      });
      const chdb = yield* backend.snapshot({
        query,
        targetVersion: mutations[mutations.length - 1]?.version ?? 1n,
      });
      expectParity(
        "chDB after mutations",
        chdb,
        executeRawQuery(rows, query, "id", queryOptions),
        query,
      );
    }).pipe(Effect.scoped),
  );
});

type NamedRawQuery = {
  readonly name: string;
  readonly query: RuntimeRawQuery;
};

type NamedGroupedQuery = {
  readonly name: string;
  readonly query: RuntimeGroupedQuery;
};

const rawParityQueries: readonly NamedRawQuery[] = [
  {
    name: "broad case-insensitive string filter",
    query: {
      fields: {
        id: true,
        symbol: true,
        status: true,
      },
      where: {
        field: "symbol",
        comparator: "equals",
        value: "aapl",
      },
      orderBy: [
        { field: "symbol", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 20,
    },
  },
  {
    name: "strict literal string filter",
    query: {
      fields: {
        id: true,
        symbol: true,
        status: true,
        price: true,
      },
      where: {
        field: "status",
        comparator: "equals",
        value: "open",
      },
      orderBy: [
        { field: "price", direction: "desc" },
        { field: "id", direction: "asc" },
      ],
      offset: 1,
      limit: 8,
    },
  },
  {
    name: "nested filters and projection",
    query: {
      fields: {
        id: true,
        symbol: true,
        side: true,
        quantity: true,
      },
      where: {
        op: "and",
        conditions: [
          {
            field: "quantity",
            comparator: "greater_than_or_equal",
            value: 20,
          },
          {
            op: "or",
            conditions: [
              {
                field: "side",
                comparator: "equals",
                value: "buy",
              },
              {
                field: "symbol",
                comparator: "starts_with",
                value: "ms",
              },
            ],
          },
        ],
      },
      orderBy: [
        { field: "quantity", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      offset: 2,
      limit: 12,
    },
  },
  {
    name: "BigDecimal filter and sort",
    query: {
      fields: {
        id: true,
        decimalPrice: true,
      },
      where: {
        field: "decimalPrice",
        comparator: "greater_than",
        value: BigDecimal.fromStringUnsafe("100.000000000000000010"),
      },
      orderBy: [
        { field: "decimalPrice", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 10,
    },
  },
  {
    name: "null sort and pagination",
    query: {
      fields: {
        id: true,
        nullableRank: true,
      },
      orderBy: [
        { field: "nullableRank", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      offset: 1,
      limit: 10,
    },
  },
  {
    name: "comparator-equal stable id tiebreak",
    query: {
      fields: {
        id: true,
        status: true,
      },
      orderBy: [
        { field: "status", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      offset: 3,
      limit: 15,
    },
  },
];

const groupedParityQueries: readonly NamedGroupedQuery[] = [
  {
    name: "grouped numeric and string aggregates",
    query: {
      groupBy: ["symbol", "side"],
      aggregates: {
        trades: {
          aggFunc: "count",
          field: "id",
        },
        statuses: {
          aggFunc: "count_distinct",
          field: "status",
        },
        totalQuantity: {
          aggFunc: "sum",
          field: "quantity",
        },
        averagePrice: {
          aggFunc: "avg",
          field: "price",
        },
        ids: {
          aggFunc: "string_concat",
          field: "id",
          joiner: "|",
          sort: "asc",
        },
      },
      where: {
        field: "quantity",
        comparator: "greater_than",
        value: 10,
      },
      orderBy: [
        { field: "symbol", direction: "asc" },
        { field: "side", direction: "asc" },
      ],
      limit: 20,
    },
  },
  {
    name: "grouped BigDecimal aggregates",
    query: {
      groupBy: ["status"],
      aggregates: {
        totalDecimalPrice: {
          aggFunc: "sum",
          field: "decimalPrice",
        },
        maxDecimalPrice: {
          aggFunc: "max",
          field: "decimalPrice",
        },
      },
      orderBy: [
        { field: "status", direction: "asc" },
        { field: "totalDecimalPrice", direction: "asc" },
      ],
      limit: 10,
    },
  },
];

const movingRawQuery = {
  fields: {
    id: true,
    symbol: true,
    status: true,
    price: true,
  },
  where: {
    field: "status",
    comparator: "equals",
    value: "open",
  },
  orderBy: [
    { field: "price", direction: "asc" },
    { field: "id", direction: "asc" },
  ],
  limit: 12,
} satisfies RuntimeRawQuery;

function deterministicRows(initialSeed: number, count: number): readonly RuntimeRow[] {
  let state = initialSeed;
  const symbols = ["AAPL", "aapl", "MSFT", "msft", "NVDA", "AMZN"];
  const statuses = ["open", "OPEN", "closed", "pending"];
  const sides = ["buy", "sell"];
  return Array.from({ length: count }, (_, index) => {
    state = nextState(state);
    const price = 50 + (state % 125) + (index % 3) * 0.25;
    state = nextState(state);
    const quantity = 1 + (state % 50);
    const decimalPrice = BigDecimal.fromStringUnsafe(
      `${price.toFixed(2)}0000000000000000${String(index % 10)}`,
    );
    return {
      id: `o-${String(index).padStart(3, "0")}`,
      symbol: symbols[index % symbols.length],
      status: statuses[(index + (state % statuses.length)) % statuses.length],
      side: sides[index % sides.length],
      price,
      quantity,
      decimalPrice,
      nullableRank: index % 7 === 0 ? null : index % 11,
    };
  });
}

function deterministicMutations(rows: readonly RuntimeRow[]): readonly MutationLogEntry[] {
  const updateIntoWindowBefore = rows.find((row) => row.id === "o-005");
  const updateMoveBefore = rows.find((row) => row.id === "o-012");
  const deleteBefore = rows.find((row) => row.id === "o-000");
  const insertAfter: RuntimeRow = {
    id: "o-new",
    symbol: "AAPL",
    status: "open",
    side: "buy",
    price: 1,
    quantity: 99,
    decimalPrice: BigDecimal.fromStringUnsafe("1.000000000000000001"),
    nullableRank: 1,
  };
  return [
    update(2n, "o-005", updateIntoWindowBefore, {
      ...safeRow(updateIntoWindowBefore),
      status: "open",
      price: 2,
    }),
    update(3n, "o-012", updateMoveBefore, {
      ...safeRow(updateMoveBefore),
      status: "open",
      price: 400,
    }),
    remove(4n, "o-000", deleteBefore),
    insert(5n, "o-new", insertAfter),
  ];
}

function insert(version: WorkerVersion, id: string, after: RuntimeRow): MutationLogEntry {
  return {
    version,
    kind: "insert",
    id,
    after,
    changedFields: new Set(Object.keys(after)),
  };
}

function update(
  version: WorkerVersion,
  id: string,
  before: RuntimeRow | undefined,
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

function remove(
  version: WorkerVersion,
  id: string,
  before: RuntimeRow | undefined,
): MutationLogEntry {
  return {
    version,
    kind: "delete",
    id,
    before,
    changedFields: new Set(["id"]),
  };
}

function applyMutation(
  rows: readonly RuntimeRow[],
  mutation: MutationLogEntry,
): readonly RuntimeRow[] {
  if (mutation.kind === "delete") {
    return rows.filter((row) => row.id !== mutation.id);
  }
  if (mutation.after === undefined) {
    return rows;
  }
  const next = [...rows];
  const index = next.findIndex((row) => row.id === mutation.id);
  if (index >= 0) {
    next[index] = mutation.after;
  } else {
    next.push(mutation.after);
  }
  return next;
}

function expectParity(
  label: string,
  actual: QueryExecutionResult,
  expected: QueryExecutionResult,
  query: RuntimeQuery,
): void {
  try {
    expect(normalizeResult(actual)).toEqual(normalizeResult(expected));
  } catch (error) {
    throw new Error(
      `Query parity failed for ${label}\nseed=${String(seed)}\nquery=${stableStringify(query)}\nactual=${stableStringify(normalizeResult(actual))}\nexpected=${stableStringify(normalizeResult(expected))}`,
      { cause: error },
    );
  }
}

function normalizeResult(result: QueryExecutionResult): QueryExecutionResult {
  return {
    rows: result.rows.map(normalizeRow),
    totalRows: result.totalRows,
  };
}

function normalizeRow(row: RuntimeRow): RuntimeRow {
  return Object.fromEntries(
    Object.entries(row)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => [key, normalizeValue(value)]),
  );
}

function normalizeValue(value: unknown): unknown {
  if (BigDecimal.isBigDecimal(value)) {
    return BigDecimal.format(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
}

function versionedRows(
  rows: readonly RuntimeRow[],
  version: WorkerVersion,
): readonly VersionedRow[] {
  return rows.map((row) => ({ row, version }));
}

function changedFields(before: RuntimeRow | undefined, after: RuntimeRow): ReadonlySet<string> {
  const fields = new Set(Object.keys(after));
  if (before !== undefined) {
    for (const key of Object.keys(before)) {
      fields.add(key);
    }
  }
  return fields;
}

function safeRow(row: RuntimeRow | undefined): RuntimeRow {
  if (row === undefined) {
    throw new Error("Expected deterministic fixture row to exist");
  }
  return row;
}

function nextState(state: number): number {
  return (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
}
