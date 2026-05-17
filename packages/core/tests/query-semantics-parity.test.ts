import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import { mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { applyDeltaOperations } from "../src/client/visible-rows.ts";
import type {
  DeltaEvent,
  DeltaOperation,
  RuntimeGroupedQuery,
  RuntimeQuery,
  RuntimeRawQuery,
  RuntimeRow,
} from "../src/protocol/index.ts";
import { rowKeyByField, rowKeyForQuery, stableStringify } from "../src/protocol/index.ts";
import { createInProcessChdbSnapshotBackend } from "../src/snapshot/chdb-in-process-backend.ts";
import type { VersionedRow } from "../src/snapshot/index.ts";
import { makeActiveRawView } from "../src/worker/active-view.ts";
import { makeIncrementalGroupedAccumulator } from "../src/worker/grouped-accumulator.ts";
import { groupedAccumulatorQueryResult } from "../src/worker/grouped-accumulator-fanout.ts";
import type { MutationLogEntry, WorkerVersion } from "../src/worker/mutation-log.ts";
import {
  diffVisibleRows,
  executeGroupedQuery,
  executeRawQuery,
  matchesFilter,
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
      const backend = createInProcessChdbSnapshotBackend();
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
      const backend = createInProcessChdbSnapshotBackend();
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
      const backend = createInProcessChdbSnapshotBackend();
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

  it.effect(
    "uses chDB as the oracle for null, missing, and direction-sensitive raw sort order",
    () =>
      Effect.gen(function* () {
        const rows = edgeCaseRows();
        const backend = createInProcessChdbSnapshotBackend();
        yield* Effect.addFinalizer(() => backend.close());
        yield* backend.init({
          topic: "orders",
          idField: "id",
          version: 1n,
          rows: versionedRows(rows, 1n),
          literalStringFields: queryOptions.literalStringFields,
        });

        for (const entry of nullSortParityQueries) {
          const memory = executeRawQuery(rows, entry.query, "id", queryOptions);
          const active = makeActiveRawView(rows, entry.query, "id", queryOptions).snapshot();
          const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });

          expectParity(`${entry.name}: memory`, memory, chdb, entry.query, {
            rows,
            seed: edgeSeed,
          });
          expectParity(`${entry.name}: active`, active, chdb, entry.query, {
            rows,
            seed: edgeSeed,
          });
        }
      }).pipe(Effect.scoped),
  );

  it.effect("matches chDB for nullish filters, booleans, one_of, and pagination boundaries", () =>
    Effect.gen(function* () {
      const rows = edgeCaseRows();
      const backend = createInProcessChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows, 1n),
        literalStringFields: queryOptions.literalStringFields,
      });

      for (const entry of filterParityQueries) {
        const memory = executeRawQuery(rows, entry.query, "id", queryOptions);
        const active = makeActiveRawView(rows, entry.query, "id", queryOptions).snapshot();
        const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });

        expectParity(`${entry.name}: memory`, memory, chdb, entry.query, {
          rows,
          seed: edgeSeed,
        });
        expectParity(`${entry.name}: active`, active, chdb, entry.query, {
          rows,
          seed: edgeSeed,
        });
      }
    }).pipe(Effect.scoped),
  );

  it.effect(
    "matches grouped aggregate semantics across memory, grouped accumulator, and chDB",
    () =>
      Effect.gen(function* () {
        const rows = edgeCaseRows();
        const backend = createInProcessChdbSnapshotBackend();
        yield* Effect.addFinalizer(() => backend.close());
        yield* backend.init({
          topic: "orders",
          idField: "id",
          version: 1n,
          rows: versionedRows(rows, 1n),
          literalStringFields: queryOptions.literalStringFields,
        });

        for (const entry of exhaustiveGroupedParityQueries) {
          const memory = executeGroupedQuery(rows, entry.query, queryOptions);
          const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });
          expectParity(`${entry.name}: memory`, memory, chdb, entry.query, {
            rows,
            seed: edgeSeed,
          });

          const accumulator = makeIncrementalGroupedAccumulator({
            rows: rows.filter((row) => matchesFilter(row, entry.query.where, queryOptions)),
            query: entry.query,
            idOf: (row) => rowKeyByField(row, "id"),
          });
          if (accumulator !== undefined) {
            const accumulated = groupedAccumulatorQueryResult({
              query: entry.query,
              groupedAccumulator: accumulator,
            });
            expectParity(`${entry.name}: accumulator`, accumulated, chdb, entry.query, {
              rows,
              seed: edgeSeed,
            });
          }
        }
      }).pipe(Effect.scoped),
  );

  it.effect("makes coalesced client deltas converge to a fresh chDB snapshot", () =>
    Effect.gen(function* () {
      let rows = edgeCaseRows();
      const query = mutationParityQuery;
      const view = makeActiveRawView(rows, query, "id", queryOptions);
      let clientRows = executeRawQuery(rows, query, "id", queryOptions).rows;
      const backend = createInProcessChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows(rows, 1n),
        literalStringFields: queryOptions.literalStringFields,
      });

      const mutations = edgeCaseMutations(rows);
      const coalescedInitial = view.snapshot();
      for (const mutation of mutations) {
        const previous = view.snapshot();
        rows = applyMutation(rows, mutation);
        view.applyMutation(mutation);
        const next = view.snapshot();
        const ops = diffVisibleRows(previous.rows, next.rows, rowKeyForQuery(query, "id"));
        clientRows = applyDeltaOperations(
          clientRows,
          deltaEvent(mutation, ops, next.totalRows),
          "id",
        );

        expectParity(
          `active after ${mutation.kind} ${mutation.version}`,
          next,
          executeRawQuery(rows, query, "id", queryOptions),
          query,
          { rows, mutations, seed: edgeSeed },
        );
      }
      const coalescedNext = view.snapshot();

      yield* backend.applyBatch({
        mutations,
        highestVersion: mutations[mutations.length - 1]?.version ?? 1n,
      });
      const chdb = yield* backend.snapshot({
        query,
        targetVersion: mutations[mutations.length - 1]?.version ?? 1n,
      });

      expectParity(
        "client delta convergence",
        { rows: clientRows, totalRows: chdb.totalRows },
        chdb,
        query,
        {
          rows,
          mutations,
          seed: edgeSeed,
        },
      );

      const coalescedOps = diffVisibleRows(
        coalescedInitial.rows,
        coalescedNext.rows,
        rowKeyForQuery(query, "id"),
      );
      const coalescedClientRows = applyDeltaOperations(
        coalescedInitial.rows,
        deltaEventForRange(
          mutations[0]?.version === undefined ? 1n : mutations[0].version - 1n,
          mutations[mutations.length - 1]?.version ?? 1n,
          coalescedOps,
          coalescedNext.totalRows,
        ),
        "id",
      );

      expectParity(
        "coalesced multi-mutation delta convergence",
        { rows: coalescedClientRows, totalRows: chdb.totalRows },
        chdb,
        query,
        {
          rows,
          mutations,
          seed: edgeSeed,
        },
      );
    }).pipe(Effect.scoped),
  );

  it.effect("runs deterministic small, medium, and large fuzz parity profiles", () =>
    Effect.gen(function* () {
      for (const profile of fuzzProfiles) {
        const rows = deterministicRows(profile.seed, profile.rows);
        const backend = createInProcessChdbSnapshotBackend();
        yield* Effect.addFinalizer(() => backend.close());
        yield* backend.init({
          topic: `orders_${profile.name}`,
          idField: "id",
          version: 1n,
          rows: versionedRows(rows, 1n),
          literalStringFields: queryOptions.literalStringFields,
        });

        for (const entry of fuzzRawQueries(profile.seed)) {
          const memory = executeRawQuery(rows, entry.query, "id", queryOptions);
          const active = makeActiveRawView(rows, entry.query, "id", queryOptions).snapshot();
          const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });
          const context = { rows, seed: profile.seed, profile: profile.name };

          expectParity(`${profile.name}/${entry.name}: memory`, memory, chdb, entry.query, context);
          expectParity(`${profile.name}/${entry.name}: active`, active, chdb, entry.query, context);
        }

        for (const entry of fuzzGroupedQueries(profile.seed)) {
          const memory = executeGroupedQuery(rows, entry.query, queryOptions);
          const chdb = yield* backend.snapshot({ query: entry.query, targetVersion: 1n });
          expectParity(`${profile.name}/${entry.name}: grouped`, memory, chdb, entry.query, {
            rows,
            seed: profile.seed,
            profile: profile.name,
          });
        }
      }
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

type ParityContext = {
  readonly seed?: number | undefined;
  readonly profile?: string | undefined;
  readonly rows?: readonly RuntimeRow[] | undefined;
  readonly mutations?: readonly MutationLogEntry[] | undefined;
};

const edgeSeed = 0x516_2026;

const commonRawFields = {
  id: true,
  symbol: true,
  status: true,
  price: true,
  quantity: true,
  decimalPrice: true,
  nullableRank: true,
  nullableText: true,
  active: true,
} satisfies RuntimeRawQuery["fields"];

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

const nullSortParityQueries: readonly NamedRawQuery[] = [
  {
    name: "nullable number ASC explicit null missing undefined and negative zero",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "nullableRank", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "nullable number DESC explicit null missing undefined and negative zero",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "nullableRank", direction: "desc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "nullable string ASC with empty uppercase lowercase accents and missing values",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "nullableText", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "nullable string DESC with empty uppercase lowercase accents and missing values",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "nullableText", direction: "desc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "BigDecimal ASC with null missing negative zero and scale variants",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "decimalPrice", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "BigDecimal DESC with null missing negative zero and scale variants",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "decimalPrice", direction: "desc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "multi-sort nullable DESC then secondary ASC",
    query: {
      fields: commonRawFields,
      orderBy: [
        { field: "nullableRank", direction: "desc" },
        { field: "nullableText", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
];

const filterParityQueries: readonly NamedRawQuery[] = [
  {
    name: "equals null matches explicit null undefined and missing fields",
    query: {
      fields: commonRawFields,
      where: { field: "nullableRank", comparator: "equals", value: null },
      orderBy: [{ field: "id", direction: "asc" }],
      limit: 50,
    },
  },
  {
    name: "not equals null excludes explicit null undefined and missing fields",
    query: {
      fields: commonRawFields,
      where: { field: "nullableRank", comparator: "not_equals", value: null },
      orderBy: [{ field: "nullableRank", direction: "asc" }],
      limit: 50,
    },
  },
  {
    name: "broad one_of handles null and mixed string casing",
    query: {
      fields: commonRawFields,
      where: { field: "symbol", comparator: "one_of", value: ["aapl", null, "MSFT", "msft"] },
      orderBy: [
        { field: "symbol", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "strict one_of preserves literal string casing",
    query: {
      fields: commonRawFields,
      where: { field: "status", comparator: "one_of", value: ["open", null] },
      orderBy: [{ field: "id", direction: "asc" }],
      limit: 50,
    },
  },
  {
    name: "boolean filter and sort",
    query: {
      fields: commonRawFields,
      where: { field: "active", comparator: "equals", value: true },
      orderBy: [
        { field: "active", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "empty one_of returns no rows",
    query: {
      fields: commonRawFields,
      where: { field: "symbol", comparator: "one_of", value: [] },
      orderBy: [{ field: "id", direction: "asc" }],
      limit: 50,
    },
  },
  {
    name: "deep contradictory filters return no rows",
    query: {
      fields: commonRawFields,
      where: {
        op: "and",
        conditions: [
          { field: "quantity", comparator: "greater_than", value: 10 },
          {
            op: "or",
            conditions: [
              { field: "quantity", comparator: "less_than", value: 0 },
              {
                op: "and",
                conditions: [
                  { field: "status", comparator: "equals", value: "open" },
                  { field: "status", comparator: "equals", value: "closed" },
                ],
              },
            ],
          },
        ],
      },
      limit: 50,
    },
  },
  {
    name: "offset at exact boundary and beyond total rows",
    query: {
      fields: commonRawFields,
      orderBy: [{ field: "id", direction: "asc" }],
      offset: 50,
      limit: 50,
    },
  },
];

const exhaustiveGroupedParityQueries: readonly NamedGroupedQuery[] = [
  {
    name: "grouped null keys and full aggregate set",
    query: {
      groupBy: ["nullableText", "active"],
      aggregates: {
        rows: { aggFunc: "count", field: "id" },
        distinctStatuses: { aggFunc: "count_distinct", field: "status" },
        totalQuantity: { aggFunc: "sum", field: "quantity" },
        averageQuantity: { aggFunc: "avg", field: "quantity" },
        minQuantity: { aggFunc: "min", field: "quantity" },
        maxQuantity: { aggFunc: "max", field: "quantity" },
        totalDecimal: { aggFunc: "sum", field: "decimalPrice" },
        minDecimal: { aggFunc: "min", field: "decimalPrice" },
        maxDecimal: { aggFunc: "max", field: "decimalPrice" },
        labels: { aggFunc: "string_concat", field: "symbol", joiner: "|", sort: "asc" },
        distinctLabels: {
          aggFunc: "string_concat_distinct",
          field: "symbol",
          joiner: "|",
          sort: "desc",
        },
      },
      orderBy: [
        { field: "nullableText", direction: "asc" },
        { field: "active", direction: "asc" },
      ],
      limit: 50,
    },
  },
  {
    name: "incremental grouped accumulator eligible numeric aggregates",
    query: {
      groupBy: ["nullableText", "active"],
      aggregates: {
        rows: { aggFunc: "count", field: "id" },
        totalQuantity: { aggFunc: "sum", field: "quantity" },
        minQuantity: { aggFunc: "min", field: "quantity" },
        maxQuantity: { aggFunc: "max", field: "quantity" },
      },
      where: {
        op: "or",
        conditions: [
          { field: "status", comparator: "equals", value: "open" },
          { field: "nullableRank", comparator: "equals", value: null },
        ],
      },
      orderBy: [
        { field: "totalQuantity", direction: "desc" },
        { field: "nullableText", direction: "asc" },
        { field: "active", direction: "asc" },
      ],
      limit: 50,
    },
  },
];

const mutationParityQuery = {
  fields: {
    id: true,
    status: true,
    price: true,
    nullableRank: true,
  },
  where: { field: "status", comparator: "equals", value: "open" },
  orderBy: [
    { field: "price", direction: "asc" },
    { field: "id", direction: "asc" },
  ],
  limit: 20,
} satisfies RuntimeRawQuery;

const fuzzProfiles = [
  { name: "small", rows: 100, seed: 0x100 },
  { name: "medium", rows: 10_000, seed: 0x10_000 },
  { name: "large", rows: 250_000, seed: 0x250_000 },
] as const;

function edgeCaseRows(): readonly RuntimeRow[] {
  return [
    {
      id: "edge-00",
      symbol: "AAPL",
      status: "open",
      side: "buy",
      price: -0,
      quantity: null,
      decimalPrice: BigDecimal.fromStringUnsafe("1.0"),
      nullableRank: null,
      nullableText: null,
      active: true,
      hidden: "alpha",
    },
    {
      id: "edge-01",
      symbol: "aapl",
      status: "OPEN",
      side: "sell",
      price: 0,
      quantity: 10,
      decimalPrice: BigDecimal.fromStringUnsafe("1.00"),
      nullableRank: 0,
      nullableText: "",
      active: false,
      hidden: "beta",
    },
    {
      id: "edge-02",
      symbol: "ÁAPL",
      status: "open",
      side: "buy",
      price: -10,
      quantity: 20,
      decimalPrice: BigDecimal.fromStringUnsafe("-999999999999999.000000000000000001"),
      nullableRank: -1,
      nullableText: "Álpha",
      active: true,
      hidden: "gamma",
    },
    {
      id: "edge-03",
      symbol: "msft",
      status: "closed",
      side: "sell",
      price: 100.25,
      quantity: 0,
      decimalPrice: BigDecimal.fromStringUnsafe("0.000000000000000000"),
      nullableRank: undefined,
      nullableText: "alpha",
      hidden: "delta",
    },
    {
      id: "edge-04",
      symbol: "MSFT",
      status: null,
      side: "buy",
      price: 100.25,
      quantity: undefined,
      decimalPrice: null,
      nullableRank: 2,
      nullableText: "Zulu",
      active: false,
      hidden: "epsilon",
    },
    {
      id: "edge-05",
      side: "sell",
      price: 3.5,
      quantity: 4,
      decimalPrice: BigDecimal.fromStringUnsafe("100000000000000000000.000000000000000001"),
      nullableText: "ångström",
      active: true,
      hidden: "zeta",
    },
    {
      id: "edge-06",
      symbol: null,
      status: "open",
      side: "buy",
      price: 3.5,
      quantity: 4,
      nullableRank: null,
      nullableText: undefined,
      active: undefined,
      hidden: "eta",
    },
    {
      id: "edge-07",
      symbol: "NVDA",
      status: "pending",
      side: "sell",
      price: 3.5,
      quantity: 4,
      decimalPrice: BigDecimal.fromStringUnsafe("-0.00"),
      nullableRank: 2,
      nullableText: "Alpha",
      active: false,
      hidden: "theta",
    },
  ];
}

function deterministicRows(initialSeed: number, count: number): readonly RuntimeRow[] {
  let state = initialSeed;
  const symbols = ["AAPL", "aapl", "MSFT", "msft", "NVDA", "AMZN", "ÁAPL", "ångström"];
  const statuses = ["open", "OPEN", "closed", "pending", null];
  const sides = ["buy", "sell"];
  return Array.from({ length: count }, (_, index) => {
    state = nextState(state);
    const price =
      index % 97 === 0 ? -0 : 50 + (state % 125) + (index % 3) * 0.25 * (index % 2 === 0 ? 1 : -1);
    state = nextState(state);
    const quantity = index % 19 === 0 ? null : 1 + (state % 50);
    const decimalPrice = BigDecimal.fromStringUnsafe(
      `${price.toFixed(2)}0000000000000000${String(index % 10)}`,
    );
    const row: RuntimeRow = {
      id: `o-${String(index).padStart(3, "0")}`,
      symbol: index % 29 === 0 ? null : symbols[index % symbols.length],
      status: statuses[(index + (state % statuses.length)) % statuses.length],
      side: sides[index % sides.length],
      price,
      quantity,
      decimalPrice: index % 31 === 0 ? null : decimalPrice,
      nullableRank: index % 7 === 0 ? null : index % 11 === 0 ? undefined : index % 11,
    };
    if (index % 13 !== 0) {
      row.nullableText = index % 17 === 0 ? null : symbols[(index + 3) % symbols.length];
    }
    if (index % 23 !== 0) {
      row.active = index % 2 === 0;
    }
    return row;
  });
}

function edgeCaseMutations(rows: readonly RuntimeRow[]): readonly MutationLogEntry[] {
  const enteringBefore = rows.find((row) => row.id === "edge-01");
  const leavingBefore = rows.find((row) => row.id === "edge-02");
  const movingBefore = rows.find((row) => row.id === "edge-06");
  const missingToValueBefore = rows.find((row) => row.id === "edge-05");
  const hiddenBefore = rows.find((row) => row.id === "edge-00");
  const deleteBefore = rows.find((row) => row.id === "edge-03");
  const valueToNullAfter: RuntimeRow = {
    ...safeRow(leavingBefore),
    nullableRank: null,
  };
  const reinsertAfter: RuntimeRow = {
    id: "edge-03",
    symbol: "MSFT",
    status: "open",
    side: "sell",
    price: -20,
    quantity: 7,
    decimalPrice: BigDecimal.fromStringUnsafe("7.00"),
    nullableRank: -5,
    nullableText: "reinserted",
    active: true,
    hidden: "reinserted",
  };
  return [
    update(2n, "edge-01", enteringBefore, {
      ...safeRow(enteringBefore),
      status: "open",
      price: -30,
    }),
    update(3n, "edge-02", leavingBefore, valueToNullAfter),
    update(4n, "edge-02", valueToNullAfter, {
      ...valueToNullAfter,
      status: "closed",
    }),
    update(5n, "edge-06", movingBefore, {
      ...safeRow(movingBefore),
      status: "open",
      price: 500,
      nullableRank: 6,
    }),
    update(6n, "edge-05", missingToValueBefore, {
      ...safeRow(missingToValueBefore),
      symbol: "AMZN",
      status: "open",
      nullableRank: 4,
    }),
    update(7n, "edge-00", hiddenBefore, {
      ...safeRow(hiddenBefore),
      hidden: "hidden-only-change",
    }),
    remove(8n, "edge-03", deleteBefore),
    insert(9n, "edge-03", reinsertAfter),
  ];
}

function fuzzRawQueries(initialSeed: number): readonly NamedRawQuery[] {
  return [
    {
      name: "nullable rank asc window",
      query: {
        fields: commonRawFields,
        orderBy: [
          { field: "nullableRank", direction: "asc" },
          { field: "id", direction: "asc" },
        ],
        offset: initialSeed % 3,
        limit: 50,
      },
    },
    {
      name: "nullable rank desc string secondary",
      query: {
        fields: commonRawFields,
        orderBy: [
          { field: "nullableRank", direction: "desc" },
          { field: "nullableText", direction: "asc" },
          { field: "id", direction: "asc" },
        ],
        offset: initialSeed % 5,
        limit: 50,
      },
    },
    {
      name: "nested filter and BigDecimal order",
      query: {
        fields: commonRawFields,
        where: {
          op: "and",
          conditions: [
            {
              op: "or",
              conditions: [
                { field: "symbol", comparator: "one_of", value: ["aapl", "msft", null] },
                { field: "nullableRank", comparator: "equals", value: null },
              ],
            },
            { field: "quantity", comparator: "not_equals", value: null },
          ],
        },
        orderBy: [
          { field: "decimalPrice", direction: "asc" },
          { field: "id", direction: "asc" },
        ],
        limit: 50,
      },
    },
  ];
}

function fuzzGroupedQueries(initialSeed: number): readonly NamedGroupedQuery[] {
  return [
    {
      name: "grouped nullable keys numeric aggregates",
      query: {
        groupBy: ["nullableText", "active"],
        aggregates: {
          rows: { aggFunc: "count", field: "id" },
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          minQuantity: { aggFunc: "min", field: "quantity" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        where: {
          op: "or",
          conditions: [
            { field: "status", comparator: "equals", value: "open" },
            { field: "nullableRank", comparator: "equals", value: null },
          ],
        },
        orderBy: [
          { field: "totalQuantity", direction: initialSeed % 2 === 0 ? "asc" : "desc" },
          { field: "nullableText", direction: "asc" },
          { field: "active", direction: "asc" },
        ],
        limit: 50,
      },
    },
  ];
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

function deltaEvent(
  mutation: MutationLogEntry,
  ops: readonly DeltaOperation<RuntimeRow>[],
  totalRows: number,
): DeltaEvent<readonly RuntimeRow[]> {
  return deltaEventForRange(mutation.version - 1n, mutation.version, ops, totalRows);
}

function deltaEventForRange(
  fromVersion: WorkerVersion,
  toVersion: WorkerVersion,
  ops: readonly DeltaOperation<RuntimeRow>[],
  totalRows: number,
): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId: "query-semantics-parity",
    ops,
    meta: {
      fromVersion: String(fromVersion),
      toVersion: String(toVersion),
      totalRows,
      serverTime: 0,
    },
  };
}

function expectParity(
  label: string,
  actual: QueryExecutionResult,
  expected: QueryExecutionResult,
  query: RuntimeQuery,
  context: ParityContext = {},
): void {
  const actualResult = normalizeResult(actual);
  const expectedResult = normalizeResult(expected);
  try {
    expect(actualResult).toEqual(expectedResult);
  } catch (error) {
    const artifact = writeParityFailureArtifact({
      label,
      query,
      actual: actualResult,
      expected: expectedResult,
      context,
    });
    throw new Error(
      `Query parity failed for ${label}\nseed=${String(context.seed ?? seed)}\nquery=${stableStringify(query)}\nartifact=${artifact}\nactual=${stableStringify(actualResult)}\nexpected=${stableStringify(expectedResult)}`,
      { cause: error },
    );
  }
}

function writeParityFailureArtifact(args: {
  readonly label: string;
  readonly query: RuntimeQuery;
  readonly actual: QueryExecutionResult;
  readonly expected: QueryExecutionResult;
  readonly context: ParityContext;
}): string {
  const directory = join(tmpdir(), "view-server-query-parity");
  mkdirSync(directory, { recursive: true });
  const fileName = `${sanitizeFileName(args.context.profile ?? "parity")}-${String(args.context.seed ?? seed)}-${sanitizeFileName(args.label)}.json`;
  const path = join(directory, fileName);
  writeFileSync(
    path,
    `${JSON.stringify(
      {
        label: args.label,
        seed: args.context.seed ?? seed,
        profile: args.context.profile,
        query: artifactValue(args.query),
        rows: artifactValue(args.context.rows),
        mutations: artifactValue(args.context.mutations),
        actual: artifactValue(args.actual),
        expected: artifactValue(args.expected),
      },
      null,
      2,
    )}\n`,
  );
  return path;
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

function artifactValue(value: unknown): unknown {
  if (BigDecimal.isBigDecimal(value)) {
    return { type: "BigDecimal", value: BigDecimal.format(value) };
  }
  if (typeof value === "bigint") {
    return { type: "bigint", value: value.toString() };
  }
  if (value === undefined) {
    return { type: "undefined" };
  }
  if (Array.isArray(value)) {
    return value.map(artifactValue);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, artifactValue(entry)]),
    );
  }
  return value;
}

function sanitizeFileName(value: string): string {
  return value.replace(/[^a-z0-9._-]+/gi, "_").slice(0, 120);
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
