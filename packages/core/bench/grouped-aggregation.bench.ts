import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import type {
  RuntimeAggregateDefinition,
  RuntimeAggregateMap,
  RuntimeGroupedQuery,
  RuntimeRow,
} from "../src/protocol/index.ts";
import { executeGroupedQuery, stableSortRows } from "../src/worker/query-engine.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkPrimitive,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

type Timed<T> = {
  readonly value: T;
  readonly ms: number;
};

const rows = envNumber("VS_GROUPED_AGGREGATION_ROWS", 100_000);
const groups = envNumber("VS_GROUPED_AGGREGATION_GROUPS", 1_000);
const aggregateCounts = envList("VS_GROUPED_AGGREGATION_AGGREGATES", [10, 50, 100]);
const iterations = envNumber("VS_GROUPED_AGGREGATION_ITERATIONS", 1);

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `grouped-aggregation benchmark rows=${rows} groups=${groups} aggregateCounts=${aggregateCounts.join(",")} iterations=${iterations}`,
    );
    const sourceRows = makeRows(rows, groups);
    const results: BenchmarkResult[] = [];
    for (const aggregateCount of aggregateCounts) {
      const query = groupedQuery(aggregateCount);
      const optimized = timeRepeated(iterations, () => executeGroupedQuery(sourceRows, query));
      const legacy = timeRepeated(iterations, () => legacyExecuteGroupedQuery(sourceRows, query));
      expectSameGroupedResult(optimized.value.rows, legacy.value.rows);
      const result = benchmarkResult(aggregateCount, [
        { name: "optimizedMs", value: optimized.ms, unit: "ms" },
        { name: "legacyMs", value: legacy.ms, unit: "ms" },
        {
          name: "speedupRatio",
          value: optimized.ms === 0 ? Number.MAX_SAFE_INTEGER : legacy.ms / optimized.ms,
          unit: "ratio",
          lowerIsBetter: false,
        },
        {
          name: "groupCount",
          value: optimized.value.totalRows,
          unit: "count",
          lowerIsBetter: false,
        },
        {
          name: "checksum",
          value: rowsChecksum(optimized.value.rows),
          unit: "count",
          lowerIsBetter: false,
        },
      ]);
      results.push(result);
      yield* Effect.logInfo(
        [
          `operation=groupedAggregation`,
          `aggregateCount=${aggregateCount}`,
          ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );
    }
    const artifact = yield* writeBenchmarkArtifact(
      "grouped-aggregation",
      { rows, groups, aggregateCounts: aggregateCounts.join(","), iterations },
      results,
      {
        notes: [
          "Compares single-pass grouped accumulation against the previous per-aggregate row-map implementation.",
        ],
      },
    );
    yield* Effect.logInfo(
      `grouped-aggregation benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function benchmarkResult(
  aggregateCount: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "groupedAggregation",
      scenario: "numeric-count-min-max",
      rows,
      groups,
      aggregateCount,
    },
    metrics,
  };
}

function groupedQuery(aggregateCount: number): RuntimeGroupedQuery {
  const aggregates: RuntimeAggregateMap = {};
  for (let index = 0; index < aggregateCount; index++) {
    const field = `value${index % 4}`;
    const alias = `agg${index}`;
    const mode = index % 5;
    aggregates[alias] =
      mode === 0
        ? { aggFunc: "count", field: "id" }
        : mode === 1
          ? { aggFunc: "sum", field }
          : mode === 2
            ? { aggFunc: "avg", field }
            : mode === 3
              ? { aggFunc: "min", field }
              : { aggFunc: "max", field };
  }
  return {
    groupBy: ["symbol"],
    aggregates,
    orderBy: [{ field: "symbol", direction: "asc" }],
    limit: 50,
  };
}

function makeRows(rowCount: number, groupCount: number): readonly RuntimeRow[] {
  return Array.from({ length: rowCount }, (_, index) => ({
    id: `row-${index}`,
    symbol: `SYM-${index % groupCount}`,
    value0: index % 1_000,
    value1: (index * 7) % 1_000,
    value2: (index * 13) % 1_000,
    value3: (index * 17) % 1_000,
  }));
}

function legacyExecuteGroupedQuery(
  rows: readonly RuntimeRow[],
  query: RuntimeGroupedQuery,
): { readonly rows: readonly RuntimeRow[]; readonly totalRows: number } {
  const groups = new Map<string, RuntimeRow[]>();
  for (const row of rows) {
    const key = JSON.stringify(query.groupBy.map((field) => row[field]));
    const existing = groups.get(key);
    if (existing === undefined) {
      groups.set(key, [row]);
    } else {
      existing.push(row);
    }
  }
  const result: RuntimeRow[] = [];
  for (const groupRows of groups.values()) {
    const first = groupRows[0];
    const row: RuntimeRow = {};
    for (const field of query.groupBy) {
      row[field] = first?.[field];
    }
    for (const [alias, aggregate] of Object.entries(query.aggregates)) {
      row[alias] = legacyAggregateRows(groupRows, aggregate);
    }
    result.push(row);
  }
  const sorted = stableSortRows(result, query.orderBy ?? []);
  return {
    rows: sorted.slice(0, 50),
    totalRows: sorted.length,
  };
}

function legacyAggregateRows(
  rows: readonly RuntimeRow[],
  aggregate: RuntimeAggregateDefinition,
): unknown {
  switch (aggregate.aggFunc) {
    case "count":
      return rows.length;
    case "count_distinct":
      return new Set(rows.map((row) => row[aggregate.field])).size;
    case "sum":
      return rows.map((row) => row[aggregate.field]).reduce<number>(numericSum, 0);
    case "avg":
      return rows.map((row) => row[aggregate.field]).reduce<number>(numericSum, 0) / rows.length;
    case "min":
      return Math.min(...rows.map((row) => Number(row[aggregate.field]) || 0));
    case "max":
      return Math.max(...rows.map((row) => Number(row[aggregate.field]) || 0));
    case "string_concat":
    case "string_concat_distinct":
      return "";
  }
}

function numericSum(sum: number, value: unknown): number {
  return sum + (typeof value === "number" ? value : 0);
}

function timeRepeated<T>(iterations: number, run: () => T): Timed<T> {
  let value: T | undefined;
  const started = performance.now();
  for (let index = 0; index < iterations; index++) {
    value = run();
  }
  if (value === undefined) {
    throw new Error("Benchmark must run at least once");
  }
  return { value, ms: performance.now() - started };
}

function expectSameGroupedResult(left: readonly RuntimeRow[], right: readonly RuntimeRow[]): void {
  if (left.length !== right.length) {
    throw new Error(`Grouped result length mismatch: left=${left.length} right=${right.length}`);
  }
  for (let index = 0; index < left.length; index++) {
    if (JSON.stringify(left[index]) !== JSON.stringify(right[index])) {
      throw new Error(`Grouped row mismatch at index ${index}`);
    }
  }
}

function rowsChecksum(rows: readonly RuntimeRow[]): number {
  let checksum = 0;
  for (const row of rows) {
    checksum = mixChecksum(checksum, stringChecksum(String(row.symbol)));
    for (const [key, value] of Object.entries(row)) {
      checksum = mixChecksum(checksum, stringChecksum(key));
      checksum = mixChecksum(checksum, Number(value) || 0);
    }
  }
  return checksum;
}

function mixChecksum(current: number, value: number): number {
  return (Math.imul(current ^ value, 16_777_619) >>> 0) % 1_000_000_007;
}

function stringChecksum(value: string): number {
  let checksum = 0;
  for (let index = 0; index < value.length; index++) {
    checksum = mixChecksum(checksum, value.charCodeAt(index));
  }
  return checksum;
}

function envList(name: string, fallback: readonly number[]): readonly number[] {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = value
    .split(",")
    .map((entry) => Number(entry.trim()))
    .filter((entry) => Number.isFinite(entry) && entry > 0)
    .map((entry) => Math.trunc(entry));
  return parsed.length === 0 ? fallback : parsed;
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function formatMetric(value: BenchmarkPrimitive): string {
  return typeof value === "number" ? value.toFixed(2) : String(value);
}
