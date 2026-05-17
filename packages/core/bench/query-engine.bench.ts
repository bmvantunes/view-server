import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkPrimitive,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";
import type {
  DeltaOperation,
  OrderBy,
  RuntimeRawQuery,
  RuntimeRow,
  RuntimeRowKey,
  RuntimeRowKeyFn,
} from "../src/protocol/index.ts";
import {
  compareRowsForOrder,
  diffVisibleRows,
  executeRawQuery,
  matchesFilter,
  projectRawRow,
  rawQueryOrderBy,
  rowsEqual,
  stableSortRows,
} from "../src/worker/query-engine.ts";

type BenchConfig = {
  readonly pageSizes: readonly number[];
  readonly legacyMaxSize: number;
  readonly rawLegacyMaxSize: number;
  readonly iterations: number;
  readonly rawRows: number;
};

type Timed<T> = {
  readonly value: T;
  readonly ms: number;
};

const config: BenchConfig = {
  pageSizes: pageSizes(),
  legacyMaxSize: envNumber("VS_QUERY_ENGINE_LEGACY_MAX_SIZE", 10_000),
  rawLegacyMaxSize: envNumber("VS_QUERY_ENGINE_RAW_LEGACY_MAX_SIZE", 1_000),
  iterations: envNumber("VS_QUERY_ENGINE_ITERATIONS", 1),
  rawRows: envNumber("VS_QUERY_ENGINE_RAW_ROWS", 100_000),
};

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `query-engine benchmark pageSizes=${config.pageSizes.join(",")} legacyMaxSize=${config.legacyMaxSize} iterations=${config.iterations}`,
    );

    const results: BenchmarkResult[] = [];
    for (const pageSize of config.pageSizes) {
      const diff = runDiffBenchmark(pageSize, config.iterations, config.legacyMaxSize);
      results.push(diff);
      yield* Effect.logInfo(
        [
          `operation=diffVisibleRows`,
          `scenario=worst-case-reorder`,
          `pageSize=${pageSize}`,
          ...diff.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );

      const rawWindow = runRawWindowBenchmark(
        pageSize,
        config.rawRows,
        config.iterations,
        config.rawLegacyMaxSize,
      );
      results.push(rawWindow);
      yield* Effect.logInfo(
        [
          `operation=executeRawQuery`,
          `scenario=top-window`,
          `windowEnd=${pageSize}`,
          ...rawWindow.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );

      const sort = runStableSortBenchmark(pageSize, config.iterations);
      results.push(sort);
      yield* Effect.logInfo(
        [
          `operation=stableSortRows`,
          `scenario=mixed-values`,
          `pageSize=${pageSize}`,
          ...sort.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );
    }

    const artifact = yield* writeBenchmarkArtifact(
      "query-engine",
      {
        pageSizes: config.pageSizes.join(","),
        legacyMaxSize: config.legacyMaxSize,
        rawLegacyMaxSize: config.rawLegacyMaxSize,
        iterations: config.iterations,
        rawRows: config.rawRows,
      },
      results,
      {
        notes: [
          "diffVisibleRows compares optimized output against the legacy O(n^2) algorithm up to legacyMaxSize.",
          "executeRawQuery top-window compares the bounded-heap runtime path against the previous splice-maintained window path.",
          "stableSortRows index-array timing is exploratory and is not used by runtime unless it beats the current implementation.",
        ],
      },
    );
    yield* Effect.logInfo(
      `query-engine benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function runDiffBenchmark(
  pageSize: number,
  iterations: number,
  legacyMaxSize: number,
): BenchmarkResult {
  const previousRows = makeRows(pageSize);
  const nextRows = worstCaseReorderedRows(previousRows);
  const optimized = timeRepeated(iterations, () =>
    diffVisibleRows(previousRows, nextRows, keyById),
  );
  const metrics: BenchmarkMetric[] = [
    { name: "optimizedMs", value: optimized.ms, unit: "ms" },
    { name: "operationCount", value: optimized.value.length, unit: "count", lowerIsBetter: false },
    {
      name: "checksum",
      value: operationsChecksum(optimized.value),
      unit: "count",
      lowerIsBetter: false,
    },
  ];

  if (pageSize <= legacyMaxSize) {
    const legacy = timeRepeated(iterations, () =>
      legacyDiffVisibleRows(previousRows, nextRows, keyById),
    );
    expectSameOperations(legacy.value, optimized.value);
    metrics.push(
      { name: "legacyMs", value: legacy.ms, unit: "ms" },
      {
        name: "speedupRatio",
        value: optimized.ms === 0 ? Number.MAX_SAFE_INTEGER : legacy.ms / optimized.ms,
        unit: "ratio",
        lowerIsBetter: false,
      },
    );
  }

  return benchmarkResult("diffVisibleRows", "worst-case-reorder", pageSize, metrics);
}

function runRawWindowBenchmark(
  windowEnd: number,
  rowCount: number,
  iterations: number,
  legacyMaxSize: number,
): BenchmarkResult {
  const rows = makeRows(Math.max(rowCount, windowEnd * 4));
  const query = rawWindowQuery(windowEnd);
  const current = timeRepeated(iterations, () => executeRawQuery(rows, query, "id"));
  const metrics: BenchmarkMetric[] = [
    { name: "rows", value: rows.length, unit: "count", lowerIsBetter: false },
    { name: "currentMs", value: current.ms, unit: "ms" },
    {
      name: "checksum",
      value: rowsChecksum(current.value.rows),
      unit: "count",
      lowerIsBetter: false,
    },
  ];
  if (windowEnd <= legacyMaxSize) {
    const legacy = timeRepeated(iterations, () => legacyExecuteRawQueryWindowed(rows, query, "id"));
    expectSameRows(current.value.rows, legacy.value.rows);
    if (current.value.totalRows !== legacy.value.totalRows) {
      throw new Error(
        `Raw query totalRows mismatch: current=${current.value.totalRows} legacy=${legacy.value.totalRows}`,
      );
    }
    metrics.push(
      { name: "legacySpliceMs", value: legacy.ms, unit: "ms" },
      {
        name: "speedupRatio",
        value: current.ms === 0 ? Number.MAX_SAFE_INTEGER : legacy.ms / current.ms,
        unit: "ratio",
        lowerIsBetter: false,
      },
    );
  }
  if (windowEnd > 10_000) {
    const legacyFullSort = timeRepeated(iterations, () =>
      legacyExecuteRawQueryFullSort(rows, query, "id"),
    );
    expectSameRows(current.value.rows, legacyFullSort.value.rows);
    if (current.value.totalRows !== legacyFullSort.value.totalRows) {
      throw new Error(
        `Raw query full-sort totalRows mismatch: current=${current.value.totalRows} legacy=${legacyFullSort.value.totalRows}`,
      );
    }
    metrics.push(
      { name: "legacyFullSortMs", value: legacyFullSort.ms, unit: "ms" },
      {
        name: "fullSortSpeedupRatio",
        value: current.ms === 0 ? Number.MAX_SAFE_INTEGER : legacyFullSort.ms / current.ms,
        unit: "ratio",
        lowerIsBetter: false,
      },
    );
  }
  return benchmarkResult("executeRawQuery", "top-window", windowEnd, metrics);
}

function runStableSortBenchmark(pageSize: number, iterations: number): BenchmarkResult {
  const rows = makeRows(pageSize);
  const orderBy: OrderBy<RuntimeRow> = [
    { field: "bucket", direction: "asc" },
    { field: "price", direction: "desc" },
  ];
  const current = timeRepeated(iterations, () => stableSortRows(rows, orderBy));
  const indexArray = timeRepeated(iterations, () => stableSortRowsByIndexArray(rows, orderBy));
  expectSameRows(current.value, indexArray.value);
  return benchmarkResult("stableSortRows", "mixed-values", pageSize, [
    { name: "currentMs", value: current.ms, unit: "ms" },
    { name: "indexArrayMs", value: indexArray.ms, unit: "ms" },
    {
      name: "indexArraySpeedupRatio",
      value: indexArray.ms === 0 ? Number.MAX_SAFE_INTEGER : current.ms / indexArray.ms,
      unit: "ratio",
      lowerIsBetter: false,
    },
    { name: "checksum", value: rowsChecksum(current.value), unit: "count", lowerIsBetter: false },
  ]);
}

function benchmarkResult(
  operation: string,
  scenario: string,
  pageSize: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation,
      scenario,
      pageSize,
    },
    metrics,
  };
}

function rawWindowQuery(windowEnd: number): RuntimeRawQuery {
  return {
    fields: {
      id: true,
      bucket: true,
      price: true,
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
    offset: Math.max(0, windowEnd - 50),
    limit: 50,
  };
}

function legacyExecuteRawQueryWindowed(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
): { readonly rows: readonly RuntimeRow[]; readonly totalRows: number } {
  const orderBy = rawQueryOrderBy(query, idField);
  const offset = Math.max(0, Math.trunc(query.offset ?? 0));
  const limit = Math.max(0, Math.min(50, Math.trunc(query.limit ?? 50)));
  const windowEnd = offset + limit;
  const topRows: Array<{ readonly row: RuntimeRow; readonly index: number }> = [];
  let totalRows = 0;
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row === undefined || !matchesFilter(row, query.where)) {
      continue;
    }
    totalRows += 1;
    const candidate = { row, index };
    const worst = topRows[topRows.length - 1];
    if (
      topRows.length >= windowEnd &&
      worst !== undefined &&
      compareLegacySortEntries(candidate, worst, orderBy) >= 0
    ) {
      continue;
    }
    const insertIndex = legacyInsertionIndex(topRows, candidate, orderBy);
    topRows.splice(insertIndex, 0, candidate);
    if (topRows.length > windowEnd) {
      topRows.pop();
    }
  }
  return {
    rows: topRows
      .slice(offset, offset + limit)
      .map((entry) => projectRawRow(entry.row, query.fields, idField)),
    totalRows,
  };
}

function legacyExecuteRawQueryFullSort(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
): { readonly rows: readonly RuntimeRow[]; readonly totalRows: number } {
  const offset = Math.max(0, Math.trunc(query.offset ?? 0));
  const limit = Math.max(0, Math.min(50, Math.trunc(query.limit ?? 50)));
  const filtered = rows.filter((row) => matchesFilter(row, query.where));
  const sorted = stableSortRows(filtered, rawQueryOrderBy(query, idField));
  return {
    rows: sorted
      .slice(offset, offset + limit)
      .map((row) => projectRawRow(row, query.fields, idField)),
    totalRows: sorted.length,
  };
}

function legacyInsertionIndex(
  entries: readonly { readonly row: RuntimeRow; readonly index: number }[],
  candidate: { readonly row: RuntimeRow; readonly index: number },
  orderBy: OrderBy<RuntimeRow>,
): number {
  let low = 0;
  let high = entries.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const entry = entries[middle];
    if (entry !== undefined && compareLegacySortEntries(candidate, entry, orderBy) < 0) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}

function compareLegacySortEntries(
  left: { readonly row: RuntimeRow; readonly index: number },
  right: { readonly row: RuntimeRow; readonly index: number },
  orderBy: OrderBy<RuntimeRow>,
): number {
  const compared = compareRowsForOrder(left.row, right.row, orderBy);
  return compared !== 0 ? compared : left.index - right.index;
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
  return {
    value,
    ms: performance.now() - started,
  };
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

function stableSortRowsByIndexArray(
  rows: readonly RuntimeRow[],
  orderBy: OrderBy<RuntimeRow>,
): RuntimeRow[] {
  const indices = Array.from({ length: rows.length }, (_, index) => index);
  indices.sort((leftIndex, rightIndex) => {
    const left = rows[leftIndex];
    const right = rows[rightIndex];
    if (left === undefined || right === undefined) {
      return left === right ? 0 : left === undefined ? -1 : 1;
    }
    const compared = compareRowsForOrder(left, right, orderBy);
    return compared !== 0 ? compared : leftIndex - rightIndex;
  });
  const sorted: RuntimeRow[] = [];
  for (const index of indices) {
    const row = rows[index];
    if (row !== undefined) {
      sorted.push(row);
    }
  }
  return sorted;
}

function worstCaseReorderedRows(rows: readonly RuntimeRow[]): readonly RuntimeRow[] {
  return rows.toReversed().map((row, index) =>
    index % 10 === 0
      ? {
          ...row,
          price: Number(row.price) + 1,
        }
      : row,
  );
}

function makeRows(size: number): readonly RuntimeRow[] {
  return Array.from({ length: size }, (_, index) => ({
    id: `row-${index}`,
    bucket: index % 32 === 0 ? null : `bucket-${index % 128}`,
    price: (index * 97) % 10_000,
    status: index % 2 === 0 ? "open" : "closed",
  }));
}

function keyById(row: RuntimeRow): RuntimeRowKey {
  const id = row.id;
  if (typeof id !== "string" && typeof id !== "number") {
    throw new Error("Expected string or number id");
  }
  return id;
}

function expectSameOperations(
  left: readonly DeltaOperation<RuntimeRow>[],
  right: readonly DeltaOperation<RuntimeRow>[],
): void {
  if (left.length !== right.length) {
    throw new Error(`Operation length mismatch: left=${left.length} right=${right.length}`);
  }
  for (let index = 0; index < left.length; index++) {
    if (JSON.stringify(left[index]) !== JSON.stringify(right[index])) {
      throw new Error(`Operation mismatch at index ${index}`);
    }
  }
}

function expectSameRows(left: readonly RuntimeRow[], right: readonly RuntimeRow[]): void {
  if (left.length !== right.length) {
    throw new Error(`Row length mismatch: left=${left.length} right=${right.length}`);
  }
  for (let index = 0; index < left.length; index++) {
    if (keyById(left[index] ?? {}) !== keyById(right[index] ?? {})) {
      throw new Error(`Sorted row mismatch at index ${index}`);
    }
  }
}

function operationsChecksum(operations: readonly DeltaOperation<RuntimeRow>[]): number {
  let checksum = 0;
  for (const operation of operations) {
    checksum = mixChecksum(checksum, stringChecksum(operation.type));
    checksum = mixChecksum(
      checksum,
      stringChecksum(String("key" in operation ? operation.key : "")),
    );
    checksum = mixChecksum(checksum, "index" in operation ? (operation.index ?? -1) : -1);
  }
  return checksum;
}

function rowsChecksum(rows: readonly RuntimeRow[]): number {
  let checksum = 0;
  for (const row of rows) {
    checksum = mixChecksum(checksum, stringChecksum(String(row.id)));
    checksum = mixChecksum(checksum, Number(row.price) || 0);
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

function pageSizes(): readonly number[] {
  return envList("VS_QUERY_ENGINE_PAGE_SIZES", [50, 100, 1_000, 10_000, 50_000]);
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
