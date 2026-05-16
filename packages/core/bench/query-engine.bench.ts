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
  RuntimeRow,
  RuntimeRowKey,
  RuntimeRowKeyFn,
} from "../src/protocol/index.ts";
import {
  compareRowsForOrder,
  diffVisibleRows,
  rowsEqual,
  stableSortRows,
} from "../src/worker/query-engine.ts";

type BenchConfig = {
  readonly pageSizes: readonly number[];
  readonly legacyMaxSize: number;
  readonly iterations: number;
};

type Timed<T> = {
  readonly value: T;
  readonly ms: number;
};

const config: BenchConfig = {
  pageSizes: pageSizes(),
  legacyMaxSize: envNumber("VS_QUERY_ENGINE_LEGACY_MAX_SIZE", 10_000),
  iterations: envNumber("VS_QUERY_ENGINE_ITERATIONS", 1),
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
        iterations: config.iterations,
      },
      results,
      {
        notes: [
          "diffVisibleRows compares optimized output against the legacy O(n^2) algorithm up to legacyMaxSize.",
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
