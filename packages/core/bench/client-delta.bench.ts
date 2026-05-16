import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import { applyDeltaOperations } from "../src/client/live-query-store.ts";
import type { DeltaEvent, DeltaOperation, RuntimeRow } from "../src/protocol/index.ts";
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

const windowSizes = envList("VS_CLIENT_DELTA_WINDOW_SIZES", [50, 1_000, 10_000]);
const operationCounts = envList("VS_CLIENT_DELTA_OPERATION_COUNTS", [1, 10, 100]);
const iterations = envNumber("VS_CLIENT_DELTA_ITERATIONS", 1);

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `client-delta benchmark windowSizes=${windowSizes.join(",")} operationCounts=${operationCounts.join(",")} iterations=${iterations}`,
    );
    const results: BenchmarkResult[] = [];
    for (const windowSize of windowSizes) {
      for (const operationCount of operationCounts) {
        const rows = makeRows(windowSize);
        const event = deltaEvent(makeMixedOperations(rows, operationCount));
        const optimized = timeRepeated(iterations, () => applyDeltaOperations(rows, event, "id"));
        const legacy = timeRepeated(iterations, () =>
          legacyApplyDeltaOperations(rows, event, "id"),
        );
        expectSameRows(optimized.value, legacy.value);
        const result = benchmarkResult(windowSize, operationCount, [
          { name: "optimizedMs", value: optimized.ms, unit: "ms" },
          { name: "legacyMs", value: legacy.ms, unit: "ms" },
          {
            name: "speedupRatio",
            value: optimized.ms === 0 ? Number.MAX_SAFE_INTEGER : legacy.ms / optimized.ms,
            unit: "ratio",
            lowerIsBetter: false,
          },
          {
            name: "checksum",
            value: rowsChecksum(optimized.value),
            unit: "count",
            lowerIsBetter: false,
          },
        ]);
        results.push(result);
        yield* Effect.logInfo(
          [
            `operation=applyDeltaOperations`,
            `windowSize=${windowSize}`,
            `operationCount=${operationCount}`,
            ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
          ].join(" "),
        );
      }
    }
    const artifact = yield* writeBenchmarkArtifact(
      "client-delta",
      {
        windowSizes: windowSizes.join(","),
        operationCounts: operationCounts.join(","),
        iterations,
      },
      results,
      {
        notes: [
          "Compares current key-index delta application against the previous per-operation findIndex implementation.",
        ],
      },
    );
    yield* Effect.logInfo(
      `client-delta benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function benchmarkResult(
  windowSize: number,
  operationCount: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "applyDeltaOperations",
      scenario: "mixed-remove-move-upsert",
      windowSize,
      operationCount,
    },
    metrics,
  };
}

function makeRows(size: number): readonly RuntimeRow[] {
  return Array.from({ length: size }, (_, index) => ({
    id: `row-${index}`,
    price: index,
    status: index % 2 === 0 ? "open" : "closed",
  }));
}

function makeMixedOperations(
  rows: readonly RuntimeRow[],
  operationCount: number,
): readonly DeltaOperation<RuntimeRow>[] {
  const removals: DeltaOperation<RuntimeRow>[] = [];
  const placements: DeltaOperation<RuntimeRow>[] = [];
  for (let index = 0; index < operationCount; index++) {
    const rowIndex = Math.max(0, rows.length - 1 - (index % Math.max(1, rows.length)));
    const row = rows[rowIndex];
    if (row === undefined) {
      continue;
    }
    if (index % 3 === 0) {
      removals.push({ type: "remove", key: String(row.id) });
    } else if (index % 3 === 1) {
      placements.push({
        type: "upsert",
        key: String(row.id),
        row: { ...row, price: Number(row.price) + 1 },
        index: index % Math.max(1, rows.length),
      });
    } else {
      placements.push({
        type: "upsert",
        key: `new-${index}`,
        row: { id: `new-${index}`, price: index, status: "new" },
        index: index % Math.max(1, rows.length),
      });
    }
  }
  return [...removals, ...placements];
}

function deltaEvent(ops: readonly DeltaOperation<RuntimeRow>[]): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId: "bench",
    ops,
    meta: {
      fromVersion: "0",
      toVersion: "1",
      totalRows: 0,
      serverTime: 0,
    },
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
  return { value, ms: performance.now() - started };
}

function legacyApplyDeltaOperations(
  rows: readonly RuntimeRow[],
  event: DeltaEvent<readonly RuntimeRow[]>,
  idField: string,
): readonly RuntimeRow[] {
  const next = rows.map((row) => ({ ...row }));
  for (const operation of event.ops) {
    if (operation.type === "remove") {
      const index = next.findIndex((row) => row[idField] === operation.key);
      if (index >= 0) {
        next.splice(index, 1);
      }
      continue;
    }
    if (operation.type === "patch") {
      const index = next.findIndex((row) => row[idField] === operation.key);
      if (index >= 0) {
        const patched = { ...next[index], ...operation.changes };
        next.splice(index, 1);
        next.splice(normalizeIndex(operation.index, next.length, index), 0, patched);
      }
      continue;
    }
    const key = operation.key ?? operation.row[idField];
    const index = next.findIndex((row) => row[idField] === key);
    if (index >= 0) {
      next.splice(index, 1);
    }
    next.splice(
      normalizeIndex(operation.index, next.length, index >= 0 ? index : next.length),
      0,
      operation.row,
    );
  }
  return next;
}

function normalizeIndex(index: number | undefined, length: number, fallback: number): number {
  if (index === undefined || !Number.isFinite(index)) {
    return Math.max(0, Math.min(length, fallback));
  }
  return Math.max(0, Math.min(length, Math.trunc(index)));
}

function expectSameRows(left: readonly RuntimeRow[], right: readonly RuntimeRow[]): void {
  if (left.length !== right.length) {
    throw new Error(`Row length mismatch: left=${left.length} right=${right.length}`);
  }
  for (let index = 0; index < left.length; index++) {
    if (JSON.stringify(left[index]) !== JSON.stringify(right[index])) {
      throw new Error(`Row mismatch at index ${index}`);
    }
  }
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
