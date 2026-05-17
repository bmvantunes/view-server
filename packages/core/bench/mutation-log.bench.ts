import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import { MutationLog, type MutationLogEntry } from "../src/worker/mutation-log.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

const appendCounts = envList("VS_MUTATION_LOG_APPEND_COUNTS", [100_000, 1_000_000]);
const capacity = envNumber("VS_MUTATION_LOG_CAPACITY", 10_000);

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `mutation-log benchmark appendCounts=${appendCounts.join(",")} capacity=${capacity}`,
    );
    const results: BenchmarkResult[] = [];
    for (const appendCount of appendCounts) {
      const ringMs = time(() => appendRingLog(appendCount, capacity));
      const legacyMs = time(() => appendLegacyLog(appendCount, capacity));
      const result = benchmarkResult(appendCount, capacity, [
        { name: "ringMs", value: ringMs, unit: "ms" },
        { name: "legacyShiftMs", value: legacyMs, unit: "ms" },
        {
          name: "speedupRatio",
          value: ringMs === 0 ? Number.MAX_SAFE_INTEGER : legacyMs / ringMs,
          unit: "ratio",
          lowerIsBetter: false,
        },
      ]);
      results.push(result);
      yield* Effect.logInfo(
        [
          `operation=mutationLogAppend`,
          `appendCount=${appendCount}`,
          `capacity=${capacity}`,
          ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );
    }
    const artifact = yield* writeBenchmarkArtifact(
      "mutation-log",
      {
        appendCounts: appendCounts.join(","),
        capacity,
      },
      results,
      {
        notes: ["Compares fixed-capacity ring append against legacy Array.shift rollover."],
      },
    );
    yield* Effect.logInfo(
      `mutation-log benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function appendRingLog(appendCount: number, logCapacity: number): void {
  const log = new MutationLog(logCapacity);
  for (let index = 1; index <= appendCount; index++) {
    log.append(entry(index));
  }
}

function appendLegacyLog(appendCount: number, logCapacity: number): void {
  const entries: MutationLogEntry[] = [];
  for (let index = 1; index <= appendCount; index++) {
    entries.push(entry(index));
    while (entries.length > logCapacity) {
      entries.shift();
    }
  }
}

function entry(version: number): MutationLogEntry {
  return {
    version: BigInt(version),
    kind: "insert",
    id: version,
    after: {
      id: version,
    },
    changedFields: new Set(["id"]),
  };
}

function benchmarkResult(
  appendCount: number,
  logCapacity: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "mutationLogAppend",
      appendCount,
      capacity: logCapacity,
    },
    metrics,
  };
}

function time(run: () => void): number {
  const started = performance.now();
  run();
  return performance.now() - started;
}

function envList(name: string, fallback: readonly number[]): readonly number[] {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  return raw
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
}

function envNumber(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function formatMetric(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}
