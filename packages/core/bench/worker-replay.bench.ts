import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { performance } from "node:perf_hooks";
import { defineConfig } from "../src/config/index.ts";
import type { RawQuery, RuntimeRow } from "../src/protocol/index.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../src/snapshot/index.ts";
import { executeMemoryQuery } from "../src/worker/query-engine.ts";
import { makeTopicWorkerCore } from "../src/worker/topic-worker-core.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkPrimitive,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

type Timed<T> = {
  readonly value: T;
  readonly ms: number;
};

const rowCount = envNumber("VS_WORKER_REPLAY_ROWS", 50_000);
const mutationCounts = envList("VS_WORKER_REPLAY_MUTATIONS", [100, 1_000, 5_000]);
const iterations = envNumber("VS_WORKER_REPLAY_ITERATIONS", 1);

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

void Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.logInfo(
        `worker-replay benchmark rows=${rowCount} mutationCounts=${mutationCounts.join(",")} iterations=${iterations}`,
      );
      const results: BenchmarkResult[] = [];
      for (const mutationCount of mutationCounts) {
        const initialRows = makeRows(rowCount);
        const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
          initialRows,
          snapshotBackend: replayBackend(initialRows, "id"),
          mutationLogSize: mutationCount + 10,
          maxActivePlans: 0,
        });
        for (let index = 0; index < mutationCount; index++) {
          if (index % 5 === 0) {
            yield* worker.deleteById(`row-${index}`);
          } else if (index % 5 === 1) {
            yield* worker.publish({
              id: `new-${index}`,
              symbol: `SYM-${index % 100}`,
              price: rowCount + index,
            });
          } else {
            yield* worker.deltaPublish({
              id: `row-${(index * 17) % rowCount}`,
              price: rowCount + index,
            });
          }
        }
        const replay = yield* timeEffectRepeated(iterations, () => worker.query(query));
        const result = benchmarkResult(mutationCount, [
          { name: "replayQueryMs", value: replay.ms, unit: "ms" },
          { name: "rowCount", value: replay.value.totalRows, unit: "count", lowerIsBetter: false },
          {
            name: "checksum",
            value: rowsChecksum(replay.value.rows),
            unit: "count",
            lowerIsBetter: false,
          },
        ]);
        results.push(result);
        yield* Effect.logInfo(
          [
            `operation=replayMutations`,
            `mutationCount=${mutationCount}`,
            ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
          ].join(" "),
        );
        yield* worker.shutdown;
      }
      const artifact = yield* writeBenchmarkArtifact(
        "worker-replay",
        { rows: rowCount, mutationCounts: mutationCounts.join(","), iterations },
        results,
        {
          notes: [
            "Measures fenced snapshot replay when the backend returns version 0 replayRows and the mutation log covers the gap.",
          ],
        },
      );
      yield* Effect.logInfo(
        `worker-replay benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
      );
    }),
  ),
);

function replayBackend(baseRows: readonly RuntimeRow[], idField: string): SnapshotBackend {
  let replayRows = baseRows.map((row) => ({ ...row }));
  let queryOptions: Parameters<typeof executeMemoryQuery>[3] = {};
  return {
    init: (args) =>
      Effect.sync(() => {
        replayRows = args.rows.map((entry) => ({ ...entry.row }));
        queryOptions = { literalStringFields: args.literalStringFields };
      }),
    applyBatch: (_args) => Effect.void,
    snapshot: (args) =>
      Effect.sync((): SnapshotBackendResult => {
        const result = executeMemoryQuery(replayRows, args.query, idField, queryOptions);
        return {
          ...result,
          backendVersion: 0n,
          replayRows,
        };
      }),
    close: () => Effect.void,
  };
}

function makeRows(size: number): readonly OrderRow[] {
  return Array.from({ length: size }, (_, index) => ({
    id: `row-${index}`,
    symbol: `SYM-${index % 100}`,
    price: index,
  }));
}

function benchmarkResult(
  mutationCount: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "replayMutations",
      scenario: "mixed-delete-insert-update",
      rows: rowCount,
      mutationCount,
    },
    metrics,
  };
}

function timeEffectRepeated<T, E, R>(
  iterations: number,
  run: () => Effect.Effect<T, E, R>,
): Effect.Effect<Timed<T>, E, R> {
  return Effect.gen(function* () {
    let value: T | undefined;
    const started = performance.now();
    for (let index = 0; index < iterations; index++) {
      value = yield* run();
    }
    if (value === undefined) {
      return yield* Effect.die(new Error("Benchmark must run at least once"));
    }
    return { value, ms: performance.now() - started };
  });
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
