import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { performance } from "node:perf_hooks";
import { defineConfig } from "../src/config/index.ts";
import type { RuntimeMutation, RuntimeRow } from "../src/protocol/index.ts";
import type { SnapshotBackend } from "../src/snapshot/index.ts";
import { makeTopicWorkerCore } from "../src/worker/topic-worker-core.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

type BackendCounters = {
  applyCalls: number;
  appliedMutations: number;
};

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

const batchSizes = envList("VS_WORKER_MUTATION_BATCH_SIZES", [1_000, 10_000]);
const iterations = envNumber("VS_WORKER_MUTATION_BATCH_ITERATIONS", 1);

void Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.logInfo(
        `worker-mutation-batch benchmark batchSizes=${batchSizes.join(",")} iterations=${iterations}`,
      );

      const results: BenchmarkResult[] = [];
      for (const batchSize of batchSizes) {
        const mutations = makeMutations(batchSize);
        const rows = makeRows(batchSize);
        const singleCounters: BackendCounters = { applyCalls: 0, appliedMutations: 0 };
        const batchCounters: BackendCounters = { applyCalls: 0, appliedMutations: 0 };
        const singleWorker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
          snapshotBackend: countingBackend(singleCounters),
          mutationLogSize: batchSize + 10,
          maxActivePlans: 0,
        });
        const batchWorker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
          snapshotBackend: countingBackend(batchCounters),
          mutationLogSize: batchSize + 10,
          maxActivePlans: 0,
        });

        const singleMs = yield* timeEffect(iterations, () =>
          Effect.forEach(rows, (row) => singleWorker.publish(row), {
            discard: true,
          }),
        );
        yield* Effect.yieldNow;

        const batchedMs = yield* timeEffect(iterations, () => batchWorker.mutateBatch(mutations));
        yield* Effect.yieldNow;

        const result = benchmarkResult(batchSize, [
          { name: "singleMs", value: singleMs, unit: "ms" },
          { name: "batchedMs", value: batchedMs, unit: "ms" },
          {
            name: "singleMutationsPerSecond",
            value: perSecond(batchSize * iterations, singleMs),
            unit: "count",
            lowerIsBetter: false,
          },
          {
            name: "batchedMutationsPerSecond",
            value: perSecond(batchSize * iterations, batchedMs),
            unit: "count",
            lowerIsBetter: false,
          },
          {
            name: "speedupRatio",
            value: batchedMs === 0 ? Number.MAX_SAFE_INTEGER : singleMs / batchedMs,
            unit: "ratio",
            lowerIsBetter: false,
          },
          { name: "singleBackendApplyCalls", value: singleCounters.applyCalls, unit: "count" },
          { name: "batchedBackendApplyCalls", value: batchCounters.applyCalls, unit: "count" },
          {
            name: "batchedBackendMutations",
            value: batchCounters.appliedMutations,
            unit: "count",
            lowerIsBetter: false,
          },
        ]);
        results.push(result);
        yield* Effect.logInfo(
          [
            `operation=workerMutationBatch`,
            `batchSize=${batchSize}`,
            ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
          ].join(" "),
        );
        yield* singleWorker.shutdown;
        yield* batchWorker.shutdown;
      }

      const artifact = yield* writeBenchmarkArtifact(
        "worker-mutation-batch",
        {
          batchSizes: batchSizes.join(","),
          iterations,
        },
        results,
        {
          notes: [
            "Compares legacy single-row worker publish calls with TopicWorkerCore.mutateBatch for equivalent publish mutations.",
            "The backend is a counting test backend so the benchmark isolates worker mutation/gate/fanout overhead.",
          ],
        },
      );
      yield* Effect.logInfo(
        `worker-mutation-batch benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
      );
    }),
  ),
);

function countingBackend(counters: BackendCounters): SnapshotBackend {
  return {
    init: () => Effect.void,
    applyBatch: (args) =>
      Effect.sync(() => {
        counters.applyCalls += 1;
        counters.appliedMutations += args.mutations.length;
      }),
    snapshot: () =>
      Effect.succeed({
        rows: [],
        totalRows: 0,
        backendVersion: 0n,
      }),
    close: () => Effect.void,
  };
}

function makeMutations(count: number): readonly RuntimeMutation[] {
  return makeRows(count).map((entry) => ({
    type: "publish",
    row: entry,
  }));
}

function makeRows(count: number): readonly (RuntimeRow & OrderRow)[] {
  return Array.from({ length: count }, (_, index) => row(index));
}

function row(index: number): RuntimeRow & OrderRow {
  return {
    id: `row-${index}`,
    symbol: index % 2 === 0 ? "AAPL" : "MSFT",
    price: index,
  };
}

function benchmarkResult(batchSize: number, metrics: readonly BenchmarkMetric[]): BenchmarkResult {
  return {
    case: {
      operation: "workerMutationBatch",
      batchSize,
    },
    metrics,
  };
}

function timeEffect<E, R>(
  iterations: number,
  run: () => Effect.Effect<void, E, R>,
): Effect.Effect<number, E, R> {
  return Effect.gen(function* () {
    const started = performance.now();
    for (let index = 0; index < iterations; index++) {
      yield* run();
    }
    return performance.now() - started;
  });
}

function perSecond(count: number, ms: number): number {
  return ms === 0 ? Number.MAX_SAFE_INTEGER : count / (ms / 1_000);
}

function envList(name: string, fallback: readonly number[]): readonly number[] {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const parsed = raw
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.trunc(value));
  return parsed.length === 0 ? fallback : parsed;
}

function envNumber(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? Math.trunc(value) : fallback;
}

function formatMetric(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}
