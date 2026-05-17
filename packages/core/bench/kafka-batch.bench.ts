import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { performance } from "node:perf_hooks";
import { KafkaSource } from "../src/config/index.ts";
import { decodeJsonRecord, ingestKafkaBatch, applyKafkaSourceMessage } from "../src/kafka/index.ts";
import type { KafkaConsumerRecord, SourceMutation } from "../src/config/index.ts";
import type { KafkaSourceRuntime } from "../src/kafka/index.ts";
import type { ViewServerError } from "../src/errors.ts";
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

type RuntimeCounters = {
  singleCalls: number;
  batchCalls: number;
  batchMutations: number;
};

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const batchSizes = envList("VS_KAFKA_BATCH_BENCH_SIZES", [10_000, 100_000]);
const iterations = envNumber("VS_KAFKA_BATCH_BENCH_ITERATIONS", 1);

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `kafka-batch benchmark batchSizes=${batchSizes.join(",")} iterations=${iterations}`,
    );
    const results: BenchmarkResult[] = [];
    const source = KafkaSource<OrderRow, "id">({
      brokers: ["127.0.0.1:9092"],
      topic: "orders-events",
      groupId: "view-server-orders",
      decode: decodeJsonRecord<OrderRow, "id">({ topic: "orders", schema: Order }),
    });

    for (const batchSize of batchSizes) {
      const records = makeRecords(batchSize);
      const legacyCounters = runtimeCounters();
      const batchedCounters = runtimeCounters();
      const legacyRuntime = countingRuntime(legacyCounters);
      const batchedRuntime = countingRuntime(batchedCounters);
      const legacyMs = yield* timeEffect(iterations, () =>
        legacyKafkaBatch(records, legacyRuntime, source),
      );
      const batchedMs = yield* timeEffect(iterations, () =>
        ingestKafkaBatch({
          viewTopic: "orders",
          idField: "id",
          source,
          runtime: batchedRuntime,
          batch: {
            records,
            commit: Effect.void,
          },
          commitPolicy: "after-ingest",
        }),
      );
      const result = benchmarkResult(batchSize, [
        { name: "legacyMs", value: legacyMs, unit: "ms" },
        { name: "batchedMs", value: batchedMs, unit: "ms" },
        {
          name: "speedupRatio",
          value: batchedMs === 0 ? Number.MAX_SAFE_INTEGER : legacyMs / batchedMs,
          unit: "ratio",
          lowerIsBetter: false,
        },
        { name: "legacyRuntimeCalls", value: legacyCounters.singleCalls, unit: "count" },
        { name: "batchedRuntimeCalls", value: batchedCounters.batchCalls, unit: "count" },
        { name: "batchedMutations", value: batchedCounters.batchMutations, unit: "count" },
      ]);
      results.push(result);
      yield* Effect.logInfo(
        [
          `operation=kafkaBatchIngest`,
          `batchSize=${batchSize}`,
          ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );
    }

    const artifact = yield* writeBenchmarkArtifact(
      "kafka-batch",
      {
        batchSizes: batchSizes.join(","),
        iterations,
      },
      results,
      {
        notes: ["Compares legacy per-record runtime dispatch with the batched Kafka ingest path."],
      },
    );
    yield* Effect.logInfo(
      `kafka-batch benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function legacyKafkaBatch(
  records: readonly KafkaConsumerRecord[],
  runtime: KafkaSourceRuntime<OrderRow, "id">,
  source: ReturnType<typeof KafkaSource<OrderRow, "id">>,
): Effect.Effect<void, ViewServerError> {
  return Effect.forEach(
    records,
    (record) =>
      source
        .decode(record)
        .pipe(
          Effect.flatMap((message) => applyKafkaSourceMessage("orders", "id", runtime, message)),
        ),
    { discard: true },
  );
}

function countingRuntime(counters: RuntimeCounters): KafkaSourceRuntime<OrderRow, "id"> {
  return {
    publish: () =>
      Effect.sync(() => {
        counters.singleCalls += 1;
      }),
    deltaPublish: () =>
      Effect.sync(() => {
        counters.singleCalls += 1;
      }),
    deleteById: () =>
      Effect.sync(() => {
        counters.singleCalls += 1;
      }),
    mutateBatch: (mutations: readonly SourceMutation<OrderRow, "id">[]) =>
      Effect.sync(() => {
        counters.batchCalls += 1;
        counters.batchMutations += mutations.length;
      }),
  };
}

function runtimeCounters(): RuntimeCounters {
  return {
    singleCalls: 0,
    batchCalls: 0,
    batchMutations: 0,
  };
}

function makeRecords(count: number): readonly KafkaConsumerRecord[] {
  return Array.from({ length: count }, (_, index) => ({
    topic: "orders-events",
    key: `o-${index}`,
    offset: String(index),
    value: JSON.stringify({
      id: `o-${index}`,
      symbol: index % 2 === 0 ? "AAPL" : "MSFT",
      price: index,
    }),
  }));
}

function benchmarkResult(batchSize: number, metrics: readonly BenchmarkMetric[]): BenchmarkResult {
  return {
    case: {
      operation: "kafkaBatchIngest",
      batchSize,
    },
    metrics,
  };
}

function timeEffect(
  iterations: number,
  run: () => Effect.Effect<void, ViewServerError>,
): Effect.Effect<number, ViewServerError> {
  return Effect.gen(function* () {
    const started = performance.now();
    for (let index = 0; index < iterations; index++) {
      yield* run();
    }
    return performance.now() - started;
  });
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
