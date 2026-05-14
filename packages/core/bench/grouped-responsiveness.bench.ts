import { Effect, Option, Queue, Schema, Stream } from "effect";
import { performance } from "node:perf_hooks";
import type {
  RuntimeAggregateMap,
  RuntimeGroupedQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../src/protocol/index.ts";
import { makeTopicWorkerCore, type TopicWorkerCore } from "../src/worker/topic-worker-core.ts";

type BenchConfig = {
  readonly rows: number;
  readonly operations: number;
  readonly operation: BenchOperation;
  readonly aggregateCounts: readonly number[];
  readonly groupedRefreshDebounceMs: number;
  readonly operationPauseMs: number;
  readonly limit: number;
};

type BenchOperation = (typeof BENCH_OPERATIONS)[number];

type LatencySample = {
  readonly operationMs: number;
  readonly metricsMs: number;
  readonly dirtyBeforeOperation: boolean;
};

type EventStats = {
  staleStatusCount: number;
  snapshotCount: number;
  deltaCount: number;
  otherStatusCount: number;
};

const Row = Schema.Struct({
  id: Schema.String,
  bucket: Schema.String,
  value0: Schema.Number,
  value1: Schema.Number,
  value2: Schema.Number,
  value3: Schema.Number,
  value4: Schema.Number,
  value5: Schema.Number,
  value6: Schema.Number,
  value7: Schema.Number,
  value8: Schema.Number,
  value9: Schema.Number,
});

const VALUE_FIELDS = [
  "value0",
  "value1",
  "value2",
  "value3",
  "value4",
  "value5",
  "value6",
  "value7",
  "value8",
  "value9",
] as const;

const BENCH_OPERATIONS = ["publish", "deltaPublish", "deleteById"] as const;

const config: BenchConfig = {
  rows: positiveInteger("VS_GROUPED_RESPONSIVENESS_ROWS", 1_000_000),
  operations: positiveInteger("VS_GROUPED_RESPONSIVENESS_OPERATIONS", 1_000),
  operation: benchOperation("VS_GROUPED_RESPONSIVENESS_OPERATION", "deltaPublish"),
  aggregateCounts: positiveIntegerList("VS_GROUPED_RESPONSIVENESS_AGGREGATES", [10, 50, 100]),
  groupedRefreshDebounceMs: positiveInteger("VS_GROUPED_RESPONSIVENESS_DEBOUNCE_MS", 50),
  operationPauseMs: nonNegativeInteger("VS_GROUPED_RESPONSIVENESS_OPERATION_PAUSE_MS", 1),
  limit: positiveInteger("VS_GROUPED_RESPONSIVENESS_LIMIT", 50),
};

void Effect.runPromise(
  Effect.gen(function* () {
    for (const aggregateCount of config.aggregateCounts) {
      yield* runGroupedBenchmark(aggregateCount).pipe(Effect.scoped);
    }
  }),
);

function runGroupedBenchmark(aggregateCount: number) {
  return Effect.gen(function* () {
    const benchmarkStarted = performance.now();
    yield* Effect.logInfo(
      `grouped responsiveness benchmark operation=${config.operation} rows=${config.rows} operations=${config.operations} aggregates=${aggregateCount} debounceMs=${config.groupedRefreshDebounceMs} operationPauseMs=${config.operationPauseMs}`,
    );
    const rowGenerationStarted = performance.now();
    const rows = makeRows(config.rows);
    yield* Effect.logInfo(
      `grouped responsiveness rows generated rows=${rows.length} durationMs=${formatMs(performance.now() - rowGenerationStarted)}`,
    );

    const workerSeedStarted = performance.now();
    const worker = yield* makeTopicWorkerCore(
      "orders",
      {
        id: "id",
        schema: Row,
      },
      {
        initialRows: rows,
        groupedRefreshDebounceMs: config.groupedRefreshDebounceMs,
      },
    );
    yield* Effect.logInfo(
      `grouped responsiveness worker seeded durationMs=${formatMs(performance.now() - workerSeedStarted)} totalSetupMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );

    const query = makeGroupedQuery(aggregateCount);
    const events = yield* worker
      .subscribe(`grouped-responsiveness-${aggregateCount}`, query)
      .pipe(Stream.toQueue({ capacity: Math.max(16, config.operations * 2) }));
    const initialSnapshotStarted = performance.now();
    yield* Queue.take(events);
    yield* Effect.logInfo(
      `grouped responsiveness initial snapshot received durationMs=${formatMs(performance.now() - initialSnapshotStarted)} totalSetupMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );

    const samples: LatencySample[] = [];
    const eventStats: EventStats = {
      staleStatusCount: 0,
      snapshotCount: 0,
      deltaCount: 0,
      otherStatusCount: 0,
    };
    for (let index = 0; index < config.operations; index++) {
      const before = yield* worker.metrics;
      const dirtyBeforeOperation = before.maxSubscriptionLagVersions > 0;
      const operationStarted = performance.now();
      yield* runOperation(worker, config.operation, config.rows, index);
      const operationMs = performance.now() - operationStarted;
      const metricsStarted = performance.now();
      yield* worker.metrics;
      const metricsMs = performance.now() - metricsStarted;
      samples.push({ operationMs, metricsMs, dirtyBeforeOperation });
      yield* drainEvents(events, eventStats);
      if (config.operationPauseMs > 0) {
        yield* Effect.sleep(`${config.operationPauseMs} millis`);
      } else {
        yield* Effect.yieldNow;
      }
    }
    yield* Effect.sleep(`${config.groupedRefreshDebounceMs * 2} millis`);
    yield* drainEvents(events, eventStats);

    const finalMetrics = yield* worker.metrics;
    yield* worker.unsubscribe(`grouped-responsiveness-${aggregateCount}`);
    yield* Effect.logInfo(
      [
        "grouped responsiveness result",
        `operation=${config.operation}`,
        `aggregates=${aggregateCount}`,
        `samples=${samples.length}`,
        `dirtySamples=${samples.filter((sample) => sample.dirtyBeforeOperation).length}`,
        `operationP50Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.operationMs),
            0.5,
          ),
        )}`,
        `operationP95Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.operationMs),
            0.95,
          ),
        )}`,
        `operationP99Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.operationMs),
            0.99,
          ),
        )}`,
        `operationMaxMs=${formatMs(max(samples.map((sample) => sample.operationMs)))}`,
        `metricsP50Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.metricsMs),
            0.5,
          ),
        )}`,
        `metricsP95Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.metricsMs),
            0.95,
          ),
        )}`,
        `metricsP99Ms=${formatMs(
          percentile(
            samples.map((sample) => sample.metricsMs),
            0.99,
          ),
        )}`,
        `metricsMaxMs=${formatMs(max(samples.map((sample) => sample.metricsMs)))}`,
        `staleStatusCount=${eventStats.staleStatusCount}`,
        `snapshotCount=${eventStats.snapshotCount}`,
        `deltaCount=${eventStats.deltaCount}`,
        `otherStatusCount=${eventStats.otherStatusCount}`,
        `rows=${finalMetrics.rows}`,
        `version=${finalMetrics.version.toString()}`,
        `maxSubscriptionLagVersions=${finalMetrics.maxSubscriptionLagVersions}`,
      ].join(" "),
    );
  });
}

function makeRows(count: number): readonly RuntimeRow[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `o-${index}`,
    bucket: `bucket-${index % 1_024}`,
    value0: index % 101,
    value1: index % 103,
    value2: index % 107,
    value3: index % 109,
    value4: index % 113,
    value5: index % 127,
    value6: index % 131,
    value7: index % 137,
    value8: index % 139,
    value9: index % 149,
  }));
}

function makeGroupedQuery(aggregateCount: number): RuntimeGroupedQuery {
  const aggregates: RuntimeAggregateMap = {};
  for (let index = 0; index < aggregateCount; index++) {
    aggregates[`sum${index}`] = {
      aggFunc: "sum",
      field: valueField(index),
    };
  }
  return {
    groupBy: ["bucket"],
    aggregates,
    orderBy: [{ field: "sum0", direction: "desc" }],
    limit: config.limit,
  };
}

function valueField(index: number): string {
  return VALUE_FIELDS[index % VALUE_FIELDS.length] ?? "value0";
}

function runOperation(
  worker: TopicWorkerCore,
  operation: BenchOperation,
  rowCount: number,
  index: number,
) {
  switch (operation) {
    case "publish":
      return worker.publish({
        id: `live-${index}`,
        bucket: `bucket-${index % 1_024}`,
        value0: index % 101,
        value1: index % 103,
        value2: index % 107,
        value3: index % 109,
        value4: index % 113,
        value5: index % 127,
        value6: index % 131,
        value7: index % 137,
        value8: index % 139,
        value9: index % 149,
      });
    case "deltaPublish":
      return worker.deltaPublish({
        id: existingRowId(rowCount, index),
        value0: -index - 1,
      });
    case "deleteById":
      return worker.deleteById(existingRowId(rowCount, index));
  }
}

function existingRowId(rowCount: number, index: number): string {
  return `o-${rowCount - index - 1}`;
}

function drainEvents(
  queue: Queue.Dequeue<SubscriptionEvent<readonly RuntimeRow[]>, unknown>,
  stats: EventStats,
) {
  return Effect.gen(function* () {
    let draining = true;
    while (draining) {
      const next = yield* Queue.poll(queue);
      if (Option.isNone(next)) {
        draining = false;
      } else {
        recordEvent(next.value, stats);
      }
    }
  });
}

function recordEvent(event: SubscriptionEvent<readonly RuntimeRow[]>, stats: EventStats): void {
  if (event.type === "snapshot") {
    stats.snapshotCount += 1;
    return;
  }
  if (event.type === "delta") {
    stats.deltaCount += 1;
    return;
  }
  if (event.status === "stale") {
    stats.staleStatusCount += 1;
  } else {
    stats.otherStatusCount += 1;
  }
}

function percentile(values: readonly number[], percentileValue: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * percentileValue) - 1),
  );
  return sorted[index] ?? 0;
}

function max(values: readonly number[]): number {
  return values.reduce((current, value) => Math.max(current, value), 0);
}

function formatMs(value: number): string {
  return value.toFixed(2);
}

function positiveInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function nonNegativeInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : fallback;
}

function positiveIntegerList(name: string, fallback: readonly number[]): readonly number[] {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  const parsed = value
    .split(",")
    .map((entry) => Number(entry.trim()))
    .filter((entry) => Number.isFinite(entry) && entry > 0)
    .map((entry) => Math.trunc(entry));
  return parsed.length === 0 ? fallback : parsed;
}

function benchOperation(name: string, fallback: BenchOperation): BenchOperation {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  return isBenchOperation(value) ? value : fallback;
}

function isBenchOperation(value: string): value is BenchOperation {
  return BENCH_OPERATIONS.some((operation) => operation === value);
}
