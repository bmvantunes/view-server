import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { performance } from "node:perf_hooks";
import { writeBenchmarkArtifact, type BenchmarkResult } from "./benchmark-artifacts.ts";
import type {
  RuntimeAggregateMap,
  RuntimeGroupedQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../src/protocol/index.ts";
import { createChdbSnapshotBackend } from "../src/snapshot/chdb-backend.ts";
import { makeTopicWorkerCore, type TopicWorkerCore } from "../src/worker/topic-worker-core.ts";

type BenchConfig = {
  readonly rows: number;
  readonly operations: number;
  readonly backend: BenchBackend;
  readonly aggregateCount: number;
  readonly groupedRefreshDebounceMs: number;
  readonly operationPauseMs: number;
  readonly settleTimeoutMs: number;
  readonly limit: number;
};

type BenchBackend = (typeof BENCH_BACKENDS)[number];

type LatencySample = {
  readonly operationMs: number;
  readonly metricsMs: number;
  readonly startGapMs: number;
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

const BENCH_BACKENDS = ["memory", "chdb"] as const;

const config: BenchConfig = {
  rows: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_ROWS", 1_000_000),
  operations: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_OPERATIONS", 200),
  backend: benchBackend("VS_GROUPED_REFRESH_OVERLAP_BACKEND", "memory"),
  aggregateCount: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_AGGREGATES", 100),
  groupedRefreshDebounceMs: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_DEBOUNCE_MS", 1),
  operationPauseMs: nonNegativeInteger("VS_GROUPED_REFRESH_OVERLAP_OPERATION_PAUSE_MS", 1),
  settleTimeoutMs: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_SETTLE_TIMEOUT_MS", 15_000),
  limit: positiveInteger("VS_GROUPED_REFRESH_OVERLAP_LIMIT", 50),
};

void Effect.runPromise(
  Effect.gen(function* () {
    const benchmarkStarted = performance.now();
    yield* Effect.logInfo(
      `grouped refresh overlap benchmark backend=${config.backend} rows=${config.rows} operations=${config.operations} aggregates=${config.aggregateCount} debounceMs=${config.groupedRefreshDebounceMs} operationPauseMs=${config.operationPauseMs} settleTimeoutMs=${config.settleTimeoutMs}`,
    );

    const rows = makeRows(config.rows);
    yield* Effect.logInfo(
      `grouped refresh overlap rows generated rows=${rows.length} durationMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );

    const worker = yield* makeTopicWorkerCore(
      "orders",
      {
        id: "id",
        schema: Row,
      },
      {
        initialRows: rows,
        ...(config.backend === "chdb" ? { snapshotBackend: createChdbSnapshotBackend() } : {}),
        groupedRefreshDebounceMs: config.groupedRefreshDebounceMs,
      },
    );
    yield* Effect.logInfo(
      `grouped refresh overlap worker seeded totalSetupMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );

    const events = yield* worker
      .subscribe("grouped-refresh-overlap", makeGroupedQuery(config.aggregateCount))
      .pipe(Stream.toQueue({ capacity: Math.max(16, config.operations * 2) }));
    const snapshotStarted = performance.now();
    yield* Queue.take(events);
    yield* Effect.logInfo(
      `grouped refresh overlap initial snapshot durationMs=${formatMs(performance.now() - snapshotStarted)} totalSetupMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );

    const samples: LatencySample[] = [];
    const eventStats: EventStats = {
      staleStatusCount: 0,
      snapshotCount: 0,
      deltaCount: 0,
      otherStatusCount: 0,
    };
    let previousFinish = performance.now();
    for (let index = 0; index < config.operations; index++) {
      const operationStart = performance.now();
      const startGapMs = operationStart - previousFinish;
      const beforeMetricsStart = performance.now();
      const before = yield* worker.metrics;
      const beforeMetricsMs = performance.now() - beforeMetricsStart;
      const dirtyBeforeOperation = before.maxSubscriptionLagVersions > 0;
      yield* worker.publish({
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
      const operationMs = performance.now() - operationStart;
      samples.push({
        operationMs,
        metricsMs: beforeMetricsMs,
        startGapMs,
        dirtyBeforeOperation,
      });
      previousFinish = performance.now();
      yield* drainEvents(events, eventStats);
      if (config.operationPauseMs > 0) {
        yield* Effect.sleep(`${config.operationPauseMs} millis`);
      } else {
        yield* Effect.yieldNow;
      }
    }

    const settleStarted = performance.now();
    const settled = yield* waitForSettled(worker, events, eventStats);
    const settleMs = performance.now() - settleStarted;
    const finalMetrics = yield* worker.metrics;
    yield* worker.unsubscribe("grouped-refresh-overlap");
    const operationP50Ms = percentile(
      samples.map((sample) => sample.operationMs),
      0.5,
    );
    const operationP95Ms = percentile(
      samples.map((sample) => sample.operationMs),
      0.95,
    );
    const operationP99Ms = percentile(
      samples.map((sample) => sample.operationMs),
      0.99,
    );
    const operationMaxMs = max(samples.map((sample) => sample.operationMs));
    const startGapP95Ms = percentile(
      samples.map((sample) => sample.startGapMs),
      0.95,
    );
    const startGapP99Ms = percentile(
      samples.map((sample) => sample.startGapMs),
      0.99,
    );
    const startGapMaxMs = max(samples.map((sample) => sample.startGapMs));
    const metricsP99Ms = percentile(
      samples.map((sample) => sample.metricsMs),
      0.99,
    );
    const metricsMaxMs = max(samples.map((sample) => sample.metricsMs));
    const benchmarkResult: BenchmarkResult = {
      case: {
        backend: config.backend,
        rows: config.rows,
        operations: config.operations,
        aggregateCount: config.aggregateCount,
        groupedRefreshDebounceMs: config.groupedRefreshDebounceMs,
        operationPauseMs: config.operationPauseMs,
        limit: config.limit,
      },
      metrics: [
        { name: "operationP50Ms", value: operationP50Ms, unit: "ms" },
        { name: "operationP95Ms", value: operationP95Ms, unit: "ms" },
        { name: "operationP99Ms", value: operationP99Ms, unit: "ms" },
        { name: "operationMaxMs", value: operationMaxMs, unit: "ms" },
        { name: "startGapP95Ms", value: startGapP95Ms, unit: "ms" },
        { name: "startGapP99Ms", value: startGapP99Ms, unit: "ms" },
        { name: "startGapMaxMs", value: startGapMaxMs, unit: "ms" },
        { name: "metricsP99Ms", value: metricsP99Ms, unit: "ms" },
        { name: "metricsMaxMs", value: metricsMaxMs, unit: "ms" },
        { name: "settleMs", value: settleMs, unit: "ms" },
        { name: "staleStatusCount", value: eventStats.staleStatusCount, unit: "count" },
        { name: "snapshotCount", value: eventStats.snapshotCount, unit: "count" },
        { name: "deltaCount", value: eventStats.deltaCount, unit: "count" },
        {
          name: "maxSubscriptionLagVersions",
          value: finalMetrics.maxSubscriptionLagVersions,
          unit: "count",
        },
      ],
    };
    const artifact = yield* writeBenchmarkArtifact(
      "grouped-refresh-overlap",
      {
        backend: config.backend,
        rows: config.rows,
        operations: config.operations,
        aggregateCount: config.aggregateCount,
        groupedRefreshDebounceMs: config.groupedRefreshDebounceMs,
        operationPauseMs: config.operationPauseMs,
        settleTimeoutMs: config.settleTimeoutMs,
        limit: config.limit,
      },
      [benchmarkResult],
    );

    yield* Effect.logInfo(
      [
        "grouped refresh overlap result",
        `backend=${config.backend}`,
        `samples=${samples.length}`,
        `dirtySamples=${samples.filter((sample) => sample.dirtyBeforeOperation).length}`,
        `operationP50Ms=${formatMs(operationP50Ms)}`,
        `operationP95Ms=${formatMs(operationP95Ms)}`,
        `operationP99Ms=${formatMs(operationP99Ms)}`,
        `operationMaxMs=${formatMs(operationMaxMs)}`,
        `startGapP95Ms=${formatMs(startGapP95Ms)}`,
        `startGapP99Ms=${formatMs(startGapP99Ms)}`,
        `startGapMaxMs=${formatMs(startGapMaxMs)}`,
        `metricsP99Ms=${formatMs(metricsP99Ms)}`,
        `metricsMaxMs=${formatMs(metricsMaxMs)}`,
        `staleStatusCount=${eventStats.staleStatusCount}`,
        `snapshotCount=${eventStats.snapshotCount}`,
        `deltaCount=${eventStats.deltaCount}`,
        `otherStatusCount=${eventStats.otherStatusCount}`,
        `settled=${settled}`,
        `settleMs=${formatMs(settleMs)}`,
        `rows=${finalMetrics.rows}`,
        `version=${finalMetrics.version.toString()}`,
        `maxSubscriptionLagVersions=${finalMetrics.maxSubscriptionLagVersions}`,
        `artifact=${artifact.artifactPath}`,
        `baselineCompared=${artifact.compared}`,
      ].join(" "),
    );
  }).pipe(Effect.scoped),
);

function waitForSettled(
  worker: TopicWorkerCore,
  events: Queue.Dequeue<SubscriptionEvent<readonly RuntimeRow[]>, unknown>,
  stats: EventStats,
) {
  return Effect.gen(function* () {
    const started = performance.now();
    while (performance.now() - started < config.settleTimeoutMs) {
      yield* drainEvents(events, stats);
      const metrics = yield* worker.metrics;
      if (metrics.maxSubscriptionLagVersions === 0) {
        return true;
      }
      yield* Effect.sleep("10 millis");
    }
    yield* drainEvents(events, stats);
    return false;
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

function benchBackend(name: string, fallback: BenchBackend): BenchBackend {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  return isBenchBackend(value) ? value : fallback;
}

function isBenchBackend(value: string): value is BenchBackend {
  return BENCH_BACKENDS.some((backend) => backend === value);
}
