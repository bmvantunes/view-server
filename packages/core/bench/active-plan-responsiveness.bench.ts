import { Effect, Option, Queue, Schema, Stream } from "effect";
import { performance } from "node:perf_hooks";
import type { RuntimeRawQuery, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { makeTopicWorkerCore, type TopicWorkerCore } from "../src/worker/topic-worker-core.ts";

type BenchConfig = {
  readonly rows: number;
  readonly operations: number;
  readonly operation: BenchOperation;
  readonly pageSize: number;
  readonly chunkSize: number | undefined;
};

type BenchOperation = (typeof BENCH_OPERATIONS)[number];

type LatencySample = {
  readonly operationMs: number;
  readonly metricsMs: number;
  readonly activeBuildPending: boolean;
};

type EventStats = {
  staleStatusCount: number;
  snapshotCount: number;
  deltaCount: number;
  otherStatusCount: number;
};

const Row = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const BENCH_OPERATIONS = ["publish", "deltaPublish", "deleteById"] as const;

const config: BenchConfig = {
  rows: positiveInteger("VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS", 1_000_000),
  operations: positiveInteger("VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS", 50),
  operation: benchOperation("VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION", "publish"),
  pageSize: positiveInteger("VS_ACTIVE_PLAN_RESPONSIVENESS_PAGE_SIZE", 50),
  chunkSize: optionalPositiveInteger("VS_ACTIVE_PLAN_RESPONSIVENESS_CHUNK_SIZE"),
};

void Effect.runPromise(
  Effect.gen(function* () {
    const benchmarkStarted = performance.now();
    yield* Effect.logInfo(
      `active-plan responsiveness benchmark operation=${config.operation} rows=${config.rows} operations=${config.operations} pageSize=${config.pageSize} chunkSize=${config.chunkSize ?? "default"}`,
    );
    const rowGenerationStarted = performance.now();
    const rows = makeRows(config.rows);
    yield* Effect.logInfo(
      `active-plan responsiveness rows generated rows=${rows.length} durationMs=${formatMs(performance.now() - rowGenerationStarted)}`,
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
        activePlanBuildChunkSize: config.chunkSize,
      },
    );
    yield* Effect.logInfo(
      `active-plan responsiveness worker seeded durationMs=${formatMs(performance.now() - workerSeedStarted)} totalSetupMs=${formatMs(performance.now() - benchmarkStarted)}`,
    );
    const query: RuntimeRawQuery = {
      fields: {
        id: true,
        price: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: config.pageSize,
    };
    const events = yield* worker
      .subscribe("active-plan-responsiveness", query)
      .pipe(Stream.toQueue({ capacity: Math.max(16, config.operations * 2) }));
    yield* Queue.take(events);
    const building = yield* waitForActivePlanBuild(worker);
    yield* Effect.logInfo(
      `active-plan build observed queue=${building.activePlanBuildQueueDepth} building=${building.activePlanBuildingCount} pending=${building.activePlanPendingCount} elapsedMs=${formatMs(performance.now() - benchmarkStarted)}`,
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
      const activeBuildPending =
        before.activePlanBuildQueueDepth > 0 ||
        before.activePlanBuildingCount > 0 ||
        before.activePlanPendingCount > 0;
      const operationStarted = performance.now();
      yield* runOperation(worker, config.operation, config.rows, index);
      const operationMs = performance.now() - operationStarted;
      const metricsStarted = performance.now();
      yield* worker.metrics;
      const metricsMs = performance.now() - metricsStarted;
      samples.push({ operationMs, metricsMs, activeBuildPending });
      yield* Effect.yieldNow;
      yield* drainEvents(events, eventStats);
    }
    yield* waitForActivePlanIdle(worker);
    yield* drainEvents(events, eventStats);

    const finalMetrics = yield* worker.metrics;
    yield* worker.unsubscribe("active-plan-responsiveness");
    yield* Effect.logInfo(
      [
        "active-plan responsiveness result",
        `operation=${config.operation}`,
        `samples=${samples.length}`,
        `buildPendingSamples=${samples.filter((sample) => sample.activeBuildPending).length}`,
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
        `activePlanCount=${finalMetrics.activePlanCount}`,
        `activePlanBuildingCount=${finalMetrics.activePlanBuildingCount}`,
      ].join(" "),
    );
  }).pipe(Effect.scoped),
);

function makeRows(count: number): readonly RuntimeRow[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `o-${index}`,
    symbol: `SYM-${index % 1_024}`,
    price: count - index,
  }));
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
        symbol: "LIVE",
        price: rowCount + index,
      });
    case "deltaPublish":
      return worker.deltaPublish({
        id: existingRowId(rowCount, index),
        price: -index - 1,
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

function waitForActivePlanBuild(worker: TopicWorkerCore) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 1_000; attempt++) {
      const metrics = yield* worker.metrics;
      if (metrics.activePlanBuildingCount > 0 || metrics.activePlanCount > 0) {
        return metrics;
      }
      yield* Effect.yieldNow;
    }
    return yield* worker.metrics;
  });
}

function waitForActivePlanIdle(worker: TopicWorkerCore) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 10_000; attempt++) {
      const metrics = yield* worker.metrics;
      if (metrics.activePlanBuildQueueDepth === 0 && metrics.activePlanBuildingCount === 0) {
        return metrics;
      }
      yield* Effect.yieldNow;
    }
    return yield* worker.metrics;
  });
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

function optionalPositiveInteger(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : undefined;
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
