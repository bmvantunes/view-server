import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import { RuntimeHealthSyncScheduler } from "../src/server/runtime-health-sync-scheduler.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

const mutationCounts = envList("VS_HEALTH_SYNC_MUTATIONS", [10_000]);
const topicCount = envNumber("VS_HEALTH_SYNC_TOPICS", 25);

void Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.logInfo(
        `health-sync benchmark mutationCounts=${mutationCounts.join(",")} topics=${topicCount}`,
      );
      const results: BenchmarkResult[] = [];
      for (const mutationCount of mutationCounts) {
        let legacySyncCalls = 0;
        let scheduledSyncCalls = 0;
        const legacySync = Effect.sync(() => {
          legacySyncCalls += 1;
          simulateHealthProjection(topicCount);
        });
        const scheduledSync = Effect.sync(() => {
          scheduledSyncCalls += 1;
          simulateHealthProjection(topicCount);
        });
        const scope = yield* Effect.scope;
        const scheduler = new RuntimeHealthSyncScheduler({
          scope,
          delayMs: 60_000,
          syncNow: scheduledSync,
        });

        const legacyMs = yield* timeEffect(() =>
          Effect.forEach(
            Array.from({ length: mutationCount }, (_, index) => index),
            () => legacySync,
            { discard: true },
          ),
        );

        const scheduledRequestMs = yield* timeEffect(() =>
          Effect.forEach(
            Array.from({ length: mutationCount }, (_, index) => index),
            () => scheduler.request,
            { discard: true },
          ),
        );
        const scheduledFlushMs = yield* timeEffect(() => scheduler.flush);
        yield* scheduler.close;

        const scheduledTotalMs = scheduledRequestMs + scheduledFlushMs;
        const result = benchmarkResult(mutationCount, [
          { name: "legacyMs", value: legacyMs, unit: "ms" },
          { name: "scheduledRequestMs", value: scheduledRequestMs, unit: "ms" },
          { name: "scheduledFlushMs", value: scheduledFlushMs, unit: "ms" },
          { name: "scheduledTotalMs", value: scheduledTotalMs, unit: "ms" },
          { name: "legacySyncCalls", value: legacySyncCalls, unit: "count" },
          { name: "scheduledSyncCalls", value: scheduledSyncCalls, unit: "count" },
          {
            name: "speedupRatio",
            value: scheduledTotalMs === 0 ? Number.MAX_SAFE_INTEGER : legacyMs / scheduledTotalMs,
            unit: "ratio",
            lowerIsBetter: false,
          },
        ]);
        results.push(result);
        yield* Effect.logInfo(
          [
            `operation=healthSync`,
            `mutationCount=${mutationCount}`,
            ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
          ].join(" "),
        );
      }

      const artifact = yield* writeBenchmarkArtifact(
        "health-sync",
        {
          mutationCounts: mutationCounts.join(","),
          topicCount,
        },
        results,
        {
          notes: [
            "Compares legacy per-mutation health topic sync with scheduled health sync request plus one flush.",
            "The simulated projection loops topicCount health rows to model runtime health row construction cost.",
          ],
        },
      );
      yield* Effect.logInfo(
        `health-sync benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
      );
    }),
  ),
);

function simulateHealthProjection(topics: number): number {
  let checksum = 0;
  for (let topic = 0; topic < topics; topic++) {
    checksum = Math.imul(checksum ^ topic, 16_777_619) >>> 0;
    checksum = Math.imul(checksum ^ (topic * 17), 16_777_619) >>> 0;
  }
  return checksum;
}

function benchmarkResult(
  mutationCount: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "healthSync",
      mutationCount,
      topicCount,
    },
    metrics,
  };
}

function timeEffect<E, R>(run: () => Effect.Effect<void, E, R>): Effect.Effect<number, E, R> {
  return Effect.gen(function* () {
    const started = performance.now();
    yield* run();
    return performance.now() - started;
  });
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
