import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import { performance } from "node:perf_hooks";
import type { ViewServerError } from "../src/errors.ts";
import type { DeltaEvent, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { makeFanoutQueue } from "../src/worker/fanout-queue.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

const deltaCounts = envList("VS_FANOUT_QUEUE_DELTA_COUNTS", [1_000, 10_000]);
const opsPerDelta = envNumber("VS_FANOUT_QUEUE_OPS_PER_DELTA", 1);
const maxQueueDepth = envNumber("VS_FANOUT_QUEUE_MAX_DEPTH", 100_000);

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `fanout-queue benchmark deltaCounts=${deltaCounts.join(",")} opsPerDelta=${opsPerDelta} maxQueueDepth=${maxQueueDepth}`,
    );
    const results: BenchmarkResult[] = [];
    for (const deltaCount of deltaCounts) {
      const queue = yield* Queue.unbounded<
        SubscriptionEvent<readonly RuntimeRow[]>,
        ViewServerError | Cause.Done
      >();
      const state = { pendingLagVersions: 0n };
      const fanout = makeFanoutQueue({ maxQueueDepth, deltaCoalescing: true });
      const started = performance.now();
      for (let index = 0; index < deltaCount; index++) {
        const offered = yield* fanout.offerDelta(
          queue,
          state,
          delta(String(index), String(index + 1), index, opsPerDelta),
        );
        if (!offered) {
          return yield* Effect.die(new Error(`Fanout offer failed at delta ${index}`));
        }
      }
      const offerMs = performance.now() - started;
      const depth = yield* Queue.size(queue);
      const event = yield* Queue.take(queue);
      const coalescedOps = event.type === "delta" ? event.ops.length : 0;
      const result = benchmarkResult(deltaCount, opsPerDelta, [
        { name: "offerMs", value: offerMs, unit: "ms" },
        { name: "queueDepth", value: depth, unit: "count", lowerIsBetter: false },
        { name: "coalescedOps", value: coalescedOps, unit: "count", lowerIsBetter: false },
      ]);
      results.push(result);
      yield* Effect.logInfo(
        [
          `operation=fanoutQueueCoalesce`,
          `deltaCount=${deltaCount}`,
          `opsPerDelta=${opsPerDelta}`,
          ...result.metrics.map((metric) => `${metric.name}=${formatMetric(metric.value)}`),
        ].join(" "),
      );
    }
    const artifact = yield* writeBenchmarkArtifact(
      "fanout-queue",
      {
        deltaCounts: deltaCounts.join(","),
        opsPerDelta,
        maxQueueDepth,
      },
      results,
      {
        notes: ["Measures slow-consumer delta coalescing without queue drain/refill."],
      },
    );
    yield* Effect.logInfo(
      `fanout-queue benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function delta(
  fromVersion: string,
  toVersion: string,
  seed: number,
  opCount: number,
): DeltaEvent<readonly RuntimeRow[]> {
  return {
    type: "delta",
    requestId: "bench",
    ops: Array.from({ length: opCount }, (_, index) => ({
      type: "upsert",
      row: {
        id: `row-${seed}-${index}`,
        price: seed + index,
      },
    })),
    meta: {
      fromVersion,
      toVersion,
      totalRows: seed,
      serverTime: seed,
    },
  };
}

function benchmarkResult(
  deltaCount: number,
  opCount: number,
  metrics: readonly BenchmarkMetric[],
): BenchmarkResult {
  return {
    case: {
      operation: "fanoutQueueCoalesce",
      deltaCount,
      opsPerDelta: opCount,
    },
    metrics,
  };
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
