import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import { performance } from "node:perf_hooks";
import type { ViewServerError } from "../src/errors.ts";
import type { DeltaEvent, RuntimeRow, SubscriptionEvent } from "../src/protocol/index.ts";
import { makeFanoutQueue, type SubscriptionEventQueue } from "../src/worker/fanout-queue.ts";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

const deltaCounts = envList("VS_FANOUT_QUEUE_DELTA_COUNTS", [1_000, 10_000]);
const opsPerDelta = envNumber("VS_FANOUT_QUEUE_OPS_PER_DELTA", 1);
const maxQueueDepth = envNumber("VS_FANOUT_QUEUE_MAX_DEPTH", 100_000);
const compareLegacy = process.env.VS_FANOUT_QUEUE_COMPARE_LEGACY !== "0";

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `fanout-queue benchmark deltaCounts=${deltaCounts.join(",")} opsPerDelta=${opsPerDelta} maxQueueDepth=${maxQueueDepth} compareLegacy=${compareLegacy ? "on" : "off"}`,
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
      const metrics: BenchmarkMetric[] = [
        { name: "offerMs", value: offerMs, unit: "ms" },
        { name: "queueDepth", value: depth, unit: "count", lowerIsBetter: false },
        { name: "coalescedOps", value: coalescedOps, unit: "count", lowerIsBetter: false },
      ];
      if (compareLegacy) {
        const legacy = yield* legacyFanout(deltaCount, opsPerDelta);
        metrics.push(
          { name: "legacyDrainRefillMs", value: legacy.offerMs, unit: "ms" },
          {
            name: "speedupRatio",
            value: offerMs === 0 ? Number.MAX_SAFE_INTEGER : legacy.offerMs / offerMs,
            unit: "ratio",
            lowerIsBetter: false,
          },
          {
            name: "legacyCoalescedOps",
            value: legacy.coalescedOps,
            unit: "count",
            lowerIsBetter: false,
          },
        );
      }
      const result = benchmarkResult(deltaCount, opsPerDelta, metrics);
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
        compareLegacy,
      },
      results,
      {
        notes: [
          "Measures slow-consumer delta coalescing without queue drain/refill.",
          "legacyDrainRefillMs simulates the previous delta coalescing shape that drained the queue and rebuilt a coalesced delta on each offer.",
        ],
      },
    );
    yield* Effect.logInfo(
      `fanout-queue benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${results.length}`,
    );
  }),
);

function legacyFanout(
  deltaCount: number,
  opCount: number,
): Effect.Effect<
  { readonly offerMs: number; readonly coalescedOps: number },
  ViewServerError | Cause.Done
> {
  return Effect.gen(function* () {
    const queue = yield* Queue.unbounded<
      SubscriptionEvent<readonly RuntimeRow[]>,
      ViewServerError | Cause.Done
    >();
    const started = performance.now();
    for (let index = 0; index < deltaCount; index++) {
      yield* legacyOfferDelta(queue, delta(String(index), String(index + 1), index, opCount));
    }
    const offerMs = performance.now() - started;
    const event = yield* Queue.take(queue);
    return {
      offerMs,
      coalescedOps: event.type === "delta" ? event.ops.length : 0,
    };
  });
}

const legacyOfferDelta = Effect.fnUntraced(function* (
  queue: SubscriptionEventQueue,
  event: DeltaEvent<readonly RuntimeRow[]>,
) {
  const queued = yield* legacyDrainQueuedEvents(queue);
  const queuedPrefix = queued.filter((queuedEvent) => queuedEvent.type !== "delta");
  const queuedDeltas = queued.filter((queuedEvent) => queuedEvent.type === "delta");
  const nextQueued = [...queuedPrefix, legacyCoalesceDeltas([...queuedDeltas, event])];
  yield* Effect.forEach(nextQueued, (queuedEvent) => Queue.offer(queue, queuedEvent), {
    discard: true,
  });
});

const legacyDrainQueuedEvents = Effect.fnUntraced(function* (queue: SubscriptionEventQueue) {
  const events: SubscriptionEvent<readonly RuntimeRow[]>[] = [];
  let polling = true;
  while (polling) {
    const next = yield* Queue.poll(queue);
    if (Option.isSome(next)) {
      events.push(next.value);
    } else {
      polling = false;
    }
  }
  return events;
});

function legacyCoalesceDeltas(
  deltas: readonly DeltaEvent<readonly RuntimeRow[]>[],
): DeltaEvent<readonly RuntimeRow[]> {
  const first = deltas[0];
  const last = deltas[deltas.length - 1];
  if (first === undefined || last === undefined) {
    throw new Error("Cannot coalesce an empty delta list");
  }
  return {
    type: "delta",
    requestId: last.requestId,
    ops: deltas.flatMap((entry) => entry.ops),
    meta: {
      fromVersion: first.meta.fromVersion,
      toVersion: last.meta.toVersion,
      totalRows: last.meta.totalRows,
      serverTime: last.meta.serverTime,
    },
  };
}

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
