import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { defineConfig } from "../src/config/index.ts";
import type { ViewServerError } from "../src/errors.ts";
import type { GroupedQuery, RawQuery } from "../src/protocol/index.ts";
import { makeTopicWorkerCore, type TopicWorkerMetrics } from "../src/worker/topic-worker-core.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

const config = defineConfig({
  worker: {
    groupedRefreshDebounceMs: 0,
  },
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const rawQuery = {
  fields: {
    id: true,
    price: true,
  },
  where: {
    field: "price",
    comparator: "greater_than_or_equal",
    value: 0,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const groupedQuery = {
  groupBy: ["symbol"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    totalPrice: { aggFunc: "sum", field: "price" },
  },
  orderBy: [{ field: "totalPrice", direction: "desc" }],
  limit: 50,
} satisfies GroupedQuery<
  OrderRow,
  ["symbol"],
  {
    readonly orders: { readonly aggFunc: "count"; readonly field: "id" };
    readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
  }
>;

describe("topic worker soak", () => {
  it.effect(
    "does not leak lag, subscribers, or active plans under mixed live load",
    () =>
      Effect.gen(function* () {
        const shape = soakShape();
        const initialRows = Array.from({ length: shape.rows }, (_, index) => orderRow(index));
        const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
          initialRows,
          groupedRefreshDebounceMs: shape.groupedRefreshDebounceMs,
        });
        const subscriptionFibers: Fiber.Fiber<void, ViewServerError>[] = [];

        for (let index = 0; index < shape.rawSubscriptions; index++) {
          const query = {
            ...rawQuery,
            offset: (index % shape.rawPageCycle) * rawQuery.limit,
          } satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;
          const fiber = yield* worker
            .subscribe(`soak-raw-${index}`, query)
            .pipe(Stream.runDrain, Effect.forkScoped);
          subscriptionFibers.push(fiber);
        }

        for (let index = 0; index < shape.groupedSubscriptions; index++) {
          const fiber = yield* worker
            .subscribe(`soak-grouped-${index}`, groupedQuery)
            .pipe(Stream.runDrain, Effect.forkScoped);
          subscriptionFibers.push(fiber);
        }

        yield* waitForMetrics(
          worker.metrics,
          (metrics) => metrics.subscribers === shape.rawSubscriptions + shape.groupedSubscriptions,
        );
        const subscribedHeap = process.memoryUsage().heapUsed;
        const liveIds = initialRows.map((row) => row.id);
        let nextId = shape.rows;
        let deleteCursor = 0;

        for (let index = 0; index < shape.mutations; index++) {
          const operation = index % 10;
          if (operation < 5) {
            const row = orderRow(nextId);
            nextId += 1;
            liveIds.push(row.id);
            yield* worker.publish(row);
          } else if (operation < 8) {
            const id = liveIds[(index * 17) % liveIds.length];
            if (id !== undefined) {
              yield* worker.deltaPublish({
                id,
                price: (index % 1_000) + 10_000,
              });
            }
          } else {
            const id = liveIds[deleteCursor % liveIds.length];
            deleteCursor += 1;
            if (id !== undefined) {
              yield* worker.deleteById(id);
              const removedIndex = liveIds.indexOf(id);
              if (removedIndex >= 0) {
                liveIds.splice(removedIndex, 1);
              }
            }
          }
          if ((index + 1) % 100 === 0) {
            yield* Effect.yieldNow;
          }
        }

        const settled = yield* waitForMetrics(
          worker.metrics,
          (metrics) =>
            metrics.maxSubscriptionLagVersions === 0 &&
            metrics.totalSubscriptionLagVersions === 0 &&
            metrics.queueDepth === 0 &&
            metrics.activePlanBuildQueueDepth === 0 &&
            metrics.activePlanBuildingCount === 0 &&
            metrics.activePlanPendingCount === 0,
        );
        expect(settled.subscribers).toBe(shape.rawSubscriptions + shape.groupedSubscriptions);
        expect(settled.queueDepth).toBe(0);
        const loadedHeap = process.memoryUsage().heapUsed;

        for (let index = 0; index < shape.rawSubscriptions; index++) {
          yield* worker.unsubscribe(`soak-raw-${index}`);
        }
        for (let index = 0; index < shape.groupedSubscriptions; index++) {
          yield* worker.unsubscribe(`soak-grouped-${index}`);
        }

        yield* Effect.forEach(subscriptionFibers, Fiber.join, { discard: true });
        collectGarbage();

        const released = yield* waitForMetrics(
          worker.metrics,
          (metrics) =>
            metrics.subscribers === 0 &&
            metrics.queueDepth === 0 &&
            metrics.maxSubscriptionLagVersions === 0 &&
            metrics.totalSubscriptionLagVersions === 0 &&
            metrics.activePlanCount === 0 &&
            metrics.activeViewCount === 0 &&
            metrics.activePlanRows === 0 &&
            metrics.activePlanIndexEstimatedBytes === 0 &&
            metrics.activePlanPendingCount === 0,
        );
        expect(released.activePlanFallbackCount).toBe(0);

        const releasedHeap = process.memoryUsage().heapUsed;
        if (globalThis.gc !== undefined) {
          expect(releasedHeap).toBeLessThanOrEqual(Math.ceil(loadedHeap * 1.1));
        }

        yield* Effect.logInfo(
          `worker soak rows=${shape.rows} raw=${shape.rawSubscriptions} grouped=${shape.groupedSubscriptions} mutations=${shape.mutations} heapSubscribed=${subscribedHeap} heapLoaded=${loadedHeap} heapReleased=${releasedHeap}`,
        );
      }).pipe(Effect.scoped),
    envNumber("VS_WORKER_SOAK_TIMEOUT_MS", 60_000),
  );
});

type SoakShape = {
  readonly rows: number;
  readonly rawSubscriptions: number;
  readonly groupedSubscriptions: number;
  readonly mutations: number;
  readonly rawPageCycle: number;
  readonly groupedRefreshDebounceMs: number;
};

function soakShape(): SoakShape {
  return {
    rows: envNumber("VS_WORKER_SOAK_ROWS", 5_000),
    rawSubscriptions: envNumber("VS_WORKER_SOAK_RAW_SUBSCRIPTIONS", 25),
    groupedSubscriptions: envNumber("VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS", 3),
    mutations: envNumber("VS_WORKER_SOAK_MUTATIONS", 500),
    rawPageCycle: envNumber("VS_WORKER_SOAK_RAW_PAGE_CYCLE", 10),
    groupedRefreshDebounceMs: envNumber("VS_WORKER_SOAK_GROUPED_DEBOUNCE_MS", 0),
  };
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : fallback;
}

function orderRow(index: number): OrderRow {
  return {
    id: `o-${index}`,
    symbol: `SYM-${index % 100}`,
    price: index % 10_000,
  };
}

function waitForMetrics<E>(
  metricsEffect: Effect.Effect<TopicWorkerMetrics, E>,
  predicate: (metrics: TopicWorkerMetrics) => boolean,
): Effect.Effect<TopicWorkerMetrics, E> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 500; attempt++) {
      const metrics = yield* metricsEffect;
      if (predicate(metrics)) {
        return metrics;
      }
      yield* yieldToHost;
    }
    const metrics = yield* metricsEffect;
    return yield* Effect.die(
      new Error(
        `Timed out waiting for soak metrics: ${JSON.stringify({
          subscribers: metrics.subscribers,
          queueDepth: metrics.queueDepth,
          maxSubscriptionLagVersions: metrics.maxSubscriptionLagVersions,
          totalSubscriptionLagVersions: metrics.totalSubscriptionLagVersions,
          activePlanCount: metrics.activePlanCount,
          activeViewCount: metrics.activeViewCount,
          activePlanBuildQueueDepth: metrics.activePlanBuildQueueDepth,
          activePlanBuildingCount: metrics.activePlanBuildingCount,
          activePlanPendingCount: metrics.activePlanPendingCount,
        })}`,
      ),
    );
  });
}

const yieldToHost = Effect.promise<void>(() => new Promise((resolve) => setTimeout(resolve, 0)));

function collectGarbage(): void {
  globalThis.gc?.();
}
