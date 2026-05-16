import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  defineConfig,
  KafkaSource,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthRow,
} from "../src/config/index.ts";
import type { GroupedQuery, RawQuery, RuntimeFilterNode } from "../src/protocol/index.ts";
import {
  loadViewServerProductionConfigFromEnv,
  makeViewServerRuntime,
} from "../src/server/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../src/snapshot/index.ts";
import { makeTopicWorkerCore } from "../src/worker/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  region: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  desk: Schema.String,
  quantity: Schema.Number,
});

type OrderRow = typeof Order.Type;
type TradeRow = typeof Trade.Type;

const productionConfigUrl = new URL("./fixtures/production-config.ts", import.meta.url).href;

const baseConfig = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const rawOrderQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly symbol: true; readonly price: true }>;

const groupedOrderQuery = {
  groupBy: ["symbol"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    totalPrice: { aggFunc: "sum", field: "price" },
  },
  orderBy: [{ field: "orders", direction: "desc" }],
  limit: 10,
} satisfies GroupedQuery<
  OrderRow,
  ["symbol"],
  {
    readonly orders: { readonly aggFunc: "count"; readonly field: "id" };
    readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
  }
>;

const healthQuery = {
  fields: {
    id: true,
    kind: true,
    topic: true,
    subscribers: true,
    queueDepth: true,
    activePlanCount: true,
    activeViewCount: true,
    activePlanBuildQueueDepth: true,
    activePlanBuildingCount: true,
    activePlanPendingCount: true,
    status: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RawQuery<
  ViewServerHealthRow,
  {
    readonly id: true;
    readonly kind: true;
    readonly topic: true;
    readonly subscribers: true;
    readonly queueDepth: true;
    readonly activePlanCount: true;
    readonly activeViewCount: true;
    readonly activePlanBuildQueueDepth: true;
    readonly activePlanBuildingCount: true;
    readonly activePlanPendingCount: true;
    readonly status: true;
  }
>;

describe("production readiness", () => {
  it.effect("loads production config from env and fails fast on missing module env", () =>
    Effect.gen(function* () {
      const missing = yield* loadViewServerProductionConfigFromEnv({
        KAFKA_BROKERS: "127.0.0.1:9092",
        VIEW_SERVER_PORT: "3100",
        VIEW_SERVER_RPC_PATH: "/rpc",
      }).pipe(Effect.flip);
      expect(missing._tag).toBe("InvalidStartupEnv");

      const loaded = yield* loadViewServerProductionConfigFromEnv({
        KAFKA_BROKERS: "127.0.0.1:9092",
        VIEW_SERVER_PORT: "3100",
        VIEW_SERVER_RPC_PATH: "/rpc",
        VIEW_SERVER_CONFIG_MODULE: productionConfigUrl,
      });
      expect(Object.keys(loaded.config.topics)).toEqual(["orders"]);
      expect(loaded.env.configModuleUrl).toBe(productionConfigUrl);
    }),
  );

  it.effect("fails startup for invalid user config before serving", () =>
    Effect.gen(function* () {
      const invalidTopic = yield* makeViewServerRuntime(
        defineConfig({
          topics: {
            __bad: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidTopic._tag).toBe("InvalidConfig");

      const invalidId = yield* makeViewServerRuntime(
        defineConfig({
          topics: {
            orders: {
              id: "missing",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidId._tag).toBe("InvalidConfig");

      const invalidWorker = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxQueueDepth: 0,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidWorker._tag).toBe("InvalidConfig");

      const invalidMutationLog = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            mutationLogSize: 0,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidMutationLog._tag).toBe("InvalidConfig");

      const invalidActivePlanLimit = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            maxActivePlans: -1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidActivePlanLimit._tag).toBe("InvalidConfig");

      const invalidDebounce = yield* makeViewServerRuntime(
        defineConfig({
          worker: {
            groupedRefreshDebounceMs: -1,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ).pipe(Effect.flip);
      expect(invalidDebounce._tag).toBe("InvalidConfig");
    }).pipe(Effect.scoped),
  );

  it.effect("verifies all configured Kafka source topics before startup completes", () =>
    Effect.gen(function* () {
      const verified: string[][] = [];
      const runtime = yield* makeViewServerRuntime(kafkaConfig(), {
        kafkaTopicVerifier: {
          verifyTopics: ({ topics }) =>
            Effect.sync(() => {
              verified.push([...topics].sort());
            }),
        },
        kafkaConsumerFactory: () => ({
          run: () => Effect.never,
        }),
      });
      expect(verified).toEqual([["orders.kafka", "trades.kafka"]]);
      yield* runtime.close;
    }).pipe(Effect.scoped),
  );

  it.effect("blocks private system topic writes and unauthorized health reads", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(baseConfig);
      const publishError = yield* runtime.publish("__private", { id: "x" }).pipe(Effect.flip);
      const deltaError = yield* runtime.deltaPublish("__private", { id: "x" }).pipe(Effect.flip);
      const deleteError = yield* runtime.deleteById("__private", "x").pipe(Effect.flip);
      const readError = yield* runtime.query("__private", rawOrderQuery).pipe(Effect.flip);
      expect(publishError._tag).toBe("InvalidPublish");
      expect(deltaError._tag).toBe("InvalidPublish");
      expect(deleteError._tag).toBe("InvalidPublish");
      expect(readError._tag).toBe("Unauthorized");

      const guardedRuntime = yield* makeViewServerRuntime(
        defineConfig({
          auth: {
            authorizeQuery: ({ topic }) => Effect.succeed(topic !== VIEW_SERVER_HEALTH_TOPIC),
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      );
      const healthError = yield* guardedRuntime
        .query(VIEW_SERVER_HEALTH_TOPIC, healthQuery)
        .pipe(Effect.flip);
      expect(healthError._tag).toBe("Unauthorized");
    }).pipe(Effect.scoped),
  );

  it.effect("enforces production query limits as typed errors", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          limits: {
            maxPageSize: 2,
            maxAggregateCount: 1,
            maxGroupByFields: 1,
            maxFilterDepth: 2,
            maxFilterConditions: 2,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      );

      const errors = yield* Effect.all([
        runtime
          .query("orders", {
            ...rawOrderQuery,
            limit: 3,
          })
          .pipe(Effect.flip),
        runtime
          .query("orders", {
            ...groupedOrderQuery,
            aggregates: {
              orders: { aggFunc: "count", field: "id" },
              regions: { aggFunc: "count_distinct", field: "region" },
            },
          })
          .pipe(Effect.flip),
        runtime
          .query("orders", {
            ...groupedOrderQuery,
            groupBy: ["symbol", "region"],
            aggregates: {
              orders: { aggFunc: "count", field: "id" },
            },
          })
          .pipe(Effect.flip),
        runtime
          .query("orders", {
            ...rawOrderQuery,
            where: nestedFilter(3),
          })
          .pipe(Effect.flip),
        runtime
          .query("orders", {
            ...rawOrderQuery,
            where: {
              op: "and",
              conditions: [
                priceFilter("greater_than", 1),
                priceFilter("less_than", 100),
                { field: "symbol", comparator: "equals", value: "AAPL" },
              ],
            },
          })
          .pipe(Effect.flip),
      ]);

      expect(errors).toHaveLength(5);
      for (const error of errors) {
        expect(error._tag).toBe("InvalidQuery");
      }
    }).pipe(Effect.scoped),
  );

  it.effect(
    "closes active subscriptions with a typed shutdown error while active plan builds",
    () =>
      Effect.gen(function* () {
        const runtime = yield* makeViewServerRuntime(baseConfig, {
          initialRows: {
            orders: Array.from({ length: 20_000 }, (_, index) => orderRow(index)),
          },
          topicWorkerFactory: (topic, topicConfig, options) =>
            makeTopicWorkerCore(topic, topicConfig, {
              ...options,
              activePlanBuildChunkSize: 1,
            }),
        });
        const events = yield* runtime
          .subscribe("shutdown-active-build", "orders", rawOrderQuery)
          .pipe(Stream.toQueue({ capacity: 16 }));
        const snapshot = yield* Queue.take(events);
        expect(snapshot.type).toBe("snapshot");

        yield* runtime.close;

        yield* expectEventuallyShutdown(Queue.take(events).pipe(Effect.exit));
        const health = yield* runtime.health;
        expect(health.ok).toBe(false);
        expect(health.topics.orders?.subscribers).toBe(0);
        expect(health.topics.orders?.activePlanCount).toBe(0);
        expect(health.topics.orders?.activePlanBuildingCount).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "closes grouped refresh streams with a typed shutdown error while refresh is in-flight",
    () =>
      Effect.gen(function* () {
        const refreshStarted = yield* Deferred.make<void>();
        const releaseRefresh = yield* Deferred.make<void>();
        const backend = blockingGroupedRefreshBackend(refreshStarted, releaseRefresh);
        const runtime = yield* makeViewServerRuntime(groupedShutdownConfig, {
          initialRows: {
            orders: Array.from({ length: 500 }, (_, index) => orderRow(index)),
          },
          __testingSnapshotBackends: {
            orders: backend,
          },
        });
        const events = yield* runtime
          .subscribe("shutdown-grouped-refresh", "orders", groupedOrderQuery)
          .pipe(Stream.toQueue({ capacity: 16 }));
        const snapshot = yield* Queue.take(events);
        expect(snapshot.type).toBe("snapshot");

        yield* runtime.publish("orders", orderRow(1_000));
        const status = yield* Queue.take(events);
        expect(status.type).toBe("status");
        yield* Deferred.await(refreshStarted).pipe(Effect.timeout("1 second"));

        const closeFiber = yield* runtime.close.pipe(Effect.forkScoped);
        yield* Deferred.succeed(releaseRefresh, undefined).pipe(Effect.ignore);

        yield* expectEventuallyShutdown(Queue.take(events).pipe(Effect.exit));
        yield* Fiber.join(closeFiber);
        const health = yield* runtime.health;
        expect(health.ok).toBe(false);
        expect(health.topics.orders?.subscribers).toBe(0);
        expect(health.topics.orders?.queueDepth).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.effect("runs a production-like smoke and drains all runtime state on shutdown", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(twoTopicConfig, {
        initialRows: {
          orders: [orderRow(1), orderRow(2), orderRow(3)],
          trades: [tradeRow(1), tradeRow(2)],
        },
      });
      const rawEvents = yield* runtime
        .subscribe("smoke-raw", "orders", rawOrderQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      const groupedEvents = yield* runtime
        .subscribe("smoke-grouped", "orders", groupedOrderQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));
      expect((yield* Queue.take(rawEvents)).type).toBe("snapshot");
      expect((yield* Queue.take(groupedEvents)).type).toBe("snapshot");

      yield* runtime.publish("orders", orderRow(10));
      yield* runtime.deltaPublish("orders", { id: "order-1", price: 999 });
      yield* runtime.deleteById("orders", "order-2");
      yield* runtime.publish("trades", tradeRow(3));

      const healthRows = yield* runtime.query(VIEW_SERVER_HEALTH_TOPIC, healthQuery);
      expect(healthRows.rows.some((row) => row.id === "server")).toBe(true);
      expect(healthRows.rows.some((row) => row.id === "topic:orders")).toBe(true);

      yield* runtime.close;
      yield* expectEventuallyShutdown(Queue.take(rawEvents).pipe(Effect.exit));
      yield* expectEventuallyShutdown(Queue.take(groupedEvents).pipe(Effect.exit));
      const health = yield* runtime.health;
      expect(health.ok).toBe(false);
      for (const topic of Object.values(health.topics)) {
        expect(topic.subscribers).toBe(0);
        expect(topic.queueDepth).toBe(0);
        expect(topic.activePlanCount).toBe(0);
        expect(topic.activeViewCount).toBe(0);
        expect(topic.activePlanBuildQueueDepth).toBe(0);
        expect(topic.activePlanBuildingCount).toBe(0);
        expect(topic.activePlanPendingCount).toBe(0);
      }
    }).pipe(Effect.scoped),
  );
});

const twoTopicConfig = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
    trades: {
      id: "id",
      schema: Trade,
    },
  },
});

const groupedShutdownConfig = defineConfig({
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

function kafkaConfig() {
  return defineConfig({
    topics: {
      orders: {
        id: "id",
        schema: Order,
        source: KafkaSource({
          brokers: ["127.0.0.1:9092"],
          topic: "orders.kafka",
          groupId: "view-server-orders",
          decode: () => Effect.succeed({ id: "order-1", symbol: "AAPL", region: "US", price: 1 }),
        }),
      },
      trades: {
        id: "id",
        schema: Trade,
        source: KafkaSource({
          brokers: ["127.0.0.1:9092"],
          topic: "trades.kafka",
          groupId: "view-server-trades",
          decode: () => Effect.succeed({ id: "trade-1", desk: "LDN", quantity: 1 }),
        }),
      },
    },
  });
}

function orderRow(index: number): OrderRow {
  return {
    id: `order-${index}`,
    symbol: index % 2 === 0 ? "AAPL" : "MSFT",
    region: index % 3 === 0 ? "US" : "EU",
    price: index,
  };
}

function tradeRow(index: number): TradeRow {
  return {
    id: `trade-${index}`,
    desk: index % 2 === 0 ? "LDN" : "NYC",
    quantity: index,
  };
}

function priceFilter(comparator: "greater_than" | "less_than", value: number): RuntimeFilterNode {
  return {
    field: "price",
    comparator,
    value,
  };
}

function nestedFilter(depth: number): RuntimeFilterNode {
  if (depth <= 1) {
    return priceFilter("greater_than", 1);
  }
  return {
    op: "and",
    conditions: [nestedFilter(depth - 1)],
  };
}

function blockingGroupedRefreshBackend(
  started: Deferred.Deferred<void>,
  release: Deferred.Deferred<void>,
): SnapshotBackend {
  const memory = createMemorySnapshotBackend();
  return {
    supportsGroupedRefreshSnapshots: true,
    init: memory.init,
    applyBatch: memory.applyBatch,
    snapshot: memory.snapshot,
    groupedRefreshSnapshot: (args) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(started, undefined).pipe(Effect.ignore);
        yield* Deferred.await(release);
        return yield* memory.snapshot(args);
      }),
    close: memory.close,
  };
}

function expectShutdownExit<TSuccess, TError>(exit: Exit.Exit<TSuccess, TError>): void {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    expect(Cause.pretty(exit.cause)).toContain("ServerShutdown");
  }
}

function expectEventuallyShutdown<TSuccess, TError>(
  takeExit: Effect.Effect<Exit.Exit<TSuccess, TError>>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    for (let index = 0; index < 32; index++) {
      const exit = yield* takeExit;
      if (Exit.isFailure(exit)) {
        expectShutdownExit(exit);
        return;
      }
    }
    throw new Error("Expected subscription stream to close with ServerShutdown");
  });
}
