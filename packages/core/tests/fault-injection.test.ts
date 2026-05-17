import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpServer } from "effect/unstable/http";
import type { ActiveSubscription, ViewServerClient } from "../src/client/index.ts";
import {
  defineConfig,
  EffectSource,
  KafkaSource,
  type RowObject,
  type TopicConfig,
} from "../src/config/index.ts";
import {
  backpressureExceeded,
  kafkaIngestFailed,
  snapshotBackendFailed,
  type ViewServerError,
} from "../src/errors.ts";
import {
  decodeJsonRecord,
  type KafkaRecordBatch,
  type KafkaTopicConsumer,
  type KafkaTopicConsumerRunArgs,
  type KafkaTopicVerifier,
  type KafkaTopicVerificationArgs,
} from "../src/kafka/index.ts";
import type {
  GroupedQuery,
  RawQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../src/protocol/index.ts";
import { layerViewServerWebsocketServer, makeNodeWebsocketClient } from "../src/rpc/websocket.ts";
import {
  makeInternalTestingViewServerRuntime,
  layerViewServerRuntime,
  makeViewServerRuntime,
  type HealthResponse,
} from "../src/server/index.ts";
import {
  createMemorySnapshotBackend,
  type SnapshotBackend,
} from "../src/snapshot/snapshot-backend.ts";
import {
  makeTopicWorkerCore,
  type TopicWorkerCore,
  type TopicWorkerHostOptions,
} from "../src/worker/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../src/worker/mutation-log.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

type OrderRow = typeof Order.Type;

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const smallQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const groupedQuery = {
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

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

describe("fault injection", () => {
  it.effect(
    "marks readiness degraded when an Effect source fails and still shuts down stuck sources",
    () =>
      Effect.gen(function* () {
        const failSource = yield* Deferred.make<void>();
        const runtime = yield* makeViewServerRuntime(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
                source: EffectSource<RowObject, string>({
                  run: () =>
                    Deferred.await(failSource).pipe(
                      Effect.flatMap(() =>
                        Effect.fail(kafkaIngestFailed("orders", new Error("source boom"))),
                      ),
                    ),
                }),
              },
            },
          }),
        );

        expect((yield* runtime.health).ok).toBe(true);
        yield* Deferred.succeed(failSource, undefined);
        const degraded = yield* waitForRuntimeHealth(
          runtime.health,
          (health) => health.topics.orders?.status === "degraded",
        );
        expect(degraded.ok).toBe(false);
        yield* runtime.close.pipe(Effect.timeout("1 second"));

        const stuckRuntime = yield* makeViewServerRuntime(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
                source: EffectSource<RowObject, string>({
                  run: () => Effect.never,
                }),
              },
            },
          }),
        );
        yield* stuckRuntime.close.pipe(Effect.timeout("1 second"));
        const stopped = yield* stuckRuntime.health;
        expect(stopped.ok).toBe(false);
        expect(stopped.topics.orders?.status).toBe("stopping");

        const resumeSource = yield* Deferred.make<void>();
        const resumedRuntime = yield* makeViewServerRuntime(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
                source: EffectSource<RowObject, string>({
                  run: (context) =>
                    Deferred.await(resumeSource).pipe(
                      Effect.flatMap(() => context.publish(orderRow(1, 100))),
                      Effect.flatMap(() => Effect.never),
                    ),
                }),
              },
            },
          }),
        );
        expect((yield* resumedRuntime.query("orders", query)).totalRows).toBe(0);
        yield* Deferred.succeed(resumeSource, undefined);
        const resumed = yield* waitForRuntimeHealth(
          resumedRuntime.health,
          (health) => health.topics.orders?.rows === 1,
        );
        expect(resumed.ok).toBe(true);
        expect((yield* resumedRuntime.query("orders", query)).rows).toEqual([
          { id: "o-1", price: 100 },
        ]);
        yield* resumedRuntime.close.pipe(Effect.timeout("1 second"));
      }).pipe(Effect.scoped),
  );

  it.effect(
    "does not commit Kafka batches until ingest succeeds and tolerates duplicate/out-of-order batches",
    () =>
      Effect.gen(function* () {
        const consumer = new FaultyKafkaTopicConsumer();
        const runtime = yield* makeViewServerRuntime(kafkaConfig(), {
          kafkaConsumerFactory: () => consumer,
          kafkaTopicVerifier: new StaticKafkaTopicVerifier(["orders-events"]),
        });
        yield* consumer.awaitTopic("orders-events");

        const failed = yield* consumer
          .offer("orders-events", {
            records: [
              kafkaRecord("o-1", "1", { id: "o-1", symbol: "AAPL", price: 100 }),
              kafkaRecord("bad", "2", { id: "bad", price: 200 }),
            ],
            commit: Effect.sync(() => {
              consumer.commits += 1;
            }),
          })
          .pipe(Effect.exit);
        expect(Exit.isFailure(failed)).toBe(true);
        expect(consumer.commits).toBe(0);
        expect((yield* runtime.query("orders", query)).rows).toEqual([{ id: "o-1", price: 100 }]);

        yield* consumer.offer("orders-events", {
          records: [
            kafkaRecord("o-2", "4", { id: "o-2", symbol: "MSFT", price: 400 }),
            kafkaRecord("o-1", "3", { id: "o-1", symbol: "AAPL", price: 150 }),
            kafkaRecord("o-1", "3", { id: "o-1", symbol: "AAPL", price: 150 }),
          ],
          commit: Effect.sync(() => {
            consumer.commits += 1;
          }),
        });

        const result = yield* runtime.query("orders", query);
        expect(result.totalRows).toBe(2);
        expect(result.rows).toEqual([
          { id: "o-2", price: 400 },
          { id: "o-1", price: 150 },
        ]);
        expect(consumer.commits).toBe(1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps memory authoritative when snapshots fail, applyBatch fails, and grouped refresh fails",
    () =>
      Effect.gen(function* () {
        const backend = new FaultySnapshotBackend();
        backend.failSnapshots = 1;
        backend.failApplyBatches = 1;
        backend.failGroupedRefreshes = 1;
        const runtime = yield* makeInternalTestingViewServerRuntime(
          defineConfig({
            worker: {
              groupedRefreshDebounceMs: 0,
            },
            topics: {
              orders: {
                id: "id",
                schema: Order,
              },
            },
          }),
          {
            initialRows: {
              orders: [orderRow(1, 100), orderRow(2, 200)],
            },
            __testingSnapshotBackends: {
              orders: backend,
            },
          },
        );
        const snapshotFallback = yield* runtime.query("orders", query);
        expect(snapshotFallback.rows).toEqual([
          { id: "o-2", price: 200 },
          { id: "o-1", price: 100 },
        ]);
        expect((yield* runtime.health).topics.orders?.status).toBe("degraded");

        yield* runtime.query("orders", query);
        expect((yield* runtime.health).topics.orders?.status).toBe("ready");

        yield* runtime.publish("orders", orderRow(3, 300));
        yield* sleepHost(10);
        const degraded = yield* waitForRuntimeHealth(
          runtime.health,
          (health) => health.topics.orders?.status === "degraded",
        );
        expect(degraded.ok).toBe(false);
        expect((yield* runtime.query("orders", query)).rows[0]).toEqual({ id: "o-3", price: 300 });

        yield* runtime.publish("orders", orderRow(4, 400));
        const recovered = yield* waitForRuntimeHealth(
          runtime.health,
          (health) => health.topics.orders?.status === "ready",
        );
        expect(recovered.ok).toBe(true);

        const initialGroupedSnapshot =
          yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
        const refreshedGroupedSnapshot =
          yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
        let groupedSnapshots = 0;
        const groupedFiber = yield* runtime.subscribe("grouped-fault", "orders", groupedQuery).pipe(
          Stream.tap((event) => {
            if (event.type !== "snapshot") {
              return Effect.void;
            }
            groupedSnapshots += 1;
            return groupedSnapshots === 1
              ? Deferred.succeed(initialGroupedSnapshot, event).pipe(Effect.asVoid)
              : Deferred.succeed(refreshedGroupedSnapshot, event).pipe(Effect.asVoid);
          }),
          Stream.runDrain,
          Effect.forkScoped,
        );
        expect(
          (yield* Deferred.await(initialGroupedSnapshot).pipe(Effect.timeout("1 second"))).type,
        ).toBe("snapshot");
        yield* runtime.publish("orders", orderRow(5, 500));
        const refresh = yield* Deferred.await(refreshedGroupedSnapshot).pipe(
          Effect.timeout("1 second"),
        );
        expect(refresh.meta.totalRows).toBe(2);
        expect((yield* runtime.health).topics.orders?.status).toBe("degraded");
        yield* runtime.unsubscribe("grouped-fault");
        yield* Fiber.interrupt(groupedFiber);
        yield* runtime.close.pipe(Effect.timeout("1 second"));
      }).pipe(Effect.scoped),
  );

  it.effect("interrupts a hung snapshot and keeps the worker usable", () =>
    Effect.gen(function* () {
      const snapshotInterrupted = yield* Deferred.make<void>();
      const backend = new FaultySnapshotBackend(snapshotInterrupted);
      backend.hangSnapshots = true;
      const worker = yield* makeTopicWorkerCore("orders", config.topics.orders, {
        initialRows: [orderRow(1, 100)],
        snapshotBackend: backend,
      });
      const fiber = yield* worker.query(query).pipe(Effect.forkScoped);
      yield* sleepHost(20);
      yield* Fiber.interrupt(fiber);
      yield* Deferred.await(snapshotInterrupted).pipe(Effect.timeout("1 second"));
      yield* worker.publish(orderRow(2, 200)).pipe(Effect.timeout("1 second"));
      const rows = yield* worker.getRowsForTest;
      expect(
        rows.map((row) => String(row.id)).toSorted((left, right) => left.localeCompare(right)),
      ).toEqual(["o-1", "o-2"]);
    }).pipe(Effect.scoped),
  );

  it.effect("survives websocket reconnect storms without subscriber or active plan leaks", () =>
    Effect.gen(function* () {
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(config, {
            initialRows: {
              orders: Array.from({ length: 250 }, (_, index) => orderRow(index, index)),
            },
          }),
        ),
      );
      const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

      yield* Effect.gen(function* () {
        const url = yield* websocketUrl();
        const firstWave = yield* Effect.forEach(
          Array.from({ length: 50 }, (_, index) => index),
          (index) => connectStormClient(url, `storm-${index}`, undefined),
          { concurrency: 10 },
        );
        yield* Effect.forEach(firstWave, (client) => client.subscription.close, {
          discard: true,
          concurrency: 10,
        });

        const probe = yield* makeNodeWebsocketClient(url, config);
        yield* waitForClientHealth(
          probe,
          (health) =>
            health.topics.orders?.subscribers === 0 &&
            health.topics.orders.activePlanCount === 0 &&
            health.topics.orders.activePlanBuildingCount === 0 &&
            health.topics.orders.activePlanPendingCount === 0,
        );

        const secondWave = yield* Effect.forEach(
          firstWave,
          (previous, index) =>
            connectStormClient(url, `storm-reconnect-${index}`, previous.requestId),
          { concurrency: 10 },
        );
        yield* probe.publish("orders", orderRow(999, 999)).pipe(Effect.timeout("1 second"));
        yield* Effect.forEach(
          secondWave.slice(0, 10),
          (client) => Deferred.await(client.firstDelta).pipe(Effect.timeout("2 seconds")),
          { discard: true, concurrency: 10 },
        );
        for (const client of secondWave) {
          expect(client.deliveredRequestIds.every((id) => id === client.requestId)).toBe(true);
        }
        yield* Effect.forEach(secondWave, (client) => client.subscription.close, {
          discard: true,
          concurrency: 10,
        });
        const finalHealth = yield* waitForClientHealth(
          probe,
          (health) =>
            health.topics.orders?.subscribers === 0 &&
            health.topics.orders.queueDepth === 0 &&
            health.topics.orders.activePlanCount === 0 &&
            health.topics.orders.activePlanBuildingCount === 0 &&
            health.topics.orders.activePlanPendingCount === 0,
        );
        expect(finalHealth.ok).toBe(true);
      }).pipe(Effect.provide(testServerLayer));
    }).pipe(Effect.scoped),
  );

  it("propagates typed backpressure over websocket and generated client retries cleanly", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
          Layer.provide(
            layerViewServerRuntime(config, {
              initialRows: {
                orders: [orderRow(1, 100)],
              },
              topicWorkerFactory: (topic, topicConfig, options) =>
                topic === "orders"
                  ? makeBackpressureOnceWorker(topic, topicConfig, options)
                  : makeTopicWorkerCore(topic, topicConfig, options),
            }),
          ),
        );
        const testServerLayer = serverLayer.pipe(Layer.provideMerge(NodeHttpServer.layerTest));

        return yield* Effect.gen(function* () {
          const url = yield* websocketUrl();
          const client = yield* makeNodeWebsocketClient(url, config);
          const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const freshSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const postRetryDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const retries: ViewServerError[] = [];
          let snapshots = 0;

          const subscription = yield* client.subscribe(
            "orders",
            smallQuery,
            (event) => {
              if (event.type === "snapshot") {
                snapshots += 1;
                return snapshots === 1
                  ? Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid)
                  : Deferred.succeed(freshSnapshot, event).pipe(Effect.asVoid);
              }
              return snapshots >= 2
                ? Deferred.succeed(postRetryDelta, event).pipe(Effect.asVoid)
                : Effect.void;
            },
            (event) =>
              Effect.sync(() => {
                if (event.type === "retry") {
                  retries.push(event.error);
                }
              }),
          );

          const initial = yield* Deferred.await(firstSnapshot).pipe(Effect.timeout("1 second"));
          expect(initial.type).toBe("snapshot");
          const failedRequestId = initial.requestId;
          expect(failedRequestId).toBe(subscription.requestId);

          const backpressure = yield* waitForCondition(() =>
            retries.find((error) => error._tag === "BackpressureExceeded"),
          );
          expect(backpressure._tag).toBe("BackpressureExceeded");
          if (backpressure._tag !== "BackpressureExceeded") {
            return yield* Effect.die(new Error("Expected BackpressureExceeded"));
          }
          expect(backpressure.requestId).toBe(failedRequestId);

          const snapshot = yield* Deferred.await(freshSnapshot).pipe(Effect.timeout("3 seconds"));
          expect(snapshot.type).toBe("snapshot");
          expect(snapshot.requestId).toBe(subscription.requestId);
          expect(snapshot.requestId).not.toBe(failedRequestId);
          expect(snapshot.meta.totalRows).toBe(1);

          yield* client.publish("orders", orderRow(2, 200)).pipe(Effect.timeout("1 second"));
          const delta = yield* Deferred.await(postRetryDelta).pipe(Effect.timeout("1 second"));
          expect(delta.type).toBe("delta");
          expect(delta.requestId).toBe(subscription.requestId);

          const health = yield* waitForClientHealth(
            client,
            (current) =>
              current.topics.orders?.subscribers === 1 && current.topics.orders.queueDepth === 0,
          );
          expect(health.ok).toBe(true);
          yield* subscription.close;
          const closed = yield* waitForClientHealth(
            client,
            (current) => current.topics.orders?.subscribers === 0,
          );
          expect(closed.topics.orders?.queueDepth).toBe(0);
        }).pipe(Effect.provide(testServerLayer));
      }).pipe(Effect.scoped),
    );
  });
});

type StormClient = {
  readonly subscription: ActiveSubscription;
  readonly requestId: string;
  readonly firstDelta: Deferred.Deferred<SubscriptionEvent<readonly RuntimeRow[]>, never>;
  readonly deliveredRequestIds: readonly string[];
};

function connectStormClient(
  url: string,
  label: string,
  previousRequestId: string | undefined,
): Effect.Effect<StormClient, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.test.storm.connect")(function* () {
    const client = yield* makeNodeWebsocketClient(url, config);
    const firstSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
    const firstDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
    const deliveredRequestIds: string[] = [];
    const subscription = yield* client.subscribe("orders", smallQuery, (event) =>
      Effect.sync(() => {
        deliveredRequestIds.push(event.requestId);
      }).pipe(Effect.flatMap(() => completeStormEvent(event, firstSnapshot, firstDelta))),
    );
    const snapshot = yield* Deferred.await(firstSnapshot).pipe(
      Effect.timeout("2 seconds"),
      Effect.orDie,
    );
    expect(snapshot.type).toBe("snapshot");
    expect(snapshot.requestId).toBe(subscription.requestId);
    if (previousRequestId !== undefined) {
      expect(snapshot.requestId).not.toBe(previousRequestId);
    }
    yield* Effect.annotateCurrentSpan({
      "view_server.request_id": subscription.requestId,
      "view_server.subscription_id": label,
    });
    const stormClient: StormClient = {
      subscription,
      requestId: subscription.requestId,
      firstDelta,
      deliveredRequestIds,
    };
    return stormClient;
  })();
}

function completeStormEvent(
  event: SubscriptionEvent<readonly RuntimeRow[]>,
  firstSnapshot: Deferred.Deferred<SubscriptionEvent<readonly RuntimeRow[]>, never>,
  firstDelta: Deferred.Deferred<SubscriptionEvent<readonly RuntimeRow[]>, never>,
): Effect.Effect<void> {
  if (event.type === "snapshot") {
    return Deferred.succeed(firstSnapshot, event).pipe(Effect.asVoid);
  }
  if (event.type === "delta") {
    return Deferred.succeed(firstDelta, event).pipe(Effect.asVoid);
  }
  return Effect.void;
}

function kafkaConfig() {
  return defineConfig({
    topics: {
      orders: {
        id: "id",
        schema: Order,
        source: KafkaSource<OrderRow, "id">({
          brokers: ["127.0.0.1:9092"],
          topic: "orders-events",
          groupId: "view-server-orders",
          decode: decodeJsonRecord<OrderRow, "id">({ topic: "orders", schema: Order }),
        }),
      },
    },
  });
}

function makeBackpressureOnceWorker(
  topic: string,
  topicConfig: TopicConfig,
  options: TopicWorkerHostOptions,
): Effect.Effect<TopicWorkerCore, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.test.worker.backpressure_once")(function* () {
    const worker = yield* makeTopicWorkerCore(topic, topicConfig, options);
    let failed = false;
    const subscribe: TopicWorkerCore["subscribe"] = (requestId, query) => {
      const stream = worker.subscribe(requestId, query);
      if (failed) {
        return stream;
      }
      failed = true;
      return Stream.concat(
        stream.pipe(Stream.take(1)),
        Stream.fail(backpressureExceeded(requestId, "fault-injected backpressure")),
      );
    };
    return {
      ...worker,
      subscribe,
    };
  })();
}

class FaultyKafkaTopicConsumer implements KafkaTopicConsumer {
  commits = 0;
  runs = 0;
  readonly handlers = new Map<string, KafkaTopicConsumerRunArgs["onBatch"]>();
  readonly readyWaiters = new Map<string, readonly (() => void)[]>();

  run(args: KafkaTopicConsumerRunArgs): Effect.Effect<void> {
    this.runs += 1;
    return Effect.callback<void>((_resume) => {
      this.handlers.set(args.topic, args.onBatch);
      this.notifyReady(args.topic);
      return Effect.sync(() => {
        this.handlers.delete(args.topic);
      });
    });
  }

  offer(topic: string, batch: KafkaRecordBatch): Effect.Effect<void, ViewServerError> {
    const handler = this.handlers.get(topic);
    if (handler === undefined) {
      return Effect.die(new Error(`No Kafka handler registered for ${topic}`));
    }
    return handler(batch);
  }

  awaitTopic(topic: string): Effect.Effect<void> {
    if (this.handlers.has(topic)) {
      return Effect.void;
    }
    return Effect.callback<void>((resume) => {
      const ready = () => resume(Effect.void);
      this.readyWaiters.set(topic, [...(this.readyWaiters.get(topic) ?? []), ready]);
      return Effect.sync(() => {
        this.readyWaiters.set(
          topic,
          (this.readyWaiters.get(topic) ?? []).filter((waiter) => waiter !== ready),
        );
      });
    });
  }

  private notifyReady(topic: string): void {
    const waiters = this.readyWaiters.get(topic) ?? [];
    this.readyWaiters.delete(topic);
    for (const waiter of waiters) {
      waiter();
    }
  }
}

class StaticKafkaTopicVerifier implements KafkaTopicVerifier {
  readonly existingTopics: ReadonlySet<string>;

  constructor(existingTopics: readonly string[]) {
    this.existingTopics = new Set(existingTopics);
  }

  verifyTopics(args: KafkaTopicVerificationArgs): Effect.Effect<void, ViewServerError> {
    const missing = args.topics.filter((topic) => !this.existingTopics.has(topic));
    return missing.length === 0
      ? Effect.void
      : Effect.fail(kafkaIngestFailed(args.topics[0] ?? "__kafka", new Error("missing topic")));
  }
}

class FaultySnapshotBackend implements SnapshotBackend {
  readonly memory = createMemorySnapshotBackend();
  readonly pendingFailedBatches: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }[] = [];
  failSnapshots = 0;
  failGroupedRefreshes = 0;
  failApplyBatches = 0;
  hangSnapshots = false;

  constructor(readonly snapshotInterrupted?: Deferred.Deferred<void, never>) {}

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return this.memory.init(args);
  }

  applyBatch(args: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }): Effect.Effect<void, ViewServerError> {
    if (this.failApplyBatches > 0) {
      this.failApplyBatches -= 1;
      this.pendingFailedBatches.push(args);
      return Effect.fail(snapshotBackendFailed("orders", new Error("applyBatch fault")));
    }
    const batches = [...this.pendingFailedBatches, args];
    this.pendingFailedBatches.length = 0;
    return Effect.forEach(batches, (batch) => this.memory.applyBatch(batch), {
      discard: true,
    });
  }

  snapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): ReturnType<SnapshotBackend["snapshot"]> {
    if (this.hangSnapshots) {
      return Effect.never.pipe(
        Effect.ensuring(
          this.snapshotInterrupted === undefined
            ? Effect.void
            : Deferred.succeed(this.snapshotInterrupted, undefined).pipe(Effect.ignore),
        ),
      );
    }
    if (this.failSnapshots > 0) {
      this.failSnapshots -= 1;
      return Effect.fail(snapshotBackendFailed("orders", new Error("snapshot fault")));
    }
    return this.memory.snapshot(args);
  }

  readonly supportsGroupedRefreshSnapshots = true;

  groupedRefreshSnapshot(
    args: Parameters<NonNullable<SnapshotBackend["groupedRefreshSnapshot"]>>[0],
  ): ReturnType<NonNullable<SnapshotBackend["groupedRefreshSnapshot"]>> {
    if (this.failGroupedRefreshes > 0) {
      this.failGroupedRefreshes -= 1;
      return Effect.fail(snapshotBackendFailed("orders", new Error("grouped refresh fault")));
    }
    return this.memory.snapshot(args);
  }

  close(): Effect.Effect<void> {
    return this.memory.close();
  }
}

function kafkaRecord(id: string, offset: string, value: unknown) {
  return {
    topic: "orders-events",
    key: id,
    value: JSON.stringify(value),
    partition: 0,
    offset,
  };
}

function orderRow(index: number, price: number): OrderRow {
  return {
    id: `o-${index}`,
    symbol: index % 2 === 0 ? "AAPL" : "MSFT",
    price,
  };
}

function websocketUrl() {
  return Effect.fn("view-server.test.websocket.url")(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address;
    if (address._tag !== "TcpAddress") {
      return yield* Effect.die(new Error("Expected test server to listen on TCP"));
    }
    return `ws://127.0.0.1:${address.port}/rpc`;
  })();
}

function waitForRuntimeHealth(
  healthEffect: Effect.Effect<HealthResponse, ViewServerError>,
  predicate: (health: HealthResponse) => boolean,
) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt++) {
      const health = yield* healthEffect;
      if (predicate(health)) {
        return health;
      }
      yield* sleepHost(10);
    }
    return yield* Effect.die(new Error("Timed out waiting for runtime health"));
  });
}

function waitForClientHealth(
  client: Pick<ViewServerClient<typeof config>, "health">,
  predicate: (health: HealthResponse) => boolean,
) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt++) {
      const health = yield* client.health();
      if (predicate(health)) {
        return health;
      }
      yield* sleepHost(10);
    }
    return yield* Effect.die(new Error("Timed out waiting for client health"));
  });
}

function waitForCondition<T>(read: () => T | undefined): Effect.Effect<T> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt++) {
      const value = read();
      if (value !== undefined) {
        return value;
      }
      yield* sleepHost(10);
    }
    return yield* Effect.die(new Error("Timed out waiting for condition"));
  });
}

function sleepHost(milliseconds: number): Effect.Effect<void> {
  return Effect.promise(() => new Promise((resolve) => setTimeout(resolve, milliseconds)));
}
