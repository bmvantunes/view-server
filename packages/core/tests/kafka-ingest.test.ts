import { NodeHttpServer, NodeSocket } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpServer } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import {
  defineConfig,
  KafkaSource,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthRow,
} from "../src/config/index.ts";
import {
  decodeJsonRecord,
  ingestKafkaBatch,
  kafkaRecordLag,
  protobufDecimalToBigDecimal,
  unscaledDecimalToBigDecimal,
} from "../src/kafka/index.ts";
import type {
  KafkaRecordBatch,
  KafkaTopicConsumer,
  KafkaTopicConsumerRunArgs,
  KafkaTopicVerificationArgs,
  KafkaTopicVerifier,
} from "../src/kafka/index.ts";
import { kafkaIngestFailed, type ViewServerError } from "../src/errors.ts";
import type { RawQuery } from "../src/protocol/index.ts";
import { ViewServerRpcs } from "../src/rpc/index.ts";
import { layerViewServerWebsocketServer } from "../src/rpc/websocket.ts";
import { layerViewServerRuntime, makeViewServerRuntime } from "../src/server/index.ts";
import { platformaticKafkaTopicConsumerOptions } from "../src/kafka/platformatic-consumer.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

const healthQuery = {
  fields: {
    id: true,
    kind: true,
    topic: true,
    kafkaLagTotal: true,
    kafkaLagMax: true,
    kafkaPartitions: true,
    lastKafkaOffset: true,
    lastKafkaEndOffset: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RawQuery<
  ViewServerHealthRow,
  {
    readonly id: true;
    readonly kind: true;
    readonly topic: true;
    readonly kafkaLagTotal: true;
    readonly kafkaLagMax: true;
    readonly kafkaPartitions: true;
    readonly lastKafkaOffset: true;
    readonly lastKafkaEndOffset: true;
  }
>;

describe("Kafka ingestion", () => {
  it.effect("decodes Kafka records and applies them to the topic worker", () =>
    Effect.gen(function* () {
      const consumer = new FakeKafkaTopicConsumer();
      const startupSteps: string[] = [];
      const verifier = new FakeKafkaTopicVerifier(["orders-events"], () => {
        startupSteps.push("verify");
      });
      const config = defineConfig({
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
      const runtime = yield* makeViewServerRuntime(config, {
        kafkaConsumerFactory: () => {
          startupSteps.push("consumer-factory");
          return consumer;
        },
        kafkaTopicVerifier: verifier,
      });

      expect(startupSteps).toEqual(["verify", "consumer-factory"]);
      expect(verifier.verified).toEqual([
        {
          brokers: ["127.0.0.1:9092"],
          topics: ["orders-events"],
        },
      ]);
      yield* consumer.offer("orders-events", {
        records: [
          {
            topic: "orders-events",
            key: "o-1",
            value: JSON.stringify({ id: "o-1", symbol: "AAPL", price: 100 }),
            partition: 0,
            offset: "1",
          },
          {
            topic: "orders-events",
            key: "o-2",
            value: JSON.stringify({
              type: "delta-publish",
              patch: { id: "o-1", price: 125 },
            }),
            partition: 0,
            offset: "2",
          },
        ],
        commit: Effect.sync(() => {
          consumer.commits += 1;
        }),
      });

      const result = yield* runtime.query("orders", query);
      expect(result.totalRows).toBe(1);
      expect(result.rows[0]?.id).toBe("o-1");
      expect(result.rows[0]?.price).toBe(125);
      expect(consumer.commits).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect("updates health topic Kafka lag metrics after Kafka batches", () =>
    Effect.gen(function* () {
      const consumer = new FakeKafkaTopicConsumer();
      const runtime = yield* makeViewServerRuntime(kafkaConfig(), {
        kafkaConsumerFactory: () => consumer,
        kafkaTopicVerifier: new FakeKafkaTopicVerifier(["orders-events"]),
      });

      yield* consumer.offer("orders-events", {
        records: [
          {
            topic: "orders-events",
            key: "o-1",
            value: JSON.stringify({ id: "o-1", symbol: "AAPL", price: 100 }),
            partition: 0,
            offset: "7",
            highWatermark: "10",
          },
        ],
        metrics: {
          lagTotal: 5,
          lagMax: 3,
          partitions: 2,
          offset: 7,
          endOffset: 10,
        },
        commit: Effect.sync(() => {
          consumer.commits += 1;
        }),
      });

      const health = yield* runtime.query(VIEW_SERVER_HEALTH_TOPIC, healthQuery);
      expect(rowById(health.rows, "server")).toMatchObject({
        kafkaLagTotal: 5,
        kafkaLagMax: 3,
        kafkaPartitions: 2,
        lastKafkaOffset: 7,
        lastKafkaEndOffset: 10,
      });
      expect(rowById(health.rows, "topic:orders")).toMatchObject({
        kind: "topic",
        topic: "orders",
        kafkaLagTotal: 5,
        kafkaLagMax: 3,
        kafkaPartitions: 2,
        lastKafkaOffset: 7,
        lastKafkaEndOffset: 10,
      });
      expect(consumer.commits).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect("decodes a Kafka batch into one runtime mutation batch", () =>
    Effect.gen(function* () {
      let singleCalls = 0;
      const seenBatches: unknown[][] = [];

      yield* ingestKafkaBatch({
        viewTopic: "orders",
        idField: "id",
        source: KafkaSource<OrderRow, "id">({
          brokers: ["127.0.0.1:9092"],
          topic: "orders-events",
          groupId: "view-server-orders",
          decode: decodeJsonRecord<OrderRow, "id">({ topic: "orders", schema: Order }),
        }),
        runtime: {
          publish: () =>
            Effect.sync(() => {
              singleCalls += 1;
            }),
          deltaPublish: () =>
            Effect.sync(() => {
              singleCalls += 1;
            }),
          deleteById: () =>
            Effect.sync(() => {
              singleCalls += 1;
            }),
          mutateBatch: (mutations) =>
            Effect.sync(() => {
              seenBatches.push([...mutations]);
            }),
        },
        batch: {
          records: [
            {
              topic: "orders-events",
              key: "o-1",
              offset: "1",
              value: JSON.stringify({ id: "o-1", symbol: "AAPL", price: 100 }),
            },
            {
              topic: "orders-events",
              key: "o-2",
              offset: "2",
              value: JSON.stringify({
                type: "delta-publish",
                patch: { id: "o-1", price: 125 },
              }),
            },
            {
              topic: "orders-events",
              key: "o-3",
              offset: "3",
              value: JSON.stringify({ type: "delete", id: "o-1" }),
            },
          ],
          commit: Effect.void,
        },
        commitPolicy: "after-ingest",
      });

      expect(singleCalls).toBe(0);
      expect(seenBatches).toHaveLength(1);
      expect(seenBatches[0]).toEqual([
        { type: "publish", row: { id: "o-1", symbol: "AAPL", price: 100 } },
        { type: "delta-publish", patch: { id: "o-1", price: 125 } },
        { type: "delete", id: "o-1" },
      ]);
    }),
  );

  it.effect("streams Kafka-ingested batches through the real websocket RPC path", () =>
    Effect.gen(function* () {
      const consumer = new FakeKafkaTopicConsumer();
      const verifier = new FakeKafkaTopicVerifier(["orders-events"]);
      const config = defineConfig({
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
      const serverLayer = layerViewServerWebsocketServer("/rpc").pipe(
        Layer.provide(
          layerViewServerRuntime(config, {
            kafkaConsumerFactory: () => consumer,
            kafkaTopicVerifier: verifier,
          }),
        ),
      );
      const socketLayer = Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const address = server.address;
        if (address._tag !== "TcpAddress") {
          return yield* Effect.die(new Error("Expected test server to listen on TCP"));
        }
        return NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`);
      }).pipe(Layer.unwrap);
      const clientLayer = RpcClient.layerProtocolSocket().pipe(
        Layer.provide(socketLayer),
        Layer.provide(RpcSerialization.layerNdjson),
      );
      const transportLayer = clientLayer.pipe(
        Layer.provideMerge(serverLayer),
        Layer.provide(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        yield* consumer.awaitTopic("orders-events");
        const rpcClient = yield* RpcClient.make(ViewServerRpcs);
        const events = yield* rpcClient
          .Subscribe({
            requestId: "kafka-websocket",
            topic: "orders",
            query,
          })
          .pipe(Stream.toQueue({ capacity: 16 }));

        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        expect(snapshot.type).toBe("snapshot");
        expect(snapshot.meta.totalRows).toBe(0);

        yield* consumer.offer("orders-events", {
          records: [
            {
              topic: "orders-events",
              key: "k-1",
              value: JSON.stringify({ id: "k-1", symbol: "AAPL", price: 100 }),
              partition: 0,
              offset: "1",
            },
          ],
          commit: Effect.sync(() => {
            consumer.commits += 1;
          }),
        });

        const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        if (delta.type !== "delta") {
          throw new Error("Expected delta");
        }
        expect(delta.meta.totalRows).toBe(1);
        expect(
          delta.ops.some((operation) => operation.type === "upsert" && operation.row.id === "k-1"),
        ).toBe(true);
        expect(consumer.commits).toBe(1);
        yield* rpcClient.Unsubscribe({ requestId: "kafka-websocket" });
      }).pipe(Effect.provide(transportLayer));
    }).pipe(Effect.scoped),
  );

  it.effect(
    "fails startup before starting consumers when Kafka topic verification is missing",
    () =>
      Effect.gen(function* () {
        const consumer = new FakeKafkaTopicConsumer();
        const error = yield* makeViewServerRuntime(kafkaConfig(), {
          kafkaConsumerFactory: () => consumer,
        }).pipe(Effect.flip);

        expect(error._tag).toBe("KafkaIngestFailed");
        expect(error.message).toMatch(/kafkaTopicVerifier/);
        expect(consumer.runs).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.effect("fails startup before starting consumers when a configured Kafka topic is absent", () =>
    Effect.gen(function* () {
      const consumer = new FakeKafkaTopicConsumer();
      const verifier = new FakeKafkaTopicVerifier([]);
      const error = yield* makeViewServerRuntime(kafkaConfig(), {
        kafkaConsumerFactory: () => consumer,
        kafkaTopicVerifier: verifier,
      }).pipe(Effect.flip);

      expect(error._tag).toBe("KafkaIngestFailed");
      expect(error.message).toMatch(/Kafka topics not found: orders-events/);
      expect(verifier.verified).toEqual([
        {
          brokers: ["127.0.0.1:9092"],
          topics: ["orders-events"],
        },
      ]);
      expect(consumer.runs).toBe(0);
    }).pipe(Effect.scoped),
  );

  it("converts protobuf decimal shapes to Effect BigDecimal without number precision loss", () => {
    expect(BigDecimal.format(protobufDecimalToBigDecimal({ value: "1234567890.123456789" }))).toBe(
      "1234567890.123456789",
    );
    expect(BigDecimal.format(unscaledDecimalToBigDecimal("1234567890123456789", 9))).toBe(
      "1234567890.123456789",
    );
    expect(BigDecimal.format(unscaledDecimalToBigDecimal(new Uint8Array([0xff, 0x85]), 2))).toBe(
      "-1.23",
    );
  });

  it("derives Kafka record lag from exposed end offsets", () => {
    expect(kafkaRecordLag({ value: "{}", offset: "7", highWatermark: "10" })).toBe(2);
    expect(kafkaRecordLag({ value: "{}", offset: "7", endOffset: "10" })).toBe(2);
    expect(kafkaRecordLag({ value: "{}", offset: "10", highWatermark: "10" })).toBe(0);
    expect(kafkaRecordLag({ value: "{}", offset: "7" })).toBeUndefined();
  });

  it("propagates Platformatic lag monitoring options from factory options", () => {
    const source = kafkaConfig().topics.orders.source;
    if (source === undefined) {
      throw new Error("Expected Kafka source");
    }

    expect(
      platformaticKafkaTopicConsumerOptions(source, {
        clientIdPrefix: "custom",
        batchSize: 25,
        sessionTimeout: 30_000,
        heartbeatInterval: 3_000,
        lagMonitoringIntervalMs: 250,
      }),
    ).toEqual({
      brokers: ["127.0.0.1:9092"],
      clientId: "custom-orders-events",
      batchSize: 25,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
      lagMonitoringIntervalMs: 250,
    });
  });
});

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

class FakeKafkaTopicConsumer implements KafkaTopicConsumer {
  commits = 0;
  runs = 0;
  #handlers = new Map<string, KafkaTopicConsumerRunArgs["onBatch"]>();
  #readyWaiters = new Map<string, readonly (() => void)[]>();

  run(args: KafkaTopicConsumerRunArgs): Effect.Effect<void> {
    this.runs += 1;
    return Effect.callback<void>((_resume) => {
      this.#handlers.set(args.topic, args.onBatch);
      this.#notifyReady(args.topic);
      return Effect.sync(() => {
        this.#handlers.delete(args.topic);
      });
    });
  }

  offer(topic: string, batch: KafkaRecordBatch) {
    const handler = this.#handlers.get(topic);
    if (handler === undefined) {
      return Effect.die(new Error(`No Kafka handler registered for ${topic}`));
    }
    return handler(batch);
  }

  awaitTopic(topic: string): Effect.Effect<void> {
    if (this.#handlers.has(topic)) {
      return Effect.void;
    }
    return Effect.callback<void>((resume) => {
      const ready = () => resume(Effect.void);
      this.#readyWaiters.set(topic, [...(this.#readyWaiters.get(topic) ?? []), ready]);
      return Effect.sync(() => {
        this.#readyWaiters.set(
          topic,
          (this.#readyWaiters.get(topic) ?? []).filter((waiter) => waiter !== ready),
        );
      });
    });
  }

  #notifyReady(topic: string): void {
    const waiters = this.#readyWaiters.get(topic) ?? [];
    this.#readyWaiters.delete(topic);
    for (const waiter of waiters) {
      waiter();
    }
  }
}

class FakeKafkaTopicVerifier implements KafkaTopicVerifier {
  readonly verified: KafkaTopicVerificationArgs[] = [];
  readonly #existingTopics: ReadonlySet<string>;
  readonly #onVerify: (() => void) | undefined;

  constructor(existingTopics: readonly string[], onVerify?: () => void) {
    this.#existingTopics = new Set(existingTopics);
    this.#onVerify = onVerify;
  }

  verifyTopics(args: KafkaTopicVerificationArgs): Effect.Effect<void, ViewServerError> {
    this.verified.push(args);
    this.#onVerify?.();
    const missing = args.topics.filter((topic) => !this.#existingTopics.has(topic));
    return missing.length === 0
      ? Effect.void
      : Effect.fail(
          kafkaIngestFailed(
            args.topics[0] ?? "__kafka",
            new Error(`Kafka topics not found: ${missing.join(", ")}`),
          ),
        );
  }
}

function rowById(
  rows: readonly Readonly<Record<string, unknown>>[],
  id: string,
): Readonly<Record<string, unknown>> {
  const row = rows.find((entry) => entry.id === id);
  expect(row).toBeDefined();
  if (row === undefined) {
    throw new Error(`Missing row ${id}`);
  }
  return row;
}
