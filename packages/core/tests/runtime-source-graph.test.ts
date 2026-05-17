import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  defineConfig,
  KafkaSource,
  normalizeConfig,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../src/config/index.ts";
import { kafkaIngestFailed } from "../src/errors.ts";
import type { KafkaTopicVerifier } from "../src/kafka/index.ts";
import { createRuntimeSourceGraph } from "../src/server/runtime-source-graph.ts";
import type { TopicWorkerHost } from "../src/worker/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  desk: Schema.String,
  quantity: Schema.Number,
});

const config = defineConfig({
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

describe("RuntimeSourceGraph", () => {
  it.effect("maps two topics to two workers and two topic-owned chDB children", () =>
    Effect.gen(function* () {
      const graph = yield* createRuntimeSourceGraph(normalizeConfig(config), {});
      yield* Effect.addFinalizer(() => shutdownWorkers(graph.workers.values()));

      expect(graph.placements.map((placement) => placement.topic)).toEqual([
        "orders",
        "trades",
        VIEW_SERVER_HEALTH_TOPIC,
      ]);
      expect(graph.sourceMappings).toEqual([
        { topic: "orders", sourceKind: "none" },
        { topic: "trades", sourceKind: "none" },
        { topic: VIEW_SERVER_HEALTH_TOPIC, sourceKind: "system" },
      ]);
      expect(graph.workerMappings).toEqual([
        {
          topic: "orders",
          workerOwnedByTopic: true,
          snapshotBackendOwnedByTopic: true,
        },
        {
          topic: "trades",
          workerOwnedByTopic: true,
          snapshotBackendOwnedByTopic: true,
        },
        {
          topic: VIEW_SERVER_HEALTH_TOPIC,
          workerOwnedByTopic: true,
          snapshotBackendOwnedByTopic: true,
        },
      ]);

      const ordersMetrics = yield* requireWorker(graph.workers, "orders").metrics;
      const tradesMetrics = yield* requireWorker(graph.workers, "trades").metrics;

      expect(ordersMetrics.chdbStatus).toBe("ready");
      expect(tradesMetrics.chdbStatus).toBe("ready");
      expect(ordersMetrics.chdbPid).toBeGreaterThan(0);
      expect(tradesMetrics.chdbPid).toBeGreaterThan(0);
      expect(ordersMetrics.chdbPid).not.toBe(tradesMetrics.chdbPid);
    }).pipe(Effect.scoped),
  );

  it.effect("fails startup when a configured Kafka source topic is missing", () =>
    Effect.gen(function* () {
      const error = yield* createRuntimeSourceGraph(normalizeConfig(kafkaConfig()), {
        kafkaTopicVerifier: missingTopicVerifier,
        __testingUseMemorySnapshotBackend: true,
      }).pipe(Effect.flip);

      expect(error._tag).toBe("KafkaIngestFailed");
      expect(error.message).toContain("Kafka topics not found: orders.kafka");
    }).pipe(Effect.scoped),
  );

  it("rejects reserved user topics before graph creation", () => {
    expect(() =>
      normalizeConfig(
        defineConfig({
          topics: {
            __bad: {
              id: "id",
              schema: Order,
            },
          },
        }),
      ),
    ).toThrow("reserved prefix");
  });
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
          decode: () => Effect.succeed({ id: "order-1", symbol: "AAPL", price: 1 }),
        }),
      },
    },
  });
}

const missingTopicVerifier = {
  verifyTopics: ({ topics }) =>
    Effect.fail(
      kafkaIngestFailed(
        topics[0] ?? "__kafka",
        new Error(`Kafka topics not found: ${topics.join(", ")}`),
      ),
    ),
} satisfies KafkaTopicVerifier;

function requireWorker(
  workers: ReadonlyMap<string, TopicWorkerHost>,
  topic: string,
): TopicWorkerHost {
  const worker = workers.get(topic);
  if (worker === undefined) {
    throw new Error(`Expected worker for topic ${topic}`);
  }
  return worker;
}

function shutdownWorkers(workers: Iterable<TopicWorkerHost>): Effect.Effect<void> {
  return Effect.forEach(Array.from(workers), (worker) => worker.shutdown, {
    discard: true,
  }).pipe(Effect.ignore);
}
