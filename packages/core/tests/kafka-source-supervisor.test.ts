import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  defineConfig,
  EffectSource,
  KafkaSource,
  normalizeConfig,
  type RowObject,
} from "../src/config/index.ts";
import { kafkaIngestFailed, type ViewServerError } from "../src/errors.ts";
import {
  decodeJsonRecord,
  type KafkaRecordBatch,
  type KafkaTopicConsumer,
  type KafkaTopicConsumerRunArgs,
  type KafkaTopicVerifier,
  type KafkaTopicVerificationArgs,
} from "../src/kafka/index.ts";
import { KafkaSourceSupervisor } from "../src/server/kafka-source-supervisor.ts";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

type OrderRow = typeof Order.Type;

describe("KafkaSourceSupervisor", () => {
  it.effect("verifies Kafka topics before starting sources", () =>
    Effect.gen(function* () {
      const verifier = new RecordingTopicVerifier(["orders-events"]);
      const supervisor = new KafkaSourceSupervisor(normalizedKafkaConfig(), {
        kafkaTopicVerifier: verifier,
      });

      yield* supervisor.verifyTopics();
      expect(verifier.verified).toEqual([
        {
          brokers: ["127.0.0.1:9092"],
          topics: ["orders-events"],
        },
      ]);

      const missing = yield* new KafkaSourceSupervisor(normalizedKafkaConfig(), {
        kafkaTopicVerifier: new RecordingTopicVerifier([]),
      })
        .verifyTopics()
        .pipe(Effect.exit);
      expect(missing._tag).toBe("Failure");
    }),
  );

  it.effect("runs Kafka sources, reports lag, commits after ingest, and stops on shutdown", () =>
    Effect.gen(function* () {
      const consumer = new RecordingKafkaConsumer();
      const supervisor = new KafkaSourceSupervisor(normalizedKafkaConfig(), {
        kafkaConsumerFactory: () => consumer,
        kafkaTopicVerifier: new RecordingTopicVerifier(["orders-events"]),
      });
      const publishedRows: RowObject[] = [];
      let healthSyncs = 0;

      yield* supervisor.verifyTopics();
      yield* supervisor.start({
        publish: (_topic, row) =>
          Effect.sync(() => {
            publishedRows.push(row);
          }),
        deltaPublish: () => Effect.void,
        deleteById: () => Effect.void,
        mutateBatch: (_topic, mutations) =>
          Effect.sync(() => {
            for (const mutation of mutations) {
              if (mutation.type === "publish" && isRowObject(mutation.row)) {
                publishedRows.push(mutation.row);
              }
            }
          }),
        syncHealth: Effect.sync(() => {
          healthSyncs += 1;
        }),
      });
      yield* consumer.awaitTopic("orders-events");

      let commits = 0;
      yield* consumer.offer("orders-events", {
        records: [kafkaRecord("o-1", "8", { id: "o-1", price: 100 })],
        metrics: {
          lagTotal: 4,
          lagMax: 4,
          partitions: 1,
          offset: 8,
          endOffset: 12,
        },
        commit: Effect.sync(() => {
          commits += 1;
        }),
      });

      expect(publishedRows).toEqual([{ id: "o-1", price: 100 }]);
      expect(commits).toBe(1);
      expect(healthSyncs).toBe(1);
      expect(supervisor.topicHealth("orders")).toEqual({
        sourceFailed: false,
        kafka: {
          lagTotal: 4,
          lagMax: 4,
          partitions: 1,
          offset: 8,
          endOffset: 12,
        },
      });

      yield* supervisor.shutdown().pipe(Effect.timeout("1 second"));
      expect(consumer.handlers.size).toBe(0);
    }).pipe(Effect.scoped),
  );

  it.effect("marks source failures degraded and interrupts stuck sources on shutdown", () =>
    Effect.gen(function* () {
      const failSource = yield* Deferred.make<void>();
      const failedSupervisor = new KafkaSourceSupervisor(
        normalizeConfig(
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
        ),
        {},
      );
      let failedHealthSyncs = 0;
      yield* failedSupervisor.start({
        publish: () => Effect.void,
        deltaPublish: () => Effect.void,
        deleteById: () => Effect.void,
        mutateBatch: () => Effect.void,
        syncHealth: Effect.sync(() => {
          failedHealthSyncs += 1;
        }),
      });
      yield* Deferred.succeed(failSource, undefined);
      yield* waitForCondition(() => failedSupervisor.sourceFailure("orders") !== undefined);
      expect(failedSupervisor.topicHealth("orders").sourceFailed).toBe(true);
      expect(failedHealthSyncs).toBe(1);

      const interrupted = yield* Deferred.make<void>();
      const stuckSupervisor = new KafkaSourceSupervisor(
        normalizeConfig(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
                source: EffectSource<RowObject, string>({
                  run: () =>
                    Effect.never.pipe(
                      Effect.ensuring(Deferred.succeed(interrupted, undefined).pipe(Effect.ignore)),
                    ),
                }),
              },
            },
          }),
        ),
        {},
      );
      yield* stuckSupervisor.start({
        publish: () => Effect.void,
        deltaPublish: () => Effect.void,
        deleteById: () => Effect.void,
        mutateBatch: () => Effect.void,
        syncHealth: Effect.void,
      });
      yield* stuckSupervisor.shutdown().pipe(Effect.timeout("1 second"));
      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"));
    }).pipe(Effect.scoped),
  );
});

class RecordingKafkaConsumer implements KafkaTopicConsumer {
  readonly handlers = new Map<string, KafkaTopicConsumerRunArgs["onBatch"]>();
  readonly readyWaiters = new Map<string, readonly (() => void)[]>();

  run(args: KafkaTopicConsumerRunArgs): Effect.Effect<void> {
    return Effect.callback<void>(() => {
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

class RecordingTopicVerifier implements KafkaTopicVerifier {
  readonly existingTopics: ReadonlySet<string>;
  readonly verified: KafkaTopicVerificationArgs[] = [];

  constructor(existingTopics: readonly string[]) {
    this.existingTopics = new Set(existingTopics);
  }

  verifyTopics(args: KafkaTopicVerificationArgs): Effect.Effect<void, ViewServerError> {
    this.verified.push(args);
    const missing = args.topics.filter((topic) => !this.existingTopics.has(topic));
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

function normalizedKafkaConfig() {
  return normalizeConfig(
    defineConfig({
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
    }),
  );
}

function kafkaRecord(key: string, offset: string, row: OrderRow) {
  return {
    topic: "orders-events",
    key,
    offset,
    value: JSON.stringify(row),
  };
}

function isRowObject(value: unknown): value is RowObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function waitForCondition(predicate: () => boolean): Effect.Effect<void> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (predicate()) {
        return;
      }
      yield* Effect.sleep("10 millis");
    }
    return yield* Effect.die(new Error("Condition did not become true"));
  });
}
