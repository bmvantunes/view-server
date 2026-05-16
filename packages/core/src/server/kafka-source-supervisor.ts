import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import type {
  EffectSourceContext,
  KafkaSourceConfig,
  NormalizedViewServerConfig,
  RowObject,
} from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import { kafkaIngestFailed, type ViewServerError } from "../errors.ts";
import {
  runKafkaSource,
  type KafkaBatchMetrics,
  type KafkaTopicConsumer,
  type KafkaTopicVerifier,
} from "../kafka/index.ts";
import type { RuntimeRow } from "../protocol/index.ts";
import {
  emptyKafkaRuntimeMetrics,
  kafkaRuntimeMetrics,
  type KafkaRuntimeMetrics,
} from "./runtime-health-projection.ts";

export type KafkaSourceConsumerFactory = (
  source: KafkaSourceConfig<RowObject, string>,
) => KafkaTopicConsumer;

export type KafkaSourceSupervisorOptions = {
  readonly kafkaConsumerFactory?: KafkaSourceConsumerFactory | undefined;
  readonly kafkaTopicVerifier?: KafkaTopicVerifier | undefined;
};

export type KafkaSourceSupervisorRuntime = {
  readonly publish: (topic: string, row: RowObject) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: (topic: string, patch: RuntimeRow) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: (topic: string, id: string | number) => Effect.Effect<void, ViewServerError>;
  readonly syncHealth: Effect.Effect<void>;
};

export type KafkaSourceHealth = {
  readonly kafka: KafkaRuntimeMetrics;
  readonly sourceFailed: boolean;
};

type KafkaSourceForVerification = {
  readonly viewTopic: string;
  readonly brokers: readonly string[];
  readonly kafkaTopic: string;
};

export class KafkaSourceSupervisor {
  readonly #config: NormalizedViewServerConfig;
  readonly #options: KafkaSourceSupervisorOptions;
  readonly #kafkaMetricsByTopic = new Map<string, KafkaRuntimeMetrics>();
  readonly #sourceFailuresByTopic = new Map<string, string>();
  #sourceFibers: Fiber.Fiber<void, ViewServerError>[] = [];

  constructor(config: NormalizedViewServerConfig, options: KafkaSourceSupervisorOptions) {
    this.#config = config;
    this.#options = options;
  }

  topicHealth(topic: string): KafkaSourceHealth {
    return {
      kafka: this.#kafkaMetricsByTopic.get(topic) ?? emptyKafkaRuntimeMetrics,
      sourceFailed: this.#sourceFailuresByTopic.has(topic),
    };
  }

  sourceFailure(topic: string): string | undefined {
    return this.#sourceFailuresByTopic.get(topic);
  }

  verifyTopics(): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.kafka_source_supervisor.verify_topics")(function* (
      supervisor: KafkaSourceSupervisor,
    ) {
      const sources = collectKafkaSources(supervisor.#config);
      yield* Effect.annotateCurrentSpan({
        "view_server.batch_size": sources.length,
      });
      if (sources.length === 0) {
        return;
      }
      const verifier = supervisor.#options.kafkaTopicVerifier;
      if (verifier === undefined) {
        return yield* Effect.fail(
          kafkaIngestFailed(
            sources[0].viewTopic,
            new Error("KafkaSource requires a kafkaTopicVerifier runtime option"),
          ),
        );
      }
      yield* Effect.forEach(
        kafkaVerificationGroups(sources),
        ({ brokers, topics }) => verifier.verifyTopics({ brokers, topics }),
        { discard: true },
      );
    })(this);
  }

  start(
    runtime: KafkaSourceSupervisorRuntime,
  ): Effect.Effect<void, ViewServerError, import("effect/Scope").Scope> {
    return Effect.fn("view-server.kafka_source_supervisor.start")(function* (
      supervisor: KafkaSourceSupervisor,
    ) {
      const entries = Object.entries(supervisor.#config.topics);
      yield* Effect.annotateCurrentSpan({
        "view_server.batch_size": entries.length,
      });
      yield* Effect.forEach(
        entries,
        ([topic, topicConfig]) =>
          Effect.gen(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            if (topic === VIEW_SERVER_HEALTH_TOPIC || topicConfig.source === undefined) {
              return;
            }
            const context: EffectSourceContext<RowObject, string> = {
              topic,
              idField: topicConfig.id,
              publish: (row) => runtime.publish(topic, row),
              deltaPublish: (patch) => runtime.deltaPublish(topic, patch),
              deleteById: (id) => runtime.deleteById(topic, id),
            };
            if (topicConfig.source._tag === "EffectSource") {
              const fiber = yield* supervisor
                .monitorSource(topic, topicConfig.source.run(context), runtime.syncHealth)
                .pipe(Effect.forkScoped({ startImmediately: true }));
              supervisor.#sourceFibers.push(fiber);
              return;
            }
            const source = topicConfig.source;
            const consumer = supervisor.#options.kafkaConsumerFactory?.(source);
            if (consumer === undefined) {
              return yield* Effect.fail(
                kafkaIngestFailed(
                  topic,
                  new Error("KafkaSource requires a kafkaConsumerFactory runtime option"),
                ),
              );
            }
            const fiber = yield* supervisor
              .monitorSource(
                topic,
                runKafkaSource({
                  viewTopic: topic,
                  idField: topicConfig.id,
                  source,
                  consumer,
                  runtime: context,
                  onBatchMetrics: (metrics) =>
                    supervisor.recordKafkaMetrics(topic, metrics, runtime),
                }),
                runtime.syncHealth,
              )
              .pipe(Effect.forkScoped({ startImmediately: true }));
            supervisor.#sourceFibers.push(fiber);
          }).pipe(Effect.withSpan("view-server.kafka_source_supervisor.source.start")),
        { discard: true },
      );
    })(this);
  }

  shutdown(): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.kafka_source_supervisor.shutdown")(function* (
      supervisor: KafkaSourceSupervisor,
    ) {
      const fibers = supervisor.#sourceFibers;
      supervisor.#sourceFibers = [];
      yield* Effect.forEach(fibers, (fiber) => Fiber.interrupt(fiber), {
        discard: true,
      }).pipe(Effect.ignore);
    })(this);
  }

  private recordKafkaMetrics(
    topic: string,
    metrics: KafkaBatchMetrics,
    runtime: KafkaSourceSupervisorRuntime,
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (supervisor: KafkaSourceSupervisor) {
      supervisor.#kafkaMetricsByTopic.set(topic, kafkaRuntimeMetrics(metrics));
      yield* runtime.syncHealth;
    })(this);
  }

  private monitorSource(
    topic: string,
    source: Effect.Effect<void, ViewServerError>,
    syncHealth: Effect.Effect<void>,
  ): Effect.Effect<void, ViewServerError> {
    return source.pipe(
      Effect.exit,
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit)
          ? this.recordSourceFailure(topic, "Source exited", syncHealth)
          : this.recordSourceFailure(topic, Cause.pretty(exit.cause), syncHealth),
      ),
    );
  }

  private recordSourceFailure(
    topic: string,
    message: string,
    syncHealth: Effect.Effect<void>,
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.kafka_source_supervisor.source.failed")(function* (
      supervisor: KafkaSourceSupervisor,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      supervisor.#sourceFailuresByTopic.set(topic, message);
      yield* Effect.logWarning(`view-server source degraded topic=${topic} reason=${message}`);
      yield* syncHealth.pipe(Effect.ignore);
    })(this);
  }
}

function collectKafkaSources(
  config: NormalizedViewServerConfig,
): readonly KafkaSourceForVerification[] {
  const sources: KafkaSourceForVerification[] = [];
  for (const [viewTopic, topicConfig] of Object.entries(config.topics)) {
    const source = topicConfig.source;
    if (viewTopic !== VIEW_SERVER_HEALTH_TOPIC && source?._tag === "KafkaSource") {
      sources.push({
        viewTopic,
        brokers: source.brokers,
        kafkaTopic: source.topic,
      });
    }
  }
  return sources;
}

function kafkaVerificationGroups(sources: readonly KafkaSourceForVerification[]) {
  const groups = new Map<
    string,
    {
      readonly brokers: readonly string[];
      readonly topicSet: Set<string>;
    }
  >();
  for (const source of sources) {
    const key = JSON.stringify(source.brokers);
    const group = groups.get(key);
    if (group === undefined) {
      groups.set(key, {
        brokers: source.brokers,
        topicSet: new Set([source.kafkaTopic]),
      });
    } else {
      group.topicSet.add(source.kafkaTopic);
    }
  }
  return Array.from(groups.values(), ({ brokers, topicSet }) => ({
    brokers,
    topics: Array.from(topicSet),
  }));
}
