import * as Effect from "effect/Effect";
import type { KafkaSourceConfig, NormalizedViewServerConfig, RowObject } from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import type { KafkaTopicConsumer, KafkaTopicVerifier } from "../kafka/index.ts";
import { KafkaSourceSupervisor, type KafkaSourceHealth } from "./kafka-source-supervisor.ts";
import {
  createTopicPlacements,
  type TopicPlacement,
  type TopicPlacementOptions,
  type TopicPlacementSet,
} from "./topic-placement.ts";

export type RuntimeSourceGraphOptions = TopicPlacementOptions & {
  readonly kafkaConsumerFactory?:
    | ((source: KafkaSourceConfig<RowObject, string>) => KafkaTopicConsumer)
    | undefined;
  readonly kafkaTopicVerifier?: KafkaTopicVerifier | undefined;
};

export type RuntimeSourceKind = "system" | "none" | "effect" | "kafka";

export type RuntimeSourceMapping = {
  readonly topic: string;
  readonly sourceKind: RuntimeSourceKind;
  readonly kafkaTopic?: string | undefined;
  readonly brokers?: readonly string[] | undefined;
};

export type RuntimeWorkerMapping = {
  readonly topic: string;
  readonly workerOwnedByTopic: true;
  readonly snapshotBackendOwnedByTopic: boolean;
};

export type RuntimeSourceGraph = {
  readonly sourceSupervisor: KafkaSourceSupervisor;
  readonly placements: readonly TopicPlacement[];
  readonly workers: TopicPlacementSet["workers"];
  readonly sourceMappings: readonly RuntimeSourceMapping[];
  readonly workerMappings: readonly RuntimeWorkerMapping[];
  readonly topicHealth: (topic: string) => KafkaSourceHealth;
};

export function createRuntimeSourceGraph(
  config: NormalizedViewServerConfig,
  options: RuntimeSourceGraphOptions,
): Effect.Effect<
  RuntimeSourceGraph,
  import("../errors.ts").ViewServerError,
  import("effect/Scope").Scope
> {
  return Effect.fn("view-server.runtime_source_graph.create")(function* () {
    const sourceSupervisor = new KafkaSourceSupervisor(config, {
      kafkaConsumerFactory: options.kafkaConsumerFactory,
      kafkaTopicVerifier: options.kafkaTopicVerifier,
    });
    yield* sourceSupervisor.verifyTopics();
    const placementSet = yield* createTopicPlacements(config, options);
    const graph = {
      sourceSupervisor,
      placements: placementSet.placements,
      workers: placementSet.workers,
      sourceMappings: sourceMappingsForConfig(config),
      workerMappings: workerMappingsForPlacements(placementSet.placements),
      topicHealth: (topic: string) => sourceSupervisor.topicHealth(topic),
    } satisfies RuntimeSourceGraph;
    yield* Effect.annotateCurrentSpan({
      "view_server.batch_size": graph.placements.length,
    });
    return graph;
  })();
}

function sourceMappingsForConfig(
  config: NormalizedViewServerConfig,
): readonly RuntimeSourceMapping[] {
  return Object.entries(config.topics).map(([topic, topicConfig]) => {
    if (topic === VIEW_SERVER_HEALTH_TOPIC) {
      return {
        topic,
        sourceKind: "system",
      };
    }
    const source = topicConfig.source;
    if (source === undefined) {
      return {
        topic,
        sourceKind: "none",
      };
    }
    if (source._tag === "EffectSource") {
      return {
        topic,
        sourceKind: "effect",
      };
    }
    return {
      topic,
      sourceKind: "kafka",
      kafkaTopic: source.topic,
      brokers: source.brokers,
    };
  });
}

function workerMappingsForPlacements(
  placements: readonly TopicPlacement[],
): readonly RuntimeWorkerMapping[] {
  return placements.map((placement) => ({
    topic: placement.topic,
    workerOwnedByTopic: true,
    snapshotBackendOwnedByTopic: placement.snapshotBackend !== undefined,
  }));
}
