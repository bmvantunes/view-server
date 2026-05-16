import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  type AuthorizationContext,
  columnCatalogForTopic,
  type KafkaSourceConfig,
  isReservedTopic,
  normalizeConfig,
  type NormalizedViewServerConfig,
  type RowObject,
  type ViewServerConfig,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import {
  invalidConfig,
  missingTopic,
  unauthorized,
  unauthorizedSystemTopic,
  type ViewServerError,
} from "../errors.ts";
import type {
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../protocol/index.ts";
import type { KafkaTopicConsumer, KafkaTopicVerifier } from "../kafka/index.ts";
import { KafkaSourceSupervisor } from "./kafka-source-supervisor.ts";
import { QueryLimitPolicy } from "./query-limit-policy.ts";
import {
  healthRowsFromResponse,
  projectRuntimeHealth,
  type HealthResponse,
  type RuntimeHealthProjectionTopicInput,
} from "./runtime-health-projection.ts";
import { RuntimeShutdownController } from "./runtime-shutdown-controller.ts";
import { createTopicPlacements, type TopicPlacementOptions } from "./topic-placement.ts";

export type { HealthResponse } from "./runtime-health-projection.ts";

export type ViewServerRuntimeShape = {
  readonly config: NormalizedViewServerConfig;
  readonly query: (
    topic: string,
    query: RuntimeQuery,
  ) => Effect.Effect<QueryResponse<readonly RuntimeRow[]>, ViewServerError>;
  readonly subscribe: (
    requestId: string,
    topic: string,
    query: RuntimeQuery,
  ) => Stream.Stream<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>;
  readonly unsubscribe: (requestId: string) => Effect.Effect<void, ViewServerError>;
  readonly publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: (topic: string, patch: RuntimeRow) => Effect.Effect<void, ViewServerError>;
  readonly deleteById: (topic: string, id: string | number) => Effect.Effect<void, ViewServerError>;
  readonly health: Effect.Effect<HealthResponse, ViewServerError>;
  readonly close: Effect.Effect<void, ViewServerError>;
};

export class ViewServerRuntime extends Context.Service<ViewServerRuntime, ViewServerRuntimeShape>()(
  "@view-server/core/ViewServerRuntime",
) {}

export type ViewServerRuntimeOptions = TopicPlacementOptions & {
  readonly kafkaConsumerFactory?:
    | ((source: KafkaSourceConfig<RowObject, string>) => KafkaTopicConsumer)
    | undefined;
  readonly kafkaTopicVerifier?: KafkaTopicVerifier | undefined;
};

export function makeViewServerRuntime(
  config: ViewServerConfig,
  options: ViewServerRuntimeOptions = {},
): Effect.Effect<ViewServerRuntimeShape, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.runtime.make")(function* () {
    const normalized = yield* Effect.try({
      try: () => normalizeConfig(config),
      catch: (error) => invalidConfig("Invalid view-server config", "config", error),
    });
    const sourceSupervisor = new KafkaSourceSupervisor(normalized, {
      kafkaConsumerFactory: options.kafkaConsumerFactory,
      kafkaTopicVerifier: options.kafkaTopicVerifier,
    });
    yield* sourceSupervisor.verifyTopics();
    const placementSet = yield* createTopicPlacements(normalized, options);
    const workers = placementSet.workers;
    const columnCatalogs = new Map(
      Object.entries(normalized.topics).map(([topic, topicConfig]) => [
        topic,
        columnCatalogForTopic(topic, topicConfig),
      ]),
    );
    const queryLimitPolicy = QueryLimitPolicy.fromConfig(normalized);
    const shutdownController = new RuntimeShutdownController();

    const workerFor = (topic: string) => {
      const worker = workers.get(topic);
      return worker === undefined ? Effect.fail(missingTopic(topic)) : Effect.succeed(worker);
    };

    const ensureRuntimeOpen = (
      operation: "query" | "subscribe" | "publish" | "delta-publish" | "delete",
      topic: string,
      requestId?: string,
    ) => shutdownController.ensureOpen(operation, topic, requestId);

    const ensureReadableTopic = (topic: string, operation: "subscribe" | "query") =>
      isReservedTopic(topic) && topic !== VIEW_SERVER_HEALTH_TOPIC
        ? Effect.fail(unauthorizedSystemTopic(topic, operation))
        : Effect.void;

    const authorizeQuery = (topic: string, operation: "subscribe" | "query", payload: unknown) =>
      normalized.auth
        .authorizeQuery({ topic, operation, payload, transport: "rpc" })
        .pipe(
          Effect.flatMap((allowed) =>
            allowed ? Effect.void : Effect.fail(unauthorized(topic, operation)),
          ),
        );

    const authorizePublish = (
      topic: string,
      operation: "publish" | "delta-publish" | "delete",
      payload: unknown,
      transport: AuthorizationContext["transport"],
    ) =>
      normalized.auth
        .authorizePublish({ topic, operation, payload, transport })
        .pipe(
          Effect.flatMap((allowed) =>
            allowed ? Effect.void : Effect.fail(unauthorized(topic, operation)),
          ),
        );

    const collectHealth = Effect.fn("view-server.runtime.health")(function* () {
      const topics: Record<string, RuntimeHealthProjectionTopicInput> = {};
      for (const [topic, worker] of workers) {
        const metrics = yield* worker.metrics;
        const sourceHealth = sourceSupervisor.topicHealth(topic);
        topics[topic] = {
          worker: metrics,
          kafka: sourceHealth.kafka,
          sourceFailed: sourceHealth.sourceFailed,
          queryRejectedCount: queryLimitPolicy.rejectedCount(topic),
        };
      }
      return projectRuntimeHealth({ closing: shutdownController.isClosing(), topics });
    });

    const syncHealthTopic = Effect.fn("view-server.runtime.health_topic.sync")(function* () {
      const healthWorker = workers.get(VIEW_SERVER_HEALTH_TOPIC);
      if (healthWorker === undefined) {
        return;
      }
      const health = yield* collectHealth();
      yield* Effect.forEach(healthRowsFromResponse(health), (row) => healthWorker.publish(row), {
        discard: true,
      });
    });

    const syncHealthTopicIgnoringErrors = syncHealthTopic().pipe(Effect.ignore);

    const publishWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      row: unknown,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(unauthorizedSystemTopic(topic, "publish"));
      }
      yield* ensureRuntimeOpen("publish", topic);
      yield* authorizePublish(topic, "publish", row, transport);
      const worker = yield* workerFor(topic);
      yield* worker.publish(row);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const publishWithTransport = (
      topic: string,
      row: unknown,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? publishWithTransportUntraced(topic, row, transport)
        : Effect.fn("view-server.runtime.publish")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* publishWithTransportUntraced(topic, row, transport);
          })();

    const deltaPublishWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      patch: RuntimeRow,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(unauthorizedSystemTopic(topic, "delta-publish"));
      }
      yield* ensureRuntimeOpen("delta-publish", topic);
      yield* authorizePublish(topic, "delta-publish", patch, transport);
      const worker = yield* workerFor(topic);
      yield* worker.deltaPublish(patch);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const deltaPublishWithTransport = (
      topic: string,
      patch: RuntimeRow,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? deltaPublishWithTransportUntraced(topic, patch, transport)
        : Effect.fn("view-server.runtime.delta_publish")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* deltaPublishWithTransportUntraced(topic, patch, transport);
          })();

    const deleteByIdWithTransportUntraced = Effect.fnUntraced(function* (
      topic: string,
      id: string | number,
      transport: AuthorizationContext["transport"],
    ) {
      if (isReservedTopic(topic) && transport !== "internal") {
        return yield* Effect.fail(unauthorizedSystemTopic(topic, "delete"));
      }
      yield* ensureRuntimeOpen("delete", topic);
      yield* authorizePublish(topic, "delete", id, transport);
      const worker = yield* workerFor(topic);
      yield* worker.deleteById(id);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopic();
      }
    });

    const deleteByIdWithTransport = (
      topic: string,
      id: string | number,
      transport: AuthorizationContext["transport"],
    ) =>
      transport === "internal"
        ? deleteByIdWithTransportUntraced(topic, id, transport)
        : Effect.fn("view-server.runtime.delete")(function* () {
            yield* Effect.annotateCurrentSpan({
              "view_server.topic": topic,
            });
            return yield* deleteByIdWithTransportUntraced(topic, id, transport);
          })();

    const queryRuntime = Effect.fn("view-server.runtime.query")(function* (
      topic: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      yield* ensureRuntimeOpen("query", topic);
      yield* ensureReadableTopic(topic, "query");
      const guardedQuery = yield* queryLimitPolicy.validate(
        topic,
        query,
        columnCatalogs.get(topic),
      );
      yield* authorizeQuery(topic, "query", guardedQuery);
      if (topic === VIEW_SERVER_HEALTH_TOPIC) {
        yield* syncHealthTopicIgnoringErrors;
      }
      const worker = yield* workerFor(topic);
      const response = yield* worker.query(guardedQuery);
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": response.rows.length,
        "view_server.total_rows": response.totalRows,
        "view_server.worker_version": response.version,
      });
      return response;
    });

    const subscribeRuntime = Effect.fn("view-server.runtime.subscribe")(function* (
      requestId: string,
      topic: string,
      query: RuntimeQuery,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": requestId,
        "view_server.subscription_id": requestId,
        "view_server.topic": topic,
      });
      yield* ensureRuntimeOpen("subscribe", topic, requestId);
      yield* ensureReadableTopic(topic, "subscribe");
      const guardedQuery = yield* queryLimitPolicy.validate(
        topic,
        query,
        columnCatalogs.get(topic),
      );
      yield* authorizeQuery(topic, "subscribe", guardedQuery);
      const worker = yield* workerFor(topic);
      return worker.subscribe(requestId, guardedQuery).pipe(
        Stream.onFirst(() => syncHealthTopicIgnoringErrors),
        Stream.ensuring(syncHealthTopicIgnoringErrors),
      );
    });

    const unsubscribeRuntime = Effect.fn("view-server.runtime.unsubscribe")(function* (
      requestId: string,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": requestId,
        "view_server.subscription_id": requestId,
      });
      yield* Effect.forEach(workers.values(), (worker) => worker.unsubscribe(requestId), {
        discard: true,
      });
      yield* syncHealthTopicIgnoringErrors;
    });

    const closeRuntime = Effect.fn("view-server.runtime.close")(function* () {
      yield* shutdownController.close({
        syncHealth: syncHealthTopicIgnoringErrors,
        stopSources: sourceSupervisor.shutdown(),
        workers: workers.values(),
      });
    });

    const runtime: ViewServerRuntimeShape = {
      config: normalized,

      query: queryRuntime,

      subscribe: (requestId, topic, query) =>
        Stream.unwrap(subscribeRuntime(requestId, topic, query)),

      unsubscribe: unsubscribeRuntime,

      publish: (topic, row) => publishWithTransport(topic, row, "rpc"),

      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "rpc"),

      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "rpc"),

      health: collectHealth(),

      close: closeRuntime(),
    };

    yield* syncHealthTopic();
    yield* sourceSupervisor.start({
      publish: (topic, row) => publishWithTransport(topic, row, "internal"),
      deltaPublish: (topic, patch) => deltaPublishWithTransport(topic, patch, "internal"),
      deleteById: (topic, id) => deleteByIdWithTransport(topic, id, "internal"),
      syncHealth: syncHealthTopicIgnoringErrors,
    });

    return runtime;
  })();
}

export const layerViewServerRuntime = (
  config: ViewServerConfig,
  options?: ViewServerRuntimeOptions,
): Layer.Layer<ViewServerRuntime, ViewServerError> =>
  Layer.effect(ViewServerRuntime, makeViewServerRuntime(config, options));
