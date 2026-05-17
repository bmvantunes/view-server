import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import {
  type AuthorizationContext,
  type ColumnCatalog,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import { missingTopic, type ViewServerError } from "../errors.ts";
import type {
  QueryResponse,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "../protocol/index.ts";
import type { TopicWorkerCore } from "../worker/topic-worker-core.ts";
import type { AuthPolicy } from "./auth-policy.ts";
import type { QueryLimitPolicy } from "./query-limit-policy.ts";
import type { RuntimeShutdownController } from "./runtime-shutdown-controller.ts";

type RuntimeOperationName = "query" | "subscribe" | "publish" | "delta-publish" | "delete";

export type RuntimeOperations = {
  readonly query: (
    topic: string,
    query: RuntimeQuery,
  ) => Effect.Effect<QueryResponse<readonly RuntimeRow[]>, ViewServerError>;
  readonly subscribe: (
    requestId: string,
    topic: string,
    query: RuntimeQuery,
  ) => Effect.Effect<
    Stream.Stream<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError>,
    ViewServerError
  >;
  readonly unsubscribe: (requestId: string) => Effect.Effect<void, ViewServerError>;
  readonly publishWithTransport: (
    topic: string,
    row: unknown,
    transport: AuthorizationContext["transport"],
  ) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublishWithTransport: (
    topic: string,
    patch: RuntimeRow,
    transport: AuthorizationContext["transport"],
  ) => Effect.Effect<void, ViewServerError>;
  readonly deleteByIdWithTransport: (
    topic: string,
    id: string | number,
    transport: AuthorizationContext["transport"],
  ) => Effect.Effect<void, ViewServerError>;
};

export function makeRuntimeOperations(args: {
  readonly workers: ReadonlyMap<string, TopicWorkerCore>;
  readonly columnCatalogs: ReadonlyMap<string, ColumnCatalog>;
  readonly authPolicy: AuthPolicy;
  readonly queryLimitPolicy: QueryLimitPolicy;
  readonly shutdownController: RuntimeShutdownController;
  readonly syncHealthTopic: Effect.Effect<void, ViewServerError>;
  readonly syncHealthTopicIgnoringErrors: Effect.Effect<void>;
}): RuntimeOperations {
  const workerFor = (topic: string) => {
    const worker = args.workers.get(topic);
    return worker === undefined ? Effect.fail(missingTopic(topic)) : Effect.succeed(worker);
  };

  const ensureRuntimeOpen = (operation: RuntimeOperationName, topic: string, requestId?: string) =>
    args.shutdownController.ensureOpen(operation, topic, requestId);

  const runMutationUntraced = <Payload>(
    topic: string,
    payload: Payload,
    operation: "publish" | "delta-publish" | "delete",
    transport: AuthorizationContext["transport"],
    dispatch: (worker: TopicWorkerCore, payload: Payload) => Effect.Effect<void, ViewServerError>,
  ) =>
    Effect.fnUntraced(function* () {
      yield* ensureRuntimeOpen(operation, topic);
      yield* args.authPolicy.canPublishTopic({ topic, operation, payload, transport });
      const worker = yield* workerFor(topic);
      yield* dispatch(worker, payload);
      if (topic !== VIEW_SERVER_HEALTH_TOPIC) {
        yield* args.syncHealthTopic;
      }
    })();

  const runMutation = <Payload>(
    topic: string,
    payload: Payload,
    operation: "publish" | "delta-publish" | "delete",
    transport: AuthorizationContext["transport"],
    dispatch: (worker: TopicWorkerCore, payload: Payload) => Effect.Effect<void, ViewServerError>,
  ) =>
    transport === "internal"
      ? runMutationUntraced(topic, payload, operation, transport, dispatch)
      : Effect.fn(`view-server.runtime.${runtimeSpanOperation(operation)}`)(function* () {
          yield* Effect.annotateCurrentSpan({
            "view_server.topic": topic,
          });
          return yield* runMutationUntraced(topic, payload, operation, transport, dispatch);
        })();

  const query = Effect.fn("view-server.runtime.query")(function* (
    topic: string,
    query: RuntimeQuery,
  ) {
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": topic,
    });
    yield* ensureRuntimeOpen("query", topic);
    yield* args.authPolicy.canReadTopic({ topic, operation: "query" });
    const guardedQuery = yield* args.queryLimitPolicy.validate(
      topic,
      query,
      args.columnCatalogs.get(topic),
    );
    yield* args.authPolicy.canReadTopic({ topic, operation: "query", payload: guardedQuery });
    if (topic === VIEW_SERVER_HEALTH_TOPIC) {
      yield* args.syncHealthTopicIgnoringErrors;
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

  const subscribe = Effect.fn("view-server.runtime.subscribe")(function* (
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
    yield* args.authPolicy.canSubscribe({ topic, requestId });
    const guardedQuery = yield* args.queryLimitPolicy.validate(
      topic,
      query,
      args.columnCatalogs.get(topic),
    );
    yield* args.authPolicy.canSubscribe({ topic, requestId, payload: guardedQuery });
    const worker = yield* workerFor(topic);
    return worker.subscribe(requestId, guardedQuery).pipe(
      Stream.onFirst(() => args.syncHealthTopicIgnoringErrors),
      Stream.ensuring(args.syncHealthTopicIgnoringErrors),
    );
  });

  const unsubscribe = Effect.fn("view-server.runtime.unsubscribe")(function* (requestId: string) {
    yield* Effect.annotateCurrentSpan({
      "view_server.request_id": requestId,
      "view_server.subscription_id": requestId,
    });
    yield* Effect.forEach(args.workers.values(), (worker) => worker.unsubscribe(requestId), {
      discard: true,
    });
    yield* args.syncHealthTopicIgnoringErrors;
  });

  return {
    query,
    subscribe,
    unsubscribe,
    publishWithTransport: (topic, row, transport) =>
      runMutation(topic, row, "publish", transport, (worker, payload) => worker.publish(payload)),
    deltaPublishWithTransport: (topic, patch, transport) =>
      runMutation(topic, patch, "delta-publish", transport, (worker, payload) =>
        worker.deltaPublish(payload),
      ),
    deleteByIdWithTransport: (topic, id, transport) =>
      runMutation(topic, id, "delete", transport, (worker, payload) => worker.deleteById(payload)),
  };
}

function runtimeSpanOperation(operation: "publish" | "delta-publish" | "delete"): string {
  return operation === "delta-publish" ? "delta_publish" : operation;
}
