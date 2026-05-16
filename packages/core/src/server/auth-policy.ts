import * as Effect from "effect/Effect";
import {
  type AuthorizationContext,
  isReservedTopic,
  type NormalizedViewServerConfig,
  VIEW_SERVER_HEALTH_TOPIC,
} from "../config/index.ts";
import { unauthorized, unauthorizedSystemTopic, type ViewServerError } from "../errors.ts";

export type AuthReadOperation = "query" | "subscribe";
export type AuthPublishOperation = "publish" | "delta-publish" | "delete";

export type AuthPolicy = {
  readonly canReadTopic: (args: {
    readonly topic: string;
    readonly operation: AuthReadOperation;
    readonly payload?: unknown;
  }) => Effect.Effect<void, ViewServerError>;
  readonly canPublishTopic: (args: {
    readonly topic: string;
    readonly operation: AuthPublishOperation;
    readonly payload: unknown;
    readonly transport: AuthorizationContext["transport"];
  }) => Effect.Effect<void, ViewServerError>;
  readonly canReadHealth: (args: {
    readonly operation: AuthReadOperation;
    readonly payload: unknown;
  }) => Effect.Effect<void, ViewServerError>;
  readonly canSubscribe: (args: {
    readonly topic: string;
    readonly requestId: string;
    readonly payload?: unknown;
  }) => Effect.Effect<void, ViewServerError>;
};

export function defaultAuthPolicy(config: NormalizedViewServerConfig): AuthPolicy {
  const canReadHealth = Effect.fn("view-server.auth.health")(function* (args: {
    readonly operation: AuthReadOperation;
    readonly payload: unknown;
  }) {
    return yield* authorizeQuery(config, VIEW_SERVER_HEALTH_TOPIC, args.operation, args.payload);
  });

  const canReadTopic = Effect.fn("view-server.auth.read_topic")(function* (args: {
    readonly topic: string;
    readonly operation: AuthReadOperation;
    readonly payload?: unknown;
  }) {
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": args.topic,
    });
    if (isReservedTopic(args.topic) && args.topic !== VIEW_SERVER_HEALTH_TOPIC) {
      return yield* Effect.fail(unauthorizedSystemTopic(args.topic, args.operation));
    }
    if (args.payload === undefined) {
      return;
    }
    if (args.topic === VIEW_SERVER_HEALTH_TOPIC) {
      return yield* canReadHealth({ operation: args.operation, payload: args.payload });
    }
    return yield* authorizeQuery(config, args.topic, args.operation, args.payload);
  });

  return {
    canReadTopic,
    canReadHealth,
    canSubscribe: Effect.fn("view-server.auth.subscribe")(function* (args: {
      readonly topic: string;
      readonly requestId: string;
      readonly payload?: unknown;
    }) {
      yield* Effect.annotateCurrentSpan({
        "view_server.request_id": args.requestId,
        "view_server.subscription_id": args.requestId,
        "view_server.topic": args.topic,
      });
      return yield* canReadTopic({
        topic: args.topic,
        operation: "subscribe",
        ...(args.payload === undefined ? {} : { payload: args.payload }),
      });
    }),
    canPublishTopic: Effect.fn("view-server.auth.publish_topic")(function* (args: {
      readonly topic: string;
      readonly operation: AuthPublishOperation;
      readonly payload: unknown;
      readonly transport: AuthorizationContext["transport"];
    }) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": args.topic,
      });
      if (isReservedTopic(args.topic) && args.transport !== "internal") {
        return yield* Effect.fail(unauthorizedSystemTopic(args.topic, args.operation));
      }
      return yield* config.auth
        .authorizePublish({
          topic: args.topic,
          operation: args.operation,
          payload: args.payload,
          transport: args.transport,
        })
        .pipe(
          Effect.flatMap((allowed) =>
            allowed ? Effect.void : Effect.fail(unauthorized(args.topic, args.operation)),
          ),
        );
    }),
  };
}

function authorizeQuery(
  config: NormalizedViewServerConfig,
  topic: string,
  operation: AuthReadOperation,
  payload: unknown,
): Effect.Effect<void, ViewServerError> {
  return config.auth
    .authorizeQuery({ topic, operation, payload, transport: "rpc" })
    .pipe(
      Effect.flatMap((allowed) =>
        allowed ? Effect.void : Effect.fail(unauthorized(topic, operation)),
      ),
    );
}
