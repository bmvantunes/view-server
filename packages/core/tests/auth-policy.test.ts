import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { defineConfig, normalizeConfig, VIEW_SERVER_HEALTH_TOPIC } from "../src/config/index.ts";
import { defaultAuthPolicy } from "../src/server/auth-policy.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

describe("AuthPolicy", () => {
  it.effect("rejects private system topics before query validation", () =>
    Effect.gen(function* () {
      const policy = defaultAuthPolicy(
        normalizeConfig(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
              },
            },
          }),
        ),
      );

      const read = yield* policy
        .canReadTopic({ topic: "__private", operation: "query" })
        .pipe(Effect.flip);
      const subscribe = yield* policy
        .canSubscribe({ topic: "__private", requestId: "request-1" })
        .pipe(Effect.flip);
      const publish = yield* policy
        .canPublishTopic({
          topic: "__private",
          operation: "publish",
          payload: { id: "x" },
          transport: "rpc",
        })
        .pipe(Effect.flip);

      expect(read._tag).toBe("UnauthorizedSystemTopic");
      expect(subscribe._tag).toBe("UnauthorizedSystemTopic");
      expect(publish._tag).toBe("UnauthorizedSystemTopic");
    }),
  );

  it.effect("allows internal system topic writes while blocking public writes", () =>
    Effect.gen(function* () {
      const policy = defaultAuthPolicy(
        normalizeConfig(
          defineConfig({
            topics: {
              orders: {
                id: "id",
                schema: Order,
              },
            },
          }),
        ),
      );

      yield* policy.canPublishTopic({
        topic: VIEW_SERVER_HEALTH_TOPIC,
        operation: "publish",
        payload: { id: "server" },
        transport: "internal",
      });
      const publicWrite = yield* policy
        .canPublishTopic({
          topic: VIEW_SERVER_HEALTH_TOPIC,
          operation: "publish",
          payload: { id: "server" },
          transport: "rpc",
        })
        .pipe(Effect.flip);

      expect(publicWrite._tag).toBe("UnauthorizedSystemTopic");
    }),
  );

  it.effect("delegates health reads, topic subscriptions, and publishes to configured auth", () =>
    Effect.gen(function* () {
      const policy = defaultAuthPolicy(
        normalizeConfig(
          defineConfig({
            auth: {
              authorizeQuery: ({ topic, operation }) =>
                Effect.succeed(topic !== VIEW_SERVER_HEALTH_TOPIC && operation !== "subscribe"),
              authorizePublish: ({ operation }) => Effect.succeed(operation !== "delete"),
            },
            topics: {
              orders: {
                id: "id",
                schema: Order,
              },
            },
          }),
        ),
      );

      const health = yield* policy
        .canReadHealth({ operation: "query", payload: { limit: 10 } })
        .pipe(Effect.flip);
      const subscription = yield* policy
        .canSubscribe({
          topic: "orders",
          requestId: "request-1",
          payload: { limit: 10 },
        })
        .pipe(Effect.flip);
      const deletePublish = yield* policy
        .canPublishTopic({
          topic: "orders",
          operation: "delete",
          payload: "order-1",
          transport: "rpc",
        })
        .pipe(Effect.flip);

      expect(health._tag).toBe("Unauthorized");
      expect(subscription._tag).toBe("Unauthorized");
      expect(deletePublish._tag).toBe("Unauthorized");

      yield* policy.canReadTopic({
        topic: "orders",
        operation: "query",
        payload: { limit: 10 },
      });
      yield* policy.canPublishTopic({
        topic: "orders",
        operation: "publish",
        payload: { id: "order-1" },
        transport: "rpc",
      });
    }),
  );
});
