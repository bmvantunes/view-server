import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { columnCatalogForTopic, defineConfig, normalizeConfig } from "../src/config/index.ts";
import type { ViewServerError } from "../src/errors.ts";
import type { RuntimeQuery } from "../src/protocol/index.ts";
import type { AuthPolicy } from "../src/server/auth-policy.ts";
import { QueryLimitPolicy } from "../src/server/query-limit-policy.ts";
import { makeRuntimeOperations } from "../src/server/runtime-operations.ts";
import { RuntimeShutdownController } from "../src/server/runtime-shutdown-controller.ts";
import type { TopicWorkerCore, TopicWorkerMetrics } from "../src/worker/topic-worker-core.ts";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const rawQuery = {
  fields: {
    id: true,
    price: true,
  },
  limit: 1,
} satisfies RuntimeQuery;

describe("RuntimeOperations", () => {
  it.effect("uses one admission path for publish, delta, and delete operations", () =>
    Effect.gen(function* () {
      const calls: string[] = [];
      let healthSyncs = 0;
      const operations = makeRuntimeOperations({
        ...operationInputs({
          worker: fakeWorker(calls),
          syncHealth: Effect.sync(() => {
            healthSyncs++;
          }),
        }),
      });

      yield* operations.publishWithTransport("orders", { id: "o-1", price: 100 }, "rpc");
      yield* operations.deltaPublishWithTransport("orders", { id: "o-1", price: 101 }, "rpc");
      yield* operations.deleteByIdWithTransport("orders", "o-1", "rpc");
      yield* operations.mutateBatchWithTransport(
        "orders",
        [
          { type: "publish", row: { id: "o-2", price: 200 } },
          { type: "delta-publish", patch: { id: "o-2", price: 201 } },
          { type: "delete", id: "o-2" },
        ],
        "internal",
      );

      expect(calls).toEqual([
        "auth:publish:rpc",
        "worker:publish",
        "auth:delta-publish:rpc",
        "worker:delta",
        "auth:delete:rpc",
        "worker:delete",
        "auth:publish:internal",
        "auth:delta-publish:internal",
        "auth:delete:internal",
        "worker:batch:3",
      ]);
      expect(healthSyncs).toBe(4);
    }),
  );

  it.effect("validates query limits before dispatch", () =>
    Effect.gen(function* () {
      const calls: string[] = [];
      const operations = makeRuntimeOperations({
        ...operationInputs({
          worker: fakeWorker(calls),
          limits: {
            maxPageSize: 1,
          },
        }),
      });

      const error = yield* operations
        .query("orders", {
          ...rawQuery,
          limit: 2,
        })
        .pipe(Effect.flip);

      expect(error._tag).toBe("QueryLimitExceeded");
      expect(calls).toEqual(["auth-read:query"]);
    }),
  );
});

function operationInputs(args: {
  readonly worker: TopicWorkerCore;
  readonly syncHealth?: Effect.Effect<void, ViewServerError> | undefined;
  readonly limits?: { readonly maxPageSize?: number | undefined } | undefined;
}) {
  const config = normalizeConfig(
    defineConfig({
      topics: {
        orders: {
          id: "id",
          schema: Order,
          ...(args.limits === undefined ? {} : { limits: args.limits }),
        },
      },
    }),
  );
  return {
    workers: new Map([["orders", args.worker]]),
    columnCatalogs: new Map([["orders", columnCatalogForTopic("orders", config.topics.orders)]]),
    authPolicy: authPolicy(),
    queryLimitPolicy: QueryLimitPolicy.fromConfig(config),
    shutdownController: new RuntimeShutdownController(),
    requestHealthTopicSync: args.syncHealth ?? Effect.void,
    flushHealthTopicIgnoringErrors: Effect.void,
  };
}

function authPolicy(): AuthPolicy {
  return {
    canReadTopic: ({ operation }) => Effect.sync(() => recordAuth(`auth-read:${operation}`)),
    canPublishTopic: ({ operation, transport }) =>
      Effect.sync(() => recordAuth(`auth:${operation}:${transport}`)),
    canReadHealth: ({ operation }) => Effect.sync(() => recordAuth(`auth-health:${operation}`)),
    canSubscribe: ({ topic }) => Effect.sync(() => recordAuth(`auth-subscribe:${topic}`)),
  };
}

let authCalls: string[] | undefined;

function recordAuth(call: string): void {
  authCalls?.push(call);
}

function fakeWorker(calls: string[]): TopicWorkerCore {
  authCalls = calls;
  return {
    topic: "orders",
    idField: "id",
    version: Effect.succeed(0n),
    metrics: Effect.succeed(topicMetrics()),
    query: () =>
      Effect.sync(() => {
        calls.push("worker:query");
        return {
          rows: [],
          totalRows: 0,
          version: "0",
        };
      }),
    subscribe: () => Stream.empty,
    unsubscribe: () =>
      Effect.sync(() => {
        calls.push("worker:unsubscribe");
      }),
    publish: () =>
      Effect.sync(() => {
        calls.push("worker:publish");
      }),
    deltaPublish: () =>
      Effect.sync(() => {
        calls.push("worker:delta");
      }),
    deleteById: () =>
      Effect.sync(() => {
        calls.push("worker:delete");
      }),
    mutateBatch: (mutations) =>
      Effect.sync(() => {
        calls.push(`worker:batch:${mutations.length}`);
      }),
    getRowsForTest: Effect.succeed([]),
    shutdown: Effect.void,
  };
}

function topicMetrics(): TopicWorkerMetrics {
  return {
    rows: 0,
    subscribers: 0,
    version: 0n,
    queueDepth: 0,
    maxSubscriptionLagVersions: 0,
    totalSubscriptionLagVersions: 0,
    activePlanCount: 0,
    activeViewCount: 0,
    activePlanRows: 0,
    activePlanIndexEstimatedBytes: 0,
    activePlanBuildQueueDepth: 0,
    activePlanBuildingCount: 0,
    activePlanPendingCount: 0,
    activePlanBuildMs: 0,
    activePlanBuildMsTotal: 0,
    activePlanBuildMsMax: 0,
    activePlanFallbackCount: 0,
    activePlanAutoBuildSkippedCount: 0,
    chdbStatus: "ready",
    chdbPid: 0,
    chdbRestarts: 0,
    chdbPendingRequests: 0,
    chdbLastError: "",
    chdbBackendVersion: 0n,
    status: "ready",
  };
}
