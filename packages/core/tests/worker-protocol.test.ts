import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Schema from "effect/Schema";
import type { RuntimeRow } from "../src/protocol/index.ts";
import { TopicWorkerRpcs as ReexportedTopicWorkerRpcs } from "../src/worker/topic-worker-rpcs.ts";
import {
  TOPIC_WORKER_RPC_NAMES,
  TopicWorkerDeleteByIdPayload,
  TopicWorkerDeltaPublishPayload,
  TopicWorkerInitialMessage,
  TopicWorkerMetricsSchema,
  TopicWorkerPublishPayload,
  TopicWorkerQueryPayload,
  TopicWorkerQueryResponse,
  TopicWorkerRpcs,
  TopicWorkerRows,
  decodeTopicWorkerMetrics,
  decodeTopicWorkerRows,
  encodeTopicWorkerMetrics,
  encodeTopicWorkerRows,
} from "../src/worker/worker-protocol.ts";
import type { TopicWorkerMetrics } from "../src/worker/worker-health-projection.ts";

describe("WorkerProtocol", () => {
  it("rejects invalid protocol messages through schemas", () => {
    const decodeInitial = Schema.decodeUnknownSync(TopicWorkerInitialMessage);
    const decodeDelete = Schema.decodeUnknownSync(TopicWorkerDeleteByIdPayload);

    expect(() =>
      decodeInitial({
        configModuleUrl: "file:///view-server.config.ts",
        topic: "orders",
        snapshotBackend: "sqlite",
      }),
    ).toThrow();
    expect(() => decodeDelete({ id: { nested: "not-a-stable-key" } })).toThrow();
  });

  it("round-trips BigDecimal query, mutation, and result payloads", () => {
    const amount = BigDecimal.fromStringUnsafe("123.000000000000000001");
    const queryPayload = {
      query: {
        fields: {
          id: true,
          amount: true,
        },
        where: {
          field: "amount",
          comparator: "greater_than",
          value: amount,
        },
        limit: 10,
      },
    } satisfies typeof TopicWorkerQueryPayload.Type;
    const decodedQueryPayload = decodeAfterEncode(TopicWorkerQueryPayload, queryPayload);
    const decodedQuery = decodedQueryPayload.query;

    if (!("where" in decodedQuery) || decodedQuery.where === undefined) {
      throw new Error("Expected decoded query where filter");
    }
    if (!("field" in decodedQuery.where)) {
      throw new Error("Expected decoded query field filter");
    }
    expect(isBigDecimalEqual(decodedQuery.where.value, amount)).toBe(true);

    const publishPayload = {
      row: {
        id: "order-1",
        amount,
      },
    } satisfies typeof TopicWorkerPublishPayload.Type;
    const decodedPublishPayload = decodeAfterEncode(TopicWorkerPublishPayload, publishPayload);

    expect(isBigDecimalEqual(decodedPublishPayload.row.amount, amount)).toBe(true);

    const deltaPayload = {
      patch: {
        id: "order-1",
        amount,
      },
    } satisfies typeof TopicWorkerDeltaPublishPayload.Type;
    const decodedDeltaPayload = decodeAfterEncode(TopicWorkerDeltaPublishPayload, deltaPayload);

    expect(isBigDecimalEqual(decodedDeltaPayload.patch.amount, amount)).toBe(true);

    const queryResponse = {
      rows: [
        {
          id: "order-1",
          amount,
        },
      ],
      totalRows: 1,
      version: "7",
    } satisfies typeof TopicWorkerQueryResponse.Type;
    const decodedQueryResponse = decodeAfterEncode(TopicWorkerQueryResponse, queryResponse);

    expect(isBigDecimalEqual(decodedQueryResponse.rows[0]?.amount, amount)).toBe(true);
  });

  it("round-trips rows through worker row codecs", () => {
    const amount = BigDecimal.fromStringUnsafe("999.000000000000000001");
    const rows: readonly RuntimeRow[] = [
      {
        id: "order-1",
        amount,
      },
    ];

    const decoded = decodeTopicWorkerRows(
      decodeAfterEncode(TopicWorkerRows, encodeTopicWorkerRows(rows)),
    );

    expect(decoded[0]?.id).toBe("order-1");
    expect(isBigDecimalEqual(decoded[0]?.amount, amount)).toBe(true);
  });

  it("round-trips metrics between in-process and node worker wire shapes", () => {
    const metrics = {
      rows: 10,
      subscribers: 2,
      version: 7n,
      queueDepth: 1,
      maxSubscriptionLagVersions: 3,
      totalSubscriptionLagVersions: 4,
      activePlanCount: 5,
      activeViewCount: 6,
      activePlanRows: 100,
      activePlanIndexEstimatedBytes: 2048,
      activePlanBuildQueueDepth: 1,
      activePlanBuildingCount: 1,
      activePlanPendingCount: 2,
      activePlanBuildMs: 12,
      activePlanBuildMsTotal: 100,
      activePlanBuildMsMax: 40,
      activePlanFallbackCount: 0,
      activePlanAutoBuildSkippedCount: 9,
      chdbStatus: "ready",
      chdbPid: 1234,
      chdbRestarts: 1,
      chdbPendingRequests: 2,
      chdbLastError: "",
      chdbBackendVersion: 6n,
      status: "ready",
    } satisfies TopicWorkerMetrics;

    const decoded = decodeTopicWorkerMetrics(
      Schema.decodeUnknownSync(TopicWorkerMetricsSchema)(encodeTopicWorkerMetrics(metrics)),
    );

    expect(decoded).toEqual(metrics);
  });

  it("keeps node and in-process worker entrypoints on the same RPC group", () => {
    expect(ReexportedTopicWorkerRpcs).toBe(TopicWorkerRpcs);
    expect([...TOPIC_WORKER_RPC_NAMES]).toEqual([
      "Subscribe",
      "Unsubscribe",
      "Query",
      "Publish",
      "DeltaPublish",
      "DeleteById",
      "RowsForTest",
      "Metrics",
      "Shutdown",
    ]);
  });
});

function decodeAfterEncode<const A, const I>(schema: Schema.Codec<A, I>, value: A): A {
  return Schema.decodeUnknownSync(schema)(Schema.encodeUnknownSync(schema)(value));
}

function isBigDecimalEqual(value: unknown, expected: BigDecimal.BigDecimal): boolean {
  return BigDecimal.isBigDecimal(value) && BigDecimal.equals(value, expected);
}
